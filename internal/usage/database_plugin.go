package usage

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const (
	defaultBufferSize      = 100
	defaultFlushInterval   = 5 * time.Second
	defaultRetentionDays   = 30
	defaultCleanupInterval = 4 * time.Hour
)

var (
	databasePlugin   *DatabasePlugin
	databasePluginMu sync.RWMutex
)

type databasePluginAdapter struct{}

func (databasePluginAdapter) HandleUsage(ctx context.Context, record coreusage.Record) {
	plugin := GetDatabasePlugin()
	if plugin == nil {
		return
	}
	plugin.HandleUsage(ctx, record)
}

func (databasePluginAdapter) DeleteAuthUsage(ctx context.Context, authIndexes []string) error {
	plugin := GetDatabasePlugin()
	if plugin == nil {
		return nil
	}
	return plugin.DeleteAuthUsage(ctx, authIndexes)
}

func init() {
	coreusage.RegisterPlugin(databasePluginAdapter{})
}

// DatabasePlugin persists usage records to database and provides combined statistics.
type DatabasePlugin struct {
	store             UsageStore
	retentionDays     int
	storeOnlySnapshot bool

	// Write buffer
	buffer    []UsageRecord
	bufferMu  sync.Mutex
	flushMu   sync.Mutex
	flushCh   chan struct{}
	closeCh   chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// InitDatabasePlugin initializes the global database plugin.
// If initialization fails, returns the error but does not prevent the system from running.
// Reads USAGE_RETENTION_DAYS environment variable for data retention period (default 30 days).
func InitDatabasePlugin(ctx context.Context, pgDSN, pgSchema, authDir string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	databasePluginMu.RLock()
	alreadyEnabled := databasePlugin != nil
	databasePluginMu.RUnlock()
	if alreadyEnabled {
		return nil
	}

	databasePluginMu.Lock()
	defer databasePluginMu.Unlock()
	if databasePlugin != nil {
		return nil
	}

	store, err := NewUsageStore(ctx, pgDSN, pgSchema, authDir)
	if err != nil {
		return err
	}

	retentionDays := defaultRetentionDays
	if envVal := os.Getenv("USAGE_RETENTION_DAYS"); envVal != "" {
		if days, parseErr := strconv.Atoi(envVal); parseErr == nil && days > 0 {
			retentionDays = days
		}
	}

	plugin := &DatabasePlugin{
		store:             store,
		retentionDays:     retentionDays,
		storeOnlySnapshot: strings.TrimSpace(pgDSN) != "",
		buffer:            make([]UsageRecord, 0, defaultBufferSize),
		flushCh:           make(chan struct{}, 1),
		closeCh:           make(chan struct{}),
	}
	plugin.wg.Add(1)
	// flushLoop owns the initial retention cleanup so InitDatabasePlugin does not block startup on large SQLite DBs.
	go plugin.flushLoop()

	databasePlugin = plugin
	return nil
}

// GetDatabasePlugin returns the global database plugin instance, or nil if not initialized.
func GetDatabasePlugin() *DatabasePlugin {
	databasePluginMu.RLock()
	defer databasePluginMu.RUnlock()
	return databasePlugin
}

// CloseDatabasePlugin closes the database connection after flushing pending writes.
func CloseDatabasePlugin() {
	databasePluginMu.Lock()
	plugin := databasePlugin
	databasePlugin = nil
	databasePluginMu.Unlock()

	if plugin == nil {
		return
	}
	plugin.closeOnce.Do(func() {
		close(plugin.closeCh)
	})
	plugin.wg.Wait()
	if plugin.store != nil {
		_ = plugin.store.Close()
	}
}

// UpdatePersistence enables or disables usage persistence based on the provided flag.
// When enabling, it reads PGSTORE_DSN/pgstore_dsn and PGSTORE_SCHEMA/pgstore_schema
// from environment variables to determine the database backend.
// If PGSTORE_DSN is empty, SQLite is used with the database stored in authDir.
func UpdatePersistence(ctx context.Context, enabled bool, authDir string) error {
	if !enabled {
		CloseDatabasePlugin()
		return nil
	}
	pgStoreDSN := util.GetEnvTrimmed("PGSTORE_DSN", "pgstore_dsn")
	pgStoreSchema := util.GetEnvTrimmed("PGSTORE_SCHEMA", "pgstore_schema")
	CloseDatabasePlugin()
	return InitDatabasePlugin(ctx, pgStoreDSN, pgStoreSchema, authDir)
}

// UsingSQLiteBackend returns true if the usage persistence would use SQLite (no PGSTORE_DSN set).
func UsingSQLiteBackend() bool {
	return util.GetEnvTrimmed("PGSTORE_DSN", "pgstore_dsn") == ""
}

// flushLoop periodically flushes the buffer to the database and cleans up old records.
func (p *DatabasePlugin) flushLoop() {
	defer p.wg.Done()
	// Run once immediately so startup cleanup is asynchronous relative to service start.
	p.cleanup(true)

	flushTicker := time.NewTicker(defaultFlushInterval)
	cleanupTicker := time.NewTicker(defaultCleanupInterval)
	defer flushTicker.Stop()
	defer cleanupTicker.Stop()

	for {
		select {
		case <-p.closeCh:
			p.flush()
			return
		case <-flushTicker.C:
			p.flush()
		case <-cleanupTicker.C:
			p.cleanup(false)
		case <-p.flushCh:
			p.flush()
		}
	}
}

// cleanup deletes records older than the retention period.
// When startup is true, successful deletions are logged at info for operators.
func (p *DatabasePlugin) cleanup(startup bool) {
	if p.store == nil || p.retentionDays <= 0 {
		return
	}
	deleted, err := p.store.DeleteOldRecords(context.Background(), p.retentionDays)
	if err != nil {
		log.WithError(err).Warn("usage: failed to cleanup old records")
		return
	}
	if deleted <= 0 {
		return
	}
	fields := log.Fields{"deleted": deleted, "retention_days": p.retentionDays}
	if startup {
		log.WithFields(fields).Info("usage: cleaned up records older than retention window")
		return
	}
	log.WithFields(fields).Debug("usage: cleaned up records older than retention window")
}

// flush writes all buffered records to the database.
func (p *DatabasePlugin) flush() {
	if p == nil || p.store == nil {
		return
	}
	p.flushMu.Lock()
	defer p.flushMu.Unlock()

	p.bufferMu.Lock()
	if len(p.buffer) == 0 {
		p.bufferMu.Unlock()
		return
	}
	records := p.buffer
	p.buffer = make([]UsageRecord, 0, defaultBufferSize)
	p.bufferMu.Unlock()

	added, skipped, err := p.store.InsertBatch(context.Background(), records)
	if err != nil {
		log.WithError(err).Warn("usage: failed to flush records to database")
	} else if skipped > 0 {
		log.WithFields(log.Fields{"added": added, "skipped": skipped}).Debug("usage: flushed records")
	}
}

// DeleteAuthUsage removes persisted and pending usage records for deleted credentials.
func (p *DatabasePlugin) DeleteAuthUsage(ctx context.Context, authIndexes []string) error {
	if p == nil {
		return nil
	}
	authIndexes = normalizedAuthIndexes(authIndexes)
	if len(authIndexes) == 0 {
		return nil
	}
	indexes := make(map[string]struct{}, len(authIndexes))
	for _, authIndex := range authIndexes {
		indexes[authIndex] = struct{}{}
	}

	p.flushMu.Lock()
	defer p.flushMu.Unlock()

	p.bufferMu.Lock()
	remaining := p.buffer[:0]
	for _, record := range p.buffer {
		if _, ok := indexes[strings.TrimSpace(record.AuthIndex)]; ok {
			continue
		}
		remaining = append(remaining, record)
	}
	p.buffer = remaining
	p.bufferMu.Unlock()

	if p.store == nil {
		return nil
	}
	if _, err := p.store.DeleteAuthUsage(ctx, authIndexes); err != nil {
		return fmt.Errorf("usage: delete auth usage: %w", err)
	}
	return nil
}

// triggerFlush signals the flush loop to flush immediately if buffer is full.
func (p *DatabasePlugin) triggerFlush() {
	select {
	case p.flushCh <- struct{}{}:
	default:
	}
}

// HandleUsage implements coreusage.Plugin interface.
// It buffers the usage record and flushes to database when buffer is full or on interval.
func (p *DatabasePlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if p == nil || p.store == nil {
		return
	}

	// Usage is dispatched asynchronously; gin may already have reused the request's
	// gin.Context, so read the snapshot captured while the handler was active.
	requestMeta := internallogging.GetClientRequestMetadata(ctx)
	method, path := requestMeta.Method, requestMeta.Path
	stream := record.Stream
	if !stream {
		stream = coreusage.StreamFromContext(ctx)
	}

	dbRecord := UsageRecord{
		APIKey:           record.APIKey,
		Model:            record.Model,
		Source:           record.Source,
		AuthIndex:        record.AuthIndex,
		Failed:           record.Failed,
		RequestedAt:      record.RequestedAt,
		InputTokens:      record.Detail.InputTokens,
		OutputTokens:     record.Detail.OutputTokens,
		ReasoningTokens:  record.Detail.ReasoningTokens,
		CachedTokens:     record.Detail.CachedTokens,
		CacheWriteTokens: record.Detail.CacheCreationTokens,
		TotalTokens:      record.Detail.TotalTokens,
		LatencyMs:        normaliseLatency(record.Latency),
		TTFTMs:           normaliseLatency(record.TTFT),
		Stream:           boolPointer(stream),
		Fast:             boolPointer(coreusage.IsFastMode(record.Provider, record.ServiceTier)),
		Method:           method,
		Path:             path,
	}

	p.bufferMu.Lock()
	p.buffer = append(p.buffer, dbRecord)
	shouldFlush := len(p.buffer) >= defaultBufferSize
	p.bufferMu.Unlock()

	if shouldFlush {
		p.triggerFlush()
	}
}

// GetCombinedSnapshot returns combined statistics from database history and current session memory.
func (p *DatabasePlugin) GetCombinedSnapshot() StatisticsSnapshot {
	memStats := GetRequestStatistics().Snapshot()
	if p == nil || p.store == nil {
		return memStats
	}

	dbStats, err := p.store.GetAggregatedStats(context.Background())
	if err != nil {
		log.WithError(err).Warn("usage: failed to query database stats, returning memory only")
		return memStats
	}

	if p.storeOnlySnapshot {
		return mergeStats(dbStats, StatisticsSnapshot{})
	}
	return mergeStats(dbStats, memStats)
}

// ImportRecords imports usage records from a snapshot into the database.
// Returns the number of records added and skipped.
func (p *DatabasePlugin) ImportRecords(snapshot StatisticsSnapshot) (added, skipped int64, err error) {
	if p == nil || p.store == nil {
		return 0, 0, nil
	}

	var records []UsageRecord
	for apiKey, apiSnapshot := range snapshot.APIs {
		for model, modelSnapshot := range apiSnapshot.Models {
			for _, detail := range modelSnapshot.Details {
				records = append(records, UsageRecord{
					APIKey:           apiKey,
					Model:            model,
					Source:           detail.Source,
					AuthIndex:        detail.AuthIndex,
					Failed:           detail.Failed,
					RequestedAt:      detail.Timestamp,
					InputTokens:      detail.Tokens.InputTokens,
					OutputTokens:     detail.Tokens.OutputTokens,
					ReasoningTokens:  detail.Tokens.ReasoningTokens,
					CachedTokens:     detail.Tokens.CachedTokens,
					CacheWriteTokens: detail.Tokens.CacheWriteTokens,
					TotalTokens:      detail.Tokens.TotalTokens,
					LatencyMs:        detail.LatencyMs,
					TTFTMs:           detail.TTFTMs,
					Stream:           detail.Stream,
					Fast:             detail.Fast,
				})
			}
		}
	}

	if len(records) == 0 {
		return 0, 0, nil
	}

	return p.store.InsertBatch(context.Background(), records)
}

// mergeStats combines database aggregated stats with in-memory session stats.
func mergeStats(db AggregatedStats, mem StatisticsSnapshot) StatisticsSnapshot {
	result := StatisticsSnapshot{
		TotalRequests:  db.TotalRequests + mem.TotalRequests,
		SuccessCount:   db.SuccessCount + mem.SuccessCount,
		FailureCount:   db.FailureCount + mem.FailureCount,
		TotalTokens:    db.TotalTokens + mem.TotalTokens,
		APIs:           make(map[string]APISnapshot),
		RequestsByDay:  make(map[string]int64),
		RequestsByHour: make(map[string]int64),
		TokensByDay:    make(map[string]int64),
		TokensByHour:   make(map[string]int64),
	}

	// Merge APIs from database (aggregated counts only, no per-request details)
	for key, as := range db.APIs {
		apiSnap := APISnapshot{
			TotalRequests: as.TotalRequests,
			TotalTokens:   as.TotalTokens,
			Models:        make(map[string]ModelSnapshot),
		}
		for model, ms := range as.Models {
			apiSnap.Models[model] = ModelSnapshot{
				TotalRequests: ms.TotalRequests,
				TotalTokens:   ms.TotalTokens,
				Details:       []RequestDetail{},
			}
		}
		result.APIs[key] = apiSnap
	}

	// Merge APIs from memory (adds current session details)
	for key, as := range mem.APIs {
		existing, ok := result.APIs[key]
		if !ok {
			result.APIs[key] = as
		} else {
			existing.TotalRequests += as.TotalRequests
			existing.TotalTokens += as.TotalTokens
			for model, ms := range as.Models {
				if existingModel, modelOK := existing.Models[model]; modelOK {
					existingModel.TotalRequests += ms.TotalRequests
					existingModel.TotalTokens += ms.TotalTokens
					existingModel.Details = append(existingModel.Details, ms.Details...)
					existing.Models[model] = existingModel // FIX: Write back to map
				} else {
					existing.Models[model] = ms
				}
			}
			result.APIs[key] = existing
		}
	}

	// Merge time-based stats
	for k, v := range db.RequestsByDay {
		result.RequestsByDay[k] = v
	}
	for k, v := range mem.RequestsByDay {
		result.RequestsByDay[k] += v
	}

	for k, v := range db.RequestsByHour {
		result.RequestsByHour[k] = v
	}
	for k, v := range mem.RequestsByHour {
		result.RequestsByHour[k] += v
	}

	for k, v := range db.TokensByDay {
		result.TokensByDay[k] = v
	}
	for k, v := range mem.TokensByDay {
		result.TokensByDay[k] += v
	}

	for k, v := range db.TokensByHour {
		result.TokensByHour[k] = v
	}
	for k, v := range mem.TokensByHour {
		result.TokensByHour[k] += v
	}

	return result
}

// GetDetails returns paginated request details from the database.
func (p *DatabasePlugin) GetDetails(ctx context.Context, offset, limit int) ([]DetailRecord, error) {
	if p == nil || p.store == nil {
		return nil, nil
	}
	return p.store.GetDetails(ctx, offset, limit)
}
