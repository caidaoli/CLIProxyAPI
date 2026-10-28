package usage

import (
	"context"
	"net/http"
	"testing"
	"time"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

type fakeUsageStore struct {
	stats AggregatedStats
}

func (s *fakeUsageStore) Insert(context.Context, UsageRecord) error { return nil }

func (s *fakeUsageStore) InsertBatch(context.Context, []UsageRecord) (int64, int64, error) {
	return 0, 0, nil
}

func (s *fakeUsageStore) GetAggregatedStats(context.Context) (AggregatedStats, error) {
	return s.stats, nil
}

func (s *fakeUsageStore) GetDetails(context.Context, int, int) ([]DetailRecord, error) {
	return nil, nil
}

func (s *fakeUsageStore) DeleteAuthUsage(context.Context, []string) (int64, error) {
	return 0, nil
}

func (s *fakeUsageStore) DeleteOldRecords(context.Context, int) (int64, error) {
	return 0, nil
}

func (s *fakeUsageStore) EnsureSchema(context.Context) error { return nil }

func (s *fakeUsageStore) Close() error { return nil }

func TestGetCombinedSnapshot_StoreOnlySnapshotIgnoresMemory(t *testing.T) {
	oldStats := defaultRequestStatistics
	defer func() {
		defaultRequestStatistics = oldStats
	}()
	defaultRequestStatistics = NewRequestStatistics()
	SetStatisticsEnabled(true)

	defaultRequestStatistics.Record(context.Background(), coreusage.Record{
		APIKey:      "mem-api",
		Model:       "mem-model",
		RequestedAt: time.Now(),
		Detail: coreusage.Detail{
			TotalTokens: 99,
		},
	})

	now := time.Now().Add(-time.Hour)
	dbStats := AggregatedStats{
		TotalRequests: 3,
		SuccessCount:  2,
		FailureCount:  1,
		TotalTokens:   30,
		APIs: map[string]APIStats{
			"db-api": {
				TotalRequests: 3,
				TotalTokens:   30,
				Models: map[string]ModelStats{
					"db-model": {TotalRequests: 3, TotalTokens: 30},
				},
			},
		},
		RequestsByDay:  map[string]int64{"2026-02-07": 3},
		RequestsByHour: map[string]int64{"10": 3},
		TokensByDay:    map[string]int64{"2026-02-07": 30},
		TokensByHour:   map[string]int64{"10": 30},
		Details: []DetailRecord{
			{
				APIKey:      "db-api",
				Model:       "db-model",
				Source:      "db-source",
				AuthIndex:   "0",
				Failed:      false,
				RequestedAt: now,
				TotalTokens: 10,
			},
		},
	}

	plugin := &DatabasePlugin{
		store:             &fakeUsageStore{stats: dbStats},
		storeOnlySnapshot: true,
	}

	snapshot := plugin.GetCombinedSnapshot()
	if snapshot.TotalRequests != dbStats.TotalRequests {
		t.Fatalf("unexpected total requests: got %d want %d", snapshot.TotalRequests, dbStats.TotalRequests)
	}
	if snapshot.TotalTokens != dbStats.TotalTokens {
		t.Fatalf("unexpected total tokens: got %d want %d", snapshot.TotalTokens, dbStats.TotalTokens)
	}
	if _, exists := snapshot.APIs["mem-api"]; exists {
		t.Fatalf("memory api should not be merged when storeOnlySnapshot is true")
	}
	if _, exists := snapshot.APIs["db-api"]; !exists {
		t.Fatalf("db api missing in snapshot")
	}
}

func TestDatabasePluginBuffersCacheWriteTokens(t *testing.T) {
	plugin := &DatabasePlugin{store: &fakeUsageStore{}}
	plugin.HandleUsage(context.Background(), coreusage.Record{
		Model: "gpt-5.6",
		Detail: coreusage.Detail{
			InputTokens:         100,
			OutputTokens:        20,
			CacheCreationTokens: 40,
		},
	})

	plugin.bufferMu.Lock()
	defer plugin.bufferMu.Unlock()
	if len(plugin.buffer) != 1 {
		t.Fatalf("buffer len = %d, want 1", len(plugin.buffer))
	}
	if plugin.buffer[0].CacheWriteTokens != 40 {
		t.Fatalf("cache write tokens = %d, want 40", plugin.buffer[0].CacheWriteTokens)
	}
}

func TestDatabasePluginUsesRequestSnapshotNotGinContext(t *testing.T) {
	ctx := internallogging.WithClientRequestMetadata(context.Background(), internallogging.ClientRequestMetadata{
		Method: http.MethodPost,
		Path:   "/v1beta/models/gemini-2.5-pro:generateContent",
	})
	ctx = withReusedGinContext(ctx)

	plugin := &DatabasePlugin{store: &fakeUsageStore{}}
	plugin.HandleUsage(ctx, coreusage.Record{Model: "gemini-2.5-pro"})

	plugin.bufferMu.Lock()
	defer plugin.bufferMu.Unlock()
	if len(plugin.buffer) != 1 {
		t.Fatalf("buffer len = %d, want 1", len(plugin.buffer))
	}
	if got := plugin.buffer[0]; got.Method != http.MethodPost || got.Path != "/v1beta/models/gemini-2.5-pro:generateContent" {
		t.Fatalf("method/path = %q %q, want snapshot request line", got.Method, got.Path)
	}
}

func TestDatabasePluginDeleteAuthUsageRemovesPersistedAndPendingRecords(t *testing.T) {
	ctx := context.Background()
	authDir := t.TempDir()
	CloseDatabasePlugin()
	t.Cleanup(CloseDatabasePlugin)
	if errInit := InitDatabasePlugin(ctx, "", "", authDir); errInit != nil {
		t.Fatalf("InitDatabasePlugin failed: %v", errInit)
	}
	plugin := GetDatabasePlugin()
	if plugin == nil {
		t.Fatal("database plugin is nil")
	}

	_, _, errImport := plugin.ImportRecords(StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"api-key": {
				Models: map[string]ModelSnapshot{
					"model": {
						Details: []RequestDetail{
							{AuthIndex: "remove-me", Timestamp: time.Now(), Tokens: TokenStats{TotalTokens: 10}},
							{AuthIndex: "keep-me", Timestamp: time.Now(), Tokens: TokenStats{TotalTokens: 20}},
						},
					},
				},
			},
		},
	})
	if errImport != nil {
		t.Fatalf("ImportRecords failed: %v", errImport)
	}
	plugin.HandleUsage(ctx, coreusage.Record{AuthIndex: "remove-me", RequestedAt: time.Now()})
	plugin.HandleUsage(ctx, coreusage.Record{AuthIndex: "keep-me", RequestedAt: time.Now()})

	if errDelete := plugin.DeleteAuthUsage(ctx, []string{"remove-me"}); errDelete != nil {
		t.Fatalf("DeleteAuthUsage failed: %v", errDelete)
	}
	CloseDatabasePlugin()
	if errInit := InitDatabasePlugin(ctx, "", "", authDir); errInit != nil {
		t.Fatalf("reinitialize database plugin: %v", errInit)
	}
	details, errDetails := GetDatabasePlugin().GetDetails(ctx, 0, 10)
	if errDetails != nil {
		t.Fatalf("get usage details: %v", errDetails)
	}
	if len(details) != 2 {
		t.Fatalf("persisted records count = %d, want 2", len(details))
	}
	for _, detail := range details {
		if detail.AuthIndex != "keep-me" {
			t.Fatalf("persisted records = %+v, want only keep-me", details)
		}
	}
}

type blockingDeleteStore struct {
	fakeUsageStore
	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
	deleted  int64
}

func (s *blockingDeleteStore) DeleteOldRecords(context.Context, int) (int64, error) {
	close(s.started)
	<-s.release
	close(s.finished)
	return s.deleted, nil
}

func TestDatabasePluginStartupCleanupDoesNotBlockConstruction(t *testing.T) {
	store := &blockingDeleteStore{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
		deleted:  7,
	}
	plugin := &DatabasePlugin{
		store:         store,
		retentionDays: 30,
		buffer:        make([]UsageRecord, 0, defaultBufferSize),
		flushCh:       make(chan struct{}, 1),
		closeCh:       make(chan struct{}),
	}
	plugin.wg.Add(1)
	go plugin.flushLoop()

	// Construction/start must proceed while cleanup is still blocked.
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("startup cleanup did not start")
	}
	select {
	case <-store.finished:
		t.Fatal("startup cleanup finished before release; expected non-blocking start")
	default:
	}

	close(store.release)
	select {
	case <-store.finished:
	case <-time.After(time.Second):
		t.Fatal("startup cleanup did not finish after release")
	}

	plugin.closeOnce.Do(func() { close(plugin.closeCh) })
	done := make(chan struct{})
	go func() {
		plugin.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("flushLoop did not exit after close")
	}
}
