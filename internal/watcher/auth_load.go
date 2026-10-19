package watcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	authLoadBatchSize     = 32
	authLoadFlushInterval = 100 * time.Millisecond
	authLoadLogInterval   = time.Second
	authLoadQueueRetry    = 5 * time.Millisecond
)

var readInitialAuthFile = os.ReadFile

type initialAuthJob struct {
	path       string
	normalized string
	generation uint64
}

type authLoadSnapshot struct {
	config  *config.Config
	parser  synthesizer.PluginAuthParser
	authDir string
	now     time.Time
}

type initialAuthReadResult struct {
	path              string
	normalized        string
	generation        uint64
	raw               []byte
	hash              string
	native            synthesizer.NativeAuthFileResult
	readTime          time.Duration
	nativeSynthTime   time.Duration
	err               error
	enumerationResult bool
	discovered        int64
	enumerationTime   time.Duration
}

type authLoadMetrics struct {
	started              time.Time
	lastLog              time.Time
	directoryEnumeration time.Duration
	fileRead             time.Duration
	nativeSynthesis      time.Duration
	pluginSynthesis      time.Duration
	batchRegistration    time.Duration
}

func (w *Watcher) StartInitialAuthLoad(ctx context.Context, workers int) <-chan struct{} {
	done := make(chan struct{})
	if w == nil || !w.fileAuthLoadingIsEnabled() {
		close(done)
		return done
	}
	if w.stopped.Load() {
		close(done)
		return done
	}
	if applyPending := w.activateAuthEventGate(); applyPending {
		// Apply pending pre-activation config without starting a second scan.
		_ = w.reloadConfigWithMode(false)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		close(done)
		return done
	}
	if workers == 0 {
		workers = config.DefaultAuthLoadWorkers
	}
	workers = max(config.MinAuthLoadWorkers, min(workers, config.MaxAuthLoadWorkers))
	loadCtx, cancel := context.WithCancel(ctx)
	w.authLoadMu.Lock()
	if w.authLoadCancel != nil {
		w.authLoadCancel()
	}
	w.authLoadSequence++
	sequence := w.authLoadSequence
	w.authLoadCancel = cancel
	w.authLoadDone = done
	isInitialScan := sequence == 1
	status := AuthLoadStatus{State: AuthLoadStateLoading, StartedAt: time.Now().UTC()}
	w.storeAuthLoadStatus(status)
	w.authLoadMu.Unlock()
	go w.runInitialAuthLoad(loadCtx, workers, done, sequence, isInitialScan, status)
	return done
}

func (w *Watcher) runInitialAuthLoad(ctx context.Context, workers int, done chan struct{}, sequence uint64, isInitialScan bool, status AuthLoadStatus) {
	defer close(done)
	w.authLoadMu.Lock()
	hooks := w.authLoadHooks
	w.authLoadMu.Unlock()
	if !w.authLoadScanActive(ctx, sequence) {
		return
	}
	if hooks.Before != nil {
		if errBefore := hooks.Before(ctx); errBefore != nil {
			w.publishTerminalAuthLoadStatus(ctx, sequence, status, true, true)
			return
		}
	}
	if !w.authLoadScanActive(ctx, sequence) {
		return
	}
	snapshot := w.snapshotAuthLoadInputs()
	baseline := w.snapshotFileGenerationBaseline(isInitialScan)
	jobs := make(chan initialAuthJob, workers*2)
	results := make(chan initialAuthReadResult, workers*2)
	var workersWG sync.WaitGroup
	for i := 0; i < workers; i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			w.initialAuthReadWorker(ctx, snapshot, jobs, results)
		}()
	}
	go func() {
		w.enumerateInitialAuthFiles(ctx, snapshot, jobs, results, isInitialScan)
		close(jobs)
		workersWG.Wait()
		close(results)
	}()
	w.aggregateInitialAuthResults(ctx, sequence, status, snapshot, results, baseline, hooks.After)
}

func (w *Watcher) authLoadScanActive(ctx context.Context, sequence uint64) bool {
	if w == nil || ctx == nil || ctx.Err() != nil {
		return false
	}
	w.authLoadMu.Lock()
	defer w.authLoadMu.Unlock()
	return w.authLoadSequence == sequence && ctx.Err() == nil
}

func (w *Watcher) withActiveAuthLoadScan(ctx context.Context, sequence uint64, action func()) bool {
	if w == nil || ctx == nil || ctx.Err() != nil {
		return false
	}
	w.authLoadMu.Lock()
	defer w.authLoadMu.Unlock()
	if w.authLoadSequence != sequence || ctx.Err() != nil {
		return false
	}
	action()
	return true
}

func (w *Watcher) snapshotAuthLoadInputs() authLoadSnapshot {
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	var cfg *config.Config
	if w.config != nil {
		cfg = w.config.CloneForRuntime()
	}
	return authLoadSnapshot{config: cfg, parser: w.pluginAuthParser, authDir: w.authDir, now: time.Now().UTC()}
}

func (w *Watcher) snapshotFileGenerationBaseline(isInitialScan bool) map[string]uint64 {
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	baseline := make(map[string]uint64, len(w.fileAuthsByPath))
	for path := range w.fileAuthsByPath {
		if !isInitialScan {
			baseline[path] = w.pathGenerations[path]
			continue
		}
		baseline[path] = 0
	}
	return baseline
}

func (w *Watcher) enumerateInitialAuthFiles(ctx context.Context, snapshot authLoadSnapshot, jobs chan<- initialAuthJob, results chan<- initialAuthReadResult, isInitialScan bool) {
	enumerationStarted := time.Now()
	entries, errReadDir := os.ReadDir(snapshot.authDir)
	if errReadDir != nil {
		w.sendInitialAuthResult(ctx, results, initialAuthReadResult{enumerationResult: true, err: errReadDir, enumerationTime: time.Since(enumerationStarted)})
		return
	}
	var discovered int64
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if entry == nil || entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			continue
		}
		path := filepath.Join(snapshot.authDir, entry.Name())
		normalized := w.normalizeAuthPath(path)
		generation := uint64(0)
		if !isInitialScan {
			w.clientsMutex.RLock()
			generation = w.pathGenerations[normalized]
			w.clientsMutex.RUnlock()
		}
		discovered++
		select {
		case jobs <- initialAuthJob{path: path, normalized: normalized, generation: generation}:
		case <-ctx.Done():
			return
		}
	}
	w.sendInitialAuthResult(ctx, results, initialAuthReadResult{enumerationResult: true, discovered: discovered, enumerationTime: time.Since(enumerationStarted)})
}

func (w *Watcher) initialAuthReadWorker(ctx context.Context, snapshot authLoadSnapshot, jobs <-chan initialAuthJob, results chan<- initialAuthReadResult) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			result := initialAuthReadResult{path: job.path, normalized: job.normalized, generation: job.generation}
			readStarted := time.Now()
			result.raw, result.err = readInitialAuthFile(job.path)
			result.readTime = time.Since(readStarted)
			if result.err == nil && len(result.raw) == 0 {
				result.err = os.ErrInvalid
			}
			if result.err == nil {
				sum := sha256.Sum256(result.raw)
				result.hash = hex.EncodeToString(sum[:])
				synthStarted := time.Now()
				result.native, result.err = synthesizer.SynthesizeNativeAuthFile(&synthesizer.SynthesisContext{
					Config: snapshot.config, AuthDir: snapshot.authDir, Now: snapshot.now, IDGenerator: synthesizer.NewStableIDGenerator(),
				}, job.path, result.raw)
				result.nativeSynthTime = time.Since(synthStarted)
			}
			if !w.sendInitialAuthResult(ctx, results, result) {
				return
			}
		}
	}
}

func (w *Watcher) sendInitialAuthResult(ctx context.Context, results chan<- initialAuthReadResult, result initialAuthReadResult) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case results <- result:
		return true
	case <-ctx.Done():
		return false
	}
}

func (w *Watcher) aggregateInitialAuthResults(ctx context.Context, sequence uint64, status AuthLoadStatus, snapshot authLoadSnapshot, results <-chan initialAuthReadResult, baseline map[string]uint64, after func(context.Context) error) {
	pending := make([]initialAuthReadResult, 0, authLoadBatchSize)
	seen := make(map[string]struct{})
	enumerationFailed := false
	metrics := authLoadMetrics{started: status.StartedAt, lastLog: time.Now()}
	timer := time.NewTimer(authLoadFlushInterval)
	defer timer.Stop()
	drain := func() {
		for result := range results {
			result.raw = nil
		}
	}
	flush := func() bool {
		if len(pending) == 0 {
			return w.authLoadScanActive(ctx, sequence)
		}
		if !w.authLoadScanActive(ctx, sequence) {
			pending = nil
			return false
		}
		sort.Slice(pending, func(i, j int) bool { return pending[i].normalized < pending[j].normalized })
		if !w.commitInitialAuthResults(ctx, sequence, snapshot, pending, &status, &metrics) {
			pending = nil
			return false
		}
		for i := range pending {
			pending[i].raw = nil
		}
		pending = pending[:0]
		if !w.publishAuthLoadStatusForScan(ctx, sequence, status) {
			return false
		}
		w.logAuthLoadProgress(status, &metrics, false)
		return true
	}
	for {
		if !w.authLoadScanActive(ctx, sequence) {
			pending = nil
			drain()
			return
		}
		select {
		case result, ok := <-results:
			if !w.authLoadScanActive(ctx, sequence) {
				result.raw = nil
				pending = nil
				drain()
				return
			}
			if !ok {
				if !flush() {
					return
				}
				if !enumerationFailed {
					if !w.reconcileMissingInitialAuthPaths(ctx, sequence, baseline, seen, &status, &metrics) {
						return
					}
				}
				if after != nil {
					if !w.authLoadScanActive(ctx, sequence) {
						return
					}
					if errAfter := after(ctx); errAfter != nil {
						enumerationFailed = true
					}
					if !w.authLoadScanActive(ctx, sequence) {
						return
					}
				}
				completed := time.Now().UTC()
				status.CompletedAt = &completed
				if status.FilesFailed > 0 || enumerationFailed {
					status.State = AuthLoadStateDegraded
				} else {
					status.State = AuthLoadStateReady
				}
				if !w.publishAuthLoadStatusForScan(ctx, sequence, status) {
					return
				}
				w.logAuthLoadProgress(status, &metrics, true)
				return
			}
			if result.enumerationResult {
				status.FilesDiscovered = result.discovered
				status.ScanComplete = true
				metrics.directoryEnumeration += result.enumerationTime
				if result.err != nil {
					enumerationFailed = true
				}
				if !w.publishAuthLoadStatusForScan(ctx, sequence, status) {
					return
				}
				w.logAuthLoadProgress(status, &metrics, false)
				continue
			}
			metrics.fileRead += result.readTime
			metrics.nativeSynthesis += result.nativeSynthTime
			seen[result.normalized] = struct{}{}
			pending = append(pending, result)
			if len(pending) >= authLoadBatchSize {
				if !flush() {
					drain()
					return
				}
			}
		case <-timer.C:
			if !w.authLoadScanActive(ctx, sequence) {
				pending = nil
				drain()
				return
			}
			if !flush() {
				drain()
				return
			}
			timer.Reset(authLoadFlushInterval)
		case <-ctx.Done():
			pending = nil
			drain()
			return
		}
	}
}

func (w *Watcher) commitInitialAuthResults(ctx context.Context, sequence uint64, snapshot authLoadSnapshot, results []initialAuthReadResult, status *AuthLoadStatus, metrics *authLoadMetrics) bool {
	updates := make([]AuthUpdate, 0, len(results))
	pathByID := make(map[string]string)
	unchanged := make(map[string]int64)
	failedPaths := make(map[string]struct{})
	for i := range results {
		if !w.authLoadScanActive(ctx, sequence) {
			return false
		}
		result := &results[i]
		status.FilesProcessed++
		if result.err != nil {
			status.FilesFailed++
			if !w.withActiveAuthLoadScan(ctx, sequence, func() {
				w.clientsMutex.Lock()
				defer w.clientsMutex.Unlock()
				if w.pathGenerationCurrentLocked(result.normalized, result.generation) {
					if _, known := w.lastAuthHashes[result.normalized]; known {
						w.lastAuthHashes[result.normalized] = ""
					}
				}
			}) {
				return false
			}
			continue
		}
		generationCurrent := false
		if !w.withActiveAuthLoadScan(ctx, sequence, func() {
			w.clientsMutex.Lock()
			generationCurrent = w.pathGenerationCurrentLocked(result.normalized, result.generation)
			w.clientsMutex.Unlock()
		}) {
			return false
		}
		if !generationCurrent {
			status.FilesSkipped++
			continue
		}
		pluginStarted := time.Now()
		pluginAuths, handled, errPlugin := synthesizer.SynthesizePluginAuthFile(&synthesizer.SynthesisContext{
			Config: snapshot.config, AuthDir: snapshot.authDir, Now: snapshot.now, IDGenerator: synthesizer.NewStableIDGenerator(), PluginAuthParser: snapshot.parser,
		}, result.path, result.raw)
		metrics.pluginSynthesis += time.Since(pluginStarted)
		if errPlugin != nil {
			status.FilesFailed++
			continue
		}
		auths := result.native.Auths
		if handled {
			auths = pluginAuths
		}
		newByID := authSliceToMap(auths)
		generationCurrent = false
		var pathUpdates []AuthUpdate
		if !w.withActiveAuthLoadScan(ctx, sequence, func() {
			w.clientsMutex.Lock()
			defer w.clientsMutex.Unlock()
			generationCurrent = w.pathGenerationCurrentLocked(result.normalized, result.generation)
			if !generationCurrent {
				return
			}
			if w.lastAuthHashes == nil {
				w.lastAuthHashes = make(map[string]string)
			}
			if w.fileAuthsByPath == nil {
				w.fileAuthsByPath = make(map[string]map[string]*coreauth.Auth)
			}
			w.lastAuthHashes[result.normalized] = result.hash
			oldByID := cloneAuthMap(w.fileAuthsByPath[result.normalized])
			if len(newByID) == 0 {
				delete(w.fileAuthsByPath, result.normalized)
			} else {
				w.fileAuthsByPath[result.normalized] = cloneAuthMap(newByID)
			}
			pathUpdates = w.computePerPathUpdatesLocked(oldByID, newByID)
		}) {
			return false
		}
		if !generationCurrent {
			status.FilesSkipped++
			continue
		}
		for _, auth := range newByID {
			if auth == nil {
				continue
			}
			changed := false
			for _, update := range pathUpdates {
				if update.ID == auth.ID {
					changed = true
					break
				}
			}
			if !changed {
				unchanged[result.normalized]++
			}
		}
		for _, update := range pathUpdates {
			updates = append(updates, update)
			pathByID[update.ID] = result.normalized
		}
	}
	for _, count := range unchanged {
		status.AuthsLoaded += count
	}
	batchStarted := time.Now()
	acknowledged, dispatched := w.dispatchInitialAuthBatch(ctx, sequence, updates)
	metrics.batchRegistration += time.Since(batchStarted)
	if !dispatched || !w.authLoadScanActive(ctx, sequence) {
		return false
	}
	loadedByID := make(map[string]bool, len(acknowledged))
	for _, result := range acknowledged {
		loadedByID[result.ID] = result.Loaded
	}
	for _, update := range updates {
		if loadedByID[update.ID] {
			status.AuthsLoaded++
			continue
		}
		failedPaths[pathByID[update.ID]] = struct{}{}
	}
	status.FilesFailed += int64(len(failedPaths))
	return true
}

func (w *Watcher) reconcileMissingInitialAuthPaths(ctx context.Context, sequence uint64, baseline map[string]uint64, seen map[string]struct{}, status *AuthLoadStatus, metrics *authLoadMetrics) bool {
	updates := make([]AuthUpdate, 0)
	if !w.withActiveAuthLoadScan(ctx, sequence, func() {
		w.clientsMutex.Lock()
		defer w.clientsMutex.Unlock()
		for path, generation := range baseline {
			if _, present := seen[path]; present || !w.pathGenerationCurrentLocked(path, generation) {
				continue
			}
			oldByID := cloneAuthMap(w.fileAuthsByPath[path])
			delete(w.fileAuthsByPath, path)
			delete(w.lastAuthHashes, path)
			delete(w.lastAuthContents, path)
			updates = append(updates, w.computePerPathUpdatesLocked(oldByID, map[string]*coreauth.Auth{})...)
		}
	}) {
		return false
	}
	if len(updates) > 0 {
		batchStarted := time.Now()
		_, dispatched := w.dispatchInitialAuthBatch(ctx, sequence, updates)
		metrics.batchRegistration += time.Since(batchStarted)
		if !dispatched {
			return false
		}
	}
	return w.authLoadScanActive(ctx, sequence)
}

func cloneAuthMap(auths map[string]*coreauth.Auth) map[string]*coreauth.Auth {
	cloned := make(map[string]*coreauth.Auth, len(auths))
	for id, auth := range auths {
		if auth != nil {
			cloned[id] = auth.Clone()
		}
	}
	return cloned
}

func (w *Watcher) publishTerminalAuthLoadStatus(ctx context.Context, sequence uint64, status AuthLoadStatus, degraded, scanComplete bool) {
	if !w.authLoadScanActive(ctx, sequence) {
		return
	}
	status.ScanComplete = scanComplete
	completed := time.Now().UTC()
	status.CompletedAt = &completed
	if degraded {
		status.State = AuthLoadStateDegraded
	} else {
		status.State = AuthLoadStateReady
	}
	w.publishAuthLoadStatusForScan(ctx, sequence, status)
}

func (w *Watcher) logAuthLoadProgress(status AuthLoadStatus, metrics *authLoadMetrics, complete bool) {
	if metrics == nil {
		return
	}
	now := time.Now()
	if !complete && now.Sub(metrics.lastLog) < authLoadLogInterval {
		return
	}
	metrics.lastLog = now
	fields := log.Fields{
		"files_discovered":      status.FilesDiscovered,
		"files_processed":       status.FilesProcessed,
		"auths_loaded":          status.AuthsLoaded,
		"files_failed":          status.FilesFailed,
		"files_skipped":         status.FilesSkipped,
		"directory_enumeration": metrics.directoryEnumeration,
		"file_read":             metrics.fileRead,
		"native_synthesis":      metrics.nativeSynthesis,
		"plugin_synthesis":      metrics.pluginSynthesis,
		"batch_registration":    metrics.batchRegistration,
		"total_load":            now.Sub(metrics.started),
	}
	entry := log.WithFields(fields)
	if complete {
		entry.Info("auth file load complete")
		return
	}
	entry.Debug("auth file load progress")
}
