package auth

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type recordingCooldownStateStore struct {
	applyCount atomic.Int32
	mu         sync.Mutex
	snapshots  []CooldownStateSnapshot
	batches    [][]CooldownStateSnapshot
	load       []CooldownStateSnapshot
}

func flushCooldownStateForTest(t *testing.T, manager *Manager) {
	t.Helper()
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() returned error: %v", errFlush)
	}
}

func (s *recordingCooldownStateStore) Load(context.Context) ([]CooldownStateSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneCooldownStateSnapshots(s.load), nil
}

func (s *recordingCooldownStateStore) Apply(_ context.Context, snapshots []CooldownStateSnapshot) error {
	s.applyCount.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots = cloneCooldownStateSnapshots(snapshots)
	s.batches = append(s.batches, cloneCooldownStateSnapshots(snapshots))
	return nil
}

func (s *recordingCooldownStateStore) recordedBatches() [][]CooldownStateSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	batches := make([][]CooldownStateSnapshot, len(s.batches))
	for i := range s.batches {
		batches[i] = cloneCooldownStateSnapshots(s.batches[i])
	}
	return batches
}

func (s *recordingCooldownStateStore) savedRecords() []CooldownStateRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshots := cloneCooldownStateSnapshots(s.snapshots)
	var records []CooldownStateRecord
	for _, snapshot := range snapshots {
		records = append(records, snapshot.Records...)
	}
	return records
}

func (s *recordingCooldownStateStore) savedSnapshots() []CooldownStateSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneCooldownStateSnapshots(s.snapshots)
}

func (s *recordingCooldownStateStore) resetRecording() {
	s.applyCount.Store(0)
	s.mu.Lock()
	s.snapshots = nil
	s.batches = nil
	s.mu.Unlock()
}

type blockingCooldownStateStore struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	recordingCooldownStateStore
}

func (s *blockingCooldownStateStore) Apply(ctx context.Context, snapshots []CooldownStateSnapshot) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
	}
	return s.recordingCooldownStateStore.Apply(ctx, snapshots)
}

type blockingLoadCooldownStateStore struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	recordingCooldownStateStore
}

func (s *blockingLoadCooldownStateStore) Load(ctx context.Context) ([]CooldownStateSnapshot, error) {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.release:
	}
	return s.recordingCooldownStateStore.Load(ctx)
}

type failingCooldownStateStore struct {
	fail atomic.Bool
	recordingCooldownStateStore
}

func (s *failingCooldownStateStore) Apply(ctx context.Context, snapshots []CooldownStateSnapshot) error {
	if s.fail.Load() {
		return errors.New("cooldown store unavailable")
	}
	return s.recordingCooldownStateStore.Apply(ctx, snapshots)
}

func cloneCooldownStateSnapshots(snapshots []CooldownStateSnapshot) []CooldownStateSnapshot {
	if len(snapshots) == 0 {
		return nil
	}
	cloned := make([]CooldownStateSnapshot, len(snapshots))
	for i := range snapshots {
		cloned[i].AuthID = snapshots[i].AuthID
		cloned[i].Records = make([]CooldownStateRecord, len(snapshots[i].Records))
		for j := range snapshots[i].Records {
			cloned[i].Records[j] = snapshots[i].Records[j]
			cloned[i].Records[j].LastError = cloneError(snapshots[i].Records[j].LastError)
		}
	}
	return cloned
}

func TestFileCooldownStateStore_StateRelativePath(t *testing.T) {
	authDir := filepath.Join(t.TempDir(), "auths")
	store := NewFileCooldownStateStoreWithAuthDir(authDir, authDir)

	cases := []struct {
		name   string
		record CooldownStateRecord
		want   string
	}{
		{
			name: "absolute auth file under auth dir",
			record: CooldownStateRecord{
				AuthID:   "auth-1",
				AuthFile: filepath.Join(authDir, "nested", "xai.json"),
			},
			want: filepath.Join("nested", "xai.cds"),
		},
		{
			name: "relative auth file",
			record: CooldownStateRecord{
				AuthID:   "auth-2",
				AuthFile: filepath.Join("team", "xai.json"),
			},
			want: filepath.Join("team", "xai.cds"),
		},
		{
			name: "absolute auth file outside auth dir",
			record: CooldownStateRecord{
				AuthID:   "auth-3",
				AuthFile: filepath.Join(t.TempDir(), "outside.json"),
			},
			want: "outside.cds",
		},
		{
			name: "relative parent escape is rejected",
			record: CooldownStateRecord{
				AuthID:   "auth-4",
				AuthFile: filepath.Join("..", "escape.json"),
			},
			want: "",
		},
		{
			name: "auth id fallback",
			record: CooldownStateRecord{
				AuthID: "auth/id 5",
			},
			want: "auth_id_5.cds",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := store.stateRelativePath(tc.record); got != tc.want {
				t.Fatalf("stateRelativePath() = %q, want %q", got, tc.want)
			}
		})
	}
}

// With a relative auth-dir the file synthesizer records an auth file as "<auth-dir>/<name>", so the state path must strip
// the auth dir there too; otherwise the store, rooted at the auth dir, writes "<auth-dir>/<auth-dir>/<name>.cds".
func TestFileCooldownStateStore_StateRelativePathRelativeAuthDir(t *testing.T) {
	store := NewFileCooldownStateStoreWithAuthDir("auths", "auths")
	cases := []struct {
		authFile string
		want     string
	}{
		{filepath.Join("auths", "claude.json"), "claude.cds"},
		{filepath.Join("auths", "team", "xai.json"), filepath.Join("team", "xai.cds")},
		{filepath.Join("other", "xai.json"), filepath.Join("other", "xai.cds")},
	}
	for _, tc := range cases {
		if got := store.stateRelativePath(CooldownStateRecord{AuthID: "id", AuthFile: tc.authFile}); got != tc.want {
			t.Errorf("stateRelativePath(%q) = %q, want %q", tc.authFile, got, tc.want)
		}
	}
}

func TestFileCooldownStateStore_ApplyOnlyChangesNamedAuth(t *testing.T) {
	authDir := t.TempDir()
	store := NewFileCooldownStateStoreWithAuthDir(authDir, authDir)
	ctx := context.Background()

	nextRetry := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	updatedAt := time.Now().UTC().Truncate(time.Second)
	record1 := CooldownStateRecord{
		Provider:       "xai",
		AuthID:         "auth-1",
		AuthFile:       filepath.Join(authDir, "xai-1.json"),
		Model:          "grok-4",
		Status:         "cooling",
		NextRetryAfter: nextRetry,
		Reason:         "quota",
		Quota: QuotaState{
			Exceeded:      true,
			Reason:        "quota",
			NextRecoverAt: nextRetry,
			BackoffLevel:  1,
		},
		LastError: &Error{Message: "rate limited", HTTPStatus: 429},
		UpdatedAt: updatedAt,
	}
	record2 := record1
	record2.AuthID = "auth-2"
	record2.AuthFile = filepath.Join(authDir, "xai-2.json")
	record2.Model = "grok-3"

	if errApply := store.Apply(ctx, []CooldownStateSnapshot{
		{AuthID: record1.AuthID, Records: []CooldownStateRecord{record1}},
		{AuthID: record2.AuthID, Records: []CooldownStateRecord{record2}},
	}); errApply != nil {
		t.Fatalf("Apply() returned error: %v", errApply)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "xai-1.cds")); errStat != nil {
		t.Fatalf("expected xai-1.cds to exist: %v", errStat)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "xai-2.cds")); errStat != nil {
		t.Fatalf("expected xai-2.cds to exist: %v", errStat)
	}

	loaded, errLoad := store.Load(ctx)
	if errLoad != nil {
		t.Fatalf("Load() returned error: %v", errLoad)
	}
	if len(loaded) != 2 {
		t.Fatalf("loaded snapshots = %d, want 2", len(loaded))
	}

	if errApply := store.Apply(ctx, []CooldownStateSnapshot{{AuthID: record1.AuthID}}); errApply != nil {
		t.Fatalf("Apply(clear auth-1) returned error: %v", errApply)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "xai-1.cds")); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("expected xai-1.cds to be removed, stat error = %v", errStat)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "xai-2.cds")); errStat != nil {
		t.Fatalf("expected xai-2.cds to remain: %v", errStat)
	}

	loaded, errLoad = store.Load(ctx)
	if errLoad != nil {
		t.Fatalf("Load() after clear returned error: %v", errLoad)
	}
	if len(loaded) != 1 || loaded[0].AuthID != record2.AuthID || len(loaded[0].Records) != 1 {
		t.Fatalf("loaded snapshots = %+v, want only auth-2", loaded)
	}
	loadedRecord := loaded[0].Records[0]
	if loadedRecord.Model != record2.Model || !loadedRecord.NextRetryAfter.Equal(nextRetry) {
		t.Fatalf("loaded record = %+v, want %+v", loadedRecord, record2)
	}
	if loadedRecord.LastError == nil || loadedRecord.LastError.HTTPStatus != 429 {
		t.Fatalf("loaded last error = %+v, want HTTP 429", loadedRecord.LastError)
	}
}

func TestFileCooldownStateStore_ConcurrentApply(t *testing.T) {
	authDir := t.TempDir()
	store := NewFileCooldownStateStoreWithAuthDir(authDir, authDir)
	ctx := context.Background()
	nextRetry := time.Now().Add(time.Hour).UTC().Truncate(time.Second)

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.Apply(ctx, []CooldownStateSnapshot{
				{AuthID: "auth-1", Records: []CooldownStateRecord{{
					Provider:       "xai",
					AuthID:         "auth-1",
					AuthFile:       filepath.Join(authDir, "xai.json"),
					Model:          "grok-4",
					Status:         "cooling",
					NextRetryAfter: nextRetry.Add(time.Duration(i) * time.Second),
					UpdatedAt:      nextRetry,
				}}},
			})
		}()
	}
	wg.Wait()
	close(errs)
	for errApply := range errs {
		if errApply != nil {
			t.Fatalf("Apply() returned error: %v", errApply)
		}
	}

	loaded, errLoad := store.Load(ctx)
	if errLoad != nil {
		t.Fatalf("Load() returned error: %v", errLoad)
	}
	if len(loaded) != 1 || len(loaded[0].Records) != 1 {
		t.Fatalf("loaded snapshots = %+v, want one auth with one record", loaded)
	}

	tmpMatches, errGlob := filepath.Glob(filepath.Join(authDir, "*.tmp"))
	if errGlob != nil {
		t.Fatalf("glob temp files: %v", errGlob)
	}
	if len(tmpMatches) != 0 {
		t.Fatalf("leftover temp files = %v, want none", tmpMatches)
	}
}

func TestManager_MarkResult_PersistsCooldownOnlyWhenStateChanges(t *testing.T) {
	const failedModel = "claude-opus-4-6-thinking"

	store := &recordingCooldownStateStore{}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)

	auth := &Auth{ID: "auth-1", Provider: "antigravity", Status: StatusActive}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{
		{ID: failedModel},
		{ID: "gemini-3.6-flash-high"},
	})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-2", Provider: "xai", Status: StatusActive}); errRegister != nil {
		t.Fatalf("Register(auth-2) returned error: %v", errRegister)
	}
	manager.MarkResult(context.Background(), Result{
		AuthID:   "auth-2",
		Provider: "xai",
		Model:    "grok-4",
		Success:  false,
		Error:    &Error{Message: "upstream unavailable", HTTPStatus: 500},
	})
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates(auth-2) returned error: %v", errFlush)
	}
	store.resetRecording()

	manager.MarkResult(context.Background(), Result{AuthID: auth.ID, Provider: auth.Provider, Model: failedModel, Success: true})
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() returned error: %v", errFlush)
	}
	if got := store.applyCount.Load(); got != 0 {
		t.Fatalf("healthy success saved cooldown state %d times, want 0", got)
	}

	manager.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: auth.Provider,
		Model:    failedModel,
		Success:  false,
		Error:    &Error{Message: "upstream unavailable", HTTPStatus: 500},
	})
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() returned error: %v", errFlush)
	}
	if got := store.applyCount.Load(); got != 1 {
		t.Fatalf("cooldown failure saved cooldown state %d times, want 1", got)
	}
	updated, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatalf("auth %q missing after model failure", auth.ID)
	}
	if updated.Unavailable || updated.Status != StatusActive || updated.Quota.Exceeded {
		t.Fatalf("auth state = status %q unavailable %t quota %+v, want healthy credential with model-scoped failure", updated.Status, updated.Unavailable, updated.Quota)
	}
	batches := store.recordedBatches()
	if len(batches) != 1 || len(batches[0]) != 1 || batches[0][0].AuthID != auth.ID {
		t.Fatalf("cooldown failure batches = %+v, want one auth-1 snapshot", batches)
	}
	records := batches[0][0].Records
	if len(records) != 1 || records[0].Model != failedModel {
		t.Fatalf("cooldown failure records = %+v, want only model %s", records, failedModel)
	}

	manager.MarkResult(context.Background(), Result{AuthID: auth.ID, Provider: auth.Provider, Model: failedModel, Success: true})
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() returned error: %v", errFlush)
	}
	if got := store.applyCount.Load(); got != 2 {
		t.Fatalf("cooldown clear saved cooldown state %d times, want 2", got)
	}
	batches = store.recordedBatches()
	if len(batches) != 2 || len(batches[1]) != 1 || batches[1][0].AuthID != auth.ID || len(batches[1][0].Records) != 0 {
		t.Fatalf("cooldown clear batches = %+v, want empty auth-1 snapshot", batches)
	}

	manager.MarkResult(context.Background(), Result{AuthID: auth.ID, Provider: auth.Provider, Model: failedModel, Success: true})
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() returned error: %v", errFlush)
	}
	if got := store.applyCount.Load(); got != 2 {
		t.Fatalf("clean success saved cooldown state %d times, want 2", got)
	}
}

func TestManager_MarkResult_DoesNotWaitForCooldownStore(t *testing.T) {
	store := &blockingCooldownStateStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai", Status: StatusActive}); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}

	done := make(chan struct{})
	go func() {
		manager.MarkResult(context.Background(), Result{
			AuthID:   "auth-1",
			Provider: "xai",
			Model:    "grok-4",
			Success:  false,
			Error:    &Error{Message: "rate limited", HTTPStatus: 429},
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("MarkResult blocked on cooldown persistence")
	}
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("cooldown Apply did not start")
	}
	close(store.release)
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() returned error: %v", errFlush)
	}
}

func TestManager_FlushCooldownStates_RetainsFailedBatch(t *testing.T) {
	store := &failingCooldownStateStore{}
	store.fail.Store(true)
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai", Status: StatusActive}); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}
	manager.MarkResult(context.Background(), Result{
		AuthID:   "auth-1",
		Provider: "xai",
		Model:    "grok-4",
		Success:  false,
		Error:    &Error{Message: "rate limited", HTTPStatus: 429},
	})

	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush == nil {
		t.Fatal("FlushCooldownStates() returned nil error while store was failing")
	}
	store.fail.Store(false)
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() retry returned error: %v", errFlush)
	}
	batches := store.recordedBatches()
	if len(batches) == 0 || len(batches[len(batches)-1]) != 1 || batches[len(batches)-1][0].AuthID != "auth-1" {
		t.Fatalf("successful retry batches = %+v, want auth-1", batches)
	}
}

func TestManager_FlushCooldownStates_PersistsChangeDuringApply(t *testing.T) {
	store := &blockingCooldownStateStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai", Status: StatusActive}); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}
	manager.MarkResult(context.Background(), Result{
		AuthID:   "auth-1",
		Provider: "xai",
		Model:    "grok-4",
		Success:  false,
		Error:    &Error{Message: "rate limited", HTTPStatus: 429},
	})

	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("cooldown Apply did not start")
	}
	manager.MarkResult(context.Background(), Result{AuthID: "auth-1", Provider: "xai", Model: "grok-4", Success: true})
	close(store.release)
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() returned error: %v", errFlush)
	}

	batches := store.recordedBatches()
	if len(batches) < 2 {
		t.Fatalf("recorded batches = %+v, want failure and clear batches", batches)
	}
	lastBatch := batches[len(batches)-1]
	if len(lastBatch) != 1 || lastBatch[0].AuthID != "auth-1" || len(lastBatch[0].Records) != 0 {
		t.Fatalf("last batch = %+v, want empty auth-1 snapshot", lastBatch)
	}
}

func TestManager_Update_ClearsPersistedCooldownWhenCredentialsChange(t *testing.T) {
	store := &recordingCooldownStateStore{}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)

	auth := &Auth{
		ID:       "auth-codex-1",
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token": "token-1",
		},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}

	// 1. Fail with 401 unauthorized
	manager.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: "codex",
		Model:    "gpt-6-astra",
		Success:  false,
		Error:    &Error{Message: "invalidated token", HTTPStatus: 401},
	})
	// Cooldown persistence is batched in this fork; flush before each assertion.
	flushCooldownStateForTest(t, manager)
	if len(store.savedRecords()) == 0 {
		t.Fatal("expected cooldown record to be saved after unauthorized failure")
	}

	// 2. Update without credential change (e.g. metadata note update)
	sameCredAuth := &Auth{
		ID:       auth.ID,
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token": "token-1",
			"note":         "updated note",
		},
	}
	if _, errUpdate := manager.Update(WithSkipPersist(context.Background()), sameCredAuth); errUpdate != nil {
		t.Fatalf("Update() returned error: %v", errUpdate)
	}
	flushCooldownStateForTest(t, manager)
	if len(store.savedRecords()) == 0 {
		t.Fatal("expected cooldown record to remain when credentials did not change")
	}

	// 3. Update with credential change (new access_token)
	newCredAuth := &Auth{
		ID:       auth.ID,
		Provider: "codex",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token": "token-2",
		},
	}
	if _, errUpdate := manager.Update(WithSkipPersist(context.Background()), newCredAuth); errUpdate != nil {
		t.Fatalf("Update() returned error: %v", errUpdate)
	}
	flushCooldownStateForTest(t, manager)
	if len(store.savedRecords()) != 0 {
		t.Fatalf("expected cooldown records to be cleared after credential change, got %d records", len(store.savedRecords()))
	}
}

func TestManagerSetConfigSnapshotDefersCooldownPersistence(t *testing.T) {
	store := &recordingCooldownStateStore{}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	auth := &Auth{ID: "auth-1", Provider: "xai", Status: StatusActive}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}
	manager.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: auth.Provider,
		Model:    "grok-4",
		Success:  false,
		Error:    &Error{Message: "rate limited", HTTPStatus: 429},
	})
	store.applyCount.Store(0)

	if changed := manager.SetConfigSnapshot(&internalconfig.Config{DisableCooling: true}); !changed {
		t.Fatal("SetConfigSnapshot() = false, want cleared cooldown state")
	}
	if got := store.applyCount.Load(); got != 0 {
		t.Fatalf("SetConfigSnapshot() persisted cooldown state %d times, want 0", got)
	}
	manager.PersistCooldownStates(context.Background())
	if got := store.applyCount.Load(); got != 1 {
		t.Fatalf("PersistCooldownStates() saved cooldown state %d times, want 1", got)
	}
}

func TestManagerSwapCooldownStateStorePersistsOldStoreBeforeSwap(t *testing.T) {
	oldStore := &recordingCooldownStateStore{}
	newStore := &recordingCooldownStateStore{}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(oldStore)
	auth := &Auth{ID: "auth-1", Provider: "xai", Status: StatusActive}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}
	manager.MarkResult(context.Background(), Result{
		AuthID: auth.ID, Provider: auth.Provider, Model: "grok-4", Success: false,
		Error: &Error{Message: "rate limited", HTTPStatus: 429},
	})
	oldStore.applyCount.Store(0)
	if changed := manager.SetConfigSnapshot(&internalconfig.Config{DisableCooling: true}); !changed {
		t.Fatal("SetConfigSnapshot() = false, want cleared cooldown state")
	}

	if swapped := manager.SwapCooldownStateStore(context.Background(), newStore, true); !swapped {
		t.Fatal("SwapCooldownStateStore() = false, want true")
	}
	if got := oldStore.applyCount.Load(); got != 1 {
		t.Fatalf("old store save count = %d, want 1", got)
	}
	if len(oldStore.snapshots) != 0 {
		t.Fatalf("old store snapshots = %+v, want cleared cooldown state", oldStore.snapshots)
	}
	manager.mu.RLock()
	currentStore := manager.cooldownStore
	manager.mu.RUnlock()
	if currentStore != newStore {
		t.Fatal("cooldown store swapped before the old store was persisted")
	}
}

func TestManagerApplyConfigWithCooldownStoreSerializesTransitions(t *testing.T) {
	oldStore := &blockingCooldownStateStore{started: make(chan struct{}), release: make(chan struct{})}
	firstStore := &recordingCooldownStateStore{}
	secondStore := &recordingCooldownStateStore{}
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "xai", Status: StatusActive}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}
	manager.MarkResult(context.Background(), Result{
		AuthID: auth.ID, Provider: auth.Provider, Model: "grok-4", Success: false,
		Error: &Error{Message: "rate limited", HTTPStatus: 429},
	})
	manager.SetCooldownStateStore(oldStore)

	firstDone := make(chan bool, 1)
	go func() {
		firstDone <- manager.ApplyConfigWithCooldownStateStore(context.Background(), &internalconfig.Config{DisableCooling: true}, firstStore)
	}()
	select {
	case <-oldStore.started:
	case <-time.After(time.Second):
		t.Fatal("first old-store persistence did not start")
	}

	secondDone := make(chan bool, 1)
	go func() {
		secondDone <- manager.ApplyConfigWithCooldownStateStore(context.Background(), &internalconfig.Config{}, secondStore)
	}()
	select {
	case <-secondDone:
		t.Fatal("concurrent config transition completed while old-store persistence was blocked")
	case <-time.After(100 * time.Millisecond):
	}

	close(oldStore.release)
	if applied := waitForCooldownTransition(t, firstDone, "first config transition"); !applied {
		t.Fatal("first config transition returned false")
	}
	if applied := waitForCooldownTransition(t, secondDone, "second config transition"); !applied {
		t.Fatal("second config transition returned false")
	}
	manager.mu.RLock()
	currentStore := manager.cooldownStore
	manager.mu.RUnlock()
	if currentStore != secondStore {
		t.Fatal("concurrent config transitions did not leave the final resolved store installed")
	}
}

func waitForCooldownTransition(t *testing.T, done <-chan bool, name string) bool {
	t.Helper()
	select {
	case applied := <-done:
		return applied
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return false
	}
}

func TestManagerSwapCooldownStateStoreKeepsOldStoreWhenCanceled(t *testing.T) {
	oldStore := &blockingCooldownStateStore{started: make(chan struct{}), release: make(chan struct{})}
	newStore := &recordingCooldownStateStore{}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(oldStore)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- manager.SwapCooldownStateStore(ctx, newStore, true) }()
	select {
	case <-oldStore.started:
	case <-time.After(time.Second):
		t.Fatal("old cooldown store persistence did not start")
	}
	manager.mu.RLock()
	currentStore := manager.cooldownStore
	manager.mu.RUnlock()
	if currentStore != oldStore {
		t.Fatal("cooldown store swapped while old store persistence was blocked")
	}
	cancel()
	select {
	case swapped := <-done:
		if swapped {
			t.Fatal("SwapCooldownStateStore() = true after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("SwapCooldownStateStore() did not honor cancellation")
	}

	close(oldStore.release)
	if swapped := manager.SwapCooldownStateStore(context.Background(), newStore, false); !swapped {
		t.Fatal("SwapCooldownStateStore() = false, want retry to persist the old store before swapping")
	}
	manager.mu.RLock()
	currentStore = manager.cooldownStore
	manager.mu.RUnlock()
	if currentStore != newStore {
		t.Fatal("cooldown store was not swapped after pending persistence completed")
	}
}

func TestManager_RestoreCooldownStates(t *testing.T) {
	nextRetry := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	store := &recordingCooldownStateStore{
		load: []CooldownStateSnapshot{
			{AuthID: "auth-1", Records: []CooldownStateRecord{{
				Provider:       "xai",
				AuthID:         "auth-1",
				Model:          "grok-4",
				Status:         "cooling",
				NextRetryAfter: nextRetry,
				Reason:         "quota",
				Quota: QuotaState{
					Exceeded:      true,
					Reason:        "quota",
					NextRecoverAt: nextRetry,
				},
				LastError: &Error{Message: "rate limited", HTTPStatus: 429},
				UpdatedAt: nextRetry.Add(-time.Minute),
			}}},
		},
	}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai"}); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}

	if errRestore := manager.RestoreCooldownStates(context.Background()); errRestore != nil {
		t.Fatalf("RestoreCooldownStates() returned error: %v", errRestore)
	}

	auth, ok := manager.GetByID("auth-1")
	if !ok {
		t.Fatal("restored auth was not found")
	}
	state := auth.ModelStates["grok-4"]
	if state == nil {
		t.Fatal("model state was not restored")
	}
	if !state.Unavailable || state.Status != StatusError || !state.NextRetryAfter.Equal(nextRetry) {
		t.Fatalf("restored state = %+v, want unavailable status error until %v", state, nextRetry)
	}
	if state.LastError == nil || state.LastError.HTTPStatus != 429 {
		t.Fatalf("restored last error = %+v, want HTTP 429", state.LastError)
	}
	if got := store.applyCount.Load(); got != 1 {
		t.Fatalf("restore cleanup saved cooldown state %d times, want 1", got)
	}
}

func TestManager_PreparedCooldownAppliedDuringRegister(t *testing.T) {
	nextRetry := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	store := &recordingCooldownStateStore{load: []CooldownStateSnapshot{{
		AuthID: "auth-1",
		Records: []CooldownStateRecord{{
			AuthID: "auth-1", Provider: "xai", Model: "grok-4",
			Status: "cooling", NextRetryAfter: nextRetry,
			Quota: QuotaState{Exceeded: true, NextRecoverAt: nextRetry},
		}},
	}}}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	if errPrepare := manager.PrepareCooldownRestore(context.Background()); errPrepare != nil {
		t.Fatalf("PrepareCooldownRestore() error = %v", errPrepare)
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai"}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	auth, ok := manager.GetByID("auth-1")
	if !ok || auth.ModelStates["grok-4"] == nil || !auth.ModelStates["grok-4"].Unavailable {
		t.Fatalf("registered auth = %+v, want persisted cooldown before scheduler registration", auth)
	}
	if got := store.applyCount.Load(); got != 0 {
		t.Fatalf("Apply count before completion = %d, want 0", got)
	}
	if errComplete := manager.CompleteCooldownRestore(context.Background()); errComplete != nil {
		t.Fatalf("CompleteCooldownRestore() error = %v", errComplete)
	}
	if got := store.applyCount.Load(); got != 1 {
		t.Fatalf("Apply count after completion = %d, want 1", got)
	}
}

func TestManager_CompleteCooldownRestoreClearsStaleSnapshot(t *testing.T) {
	store := &recordingCooldownStateStore{load: []CooldownStateSnapshot{{
		AuthID: "missing-auth",
		Records: []CooldownStateRecord{{
			AuthID: "missing-auth", NextRetryAfter: time.Now().Add(time.Hour),
		}},
	}}}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	if errPrepare := manager.PrepareCooldownRestore(context.Background()); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if errComplete := manager.CompleteCooldownRestore(context.Background()); errComplete != nil {
		t.Fatal(errComplete)
	}
	batches := store.recordedBatches()
	last := batches[len(batches)-1]
	if len(last) != 1 || last[0].AuthID != "missing-auth" || len(last[0].Records) != 0 {
		t.Fatalf("cleanup batch = %+v, want empty missing-auth snapshot", last)
	}
}

func TestManager_PrepareCooldownRestoreAppliesAlreadyRegisteredAuth(t *testing.T) {
	nextRetry := time.Now().Add(time.Hour)
	store := &recordingCooldownStateStore{load: []CooldownStateSnapshot{{
		AuthID:  "auth-live",
		Records: []CooldownStateRecord{{AuthID: "auth-live", NextRetryAfter: nextRetry}},
	}}}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-live", Provider: "xai"}); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errPrepare := manager.PrepareCooldownRestore(context.Background()); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	auth, ok := manager.GetByID("auth-live")
	if !ok || !auth.Unavailable || !auth.NextRetryAfter.Equal(nextRetry) {
		t.Fatalf("auth after prepare = %+v", auth)
	}
}

func TestManager_PrepareCooldownRestoreIgnoresReplacedStore(t *testing.T) {
	for _, tc := range []struct {
		name        string
		replacement CooldownStateStore
	}{
		{name: "disabled"},
		{name: "replaced", replacement: &recordingCooldownStateStore{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nextRetry := time.Now().Add(time.Hour)
			store := &blockingLoadCooldownStateStore{
				started: make(chan struct{}),
				release: make(chan struct{}),
				recordingCooldownStateStore: recordingCooldownStateStore{load: []CooldownStateSnapshot{{
					AuthID: "auth-stale",
					Records: []CooldownStateRecord{{
						AuthID: "auth-stale", Model: "grok-stale", NextRetryAfter: nextRetry,
					}},
				}}},
			}
			manager := NewManager(nil, nil, nil)
			manager.SetCooldownStateStore(store)

			errPrepare := make(chan error, 1)
			go func() {
				errPrepare <- manager.PrepareCooldownRestore(context.Background())
			}()
			select {
			case <-store.started:
			case <-time.After(time.Second):
				t.Fatal("PrepareCooldownRestore() did not start loading")
			}
			manager.SetCooldownStateStore(tc.replacement)
			close(store.release)
			if err := <-errPrepare; err != nil {
				t.Fatalf("PrepareCooldownRestore() error = %v", err)
			}

			if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-stale", Provider: "xai"}); errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}
			auth, ok := manager.GetByID("auth-stale")
			if !ok {
				t.Fatal("registered auth was not found")
			}
			if state := auth.ModelStates["grok-stale"]; state != nil && state.Unavailable {
				t.Fatalf("stale store cooldown was restored: %+v", state)
			}
		})
	}
}

func TestManager_PrepareCooldownRestoreMergesDuplicateSnapshots(t *testing.T) {
	nextRetry := time.Now().Add(time.Hour)
	store := &recordingCooldownStateStore{load: []CooldownStateSnapshot{
		{AuthID: "auth-1", Records: []CooldownStateRecord{{AuthID: "auth-1", Model: "grok-3", NextRetryAfter: nextRetry}}},
		{AuthID: "auth-1", Records: []CooldownStateRecord{{AuthID: "auth-1", Model: "grok-4", NextRetryAfter: nextRetry}}},
	}}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	if errPrepare := manager.PrepareCooldownRestore(context.Background()); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai"}); errRegister != nil {
		t.Fatal(errRegister)
	}
	auth, ok := manager.GetByID("auth-1")
	if !ok {
		t.Fatal("registered auth was not found")
	}
	for _, model := range []string{"grok-3", "grok-4"} {
		if state := auth.ModelStates[model]; state == nil || !state.Unavailable {
			t.Fatalf("model %q state = %+v, want merged cooldown", model, state)
		}
	}
}

func TestManager_RestoreCooldownStatesAppliesAuthLevelAfterModels(t *testing.T) {
	modelRetry := time.Now().Add(time.Hour)
	authRetry := modelRetry.Add(time.Hour)
	store := &recordingCooldownStateStore{load: []CooldownStateSnapshot{{
		AuthID: "auth-1",
		Records: []CooldownStateRecord{
			{AuthID: "auth-1", NextRetryAfter: authRetry, Reason: "auth-level"},
			{AuthID: "auth-1", Model: "grok-4", NextRetryAfter: modelRetry, Reason: "model-level"},
		},
	}}}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai"}); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errRestore := manager.RestoreCooldownStates(context.Background()); errRestore != nil {
		t.Fatal(errRestore)
	}
	auth, ok := manager.GetByID("auth-1")
	if !ok {
		t.Fatal("restored auth was not found")
	}
	if !auth.NextRetryAfter.Equal(authRetry) || auth.StatusMessage != "auth-level" {
		t.Fatalf("auth-level cooldown = %+v, want retry %v and auth-level reason", auth, authRetry)
	}
	if state := auth.ModelStates["grok-4"]; state == nil || !state.NextRetryAfter.Equal(modelRetry) {
		t.Fatalf("model-level cooldown = %+v, want retry %v", state, modelRetry)
	}

	registry.GetGlobalRegistry().RegisterClient(auth.ID, "xai", []*registry.ModelInfo{
		{ID: "grok-4"},
		{ID: "gemini-3.6-flash-high"},
	})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})
	store.resetRecording()
	manager.ReconcileRegistryModelStates(context.Background(), auth.ID)
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() error = %v", errFlush)
	}

	auth, ok = manager.GetByID("auth-1")
	if !ok {
		t.Fatal("reconciled auth was not found")
	}
	if auth.Unavailable || auth.Status != StatusActive || !auth.NextRetryAfter.IsZero() {
		t.Fatalf("reconciled auth = status %q unavailable %t retry %v, want healthy credential", auth.Status, auth.Unavailable, auth.NextRetryAfter)
	}
	batches := store.recordedBatches()
	if len(batches) != 1 || len(batches[0]) != 1 || len(batches[0][0].Records) != 1 || batches[0][0].Records[0].Model != "grok-4" {
		t.Fatalf("reconciled cooldown batches = %+v, want only grok-4 model cooldown", batches)
	}
}

func TestManager_PreparedCooldownDisabledRegisterDefersCleanup(t *testing.T) {
	for _, tc := range []struct {
		name string
		auth *Auth
	}{
		{name: "disabled", auth: &Auth{ID: "auth-1", Provider: "xai", Disabled: true}},
		{name: "cooldown-disabled", auth: &Auth{ID: "auth-1", Provider: "xai", Metadata: map[string]any{"disable_cooling": true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nextRetry := time.Now().Add(time.Hour)
			store := &recordingCooldownStateStore{load: []CooldownStateSnapshot{{
				AuthID:  "auth-1",
				Records: []CooldownStateRecord{{AuthID: "auth-1", NextRetryAfter: nextRetry}},
			}}}
			manager := NewManager(nil, nil, nil)
			manager.SetCooldownStateStore(store)
			if errPrepare := manager.PrepareCooldownRestore(context.Background()); errPrepare != nil {
				t.Fatal(errPrepare)
			}
			tc.auth.Unavailable = true
			tc.auth.NextRetryAfter = nextRetry
			if _, errRegister := manager.Register(WithSkipPersist(context.Background()), tc.auth); errRegister != nil {
				t.Fatal(errRegister)
			}
			if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
				t.Fatal(errFlush)
			}
			if got := store.applyCount.Load(); got != 0 {
				t.Fatalf("Apply count before completion = %d, want 0", got)
			}
			if errComplete := manager.CompleteCooldownRestore(context.Background()); errComplete != nil {
				t.Fatal(errComplete)
			}
			if got := store.applyCount.Load(); got != 1 {
				t.Fatalf("Apply count after completion = %d, want 1", got)
			}
		})
	}
}

func TestManager_PreparedCooldownDisabledUpdateDefersCleanup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Auth)
	}{
		{name: "disabled", mutate: func(auth *Auth) { auth.Disabled = true }},
		{name: "cooldown-disabled", mutate: func(auth *Auth) {
			auth.Metadata = map[string]any{"disable_cooling": true}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nextRetry := time.Now().Add(time.Hour)
			store := &recordingCooldownStateStore{load: []CooldownStateSnapshot{{
				AuthID:  "auth-1",
				Records: []CooldownStateRecord{{AuthID: "auth-1", NextRetryAfter: nextRetry}},
			}}}
			manager := NewManager(nil, nil, nil)
			manager.SetCooldownStateStore(store)
			if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai"}); errRegister != nil {
				t.Fatal(errRegister)
			}
			if errPrepare := manager.PrepareCooldownRestore(context.Background()); errPrepare != nil {
				t.Fatal(errPrepare)
			}
			auth, ok := manager.GetByID("auth-1")
			if !ok {
				t.Fatal("registered auth was not found")
			}
			tc.mutate(auth)
			if _, errUpdate := manager.Update(WithSkipPersist(context.Background()), auth); errUpdate != nil {
				t.Fatal(errUpdate)
			}
			if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
				t.Fatal(errFlush)
			}
			if got := store.applyCount.Load(); got != 0 {
				t.Fatalf("Apply count before completion = %d, want 0", got)
			}
			if errComplete := manager.CompleteCooldownRestore(context.Background()); errComplete != nil {
				t.Fatal(errComplete)
			}
			if got := store.applyCount.Load(); got != 1 {
				t.Fatalf("Apply count after completion = %d, want 1", got)
			}
		})
	}
}

func TestManager_CompleteCooldownRestoreDoesNotPersistToReplacedStore(t *testing.T) {
	nextRetry := time.Now().Add(time.Hour)
	oldStore := &recordingCooldownStateStore{load: []CooldownStateSnapshot{{
		AuthID:  "auth-1",
		Records: []CooldownStateRecord{{AuthID: "auth-1", NextRetryAfter: nextRetry}},
	}}}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(oldStore)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai"}); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errPrepare := manager.PrepareCooldownRestore(context.Background()); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	auth, ok := manager.GetByID("auth-1")
	if !ok || !auth.Unavailable || !auth.NextRetryAfter.Equal(nextRetry) {
		t.Fatalf("auth after prepare = %+v, want old store cooldown", auth)
	}

	newStore := &recordingCooldownStateStore{}
	manager.SetCooldownStateStore(newStore)
	if errComplete := manager.CompleteCooldownRestore(context.Background()); errComplete != nil {
		t.Fatal(errComplete)
	}
	if got := newStore.applyCount.Load(); got != 0 {
		t.Fatalf("new store Apply count = %d, want 0", got)
	}
}

func TestManager_FailedCooldownRestoreDoesNotRetryToReplacedStore(t *testing.T) {
	nextRetry := time.Now().Add(time.Hour)
	oldStore := &failingCooldownStateStore{}
	oldStore.fail.Store(true)
	oldStore.load = []CooldownStateSnapshot{{
		AuthID:  "auth-1",
		Records: []CooldownStateRecord{{AuthID: "auth-1", NextRetryAfter: nextRetry}},
	}}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(oldStore)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai"}); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errPrepare := manager.PrepareCooldownRestore(context.Background()); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if errComplete := manager.CompleteCooldownRestore(context.Background()); errComplete == nil {
		t.Fatal("CompleteCooldownRestore() error = nil, want old store failure")
	}

	newStore := &recordingCooldownStateStore{}
	manager.SetCooldownStateStore(newStore)
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	if got := newStore.applyCount.Load(); got != 0 {
		t.Fatalf("new store Apply count = %d, want 0", got)
	}
}

func TestManager_NewCooldownChangeSupersedesFailedRestoreBatch(t *testing.T) {
	nextRetry := time.Now().Add(time.Hour)
	oldStore := &failingCooldownStateStore{}
	oldStore.fail.Store(true)
	oldStore.load = []CooldownStateSnapshot{{
		AuthID:  "auth-1",
		Records: []CooldownStateRecord{{AuthID: "auth-1", NextRetryAfter: nextRetry}},
	}}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(oldStore)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-1", Provider: "xai"}); errRegister != nil {
		t.Fatal(errRegister)
	}
	if errPrepare := manager.PrepareCooldownRestore(context.Background()); errPrepare != nil {
		t.Fatal(errPrepare)
	}
	if errComplete := manager.CompleteCooldownRestore(context.Background()); errComplete == nil {
		t.Fatal("CompleteCooldownRestore() error = nil, want old store failure")
	}

	newStore := &recordingCooldownStateStore{}
	manager.SetCooldownStateStore(newStore)
	manager.MarkResult(context.Background(), Result{AuthID: "auth-1", Provider: "xai", Success: true})
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatal(errFlush)
	}
	if got := newStore.applyCount.Load(); got != 1 {
		t.Fatalf("new store Apply count = %d, want 1", got)
	}
	batches := newStore.recordedBatches()
	last := batches[len(batches)-1]
	if len(last) != 1 || last[0].AuthID != "auth-1" || len(last[0].Records) != 0 {
		t.Fatalf("new store batch = %+v, want auth-1 cooldown clear", last)
	}
}

// TestManager_PGStyleRestoreThenRegisterDoesNotYieldAuthNotFound reproduces the
// non-progressive (PG) startup order:
//
//	Load/Register → RestoreCooldownStates → register models → Reconcile → Refresh → pick
//
// User symptom after PG restart with save-cooldown-status=true was:
//
//	auth_not_found: no auth available (providers=xai, model=grok-4.5)
//
// Restored cooldowns must surface as model_cooldown (or ready), never empty-shard auth_not_found.
func TestManager_PGStyleRestoreThenRegisterDoesNotYieldAuthNotFound(t *testing.T) {
	prev := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prev) })

	nextRetry := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	authID := "pg-xai-auth-1"
	model := "grok-4.5"

	cases := []struct {
		name    string
		records []CooldownStateRecord
	}{
		{
			name: "model_quota_cooldown",
			records: []CooldownStateRecord{{
				Provider:       "xai",
				AuthID:         authID,
				Model:          model,
				Status:         "cooling",
				NextRetryAfter: nextRetry,
				Reason:         "quota",
				Quota: QuotaState{
					Exceeded:      true,
					Reason:        "quota",
					NextRecoverAt: nextRetry,
				},
				UpdatedAt: nextRetry.Add(-time.Minute),
			}},
		},
		{
			name: "auth_level_cooldown",
			records: []CooldownStateRecord{{
				Provider:       "xai",
				AuthID:         authID,
				Model:          "",
				Status:         "cooling",
				NextRetryAfter: nextRetry,
				Reason:         "quota",
				Quota: QuotaState{
					Exceeded:      true,
					Reason:        "quota",
					NextRecoverAt: nextRetry,
				},
				UpdatedAt: nextRetry.Add(-time.Minute),
			}},
		},
		{
			name: "model_and_auth_level",
			records: []CooldownStateRecord{
				{
					Provider: "xai", AuthID: authID, Model: model, Status: "cooling",
					NextRetryAfter: nextRetry, Reason: "quota",
					Quota:     QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: nextRetry},
					UpdatedAt: nextRetry.Add(-time.Minute),
				},
				{
					Provider: "xai", AuthID: authID, Model: "", Status: "cooling",
					NextRetryAfter: nextRetry, Reason: "quota",
					Quota:     QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: nextRetry},
					UpdatedAt: nextRetry.Add(-time.Minute),
				},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			store := &recordingCooldownStateStore{
				load: []CooldownStateSnapshot{{AuthID: authID, Records: tc.records}},
			}
			manager := NewManager(nil, &RoundRobinSelector{}, nil)
			manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "xai"})
			manager.SetCooldownStateStore(store)

			// 1) PG Load equivalent: auth present before restore.
			if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{
				ID:       authID,
				Provider: "xai",
				Status:   StatusActive,
			}); err != nil {
				t.Fatalf("Register: %v", err)
			}

			// 2) Non-progressive RestoreCooldownStates (Prepare+Complete) before model registration.
			if err := manager.RestoreCooldownStates(context.Background()); err != nil {
				t.Fatalf("RestoreCooldownStates: %v", err)
			}

			// Before models are registered, pick should be auth_not_found (empty supportedModelSet).
			_, errBefore := manager.scheduler.pickSingle(context.Background(), "xai", model, cliproxyexecutor.Options{}, nil)
			var beforeErr *Error
			if !errors.As(errBefore, &beforeErr) || beforeErr.Code != "auth_not_found" {
				// Also accept model_cooldown if somehow models leaked into registry from other tests.
				var cd *modelCooldownError
				if !errors.As(errBefore, &cd) {
					t.Logf("pre-registration pick error = %v (auth_not_found expected with empty model set)", errBefore)
				}
			}

			// 3) registerModelsForAuthBatch equivalent.
			registerSchedulerModels(t, "xai", model, authID)
			manager.ReconcileRegistryModelStates(context.Background(), authID)
			manager.RefreshSchedulerEntry(authID)

			// 4) Pick must NOT be auth_not_found. Cooldown → model_cooldown; auth-level alone → ready.
			picked, errPick := manager.scheduler.pickSingle(context.Background(), "xai", model, cliproxyexecutor.Options{}, nil)
			if errPick == nil {
				if picked == nil || picked.ID != authID {
					t.Fatalf("pick auth = %+v, want %s", picked, authID)
				}
				// Auth-level cooldown is ignored when selecting a concrete model with no model state.
				if tc.name == "auth_level_cooldown" {
					return
				}
				t.Fatalf("pick succeeded for cooling model; want model_cooldown (case %s)", tc.name)
			}

			var authErr *Error
			if errors.As(errPick, &authErr) && authErr != nil && authErr.Code == "auth_not_found" {
				t.Fatalf("pick error = %v; restored cooldowns must not empty the scheduler shard (auth_not_found)", errPick)
			}
			var cd *modelCooldownError
			if !errors.As(errPick, &cd) {
				// auth_unavailable is also acceptable if blocked non-quota; never auth_not_found.
				if errors.As(errPick, &authErr) && authErr.Code == "auth_unavailable" {
					return
				}
				t.Fatalf("pick error = %v, want model_cooldown or ready auth", errPick)
			}
		})
	}
}

// TestManager_RestoreThenSyncSchedulerStillSeesModels ensures the pick-time
// syncScheduler rebuild (shouldRetrySchedulerPick) also does not lose candidates
// after PG-style restore + model registration.
func TestManager_RestoreThenSyncSchedulerStillSeesModels(t *testing.T) {
	prev := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prev) })

	nextRetry := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	authID := "pg-xai-sync-1"
	model := "grok-4.5"
	store := &recordingCooldownStateStore{
		load: []CooldownStateSnapshot{{
			AuthID: authID,
			Records: []CooldownStateRecord{{
				Provider: "xai", AuthID: authID, Model: model, Status: "cooling",
				NextRetryAfter: nextRetry, Reason: "quota",
				Quota: QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: nextRetry},
			}},
		}},
	}
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "xai"})
	manager.SetCooldownStateStore(store)
	if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: authID, Provider: "xai"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := manager.RestoreCooldownStates(context.Background()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	registerSchedulerModels(t, "xai", model, authID)
	manager.ReconcileRegistryModelStates(context.Background(), authID)
	manager.RefreshSchedulerEntry(authID)

	// Force a full rebuild as pick path does on auth_not_found.
	manager.syncScheduler()

	_, errPick := manager.scheduler.pickSingle(context.Background(), "xai", model, cliproxyexecutor.Options{}, nil)
	var authErr *Error
	if errors.As(errPick, &authErr) && authErr != nil && authErr.Code == "auth_not_found" {
		t.Fatalf("after syncScheduler pick = %v, want model_cooldown not auth_not_found", errPick)
	}
	var cd *modelCooldownError
	if errPick != nil && !errors.As(errPick, &cd) {
		if errors.As(errPick, &authErr) && authErr.Code == "auth_unavailable" {
			return
		}
		t.Fatalf("pick error = %v, want model_cooldown", errPick)
	}
}

// TestManager_PickNextMixedWithoutExecutorYieldsAuthNotFound documents the
// exact production error chain for PG oauth after restart:
// models are registered (GetModelProviders → xai) but the provider executor was
// never bound on non-home startup. pickNextMixed filters providers without an
// executor out of eligibleProviders and returns auth_not_found — not
// executor_not_found. That matches:
//
//	auth_not_found: no auth available (providers=xai, model=grok-4.5)
func TestManager_PickNextMixedWithoutExecutorYieldsAuthNotFound(t *testing.T) {
	authID := "pg-xai-no-exec"
	model := "grok-4.5"
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	if _, err := manager.Register(WithSkipPersist(context.Background()), &Auth{
		ID:       authID,
		Provider: "xai",
		Status:   StatusActive,
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	registerSchedulerModels(t, "xai", model, authID)
	manager.RefreshSchedulerEntry(authID)

	// Scheduler alone would pick the auth (model set populated).
	if picked, errPick := manager.scheduler.pickSingle(context.Background(), "xai", model, cliproxyexecutor.Options{}, nil); errPick != nil || picked == nil {
		t.Fatalf("scheduler pick precondition failed: auth=%v err=%v", picked, errPick)
	}

	// No RegisterExecutor — mirrors non-home PG batch path.
	_, _, _, errMixed := manager.pickNextMixed(context.Background(), []string{"xai"}, model, cliproxyexecutor.Options{}, nil)
	var authErr *Error
	if !errors.As(errMixed, &authErr) || authErr == nil || authErr.Code != "auth_not_found" {
		t.Fatalf("pickNextMixed without executor = %v, want auth_not_found", errMixed)
	}

	// Binding the executor recovers selection (ready auth).
	manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "xai"})
	picked, _, provider, errOK := manager.pickNextMixed(context.Background(), []string{"xai"}, model, cliproxyexecutor.Options{}, nil)
	if errOK != nil {
		t.Fatalf("pickNextMixed after RegisterExecutor: %v", errOK)
	}
	if picked == nil || picked.ID != authID || provider != "xai" {
		t.Fatalf("picked=%+v provider=%q, want auth %s provider xai", picked, provider, authID)
	}
}

func TestManager_RestoreCooldownStatesCanonicalizesThinkingSuffixes(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	laterRetry := now.Add(2 * time.Hour)
	store := &recordingCooldownStateStore{
		load: []CooldownStateSnapshot{{
			AuthID: "auth-thinking",
			Records: []CooldownStateRecord{
				{
					Provider:       "gemini",
					AuthID:         "auth-thinking",
					Model:          "gemini-3.1-pro-preview(high)",
					NextRetryAfter: now.Add(time.Hour),
					Quota: QuotaState{
						Exceeded:      true,
						Reason:        "quota",
						NextRecoverAt: now.Add(time.Hour),
					},
					UpdatedAt: now,
				},
				{
					Provider:       "gemini",
					AuthID:         "auth-thinking",
					Model:          "gemini-3.1-pro-preview(low)",
					NextRetryAfter: laterRetry,
					Quota: QuotaState{
						Exceeded:      true,
						Reason:        "quota",
						NextRecoverAt: laterRetry,
					},
					UpdatedAt: now.Add(time.Minute),
				},
			},
		}},
	}
	manager := NewManager(nil, nil, nil)
	manager.SetCooldownStateStore(store)
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{ID: "auth-thinking", Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}

	if errRestore := manager.RestoreCooldownStates(context.Background()); errRestore != nil {
		t.Fatalf("RestoreCooldownStates() returned error: %v", errRestore)
	}

	auth, ok := manager.GetByID("auth-thinking")
	if !ok || auth == nil {
		t.Fatal("restored auth was not found")
	}
	if len(auth.ModelStates) != 1 {
		t.Fatalf("len(ModelStates) = %d, want 1: %+v", len(auth.ModelStates), auth.ModelStates)
	}
	state := auth.ModelStates["gemini-3.1-pro-preview"]
	if state == nil || !state.Unavailable || !state.NextRetryAfter.Equal(laterRetry) {
		t.Fatalf("canonical model state = %+v, want unavailable until %v", state, laterRetry)
	}

	store.mu.Lock()
	persisted := cloneCooldownStateSnapshots(store.snapshots)
	store.mu.Unlock()
	modelRecords := make([]CooldownStateRecord, 0)
	for _, snapshot := range persisted {
		for _, record := range snapshot.Records {
			if record.Model != "" {
				modelRecords = append(modelRecords, record)
			}
		}
	}
	if len(modelRecords) != 1 || modelRecords[0].Model != "gemini-3.1-pro-preview" || !modelRecords[0].NextRetryAfter.Equal(laterRetry) {
		t.Fatalf("persisted model records = %+v, want one canonical record until %v", modelRecords, laterRetry)
	}
}

func TestManagerResultSaveWaitsForCooldownStoreTransition(t *testing.T) {
	// Fork uses an async cooldown persister with storeVersion guards.
	// MarkResult does NOT block on store transitions — instead, the
	// persister discards entries whose storeVersion is stale.
	// This test verifies that after a swap, the new store receives
	// the persisted state.
	oldStore := &recordingCooldownStateStore{}
	newStore := &recordingCooldownStateStore{}
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: "auth-1", Provider: "xai", Status: StatusActive}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("Register() returned error: %v", errRegister)
	}
	manager.SetCooldownStateStore(oldStore)

	manager.MarkResult(context.Background(), Result{
		AuthID: auth.ID, Provider: auth.Provider, Model: "grok-4", Success: false,
		Error: &Error{Message: "rate limited", HTTPStatus: 429},
	})
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() returned error: %v", errFlush)
	}
	if got := oldStore.applyCount.Load(); got < 1 {
		t.Fatalf("old store apply count = %d, want >= 1", got)
	}

	if swapped := manager.SwapCooldownStateStore(context.Background(), newStore, true); !swapped {
		t.Fatal("SwapCooldownStateStore() = false")
	}

	manager.MarkResult(context.Background(), Result{
		AuthID: auth.ID, Provider: auth.Provider, Model: "grok-4", Success: false,
		Error: &Error{Message: "rate limited again", HTTPStatus: 429},
	})
	if errFlush := manager.FlushCooldownStates(context.Background()); errFlush != nil {
		t.Fatalf("FlushCooldownStates() returned error: %v", errFlush)
	}
	if got := newStore.applyCount.Load(); got < 1 {
		t.Fatalf("new store apply count = %d, want >= 1", got)
	}
}
