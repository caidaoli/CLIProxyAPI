package watcher

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/redisqueue"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher/diff"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher/synthesizer"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

func TestApplyAuthExcludedModelsMeta_APIKey(t *testing.T) {
	auth := &coreauth.Auth{Attributes: map[string]string{}}
	perKey := []string{" Model-1 ", "model-2"}

	synthesizer.ApplyAuthExcludedModelsMeta(auth, perKey, "apikey")

	expected := diff.ComputeExcludedModelsHash([]string{"model-1", "model-2"})
	if got := auth.Attributes["excluded_models_hash"]; got != expected {
		t.Fatalf("expected hash %s, got %s", expected, got)
	}
	if got := auth.Attributes["auth_kind"]; got != "apikey" {
		t.Fatalf("expected auth_kind=apikey, got %s", got)
	}
}

func TestApplyAuthExcludedModelsMeta_OAuthProvider(t *testing.T) {
	auth := &coreauth.Auth{
		Provider:   "TestProv",
		Attributes: map[string]string{},
	}

	synthesizer.ApplyAuthExcludedModelsMeta(auth, nil, "oauth")

	// Global oauth-excluded-models are resolved at model registration, not stored on the auth.
	for _, key := range []string{"excluded_models", "excluded_models_hash"} {
		if got, ok := auth.Attributes[key]; ok {
			t.Fatalf("expected %s to be absent, got %q", key, got)
		}
	}
	if got := auth.Attributes["auth_kind"]; got != "oauth" {
		t.Fatalf("expected auth_kind=oauth, got %s", got)
	}
}

func TestBuildAPIKeyClientsCounts(t *testing.T) {
	cfg := &config.Config{
		GeminiKey:       []config.GeminiKey{{APIKey: "g1"}, {APIKey: "g2"}},
		InteractionsKey: []config.GeminiKey{{APIKey: "i1"}},
		VertexCompatAPIKey: []config.VertexCompatKey{
			{APIKey: "v1"},
		},
		ClaudeKey: []config.ClaudeKey{{APIKey: "c1"}},
		CodexKey:  []config.CodexKey{{APIKey: "c1"}, {APIKey: "c2"}},
		XAIKey:    []config.XAIKey{{APIKey: "x1"}},
		MetaKey:   []config.MetaKey{{APIKey: "m1"}},
		OpenAICompatibility: []config.OpenAICompatibility{
			{APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "o1"}, {APIKey: "o2"}}},
		},
	}

	gemini, vertex, claude, codex, xai, meta, compat := BuildAPIKeyClients(cfg)
	if gemini != 3 || vertex != 1 || claude != 1 || codex != 2 || xai != 1 || meta != 1 || compat != 2 {
		t.Fatalf("unexpected counts: %d %d %d %d %d %d %d", gemini, vertex, claude, codex, xai, meta, compat)
	}
}

func TestNormalizeAuthStripsTemporalFields(t *testing.T) {
	now := time.Now()
	auth := &coreauth.Auth{
		CreatedAt:        now,
		UpdatedAt:        now,
		LastRefreshedAt:  now,
		NextRefreshAfter: now,
		Quota: coreauth.QuotaState{
			NextRecoverAt: now,
		},
		Runtime: map[string]any{"k": "v"},
	}

	normalized := normalizeAuth(auth)
	if !normalized.CreatedAt.IsZero() || !normalized.UpdatedAt.IsZero() || !normalized.LastRefreshedAt.IsZero() || !normalized.NextRefreshAfter.IsZero() {
		t.Fatal("expected time fields to be zeroed")
	}
	if normalized.Runtime != nil {
		t.Fatal("expected runtime to be nil")
	}
	if !normalized.Quota.NextRecoverAt.IsZero() {
		t.Fatal("expected quota.NextRecoverAt to be zeroed")
	}
}

func TestMatchProvider(t *testing.T) {
	if _, ok := matchProvider("OpenAI", []string{"openai", "claude"}); !ok {
		t.Fatal("expected match to succeed ignoring case")
	}
	if _, ok := matchProvider("missing", []string{"openai"}); ok {
		t.Fatal("expected match to fail for unknown provider")
	}
}

func TestSnapshotCoreAuths_ConfigAndAuthFiles(t *testing.T) {
	authDir := t.TempDir()
	metadata := map[string]any{
		"type":       "gemini",
		"email":      "user@example.com",
		"project_id": "proj-a, proj-b",
		"proxy_url":  "https://proxy",
	}
	authFile := filepath.Join(authDir, "gemini.json")
	data, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("failed to marshal metadata: %v", err)
	}
	if err = os.WriteFile(authFile, data, 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	cfg := &config.Config{
		AuthDir: authDir,
		GeminiKey: []config.GeminiKey{
			{
				APIKey:         "g-key",
				BaseURL:        "https://gemini",
				ExcludedModels: []string{"Model-A", "model-b"},
				Headers:        map[string]string{"X-Req": "1"},
			},
		},
	}

	w := &Watcher{authDir: authDir}
	w.SetConfig(cfg)

	auths := w.SnapshotCoreAuths()
	if len(auths) != 1 {
		t.Fatalf("expected 1 config auth entry, got %d", len(auths))
	}

	var geminiAPIKeyAuth *coreauth.Auth
	for _, a := range auths {
		if a.Provider == "gemini" && a.Attributes["api_key"] == "g-key" {
			geminiAPIKeyAuth = a
		}
	}
	if geminiAPIKeyAuth == nil {
		t.Fatal("expected synthesized Gemini API key auth")
	}
	expectedAPIKeyHash := diff.ComputeExcludedModelsHash([]string{"Model-A", "model-b"})
	if geminiAPIKeyAuth.Attributes["excluded_models_hash"] != expectedAPIKeyHash {
		t.Fatalf("expected API key excluded hash %s, got %s", expectedAPIKeyHash, geminiAPIKeyAuth.Attributes["excluded_models_hash"])
	}
	if geminiAPIKeyAuth.Attributes["auth_kind"] != "apikey" {
		t.Fatalf("expected auth_kind=apikey, got %s", geminiAPIKeyAuth.Attributes["auth_kind"])
	}
}

func TestReloadConfigIfChanged_TriggersOnChangeAndSkipsUnchanged(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}

	configPath := filepath.Join(tmpDir, "config.yaml")
	writeConfig := func(port int, allowRemote bool) {
		cfg := &config.Config{
			Port:               port,
			AuthDir:            authDir,
			CredentialInFlight: config.DefaultCredentialInFlightConfig(),
			RemoteManagement: config.RemoteManagement{
				AllowRemote: allowRemote,
			},
		}
		data, err := yaml.Marshal(cfg)
		if err != nil {
			t.Fatalf("failed to marshal config: %v", err)
		}
		if err = os.WriteFile(configPath, data, 0o644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}
	}

	writeConfig(8080, false)

	reloads := 0
	w := &Watcher{
		configPath:     configPath,
		authDir:        authDir,
		reloadCallback: func(*config.Config) { reloads++ },
	}

	w.reloadConfigIfChanged()
	if reloads != 1 {
		t.Fatalf("expected first reload to trigger callback once, got %d", reloads)
	}

	// Same content should be skipped by hash check.
	w.reloadConfigIfChanged()
	if reloads != 1 {
		t.Fatalf("expected unchanged config to be skipped, callback count %d", reloads)
	}

	writeConfig(9090, true)
	w.reloadConfigIfChanged()
	if reloads != 2 {
		t.Fatalf("expected changed config to trigger reload, callback count %d", reloads)
	}
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if w.config == nil || w.config.Port != 9090 || !w.config.RemoteManagement.AllowRemote {
		t.Fatalf("expected config to be updated after reload, got %+v", w.config)
	}
}

func TestStartAndStopSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir), 0o644); err != nil {
		t.Fatalf("failed to create config file: %v", err)
	}

	var reloads int32
	w, err := NewWatcher(configPath, authDir, func(*config.Config) {
		atomic.AddInt32(&reloads, 1)
	})
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("expected Start to succeed: %v", err)
	}
	cancel()
	if err := w.Stop(); err != nil {
		t.Fatalf("expected Stop to succeed: %v", err)
	}
	if got := atomic.LoadInt32(&reloads); got != 0 {
		t.Fatalf("expected no reload callback, got %d", got)
	}
}

func TestStartFailsWhenConfigMissing(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "missing-config.yaml")

	w, err := NewWatcher(configPath, authDir, nil)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}
	defer w.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err == nil {
		t.Fatal("expected Start to fail for missing config file")
	}
}

func TestDispatchRuntimeAuthUpdateEnqueuesAndUpdatesState(t *testing.T) {
	queue := make(chan AuthUpdateBatch, 4)
	w := &Watcher{}
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()

	auth := &coreauth.Auth{ID: "auth-1", Provider: "test"}
	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionAdd, Auth: auth}); !ok {
		t.Fatal("expected DispatchRuntimeAuthUpdate to enqueue")
	}

	select {
	case batch := <-queue:
		if len(batch.Updates) != 1 {
			t.Fatalf("unexpected batch: %+v", batch)
		}
		update := batch.Updates[0]
		if update.Action != AuthUpdateActionAdd || update.Auth.ID != "auth-1" {
			t.Fatalf("unexpected update: %+v", update)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for auth update")
	}

	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionDelete, ID: "auth-1"}); !ok {
		t.Fatal("expected delete update to enqueue")
	}
	select {
	case batch := <-queue:
		if len(batch.Updates) != 1 {
			t.Fatalf("unexpected batch: %+v", batch)
		}
		update := batch.Updates[0]
		if update.Action != AuthUpdateActionDelete || update.ID != "auth-1" {
			t.Fatalf("unexpected delete update: %+v", update)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delete update")
	}
	w.clientsMutex.RLock()
	if _, exists := w.runtimeAuths["auth-1"]; exists {
		w.clientsMutex.RUnlock()
		t.Fatal("expected runtime auth to be cleared after delete")
	}
	w.clientsMutex.RUnlock()
}

func TestAddOrUpdateClientSkipsUnchanged(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo"}`), 0o644); err != nil {
		t.Fatalf("failed to create auth file: %v", err)
	}
	data, _ := os.ReadFile(authFile)
	sum := sha256.Sum256(data)

	var reloads int32
	w := &Watcher{
		authDir:        tmpDir,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) {
			atomic.AddInt32(&reloads, 1)
		},
	}
	w.SetConfig(&config.Config{AuthDir: tmpDir})
	// Use normalizeAuthPath to match how addOrUpdateClient stores the key
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = hexString(sum[:])

	w.addOrUpdateClient(authFile)
	if got := atomic.LoadInt32(&reloads); got != 0 {
		t.Fatalf("expected no reload for unchanged file, got %d", got)
	}
}

func TestAddOrUpdateClientTriggersReloadAndHash(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo","api_key":"k"}`), 0o644); err != nil {
		t.Fatalf("failed to create auth file: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        tmpDir,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) {
			atomic.AddInt32(&reloads, 1)
		},
	}
	w.SetConfig(&config.Config{AuthDir: tmpDir})

	w.addOrUpdateClient(authFile)

	if got := atomic.LoadInt32(&reloads); got != 0 {
		t.Fatalf("expected no reload callback for auth update, got %d", got)
	}
	// Use normalizeAuthPath to match how addOrUpdateClient stores the key
	normalized := w.normalizeAuthPath(authFile)
	if _, ok := w.lastAuthHashes[normalized]; !ok {
		t.Fatalf("expected hash to be stored for %s", normalized)
	}
}

func TestRemoveClientRemovesHash(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	var reloads int32

	w := &Watcher{
		authDir:        tmpDir,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) {
			atomic.AddInt32(&reloads, 1)
		},
	}
	w.SetConfig(&config.Config{AuthDir: tmpDir})
	// Use normalizeAuthPath to set up the hash with the correct key format
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = "hash"

	w.removeClient(authFile)
	if _, ok := w.lastAuthHashes[w.normalizeAuthPath(authFile)]; ok {
		t.Fatal("expected hash to be removed after deletion")
	}
	if got := atomic.LoadInt32(&reloads); got != 0 {
		t.Fatalf("expected no reload callback for auth removal, got %d", got)
	}
}

func TestAuthFileClientChangesNotifyUsageSubscribersToRefresh(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo","api_key":"k"}`), 0o644); err != nil {
		t.Fatalf("failed to create auth file: %v", err)
	}

	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)
	t.Cleanup(func() { redisqueue.SetEnabled(false) })

	subscriber, unsubscribe := redisqueue.SubscribeUsage()
	defer unsubscribe()
	requireWatcherUsagePayload(t, subscriber, `{"support_refresh":true}`)

	w := &Watcher{
		authDir:        tmpDir,
		lastAuthHashes: make(map[string]string),
	}
	w.SetConfig(&config.Config{AuthDir: tmpDir})

	w.addOrUpdateClient(authFile)
	requireWatcherUsagePayload(t, subscriber, `{"refresh":true}`)

	w.removeClient(authFile)
	requireWatcherUsagePayload(t, subscriber, `{"refresh":true}`)
}

func TestAuthFileEventsDoNotInvokeSnapshotCoreAuths(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"codex","email":"u@example.com"}`), 0o644); err != nil {
		t.Fatalf("failed to create auth file: %v", err)
	}

	origSnapshot := snapshotCoreAuthsFunc
	var snapshotCalls int32
	snapshotCoreAuthsFunc = func(cfg *config.Config, authDir string, parser synthesizer.PluginAuthParser) ([]*coreauth.Auth, map[string]struct{}) {
		atomic.AddInt32(&snapshotCalls, 1)
		return origSnapshot(cfg, authDir, parser)
	}
	defer func() { snapshotCoreAuthsFunc = origSnapshot }()

	w := &Watcher{
		authDir:          tmpDir,
		lastAuthHashes:   make(map[string]string),
		lastAuthContents: make(map[string]*coreauth.Auth),
		fileAuthsByPath:  make(map[string]map[string]*coreauth.Auth),
	}
	w.SetConfig(&config.Config{AuthDir: tmpDir})

	w.addOrUpdateClient(authFile)
	w.removeClient(authFile)

	if got := atomic.LoadInt32(&snapshotCalls); got != 0 {
		t.Fatalf("expected auth file events to avoid full snapshot, got %d calls", got)
	}
}

func TestAuthSliceToMap(t *testing.T) {
	t.Parallel()

	valid1 := &coreauth.Auth{ID: "a"}
	valid2 := &coreauth.Auth{ID: "b"}
	dupOld := &coreauth.Auth{ID: "dup", Label: "old"}
	dupNew := &coreauth.Auth{ID: "dup", Label: "new"}
	empty := &coreauth.Auth{ID: "  "}

	tests := []struct {
		name string
		in   []*coreauth.Auth
		want map[string]*coreauth.Auth
	}{
		{
			name: "nil input",
			in:   nil,
			want: map[string]*coreauth.Auth{},
		},
		{
			name: "empty input",
			in:   []*coreauth.Auth{},
			want: map[string]*coreauth.Auth{},
		},
		{
			name: "filters invalid auths",
			in:   []*coreauth.Auth{nil, empty},
			want: map[string]*coreauth.Auth{},
		},
		{
			name: "keeps valid auths",
			in:   []*coreauth.Auth{valid1, nil, valid2},
			want: map[string]*coreauth.Auth{"a": valid1, "b": valid2},
		},
		{
			name: "last duplicate wins",
			in:   []*coreauth.Auth{dupOld, dupNew},
			want: map[string]*coreauth.Auth{"dup": dupNew},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := authSliceToMap(tc.in)
			if len(tc.want) == 0 {
				if got == nil {
					t.Fatal("expected empty map, got nil")
				}
				if len(got) != 0 {
					t.Fatalf("expected empty map, got %#v", got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("unexpected map length: got %d, want %d", len(got), len(tc.want))
			}
			for id, wantAuth := range tc.want {
				gotAuth, ok := got[id]
				if !ok {
					t.Fatalf("missing id %q in result map", id)
				}
				if !authEqual(gotAuth, wantAuth) {
					t.Fatalf("unexpected auth for id %q: got %#v, want %#v", id, gotAuth, wantAuth)
				}
			}
		})
	}
}

func TestTriggerServerUpdateCancelsPendingTimerOnImmediate(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{AuthDir: tmpDir}

	var reloads int32
	w := &Watcher{
		reloadCallback: func(*config.Config) {
			atomic.AddInt32(&reloads, 1)
		},
	}
	w.SetConfig(cfg)

	w.serverUpdateMu.Lock()
	w.serverUpdateLast = time.Now().Add(-(serverUpdateDebounce - 100*time.Millisecond))
	w.serverUpdateMu.Unlock()
	w.triggerServerUpdate(cfg)

	if got := atomic.LoadInt32(&reloads); got != 0 {
		t.Fatalf("expected no immediate reload, got %d", got)
	}

	w.serverUpdateMu.Lock()
	if !w.serverUpdatePend || w.serverUpdateTimer == nil {
		w.serverUpdateMu.Unlock()
		t.Fatal("expected a pending server update timer")
	}
	w.serverUpdateLast = time.Now().Add(-(serverUpdateDebounce + 10*time.Millisecond))
	w.serverUpdateMu.Unlock()

	w.triggerServerUpdate(cfg)
	if got := atomic.LoadInt32(&reloads); got != 1 {
		t.Fatalf("expected immediate reload once, got %d", got)
	}

	time.Sleep(250 * time.Millisecond)
	if got := atomic.LoadInt32(&reloads); got != 1 {
		t.Fatalf("expected pending timer to be cancelled, got %d reloads", got)
	}
}

func TestShouldDebounceRemove(t *testing.T) {
	w := &Watcher{}
	path := filepath.Clean("test.json")

	if w.shouldDebounceRemove(path, time.Now()) {
		t.Fatal("first call should not debounce")
	}
	if !w.shouldDebounceRemove(path, time.Now()) {
		t.Fatal("second call within window should debounce")
	}

	w.clientsMutex.Lock()
	w.lastRemoveTimes = map[string]time.Time{path: time.Now().Add(-2 * authRemoveDebounceWindow)}
	w.clientsMutex.Unlock()

	if w.shouldDebounceRemove(path, time.Now()) {
		t.Fatal("call after window should not debounce")
	}
}

func TestAuthFileUnchangedUsesHash(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	content := []byte(`{"type":"demo"}`)
	if err := os.WriteFile(authFile, content, 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	w := &Watcher{lastAuthHashes: make(map[string]string)}
	unchanged, err := w.authFileUnchanged(authFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if unchanged {
		t.Fatal("expected first check to report changed")
	}

	sum := sha256.Sum256(content)
	// Use normalizeAuthPath to match how authFileUnchanged looks up the key
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = hexString(sum[:])

	unchanged, err = w.authFileUnchanged(authFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !unchanged {
		t.Fatal("expected hash match to report unchanged")
	}
}

func TestAuthFileUnchangedEmptyAndMissing(t *testing.T) {
	tmpDir := t.TempDir()
	emptyFile := filepath.Join(tmpDir, "empty.json")
	if err := os.WriteFile(emptyFile, []byte(""), 0o644); err != nil {
		t.Fatalf("failed to write empty auth file: %v", err)
	}

	w := &Watcher{lastAuthHashes: make(map[string]string)}
	unchanged, err := w.authFileUnchanged(emptyFile)
	if err != nil {
		t.Fatalf("unexpected error for empty file: %v", err)
	}
	if unchanged {
		t.Fatal("expected empty file to be treated as changed")
	}

	_, err = w.authFileUnchanged(filepath.Join(tmpDir, "missing.json"))
	if err == nil {
		t.Fatal("expected error for missing auth file")
	}
}

func TestReloadClientsCachesAuthHashes(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "one.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	w := &Watcher{
		authDir:                tmpDir,
		config:                 &config.Config{AuthDir: tmpDir},
		fileAuthLoadingEnabled: true,
	}

	w.reloadClients(true, nil, false)

	eventually(t, time.Second, func() bool {
		w.clientsMutex.RLock()
		defer w.clientsMutex.RUnlock()
		return len(w.lastAuthHashes) == 1
	})
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if len(w.lastAuthHashes) != 1 {
		t.Fatalf("expected hash cache for one auth file, got %d", len(w.lastAuthHashes))
	}
}

func TestReloadClientsLogsConfigDiffs(t *testing.T) {
	tmpDir := t.TempDir()
	oldCfg := &config.Config{AuthDir: tmpDir, Port: 1, Debug: false}
	newCfg := &config.Config{AuthDir: tmpDir, Port: 2, Debug: true}

	w := &Watcher{
		authDir: tmpDir,
		config:  oldCfg,
	}
	w.SetConfig(oldCfg)
	w.oldConfigYaml, _ = yaml.Marshal(oldCfg)

	w.clientsMutex.Lock()
	w.config = newCfg
	w.clientsMutex.Unlock()

	w.reloadClients(false, nil, false)
}

func TestReloadClientsHandlesNilConfig(t *testing.T) {
	w := &Watcher{}
	w.reloadClients(true, nil, false)
}

func TestReloadClientsNotifiesUsageSubscribersToRefresh(t *testing.T) {
	tmp := t.TempDir()
	redisqueue.SetEnabled(false)
	redisqueue.SetEnabled(true)
	t.Cleanup(func() { redisqueue.SetEnabled(false) })

	subscriber, unsubscribe := redisqueue.SubscribeUsage()
	defer unsubscribe()
	requireWatcherUsagePayload(t, subscriber, `{"support_refresh":true}`)

	w := &Watcher{
		authDir: tmp,
		config:  &config.Config{AuthDir: tmp},
	}
	w.reloadClients(false, nil, false)

	requireWatcherUsagePayload(t, subscriber, `{"refresh":true}`)
}

func TestReloadClientsFiltersProvidersWithNilCurrentAuths(t *testing.T) {
	tmp := t.TempDir()
	w := &Watcher{
		authDir: tmp,
		config:  &config.Config{AuthDir: tmp},
	}
	w.reloadClients(false, []string{"match"}, false)
	if w.currentAuths != nil && len(w.currentAuths) != 0 {
		t.Fatalf("expected currentAuths to be nil or empty, got %d", len(w.currentAuths))
	}
}

func requireWatcherUsagePayload(t *testing.T, subscriber <-chan []byte, want string) {
	t.Helper()

	select {
	case got, ok := <-subscriber:
		if !ok {
			t.Fatalf("subscriber closed before receiving %q", want)
		}
		if string(got) != want {
			t.Fatalf("subscriber payload = %q, want %q", string(got), want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for subscriber payload %q", want)
	}
}

func TestSetAuthUpdateQueueNilResetsDispatch(t *testing.T) {
	w := &Watcher{}
	queue := make(chan AuthUpdateBatch, 1)
	w.SetAuthUpdateQueue(queue)
	if w.dispatchCond == nil || w.dispatchCancel == nil {
		t.Fatal("expected dispatch to be initialized")
	}
	w.SetAuthUpdateQueue(nil)
	if w.dispatchCancel != nil {
		t.Fatal("expected dispatch cancel to be cleared when queue nil")
	}
}

func TestPersistAsyncEarlyReturns(t *testing.T) {
	var nilWatcher *Watcher
	nilWatcher.persistConfigAsync()
	nilWatcher.persistAuthAsync("msg", "a")

	w := &Watcher{}
	w.persistConfigAsync()
	w.persistAuthAsync("msg", "   ", "")
}

type errorPersister struct {
	configCalls int32
	authCalls   int32
}

func (p *errorPersister) PersistConfig(context.Context) error {
	atomic.AddInt32(&p.configCalls, 1)
	return fmt.Errorf("persist config error")
}

func (p *errorPersister) PersistAuthFiles(context.Context, string, ...string) error {
	atomic.AddInt32(&p.authCalls, 1)
	return fmt.Errorf("persist auth error")
}

func TestPersistAsyncErrorPaths(t *testing.T) {
	p := &errorPersister{}
	w := &Watcher{storePersister: p}
	w.persistConfigAsync()
	w.persistAuthAsync("msg", "a")
	time.Sleep(30 * time.Millisecond)
	if atomic.LoadInt32(&p.configCalls) != 1 {
		t.Fatalf("expected PersistConfig to be called once, got %d", p.configCalls)
	}
	if atomic.LoadInt32(&p.authCalls) != 1 {
		t.Fatalf("expected PersistAuthFiles to be called once, got %d", p.authCalls)
	}
}

func TestStopConfigReloadTimerSafeWhenNil(t *testing.T) {
	w := &Watcher{}
	w.stopConfigReloadTimer()
	w.configReloadMu.Lock()
	w.configReloadTimer = time.AfterFunc(10*time.Millisecond, func() {})
	w.configReloadMu.Unlock()
	time.Sleep(1 * time.Millisecond)
	w.stopConfigReloadTimer()
}

func TestHandleEventRemovesAuthFile(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "remove.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	if err := os.Remove(authFile); err != nil {
		t.Fatalf("failed to remove auth file pre-check: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        tmpDir,
		config:         &config.Config{AuthDir: tmpDir},
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) {
			atomic.AddInt32(&reloads, 1)
		},
	}
	// Use normalizeAuthPath to set up the hash with the correct key format
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = "hash"

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Remove})

	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected no reload callback for auth removal, got %d", reloads)
	}
	if _, ok := w.lastAuthHashes[w.normalizeAuthPath(authFile)]; ok {
		t.Fatal("expected hash entry to be removed")
	}
}

func TestDispatchAuthUpdatesFlushesQueue(t *testing.T) {
	queue := make(chan AuthUpdateBatch, 4)
	w := &Watcher{}
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()

	w.dispatchAuthUpdates([]AuthUpdate{
		{Action: AuthUpdateActionAdd, ID: "a"},
		{Action: AuthUpdateActionModify, ID: "b"},
	})

	var got []AuthUpdate
	select {
	case batch := <-queue:
		got = batch.Updates
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update batch")
	}
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("unexpected updates order/content: %+v", got)
	}
}

func TestDispatchLoopExitsOnContextDoneWhileSending(t *testing.T) {
	queue := make(chan AuthUpdateBatch) // unbuffered to block sends
	w := &Watcher{
		authQueue: queue,
		pendingUpdates: map[string]AuthUpdate{
			"k": {Action: AuthUpdateActionAdd, ID: "k"},
		},
		pendingOrder: []string{"k"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.dispatchLoop(ctx)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("expected dispatchLoop to exit after ctx canceled while blocked on send")
	}
}

func TestProcessEventsHandlesEventErrorAndChannelClose(t *testing.T) {
	w := &Watcher{
		watcher: &fsnotify.Watcher{
			Events: make(chan fsnotify.Event, 2),
			Errors: make(chan error, 2),
		},
		configPath: "config.yaml",
		authDir:    "auth",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		w.processEvents(ctx)
		close(done)
	}()

	w.watcher.Events <- fsnotify.Event{Name: "unrelated.txt", Op: fsnotify.Write}
	w.watcher.Errors <- fmt.Errorf("watcher error")

	time.Sleep(20 * time.Millisecond)
	close(w.watcher.Events)
	close(w.watcher.Errors)

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("processEvents did not exit after channels closed")
	}
}

func TestProcessEventsReturnsWhenErrorsChannelClosed(t *testing.T) {
	w := &Watcher{
		watcher: &fsnotify.Watcher{
			Events: nil,
			Errors: make(chan error),
		},
	}

	close(w.watcher.Errors)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		w.processEvents(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("processEvents did not exit after errors channel closed")
	}
}

func TestHandleEventIgnoresUnrelatedFiles(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.handleEvent(fsnotify.Event{Name: filepath.Join(tmpDir, "note.txt"), Op: fsnotify.Write})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected no reloads for unrelated file, got %d", reloads)
	}
}

func TestHandleEventConfigChangeSchedulesReload(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.handleEvent(fsnotify.Event{Name: configPath, Op: fsnotify.Write})

	time.Sleep(400 * time.Millisecond)
	if atomic.LoadInt32(&reloads) != 1 {
		t.Fatalf("expected config change to trigger reload once, got %d", reloads)
	}
}

func TestHandleEventAuthWriteTriggersUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "a.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Write})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected auth write to avoid global reload callback, got %d", reloads)
	}
}

func TestHandleEventRemoveDebounceSkips(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "remove.json")

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		lastRemoveTimes: map[string]time.Time{
			filepath.Clean(authFile): time.Now(),
		},
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Remove})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected remove to be debounced, got %d", reloads)
	}
}

func TestHandleEventAtomicReplaceUnchangedSkips(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "same.json")
	content := []byte(`{"type":"demo"}`)
	if err := os.WriteFile(authFile, content, 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	sum := sha256.Sum256(content)

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = hexString(sum[:])

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Rename})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected unchanged atomic replace to be skipped, got %d", reloads)
	}
}

func TestHandleEventAtomicReplaceChangedTriggersUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "change.json")
	oldContent := []byte(`{"type":"demo","v":1}`)
	newContent := []byte(`{"type":"demo","v":2}`)
	if err := os.WriteFile(authFile, newContent, 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	oldSum := sha256.Sum256(oldContent)

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = hexString(oldSum[:])

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Rename})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected changed atomic replace to avoid global reload, got %d", reloads)
	}
}

func TestHandleEventRemoveUnknownFileIgnored(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "unknown.json")

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Remove})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected unknown remove to be ignored, got %d", reloads)
	}
}

func TestHandleEventAtomicReplaceDelayedStatPreservesClient(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "token.json")
	oldContent := []byte(`{"type":"demo","v":1}`)
	newContent := []byte(`{"type":"demo","v":2}`)
	oldSum := sha256.Sum256(oldContent)

	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
	}
	w.SetConfig(&config.Config{AuthDir: authDir})
	normalized := w.normalizeAuthPath(authFile)
	w.lastAuthHashes[normalized] = hexString(oldSum[:])

	mockAuth := &coreauth.Auth{
		ID:       "token.json",
		FileName: "token.json",
		Provider: "demo",
		Status:   coreauth.StatusActive,
	}
	w.currentAuths = map[string]*coreauth.Auth{"token.json": mockAuth}
	w.fileAuthsByPath = map[string]map[string]*coreauth.Auth{
		normalized: {"token.json": mockAuth},
	}

	// Delay writing the new file until 60ms (after the initial 50ms check, within retry window)
	errChan := make(chan error, 1)
	go func() {
		time.Sleep(60 * time.Millisecond)
		errChan <- os.WriteFile(authFile, newContent, 0o644)
	}()

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Rename})
	if errWrite := <-errChan; errWrite != nil {
		t.Fatalf("async WriteFile failed: %v", errWrite)
	}

	// If the retry loop settles and detects the replaced file, it should NOT have been removed.
	w.clientsMutex.RLock()
	_, stillKnown := w.lastAuthHashes[normalized]
	_, stillInCurrent := w.currentAuths["token.json"]
	pathAuths := w.fileAuthsByPath[normalized]
	w.clientsMutex.RUnlock()

	if !stillKnown {
		t.Fatal("expected known auth file to survive atomic replace with delayed stat, but hash was removed")
	}
	if !stillInCurrent {
		t.Fatal("expected client to remain registered in currentAuths, but was removed")
	}
	if len(pathAuths) == 0 {
		t.Fatal("expected client to remain registered in fileAuthsByPath, but was removed")
	}
}

func TestHandleEventRemoveKnownFileDeletes(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "known.json")

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = "hash"

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Remove})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected known remove to avoid global reload, got %d", reloads)
	}
	if _, ok := w.lastAuthHashes[w.normalizeAuthPath(authFile)]; ok {
		t.Fatal("expected known auth hash to be deleted")
	}
}

func TestNormalizeAuthPathAndDebounceCleanup(t *testing.T) {
	w := &Watcher{}
	if got := w.normalizeAuthPath("   "); got != "" {
		t.Fatalf("expected empty normalize result, got %q", got)
	}
	if got := w.normalizeAuthPath("  a/../b  "); got != filepath.Clean("a/../b") {
		t.Fatalf("unexpected normalize result: %q", got)
	}

	w.clientsMutex.Lock()
	w.lastRemoveTimes = make(map[string]time.Time, 140)
	old := time.Now().Add(-3 * authRemoveDebounceWindow)
	for i := 0; i < 129; i++ {
		w.lastRemoveTimes[fmt.Sprintf("old-%d", i)] = old
	}
	w.clientsMutex.Unlock()

	w.shouldDebounceRemove("new-path", time.Now())

	w.clientsMutex.Lock()
	gotLen := len(w.lastRemoveTimes)
	w.clientsMutex.Unlock()
	if gotLen >= 129 {
		t.Fatalf("expected debounce cleanup to shrink map, got %d", gotLen)
	}
}

func TestRefreshAuthStateDispatchesRuntimeAuths(t *testing.T) {
	queue := make(chan AuthUpdateBatch, 8)
	w := &Watcher{
		authDir:        t.TempDir(),
		lastAuthHashes: make(map[string]string),
	}
	w.SetConfig(&config.Config{AuthDir: w.authDir})
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()

	w.clientsMutex.Lock()
	w.runtimeAuths = map[string]*coreauth.Auth{
		"nil": nil,
		"r1":  {ID: "r1", Provider: "runtime"},
	}
	w.clientsMutex.Unlock()

	w.refreshAuthState(false)

	select {
	case batch := <-queue:
		if len(batch.Updates) != 1 {
			t.Fatalf("unexpected batch: %+v", batch)
		}
		u := batch.Updates[0]
		if u.Action != AuthUpdateActionAdd || u.ID != "r1" {
			t.Fatalf("unexpected auth update: %+v", u)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runtime auth update")
	}
}

func TestAddOrUpdateClientEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := tmpDir
	authFile := filepath.Join(tmpDir, "edge.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	emptyFile := filepath.Join(tmpDir, "empty.json")
	if err := os.WriteFile(emptyFile, []byte(""), 0o644); err != nil {
		t.Fatalf("failed to write empty auth file: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}

	w.addOrUpdateClient(filepath.Join(tmpDir, "missing.json"))
	w.addOrUpdateClient(emptyFile)
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected no reloads for missing/empty file, got %d", reloads)
	}

	w.addOrUpdateClient(authFile) // config nil -> should not panic or update
	if len(w.lastAuthHashes) != 0 {
		t.Fatalf("expected no hash entries without config, got %d", len(w.lastAuthHashes))
	}
}

func TestReloadConfigIfChangedHandlesMissingAndEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}

	w := &Watcher{
		configPath: filepath.Join(tmpDir, "missing.yaml"),
		authDir:    authDir,
	}
	w.reloadConfigIfChanged() // missing file -> log + return

	emptyPath := filepath.Join(tmpDir, "empty.yaml")
	if err := os.WriteFile(emptyPath, []byte(""), 0o644); err != nil {
		t.Fatalf("failed to write empty config: %v", err)
	}
	w.configPath = emptyPath
	w.reloadConfigIfChanged() // empty file -> early return
}

func TestReloadConfigUsesMirroredAuthDir(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}

	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+filepath.Join(tmpDir, "other")+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	w := &Watcher{
		configPath:             configPath,
		authDir:                authDir,
		mirroredAuthDir:        authDir,
		lastAuthHashes:         make(map[string]string),
		fileAuthLoadingEnabled: true,
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	if ok := w.reloadConfig(); !ok {
		t.Fatal("expected reloadConfig to succeed")
	}
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if w.config == nil || w.config.AuthDir != authDir {
		t.Fatalf("expected AuthDir to be overridden by mirroredAuthDir %s, got %+v", authDir, w.config)
	}
}

func TestReloadConfigFiltersAffectedOAuthProviders(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")

	// Ensure SnapshotCoreAuths yields a provider that is NOT affected, so we can assert it survives.
	if err := os.WriteFile(filepath.Join(authDir, "provider-b.json"), []byte(`{"type":"provider-b","email":"b@example.com"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	oldCfg := &config.Config{
		AuthDir:            authDir,
		CredentialInFlight: config.DefaultCredentialInFlightConfig(),
		OAuthExcludedModels: map[string][]string{
			"provider-a": {"m1"},
		},
	}
	newCfg := &config.Config{
		AuthDir:            authDir,
		CredentialInFlight: config.DefaultCredentialInFlightConfig(),
		OAuthExcludedModels: map[string][]string{
			"provider-a": {"m2"},
		},
	}
	data, err := yaml.Marshal(newCfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}
	if err = os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	w := &Watcher{
		configPath:             configPath,
		authDir:                authDir,
		lastAuthHashes:         make(map[string]string),
		fileAuthLoadingEnabled: true,
		currentAuths: map[string]*coreauth.Auth{
			"a": {ID: "a", Provider: "provider-a"},
		},
	}
	w.SetConfig(oldCfg)

	if ok := w.reloadConfig(); !ok {
		t.Fatal("expected reloadConfig to succeed")
	}
	eventually(t, time.Second, func() bool {
		w.clientsMutex.RLock()
		defer w.clientsMutex.RUnlock()
		for _, auth := range w.currentAuths {
			if auth != nil && auth.Provider == "provider-b" {
				return true
			}
		}
		return false
	})

	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	for _, auth := range w.currentAuths {
		if auth != nil && auth.Provider == "provider-a" {
			t.Fatal("expected affected provider auth to be filtered")
		}
	}
	foundB := false
	for _, auth := range w.currentAuths {
		if auth != nil && auth.Provider == "provider-b" {
			foundB = true
			break
		}
	}
	if !foundB {
		t.Fatal("expected unaffected provider auth to remain")
	}
}

func TestReloadConfigRedispatchesUnchangedAuthForGlobalOAuthExclusionChange(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(authDir, "claude.json"), []byte(`{"type":"claude","email":"a@example.com"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	newConfig := func(excluded string) *config.Config {
		return &config.Config{
			AuthDir:             authDir,
			CredentialInFlight:  config.DefaultCredentialInFlightConfig(),
			OAuthExcludedModels: map[string][]string{"claude": {excluded}},
		}
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	data, errMarshal := yaml.Marshal(newConfig("m2"))
	if errMarshal != nil {
		t.Fatalf("failed to marshal config: %v", errMarshal)
	}
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	queue := make(chan AuthUpdateBatch, 8)
	w, errNew := NewWatcher(configPath, authDir, nil)
	if errNew != nil {
		t.Fatal(errNew)
	}
	w.SetConfig(newConfig("m1"))
	w.SetAuthUpdateQueue(queue)
	t.Cleanup(func() {
		if errStop := w.Stop(); errStop != nil {
			t.Error(errStop)
		}
		w.cancelAndWaitForAuthLoad()
	})

	nextUpdate := func(stage string) AuthUpdate {
		t.Helper()
		select {
		case batch := <-queue:
			if len(batch.Updates) != 1 {
				t.Fatalf("%s: expected one auth update, got %+v", stage, batch)
			}
			update := batch.Updates[0]
			if batch.Result != nil {
				batch.Result <- []AuthUpdateResult{{ID: update.ID, Loaded: true}}
			}
			if update.Auth == nil || update.Auth.Provider != "claude" {
				t.Fatalf("%s: unexpected auth update %+v", stage, update)
			}
			// Global rules are resolved at registration and never stored on the credential.
			if value, ok := update.Auth.Attributes["excluded_models"]; ok {
				t.Fatalf("%s: excluded_models attribute = %q, want absent", stage, value)
			}
			return update
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: timed out waiting for auth update", stage)
		}
		return AuthUpdate{}
	}

	initialDone := w.StartInitialAuthLoad(context.Background(), 1)
	initial := nextUpdate("initial load")
	<-initialDone

	// The credential is unchanged, so only the affected-provider refresh can trigger re-registration.
	if ok := w.reloadConfig(); !ok {
		t.Fatal("expected reloadConfig to succeed")
	}
	if reloaded := nextUpdate("global exclusion change"); reloaded.ID != initial.ID {
		t.Fatalf("reloaded auth ID = %q, want %q", reloaded.ID, initial.ID)
	}
}

func TestReloadConfigTriggersCallbackForMaxRetryCredentialsChange(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")

	oldCfg := &config.Config{
		AuthDir:             authDir,
		CredentialInFlight:  config.DefaultCredentialInFlightConfig(),
		MaxRetryCredentials: 0,
		RequestRetry:        1,
		MaxRetryInterval:    5,
	}
	newCfg := &config.Config{
		AuthDir:             authDir,
		CredentialInFlight:  config.DefaultCredentialInFlightConfig(),
		MaxRetryCredentials: 2,
		RequestRetry:        1,
		MaxRetryInterval:    5,
	}
	data, errMarshal := yaml.Marshal(newCfg)
	if errMarshal != nil {
		t.Fatalf("failed to marshal config: %v", errMarshal)
	}
	if errWrite := os.WriteFile(configPath, data, 0o644); errWrite != nil {
		t.Fatalf("failed to write config: %v", errWrite)
	}

	callbackCalls := 0
	callbackMaxRetryCredentials := -1
	w := &Watcher{
		configPath:     configPath,
		authDir:        authDir,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(cfg *config.Config) {
			callbackCalls++
			if cfg != nil {
				callbackMaxRetryCredentials = cfg.MaxRetryCredentials
			}
		},
	}
	w.SetConfig(oldCfg)

	if ok := w.reloadConfig(); !ok {
		t.Fatal("expected reloadConfig to succeed")
	}

	if callbackCalls != 1 {
		t.Fatalf("expected reload callback to be called once, got %d", callbackCalls)
	}
	if callbackMaxRetryCredentials != 2 {
		t.Fatalf("expected callback MaxRetryCredentials=2, got %d", callbackMaxRetryCredentials)
	}

	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if w.config == nil || w.config.MaxRetryCredentials != 2 {
		t.Fatalf("expected watcher config MaxRetryCredentials=2, got %+v", w.config)
	}
}

func TestStartFailsWhenAuthDirMissing(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+filepath.Join(tmpDir, "missing-auth")+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authDir := filepath.Join(tmpDir, "missing-auth")

	w, err := NewWatcher(configPath, authDir, nil)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}
	defer w.Stop()
	w.SetConfig(&config.Config{AuthDir: authDir})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err == nil {
		t.Fatal("expected Start to fail for missing auth dir")
	}
}

func TestDispatchRuntimeAuthUpdateReturnsFalseWithoutQueue(t *testing.T) {
	w := &Watcher{}
	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionAdd, Auth: &coreauth.Auth{ID: "a"}}); ok {
		t.Fatal("expected DispatchRuntimeAuthUpdate to return false when no queue configured")
	}
	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionDelete, Auth: &coreauth.Auth{ID: "a"}}); ok {
		t.Fatal("expected DispatchRuntimeAuthUpdate delete to return false when no queue configured")
	}
}

func TestNormalizeAuthNil(t *testing.T) {
	if normalizeAuth(nil) != nil {
		t.Fatal("expected normalizeAuth(nil) to return nil")
	}
}

// stubStore implements coreauth.Store plus watcher-specific persistence helpers.
type stubStore struct {
	mu              sync.Mutex
	authDir         string
	cfgPersisted    int
	authPersisted   int
	lastAuthMessage string
	lastAuthPaths   []string
	persisted       chan struct{}
}

func (s *stubStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }
func (s *stubStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", nil
}
func (s *stubStore) Delete(context.Context, string) error { return nil }
func (s *stubStore) PersistConfig(context.Context) error {
	s.mu.Lock()
	s.cfgPersisted++
	s.mu.Unlock()
	s.signalPersisted()
	return nil
}
func (s *stubStore) PersistAuthFiles(_ context.Context, message string, paths ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAuthMessage = message
	s.lastAuthPaths = append([]string(nil), paths...)
	s.authPersisted++
	s.signalPersisted()
	return nil
}
func (s *stubStore) AuthDir() string { return s.authDir }

func (s *stubStore) signalPersisted() {
	if s.persisted == nil {
		return
	}
	select {
	case s.persisted <- struct{}{}:
	default:
	}
}

func (s *stubStore) persistenceSnapshot() (cfgPersisted, authPersisted int, message string, paths []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfgPersisted, s.authPersisted, s.lastAuthMessage, append([]string(nil), s.lastAuthPaths...)
}

func TestNewWatcherDetectsPersisterAndAuthDir(t *testing.T) {
	tmp := t.TempDir()
	store := &stubStore{authDir: tmp}
	orig := sdkAuth.GetTokenStore()
	sdkAuth.RegisterTokenStore(store)
	defer sdkAuth.RegisterTokenStore(orig)

	w, err := NewWatcher("config.yaml", "auth", nil)
	if err != nil {
		t.Fatalf("NewWatcher failed: %v", err)
	}
	if w.storePersister == nil {
		t.Fatal("expected storePersister to be set from token store")
	}
	if w.mirroredAuthDir != tmp {
		t.Fatalf("expected mirroredAuthDir %s, got %s", tmp, w.mirroredAuthDir)
	}
}

func TestPersistConfigAndAuthAsyncInvokePersister(t *testing.T) {
	store := &stubStore{persisted: make(chan struct{}, 2)}
	w := &Watcher{
		storePersister: store,
	}

	w.persistConfigAsync()
	w.persistAuthAsync("msg", " a ", "", "b ")

	for range 2 {
		select {
		case <-store.persisted:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for asynchronous persistence")
		}
	}
	cfgPersisted, authPersisted, message, paths := store.persistenceSnapshot()
	if cfgPersisted != 1 {
		t.Fatalf("expected PersistConfig to be called once, got %d", cfgPersisted)
	}
	if authPersisted != 1 {
		t.Fatalf("expected PersistAuthFiles to be called once, got %d", authPersisted)
	}
	if message != "msg" {
		t.Fatalf("unexpected auth message: %s", message)
	}
	if len(paths) != 2 || paths[0] != "a" || paths[1] != "b" {
		t.Fatalf("unexpected filtered paths: %#v", paths)
	}
}

func TestScheduleConfigReloadDebounces(t *testing.T) {
	tmp := t.TempDir()
	authDir := tmp
	cfgPath := tmp + "/config.yaml"
	if err := os.WriteFile(cfgPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	var reloads int32
	w := &Watcher{
		configPath:     cfgPath,
		authDir:        authDir,
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.scheduleConfigReload()
	w.scheduleConfigReload()

	deadline := time.Now().Add(time.Second)
	for {
		w.clientsMutex.RLock()
		hashSet := w.lastConfigHash != ""
		w.clientsMutex.RUnlock()
		if hashSet {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for debounced config reload")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&reloads); got != 1 {
		t.Fatalf("expected single debounced reload, got %d", got)
	}
}

func TestPrepareAuthUpdatesLockedForceAndDelete(t *testing.T) {
	w := &Watcher{
		currentAuths: map[string]*coreauth.Auth{
			"a": {ID: "a", Provider: "p1"},
		},
		authQueue: make(chan AuthUpdateBatch, 4),
	}

	updates := w.prepareAuthUpdatesLocked([]*coreauth.Auth{{ID: "a", Provider: "p2"}}, false)
	if len(updates) != 1 || updates[0].Action != AuthUpdateActionModify || updates[0].ID != "a" {
		t.Fatalf("unexpected modify updates: %+v", updates)
	}

	updates = w.prepareAuthUpdatesLocked([]*coreauth.Auth{{ID: "a", Provider: "p2"}}, true)
	if len(updates) != 1 || updates[0].Action != AuthUpdateActionModify {
		t.Fatalf("expected force modify, got %+v", updates)
	}

	updates = w.prepareAuthUpdatesLocked([]*coreauth.Auth{}, false)
	if len(updates) != 1 || updates[0].Action != AuthUpdateActionDelete || updates[0].ID != "a" {
		t.Fatalf("expected delete for missing auth, got %+v", updates)
	}
}

func TestAuthEqualIgnoresTemporalFields(t *testing.T) {
	now := time.Now()
	a := &coreauth.Auth{ID: "x", CreatedAt: now}
	b := &coreauth.Auth{ID: "x", CreatedAt: now.Add(5 * time.Second)}
	if !authEqual(a, b) {
		t.Fatal("expected authEqual to ignore temporal differences")
	}
}

func TestDispatchLoopExitsWhenQueueNilAndContextCanceled(t *testing.T) {
	w := &Watcher{
		dispatchCond:   nil,
		pendingUpdates: map[string]AuthUpdate{"k": {ID: "k"}},
		pendingOrder:   []string{"k"},
	}
	w.dispatchCond = sync.NewCond(&w.dispatchMu)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.dispatchLoop(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	w.dispatchMu.Lock()
	w.dispatchCond.Broadcast()
	w.dispatchMu.Unlock()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("dispatchLoop did not exit after context cancel")
	}
}

func TestReloadClientsFiltersOAuthProvidersWithoutRescan(t *testing.T) {
	tmp := t.TempDir()
	w := &Watcher{
		authDir: tmp,
		config:  &config.Config{AuthDir: tmp},
		currentAuths: map[string]*coreauth.Auth{
			"a": {ID: "a", Provider: "Match"},
			"b": {ID: "b", Provider: "other"},
		},
		lastAuthHashes: map[string]string{"cached": "hash"},
	}

	w.SetFileAuthLoadingEnabled(false)
	w.reloadClients(false, []string{"match"}, false)

	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if _, ok := w.currentAuths["a"]; ok {
		t.Fatal("expected filtered provider to be removed")
	}
	if len(w.lastAuthHashes) != 1 {
		t.Fatalf("expected existing hash cache to be retained, got %d", len(w.lastAuthHashes))
	}
}

func TestScheduleProcessEventsStopsOnContextDone(t *testing.T) {
	w := &Watcher{
		watcher: &fsnotify.Watcher{
			Events: make(chan fsnotify.Event, 1),
			Errors: make(chan error, 1),
		},
		configPath: "config.yaml",
		authDir:    "auth",
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.processEvents(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("processEvents did not exit on context cancel")
	}
}

func hexString(data []byte) string {
	return strings.ToLower(fmt.Sprintf("%x", data))
}

func TestInitialAuthLoadHonorsWorkerLimitAndPublishesReady(t *testing.T) {
	authDir := t.TempDir()
	for i := 0; i < 40; i++ {
		mustWriteFile(t, filepath.Join(authDir, fmt.Sprintf("auth-%03d.json", i)), []byte(`{"type":"xai"}`))
	}

	originalRead := readInitialAuthFile
	defer func() { readInitialAuthFile = originalRead }()
	var active atomic.Int32
	var peak atomic.Int32
	readInitialAuthFile = func(path string) ([]byte, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for current > peak.Load() && !peak.CompareAndSwap(peak.Load(), current) {
		}
		time.Sleep(10 * time.Millisecond)
		return os.ReadFile(path)
	}

	queue := make(chan AuthUpdateBatch, 8)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	done := w.StartInitialAuthLoad(context.Background(), 4)
	loaded := 0
	for {
		select {
		case batch := <-queue:
			loaded += len(batch.Updates)
			batch.Result <- loadedResults(batch.Updates)
		case <-done:
			goto loadComplete
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for initial auth load")
		}
	}
loadComplete:
	if peak.Load() > 4 {
		t.Fatalf("peak concurrent reads = %d, want <= 4", peak.Load())
	}
	status := w.AuthLoadStatus()
	if status.State != AuthLoadStateReady || status.FilesProcessed != 40 || status.AuthsLoaded != int64(loaded) {
		t.Fatalf("status = %+v, loaded = %d", status, loaded)
	}
}

func TestInitialAuthLoadMakesFirstBatchAvailableBeforeCompletion(t *testing.T) {
	authDir := writeInitialLoadAuthFiles(t, 40)
	releaseTail := make(chan struct{})
	originalRead := readInitialAuthFile
	defer func() { readInitialAuthFile = originalRead }()
	readInitialAuthFile = func(path string) ([]byte, error) {
		if filepath.Base(path) >= "auth-032.json" {
			<-releaseTail
		}
		return os.ReadFile(path)
	}
	queue := make(chan AuthUpdateBatch, 8)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	done := w.StartInitialAuthLoad(context.Background(), 4)
	first := <-queue
	first.Result <- loadedResults(first.Updates)
	eventually(t, time.Second, func() bool { return w.AuthLoadStatus().AuthsLoaded > 0 })
	select {
	case <-done:
		t.Fatal("load completed before delayed tail was released")
	default:
	}
	close(releaseTail)
	acknowledgeInitialLoadUntilDone(t, queue, done)
}

func TestInitialAuthLoadMalformedFileDoesNotBlockValidFile(t *testing.T) {
	authDir := t.TempDir()
	mustWriteFile(t, filepath.Join(authDir, "broken.json"), []byte(`{"type":`))
	mustWriteFile(t, filepath.Join(authDir, "valid.json"), []byte(`{"type":"xai"}`))
	queue := make(chan AuthUpdateBatch, 4)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	done := w.StartInitialAuthLoad(context.Background(), 2)
	acknowledgeInitialLoadUntilDone(t, queue, done)
	status := w.AuthLoadStatus()
	if status.State != AuthLoadStateDegraded || status.FilesProcessed != 2 || status.FilesFailed != 1 || status.AuthsLoaded != 1 {
		t.Fatalf("status = %+v", status)
	}
}

func TestProgressiveProviderReloadPreservesIncompleteFile(t *testing.T) {
	for _, partial := range []string{"", `{"type":"codex"`} {
		t.Run(fmt.Sprintf("partial_%q", partial), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "account.json")
			payload := []byte(`{"type":"codex"}`)
			mustWriteFile(t, path, payload)
			queue := make(chan AuthUpdateBatch, 4)
			w := newInitialLoadTestWatcher(t, dir, queue)
			acknowledgeInitialLoadUntilDone(t, queue, w.StartInitialAuthLoad(context.Background(), 1))

			mustWriteFile(t, path, []byte(partial))
			w.SetConfig(&config.Config{AuthDir: dir, AuthLoadWorkers: 1, OAuthExcludedModels: map[string][]string{"codex": {"excluded-model"}}})
			w.reloadClients(false, []string{"codex"}, false)
			w.authLoadMu.Lock()
			done := w.authLoadDone
			w.authLoadMu.Unlock()
			acknowledgeInitialLoadUntilDone(t, queue, done)
			if status := w.AuthLoadStatus(); status.State != AuthLoadStateDegraded || status.FilesFailed != 1 {
				t.Fatalf("incomplete scan status = %+v", status)
			}
			auths := w.SnapshotCoreAuths()
			if len(auths) != 1 || auths[0].ID != "account.json" {
				t.Fatalf("incomplete provider reload lost credential: %+v", auths)
			}
			select {
			case batch := <-queue:
				t.Fatalf("incomplete provider reload emitted updates: %+v", batch.Updates)
			default:
			}

			// Restoring identical bytes must still apply the pending provider configuration.
			mustWriteFile(t, path, payload)
			w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
			select {
			case batch := <-queue:
				if len(batch.Updates) != 1 || batch.Updates[0].ID != "account.json" || batch.Updates[0].Action != AuthUpdateActionModify {
					t.Fatalf("completed write did not refresh credential: %+v", batch.Updates)
				}
			case <-time.After(time.Second):
				t.Fatal("completed write did not apply pending provider reload")
			}
		})
	}
}

func TestInitialAuthLoadPluginExpansionCountsAuths(t *testing.T) {
	authDir := t.TempDir()
	mustWriteFile(t, filepath.Join(authDir, "plugin.json"), []byte(`{"type":"plugin"}`))
	queue := make(chan AuthUpdateBatch, 4)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	w.SetPluginAuthParser(watcherMultiAuthParserFunc(func(context.Context, pluginapi.AuthParseRequest) ([]*coreauth.Auth, bool, error) {
		return []*coreauth.Auth{{ID: "virtual-a", Provider: "plugin"}, {ID: "virtual-b", Provider: "plugin"}}, true, nil
	}))
	done := w.StartInitialAuthLoad(context.Background(), 2)
	acknowledgeInitialLoadUntilDone(t, queue, done)
	status := w.AuthLoadStatus()
	if status.FilesProcessed != 1 || status.AuthsLoaded != 2 {
		t.Fatalf("status = %+v", status)
	}
}

func TestInitialAuthLoadDropsResultAfterLiveDelete(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "stale.json")
	mustWriteFile(t, path, []byte(`{"type":"xai"}`))
	readCaptured := make(chan struct{})
	releaseRead := make(chan struct{})
	originalRead := readInitialAuthFile
	defer func() { readInitialAuthFile = originalRead }()
	readInitialAuthFile = func(path string) ([]byte, error) {
		data, errRead := os.ReadFile(path)
		close(readCaptured)
		<-releaseRead
		return data, errRead
	}
	queue := make(chan AuthUpdateBatch, 4)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	done := w.StartInitialAuthLoad(context.Background(), 1)
	<-readCaptured
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatal(errRemove)
	}
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Remove})
	close(releaseRead)
	<-done
	select {
	case batch := <-queue:
		t.Fatalf("stale scan emitted batch: %+v", batch.Updates)
	default:
	}
}

func TestInitialAuthLoadStopsOnCancellation(t *testing.T) {
	authDir := writeInitialLoadAuthFiles(t, 8)
	releaseRead := make(chan struct{})
	originalRead := readInitialAuthFile
	defer func() { readInitialAuthFile = originalRead }()
	readInitialAuthFile = func(path string) ([]byte, error) {
		<-releaseRead
		return os.ReadFile(path)
	}
	queue := make(chan AuthUpdateBatch, 4)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	ctx, cancel := context.WithCancel(context.Background())
	done := w.StartInitialAuthLoad(ctx, 2)
	cancel()
	close(releaseRead)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loader did not stop after cancellation")
	}
	terminal := w.AuthLoadStatus()
	time.Sleep(20 * time.Millisecond)
	if got := w.AuthLoadStatus(); !reflect.DeepEqual(got, terminal) {
		t.Fatalf("status changed after completion: before=%+v after=%+v", terminal, got)
	}
}

func TestInitialAuthLoadMissingDirectoryIsReady(t *testing.T) {
	authDir := t.TempDir()
	queue := make(chan AuthUpdateBatch, 1)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	if errRemove := os.RemoveAll(authDir); errRemove != nil {
		t.Fatal(errRemove)
	}
	done := w.StartInitialAuthLoad(context.Background(), 2)
	<-done
	status := w.AuthLoadStatus()
	if status.State != AuthLoadStateReady || !status.ScanComplete || status.FilesDiscovered != 0 {
		t.Fatalf("status = %+v", status)
	}
}

func TestFullAuthRescanDirectoryReadErrors(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing_%t", missing), func(t *testing.T) {
			if !missing && (runtime.GOOS == "windows" || os.Geteuid() == 0) {
				t.Skip("requires Unix file permissions and a non-root user")
			}
			authDir := writeInitialLoadAuthFiles(t, 1)
			if !missing {
				t.Cleanup(func() { _ = os.Chmod(authDir, 0o700) })
			}
			queue := make(chan AuthUpdateBatch, 2)
			w := newInitialLoadTestWatcher(t, authDir, queue)
			firstDone := w.StartInitialAuthLoad(context.Background(), 1)
			acknowledgeInitialLoadUntilDone(t, queue, firstDone)
			if auths := w.SnapshotCoreAuths(); len(auths) != 1 {
				t.Fatalf("initial auth snapshot = %+v, want one auth", auths)
			}
			if missing {
				if errRemove := os.RemoveAll(authDir); errRemove != nil {
					t.Fatal(errRemove)
				}
			} else if errChmod := os.Chmod(authDir, 0); errChmod != nil {
				t.Fatal(errChmod)
			}

			secondDone := w.StartInitialAuthLoad(context.Background(), 1)
			var unexpected []AuthUpdate
			for {
				select {
				case batch := <-queue:
					unexpected = append(unexpected, batch.Updates...)
					batch.Result <- loadedResults(batch.Updates)
				case <-secondDone:
					goto rescanComplete
				case <-time.After(time.Second):
					t.Fatal("directory failure rescan did not complete")
				}
			}
		rescanComplete:
			if missing {
				if len(unexpected) != 1 || unexpected[0].ID != "auth-000.json" || unexpected[0].Action != AuthUpdateActionDelete {
					t.Fatalf("missing directory updates = %+v, want one delete", unexpected)
				}
				if auths := w.SnapshotCoreAuths(); len(auths) != 0 {
					t.Fatalf("missing directory retained auths: %+v", auths)
				}
				if status := w.AuthLoadStatus(); status.State != AuthLoadStateReady {
					t.Fatalf("status = %+v, want ready", status)
				}
				return
			}
			if len(unexpected) != 0 {
				t.Fatalf("directory failure emitted auth updates: %+v", unexpected)
			}
			if auths := w.SnapshotCoreAuths(); len(auths) != 1 {
				t.Fatalf("directory failure discarded loaded auths: %+v", auths)
			}
			if status := w.AuthLoadStatus(); status.State != AuthLoadStateDegraded {
				t.Fatalf("status = %+v, want degraded", status)
			}
			for path, hash := range w.lastAuthHashes {
				if hash != "" {
					t.Fatalf("unreadable directory retained hash for %s: %q", path, hash)
				}
			}
		})
	}
}

func TestFullAuthRescanReadErrorsMatchFileSynthesizer(t *testing.T) {
	for _, tc := range []struct {
		name       string
		breakFile  func(t *testing.T, target string)
		wantDelete bool
		wantState  AuthLoadState
		wantFailed int64
	}{
		{
			name: "dangling_symlink_unregisters",
			breakFile: func(t *testing.T, target string) {
				if errRemove := os.Remove(target); errRemove != nil {
					t.Fatal(errRemove)
				}
			},
			wantDelete: true,
			wantState:  AuthLoadStateReady,
		},
		{
			name: "unreadable_target_preserves",
			breakFile: func(t *testing.T, target string) {
				if runtime.GOOS == "windows" || os.Geteuid() == 0 {
					t.Skip("requires Unix file permissions and a non-root user")
				}
				if errChmod := os.Chmod(target, 0); errChmod != nil {
					t.Fatal(errChmod)
				}
				t.Cleanup(func() { _ = os.Chmod(target, 0o600) })
			},
			wantState:  AuthLoadStateDegraded,
			wantFailed: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authDir := t.TempDir()
			target := filepath.Join(t.TempDir(), "target.json")
			mustWriteFile(t, target, []byte(`{"type":"xai"}`))
			if errSymlink := os.Symlink(target, filepath.Join(authDir, "account.json")); errSymlink != nil {
				t.Skipf("symlinks unavailable: %v", errSymlink)
			}
			queue := make(chan AuthUpdateBatch, 4)
			w := newInitialLoadTestWatcher(t, authDir, queue)
			acknowledgeInitialLoadUntilDone(t, queue, w.StartInitialAuthLoad(context.Background(), 1))
			if auths := w.SnapshotCoreAuths(); len(auths) != 1 {
				t.Fatalf("initial auth snapshot = %+v, want one auth", auths)
			}

			tc.breakFile(t, target)
			done := w.StartInitialAuthLoad(context.Background(), 1)
			var updates []AuthUpdate
			for {
				select {
				case batch := <-queue:
					updates = append(updates, batch.Updates...)
					batch.Result <- loadedResults(batch.Updates)
				case <-done:
					goto rescanComplete
				case <-time.After(2 * time.Second):
					t.Fatal("rescan did not complete")
				}
			}
		rescanComplete:
			if tc.wantDelete {
				if len(updates) != 1 || updates[0].ID != "account.json" || updates[0].Action != AuthUpdateActionDelete {
					t.Fatalf("updates = %+v, want one delete for account.json", updates)
				}
				if auths := w.SnapshotCoreAuths(); len(auths) != 0 {
					t.Fatalf("dangling symlink kept credential: %+v", auths)
				}
			} else {
				if len(updates) != 0 {
					t.Fatalf("unreadable file emitted updates: %+v", updates)
				}
				if auths := w.SnapshotCoreAuths(); len(auths) != 1 {
					t.Fatalf("unreadable file discarded credential: %+v", auths)
				}
			}
			if status := w.AuthLoadStatus(); status.State != tc.wantState || status.FilesFailed != tc.wantFailed || status.FilesProcessed != 1 {
				t.Fatalf("status = %+v, want state=%s failed=%d processed=1", status, tc.wantState, tc.wantFailed)
			}
		})
	}
}

func TestFullAuthRescanAcceptsStablePreviouslyChangedPath(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "changed.json")
	mustWriteFile(t, path, []byte(`{"type":"xai"}`))
	queue := make(chan AuthUpdateBatch, 4)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	w.MarkAuthPathChanged(path)
	firstDone := w.StartInitialAuthLoad(context.Background(), 1)
	<-firstDone
	if status := w.AuthLoadStatus(); status.FilesSkipped != 1 {
		t.Fatalf("first scan status = %+v, want live generation skip", status)
	}
	secondDone := w.StartInitialAuthLoad(context.Background(), 1)
	acknowledgeInitialLoadUntilDone(t, queue, secondDone)
	if status := w.AuthLoadStatus(); status.AuthsLoaded != 1 {
		t.Fatalf("second scan status = %+v, want stable changed path loaded", status)
	}
}

func TestAuthLoadHooksWrapEveryFullScan(t *testing.T) {
	authDir := writeInitialLoadAuthFiles(t, 1)
	queue := make(chan AuthUpdateBatch, 4)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	var before atomic.Int32
	var after atomic.Int32
	w.SetAuthLoadHooks(AuthLoadHooks{
		Before: func(context.Context) error { before.Add(1); return nil },
		After:  func(context.Context) error { after.Add(1); return nil },
	})
	for scan := int32(1); scan <= 2; scan++ {
		if scan == 2 {
			mustWriteFile(t, filepath.Join(authDir, "auth-000.json"), []byte(`{"type":"xai","note":"changed"}`))
		}
		done := w.StartInitialAuthLoad(context.Background(), 1)
		batch := <-queue
		if got := before.Load(); got != scan {
			t.Fatalf("before count at batch = %d, want %d", got, scan)
		}
		batch.Result <- loadedResults(batch.Updates)
		<-done
		if got := after.Load(); got != scan {
			t.Fatalf("after count at completion = %d, want %d", got, scan)
		}
	}
}

func TestAuthLoadBeforeHookFailurePreventsCredentialRegistration(t *testing.T) {
	authDir := writeInitialLoadAuthFiles(t, 1)
	queue := make(chan AuthUpdateBatch, 1)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	w.SetAuthLoadHooks(AuthLoadHooks{Before: func(context.Context) error { return errors.New("cooldown unavailable") }})
	done := w.StartInitialAuthLoad(context.Background(), 1)
	<-done
	status := w.AuthLoadStatus()
	if status.State != AuthLoadStateDegraded || !status.ScanComplete || status.FilesProcessed != 0 {
		t.Fatalf("status = %+v", status)
	}
	select {
	case batch := <-queue:
		t.Fatalf("hook failure emitted auth updates: %+v", batch.Updates)
	default:
	}
}

func TestInitialAuthLoadDisabledForNonFileStore(t *testing.T) {
	authDir := writeInitialLoadAuthFiles(t, 1)
	queue := make(chan AuthUpdateBatch, 1)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	w.SetFileAuthLoadingEnabled(false)
	done := w.StartInitialAuthLoad(context.Background(), 16)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disabled loader did not return immediately")
	}
	if status := w.AuthLoadStatus(); status.State != AuthLoadStateIdle {
		t.Fatalf("status = %+v, want idle", status)
	}
}

func TestProgressiveAuthEventsWaitForInitialLoadActivation(t *testing.T) {
	authDir := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mustWriteFile(t, configPath, []byte("port: 8317\n"))
	path := filepath.Join(authDir, "pending.json")
	mustWriteFile(t, path, []byte(`{"type":"xai","note":"first"}`))
	queue := make(chan AuthUpdateBatch, 4)
	w, errNew := NewWatcher(configPath, authDir, nil)
	if errNew != nil {
		t.Fatal(errNew)
	}
	w.SetConfig(&config.Config{AuthDir: authDir, AuthLoadWorkers: 1})
	w.SetAuthUpdateQueue(queue)
	w.SetFileAuthLoadingEnabled(true)
	t.Cleanup(func() { _ = w.Stop() })
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()
	if errStart := w.Start(lifecycleCtx); errStart != nil {
		t.Fatal(errStart)
	}

	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Create})
	mustWriteFile(t, path, []byte(`{"type":"xai","note":"final"}`))
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
	if auths := w.SnapshotCoreAuths(); len(auths) != 0 {
		t.Fatalf("pre-activation auth event loaded credentials: %+v", auths)
	}
	select {
	case batch := <-queue:
		t.Fatalf("pre-activation auth event emitted batch: %+v", batch.Updates)
	default:
	}

	originalRead := readInitialAuthFile
	defer func() { readInitialAuthFile = originalRead }()
	var reads atomic.Int32
	readInitialAuthFile = func(path string) ([]byte, error) {
		reads.Add(1)
		return os.ReadFile(path)
	}
	done := w.StartInitialAuthLoad(lifecycleCtx, 1)
	acknowledgeInitialLoadUntilDone(t, queue, done)
	// A live event may supersede the scan after advancing its path generation.
	// Wait for that event to finish committing before reading the final snapshot.
	w.authRescanMu.Lock()
	defer w.authRescanMu.Unlock()
	if got := reads.Load(); got != 1 {
		t.Fatalf("initial scan reads = %d, want 1", got)
	}
	auths := w.SnapshotCoreAuths()
	if len(auths) != 1 || auths[0].Metadata["note"] != "final" {
		t.Fatalf("initial scan did not load final disk state: %+v", auths)
	}
}

func TestProgressiveConfigEventAppliesOnceBeforeInitialScan(t *testing.T) {
	authDir := writeInitialLoadAuthFiles(t, 1)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mustWriteFile(t, configPath, []byte("port: 8317\n"))
	queue := make(chan AuthUpdateBatch, 4)
	var mu sync.Mutex
	var order []string
	var callbackPorts []int
	w, errNew := NewWatcher(configPath, authDir, func(cfg *config.Config) {
		mu.Lock()
		order = append(order, "config")
		callbackPorts = append(callbackPorts, cfg.Port)
		mu.Unlock()
	})
	if errNew != nil {
		t.Fatal(errNew)
	}
	w.SetConfig(&config.Config{Port: 8317, AuthDir: authDir, AuthLoadWorkers: 1})
	w.SetAuthUpdateQueue(queue)
	w.SetFileAuthLoadingEnabled(true)
	w.SetAuthLoadHooks(AuthLoadHooks{Before: func(context.Context) error {
		mu.Lock()
		order = append(order, "before")
		mu.Unlock()
		return nil
	}})
	t.Cleanup(func() { _ = w.Stop() })
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	defer cancelLifecycle()
	if errStart := w.Start(lifecycleCtx); errStart != nil {
		t.Fatal(errStart)
	}

	mustWriteFile(t, configPath, []byte("port: 9001\n"))
	w.handleEvent(fsnotify.Event{Name: configPath, Op: fsnotify.Write})
	time.Sleep(2 * configReloadDebounce)
	mu.Lock()
	preActivationCallbacks := len(callbackPorts)
	mu.Unlock()
	if preActivationCallbacks != 0 {
		t.Fatalf("pre-activation config event invoked callback %d times", preActivationCallbacks)
	}
	if status := w.AuthLoadStatus(); status.State != AuthLoadStateIdle {
		t.Fatalf("pre-activation config event started scan: %+v", status)
	}

	done := w.StartInitialAuthLoad(lifecycleCtx, 1)
	acknowledgeInitialLoadUntilDone(t, queue, done)
	mu.Lock()
	gotOrder := append([]string(nil), order...)
	gotPorts := append([]int(nil), callbackPorts...)
	mu.Unlock()
	if !reflect.DeepEqual(gotPorts, []int{9001}) {
		t.Fatalf("callback ports = %v, want [9001]", gotPorts)
	}
	if !reflect.DeepEqual(gotOrder, []string{"config", "before"}) {
		t.Fatalf("activation order = %v, want [config before]", gotOrder)
	}
	select {
	case batch := <-queue:
		t.Fatalf("activation triggered duplicate scan batch: %+v", batch.Updates)
	default:
	}
}

func TestProgressiveConfigRescanStopsWithWatcherLifecycle(t *testing.T) {
	authDir := writeInitialLoadAuthFiles(t, 1)
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mustWriteFile(t, configPath, []byte("port: 8317\n"))
	queue := make(chan AuthUpdateBatch, 4)
	w, errNew := NewWatcher(configPath, authDir, func(*config.Config) {})
	if errNew != nil {
		t.Fatal(errNew)
	}
	w.SetConfig(&config.Config{Port: 8317, AuthDir: authDir, AuthLoadWorkers: 1})
	w.SetAuthUpdateQueue(queue)
	w.SetFileAuthLoadingEnabled(true)
	t.Cleanup(func() { _ = w.Stop() })
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	if errStart := w.Start(lifecycleCtx); errStart != nil {
		t.Fatal(errStart)
	}
	initialDone := w.StartInitialAuthLoad(lifecycleCtx, 1)
	acknowledgeInitialLoadUntilDone(t, queue, initialDone)

	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	originalRead := readInitialAuthFile
	defer func() { readInitialAuthFile = originalRead }()
	readInitialAuthFile = func(path string) ([]byte, error) {
		close(readStarted)
		<-releaseRead
		return []byte(`{"type":"xai","note":"rescan"}`), nil
	}
	newAuthDir := t.TempDir()
	mustWriteFile(t, filepath.Join(newAuthDir, "rescan.json"), []byte(`{"type":"xai"}`))
	mustWriteFile(t, configPath, []byte("port: 9002\nauth-dir: "+newAuthDir+"\n"))
	w.handleEvent(fsnotify.Event{Name: configPath, Op: fsnotify.Write})
	select {
	case <-readStarted:
	case <-time.After(time.Second):
		t.Fatal("config change did not start rescan")
	}
	cancelLifecycle()
	close(releaseRead)
	time.Sleep(50 * time.Millisecond)
	select {
	case batch := <-queue:
		t.Fatalf("lifecycle-canceled rescan emitted batch: %+v", batch.Updates)
	default:
	}
}

func TestLegacyWatcherProcessesLiveAuthWithoutProgressiveOptIn(t *testing.T) {
	authDir := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mustWriteFile(t, configPath, []byte("port: 8317\n"))
	path := filepath.Join(authDir, "legacy.json")
	mustWriteFile(t, path, []byte(`{"type":"xai"}`))
	queue := make(chan AuthUpdateBatch, 1)
	w, errNew := NewWatcher(configPath, authDir, nil)
	if errNew != nil {
		t.Fatal(errNew)
	}
	w.SetConfig(&config.Config{AuthDir: authDir})
	w.SetAuthUpdateQueue(queue)
	t.Cleanup(func() { _ = w.Stop() })

	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
	select {
	case batch := <-queue:
		if len(batch.Updates) != 1 || batch.Updates[0].ID != "legacy.json" {
			t.Fatalf("legacy live event batch = %+v", batch.Updates)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy watcher did not process live auth event")
	}
}

func TestInitialAuthLoadSupersededScanCannotCommitOrPublish(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "credential.json")
	mustWriteFile(t, path, []byte(`{"type":"xai","note":"old"}`))
	oldReadStarted := make(chan struct{})
	releaseOldRead := make(chan struct{})
	originalRead := readInitialAuthFile
	defer func() { readInitialAuthFile = originalRead }()
	var reads atomic.Int32
	readInitialAuthFile = func(path string) ([]byte, error) {
		if reads.Add(1) == 1 {
			data, errRead := os.ReadFile(path)
			close(oldReadStarted)
			<-releaseOldRead
			return data, errRead
		}
		return os.ReadFile(path)
	}

	queue := make(chan AuthUpdateBatch, 4)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	oldDone := w.StartInitialAuthLoad(context.Background(), 1)
	<-oldReadStarted
	mustWriteFile(t, path, []byte(`{"type":"xai","note":"new"}`))
	newDone := w.StartInitialAuthLoad(context.Background(), 1)
	select {
	case batch := <-queue:
		batch.Result <- loadedResults(batch.Updates)
	case <-time.After(time.Second):
		t.Fatal("new scan did not emit a batch")
	}
	<-newDone
	wantStatus := w.AuthLoadStatus()
	close(releaseOldRead)
	<-oldDone
	if got := w.AuthLoadStatus(); !reflect.DeepEqual(got, wantStatus) {
		t.Fatalf("superseded scan overwrote status: got=%+v want=%+v", got, wantStatus)
	}
	select {
	case batch := <-queue:
		t.Fatalf("superseded scan emitted batch: %+v", batch.Updates)
	default:
	}
	auths := w.SnapshotCoreAuths()
	if len(auths) != 1 || auths[0].Metadata["note"] != "new" {
		t.Fatalf("superseded scan overwrote watcher cache: %+v", auths)
	}
}

func TestInitialAuthLoadCancellationNeverDispatchesToReadyQueue(t *testing.T) {
	originalRead := readInitialAuthFile
	defer func() { readInitialAuthFile = originalRead }()
	for attempt := 0; attempt < 64; attempt++ {
		authDir := writeInitialLoadAuthFiles(t, 1)
		readStarted := make(chan struct{})
		releaseRead := make(chan struct{})
		readInitialAuthFile = func(path string) ([]byte, error) {
			close(readStarted)
			<-releaseRead
			return os.ReadFile(path)
		}
		queue := make(chan AuthUpdateBatch, 1)
		w := newInitialLoadTestWatcher(t, authDir, queue)
		ctx, cancel := context.WithCancel(context.Background())
		done := w.StartInitialAuthLoad(ctx, 1)
		<-readStarted
		cancel()
		close(releaseRead)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("attempt %d did not stop after cancellation", attempt)
		}
		select {
		case batch := <-queue:
			t.Fatalf("attempt %d dispatched after cancellation: %+v", attempt, batch.Updates)
		default:
		}
	}
}

func TestInitialAuthLoadCancellationWinsBlockedBatchSend(t *testing.T) {
	for attempt := 0; attempt < 128; attempt++ {
		authDir := writeInitialLoadAuthFiles(t, 1)
		queue := make(chan AuthUpdateBatch, 1)
		queue <- AuthUpdateBatch{Updates: []AuthUpdate{{Action: AuthUpdateActionAdd, ID: "sentinel"}}}
		w := newInitialLoadTestWatcher(t, authDir, queue)
		ctx, cancel := context.WithCancel(context.Background())
		done := w.StartInitialAuthLoad(ctx, 1)
		eventually(t, time.Second, func() bool { return len(w.SnapshotCoreAuths()) == 1 })

		start := make(chan struct{})
		cancelDone := make(chan struct{})
		drainDone := make(chan struct{})
		go func() {
			<-start
			cancel()
			close(cancelDone)
		}()
		go func() {
			<-start
			<-queue
			close(drainDone)
		}()
		close(start)
		<-cancelDone
		<-drainDone
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("attempt %d did not stop after cancellation", attempt)
		}
		select {
		case batch := <-queue:
			t.Fatalf("attempt %d delivered stale batch after cancellation: %+v", attempt, batch.Updates)
		default:
		}
	}
}

func TestInitialAuthLoadCompletionLogContainsOnlyAggregateMetrics(t *testing.T) {
	logger := log.StandardLogger()
	originalHooks := logger.ReplaceHooks(make(log.LevelHooks))
	originalLevel := logger.GetLevel()
	hook := &authLoadLogHook{}
	logger.AddHook(hook)
	logger.SetLevel(log.DebugLevel)
	defer func() {
		logger.ReplaceHooks(originalHooks)
		logger.SetLevel(originalLevel)
	}()

	authDir := t.TempDir()
	secretName := "secret-account-identity.json"
	mustWriteFile(t, filepath.Join(authDir, secretName), []byte(`{"type":"xai"}`))
	queue := make(chan AuthUpdateBatch, 1)
	w := newInitialLoadTestWatcher(t, authDir, queue)
	done := w.StartInitialAuthLoad(context.Background(), 1)
	acknowledgeInitialLoadUntilDone(t, queue, done)

	entry := hook.find("auth file load complete")
	if entry == nil {
		t.Fatal("missing auth load completion log")
	}
	for _, field := range []string{
		"files_discovered", "files_processed", "auths_loaded", "files_failed", "files_skipped",
		"directory_enumeration", "file_read", "native_synthesis", "plugin_synthesis", "batch_registration", "total_load",
	} {
		if _, ok := entry.Data[field]; !ok {
			t.Fatalf("completion log missing field %q: %+v", field, entry.Data)
		}
	}
	if strings.Contains(entry.Message, secretName) {
		t.Fatalf("completion log leaked file identity: %s", entry.Message)
	}
	for key, value := range entry.Data {
		if strings.Contains(fmt.Sprint(value), secretName) {
			t.Fatalf("completion log field %q leaked file identity", key)
		}
	}
}

type authLoadLogHook struct {
	mu      sync.Mutex
	entries []*log.Entry
}

func (h *authLoadLogHook) Levels() []log.Level { return log.AllLevels }

func (h *authLoadLogHook) Fire(entry *log.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	cloned := *entry
	cloned.Data = make(log.Fields, len(entry.Data))
	for key, value := range entry.Data {
		cloned.Data[key] = value
	}
	h.entries = append(h.entries, &cloned)
	return nil
}

func (h *authLoadLogHook) find(message string) *log.Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, entry := range h.entries {
		if entry.Message == message {
			return entry
		}
	}
	return nil
}

func newInitialLoadTestWatcher(t testing.TB, authDir string, queue chan AuthUpdateBatch) *Watcher {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	mustWriteFile(t, configPath, []byte("port: 8317\n"))
	w, errNew := NewWatcher(configPath, authDir, nil)
	if errNew != nil {
		t.Fatal(errNew)
	}
	w.SetConfig(&config.Config{AuthDir: authDir, AuthLoadWorkers: 16})
	w.SetAuthUpdateQueue(queue)
	t.Cleanup(func() { _ = w.Stop() })
	return w
}

type watcherMultiAuthParserFunc func(context.Context, pluginapi.AuthParseRequest) ([]*coreauth.Auth, bool, error)

func (f watcherMultiAuthParserFunc) ParseAuth(context.Context, pluginapi.AuthParseRequest) (*coreauth.Auth, bool, error) {
	return nil, false, nil
}

func (f watcherMultiAuthParserFunc) ParseAuths(ctx context.Context, req pluginapi.AuthParseRequest) ([]*coreauth.Auth, bool, error) {
	return f(ctx, req)
}

func mustWriteFile(t testing.TB, path string, data []byte) {
	t.Helper()
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
}

func writeInitialLoadAuthFiles(t testing.TB, count int) string {
	t.Helper()
	dir := t.TempDir()
	for i := 0; i < count; i++ {
		mustWriteFile(t, filepath.Join(dir, fmt.Sprintf("auth-%03d.json", i)), []byte(`{"type":"xai"}`))
	}
	return dir
}

func eventually(t testing.TB, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}

func loadedResults(updates []AuthUpdate) []AuthUpdateResult {
	results := make([]AuthUpdateResult, 0, len(updates))
	for _, update := range updates {
		results = append(results, AuthUpdateResult{ID: update.ID, Loaded: true})
	}
	return results
}

func acknowledgeInitialLoadUntilDone(t testing.TB, queue <-chan AuthUpdateBatch, done <-chan struct{}) {
	t.Helper()
	for {
		select {
		case batch := <-queue:
			// Late fsnotify events dispatch regular batches without a result channel.
			if batch.Result != nil {
				batch.Result <- loadedResults(batch.Updates)
			}
		case <-done:
			return
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for initial auth load")
		}
	}
}

func TestReloadConfigUpdatesAuthDirWatchAndSnapshots(t *testing.T) {
	tmpDir := t.TempDir()
	dirA := filepath.Join(tmpDir, "auth-a")
	dirB := filepath.Join(tmpDir, "auth-b")
	if errMkdirA := os.MkdirAll(dirA, 0o755); errMkdirA != nil {
		t.Fatalf("failed to create auth dir A: %v", errMkdirA)
	}
	if errMkdirB := os.MkdirAll(dirB, 0o755); errMkdirB != nil {
		t.Fatalf("failed to create auth dir B: %v", errMkdirB)
	}

	credAPath := filepath.Join(dirA, "cred-a.json")
	if errWriteA := os.WriteFile(credAPath, []byte(`{"type":"codex","email":"a@example.com"}`), 0o600); errWriteA != nil {
		t.Fatalf("failed to write cred-a.json: %v", errWriteA)
	}
	sharedAPath := filepath.Join(dirA, "shared.json")
	if errWriteSharedA := os.WriteFile(sharedAPath, []byte(`{"type":"codex","email":"shared-a@example.com"}`), 0o600); errWriteSharedA != nil {
		t.Fatalf("failed to write shared.json in dirA: %v", errWriteSharedA)
	}
	credBPath := filepath.Join(dirB, "cred-b.json")
	if errWriteB := os.WriteFile(credBPath, []byte(`{"type":"codex","email":"b@example.com"}`), 0o600); errWriteB != nil {
		t.Fatalf("failed to write cred-b.json: %v", errWriteB)
	}
	sharedBPath := filepath.Join(dirB, "shared.json")
	if errWriteSharedB := os.WriteFile(sharedBPath, []byte(`{"type":"codex","email":"shared-b@example.com"}`), 0o600); errWriteSharedB != nil {
		t.Fatalf("failed to write shared.json in dirB: %v", errWriteSharedB)
	}

	configPath := filepath.Join(tmpDir, "config.yaml")
	writeConfig := func(authDir string) {
		t.Helper()
		cfg := &config.Config{
			AuthDir:            authDir,
			CredentialInFlight: config.DefaultCredentialInFlightConfig(),
		}
		data, errMarshal := yaml.Marshal(cfg)
		if errMarshal != nil {
			t.Fatalf("failed to marshal config: %v", errMarshal)
		}
		if errWriteCfg := os.WriteFile(configPath, data, 0o600); errWriteCfg != nil {
			t.Fatalf("failed to write config: %v", errWriteCfg)
		}
	}
	writeConfig(dirA)

	w, errNewWatcher := NewWatcher(configPath, dirA, nil)
	if errNewWatcher != nil {
		t.Fatalf("NewWatcher failed: %v", errNewWatcher)
	}
	defer func() {
		if errStop := w.Stop(); errStop != nil {
			t.Errorf("failed to stop watcher: %v", errStop)
		}
	}()

	queue := make(chan AuthUpdateBatch, 16)
	w.SetAuthUpdateQueue(queue)
	w.SetConfig(&config.Config{
		AuthDir:            dirA,
		CredentialInFlight: config.DefaultCredentialInFlightConfig(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if errStart := w.Start(ctx); errStart != nil {
		t.Fatalf("Start failed: %v", errStart)
	}

	initialDone := w.StartInitialAuthLoad(ctx, 2)
	initialSeen := make(map[string]bool)
	for len(initialSeen) < 2 {
		select {
		case batch := <-queue:
			for _, initial := range batch.Updates {
				if initial.Action != AuthUpdateActionAdd {
					t.Fatalf("unexpected initial auth update: %+v", initial)
				}
				initialSeen[initial.ID] = true
			}
			if batch.Result != nil {
				batch.Result <- loadedResults(batch.Updates)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for initial updates, got %v", initialSeen)
		}
	}
	if !initialSeen["cred-a.json"] || !initialSeen["shared.json"] {
		t.Fatalf("expected cred-a.json and shared.json in initial updates, got %v", initialSeen)
	}

	<-initialDone

	// Switch auth-dir from dirA to dirB at runtime.
	writeConfig(dirB)
	w.ReloadConfigIfChanged()

	seenAddB := false
	seenModifyShared := false
	seenDeleteA := false
	for !seenAddB || !seenModifyShared || !seenDeleteA {
		select {
		case batch := <-queue:
			for _, update := range batch.Updates {
				if update.ID == "cred-b.json" && update.Action == AuthUpdateActionAdd {
					seenAddB = true
				}
				if update.ID == "shared.json" && update.Action == AuthUpdateActionModify && update.Auth != nil && update.Auth.Label == "shared-b@example.com" {
					seenModifyShared = true
				}
				if update.ID == "cred-a.json" && update.Action == AuthUpdateActionDelete {
					seenDeleteA = true
				}
			}
			if batch.Result != nil {
				batch.Result <- loadedResults(batch.Updates)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for auth-dir switch updates (seenAddB=%v, seenModifyShared=%v, seenDeleteA=%v)", seenAddB, seenModifyShared, seenDeleteA)
		}
	}

	snapshot := w.SnapshotCoreAuths()
	if len(snapshot) != 2 {
		t.Fatalf("expected SnapshotCoreAuths to return 2 entries from dirB, got %+v", snapshot)
	}
	w.clientsMutex.RLock()
	_, staleSharedIndexed := w.fileAuthsByPath[w.normalizeAuthPath(sharedAPath)]
	_, newSharedIndexed := w.fileAuthsByPath[w.normalizeAuthPath(sharedBPath)]
	w.clientsMutex.RUnlock()
	if staleSharedIndexed || !newSharedIndexed {
		t.Fatalf("expected fileAuthsByPath to drop dirA shared.json (%v) and keep dirB shared.json (%v)", staleSharedIndexed, newSharedIndexed)
	}

	fileSnapshot, errSnapshotFile := w.SnapshotAuthFile(credBPath)
	if errSnapshotFile != nil {
		t.Fatalf("SnapshotAuthFile(credBPath) failed: %v", errSnapshotFile)
	}
	if len(fileSnapshot) != 1 || fileSnapshot[0].ID != "cred-b.json" {
		t.Fatalf("expected SnapshotAuthFile(credBPath) ID=cred-b.json, got %+v", fileSnapshot)
	}

	// Events for the old directory must be ignored, while events in the new directory must be processed.
	credA2Path := filepath.Join(dirA, "cred-a2.json")
	if errWriteA2 := os.WriteFile(credA2Path, []byte(`{"type":"codex","email":"a2@example.com"}`), 0o600); errWriteA2 != nil {
		t.Fatalf("failed to write cred-a2.json: %v", errWriteA2)
	}
	w.handleEvent(fsnotify.Event{Name: credA2Path, Op: fsnotify.Create})

	credB2Path := filepath.Join(dirB, "cred-b2.json")
	if errWriteB2 := os.WriteFile(credB2Path, []byte(`{"type":"codex","email":"b2@example.com"}`), 0o600); errWriteB2 != nil {
		t.Fatalf("failed to write cred-b2.json: %v", errWriteB2)
	}

	select {
	case batch := <-queue:
		if len(batch.Updates) != 1 {
			t.Fatalf("expected one auth update, got %+v", batch.Updates)
		}
		update := batch.Updates[0]
		if update.Action != AuthUpdateActionAdd || update.ID != "cred-b2.json" {
			t.Fatalf("expected add update for cred-b2.json, got %+v", update)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fsnotify update from new auth-dir dirB")
	}
}

func TestSetConfigUpdatesAuthDir(t *testing.T) {
	tmpDir := t.TempDir()
	dirA := filepath.Join(tmpDir, "auth-a")
	dirB := filepath.Join(tmpDir, "auth-b")
	if errMkdirA := os.MkdirAll(dirA, 0o755); errMkdirA != nil {
		t.Fatalf("failed to create auth dir A: %v", errMkdirA)
	}
	if errMkdirB := os.MkdirAll(dirB, 0o755); errMkdirB != nil {
		t.Fatalf("failed to create auth dir B: %v", errMkdirB)
	}
	credBPath := filepath.Join(dirB, "cred-b.json")
	if errWriteB := os.WriteFile(credBPath, []byte(`{"type":"codex","email":"b@example.com"}`), 0o600); errWriteB != nil {
		t.Fatalf("failed to write cred-b.json: %v", errWriteB)
	}

	w, errNewWatcher := NewWatcher(filepath.Join(tmpDir, "config.yaml"), dirA, nil)
	if errNewWatcher != nil {
		t.Fatalf("NewWatcher failed: %v", errNewWatcher)
	}
	defer func() {
		if errStop := w.Stop(); errStop != nil {
			t.Errorf("failed to stop watcher: %v", errStop)
		}
	}()

	w.SetConfig(&config.Config{AuthDir: dirB})
	<-w.StartInitialAuthLoad(context.Background(), 1)

	snapshot := w.SnapshotCoreAuths()
	if len(snapshot) != 1 || snapshot[0].ID != "cred-b.json" {
		t.Fatalf("expected SnapshotCoreAuths to return cred-b.json from dirB, got %+v", snapshot)
	}
	fileSnapshot, errSnapshotFile := w.SnapshotAuthFile(credBPath)
	if errSnapshotFile != nil {
		t.Fatalf("SnapshotAuthFile failed: %v", errSnapshotFile)
	}
	if len(fileSnapshot) != 1 || fileSnapshot[0].ID != "cred-b.json" {
		t.Fatalf("expected SnapshotAuthFile ID=cred-b.json, got %+v", fileSnapshot)
	}
}

type blockingAuthDirSwitchParser struct {
	blockPath string
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (p *blockingAuthDirSwitchParser) ParseAuth(_ context.Context, req pluginapi.AuthParseRequest) (*coreauth.Auth, bool, error) {
	if filepath.Clean(req.Path) == filepath.Clean(p.blockPath) {
		p.once.Do(func() {
			close(p.entered)
		})
		<-p.release
	}
	return nil, false, nil
}

func TestAddOrUpdateClientDiscardsStaleDirEventAcrossAuthDirSwitch(t *testing.T) {
	tmpDir := t.TempDir()
	dirA := filepath.Join(tmpDir, "auth-a")
	dirB := filepath.Join(tmpDir, "auth-b")
	if errMkdirA := os.MkdirAll(dirA, 0o755); errMkdirA != nil {
		t.Fatalf("failed to create auth dir A: %v", errMkdirA)
	}
	if errMkdirB := os.MkdirAll(dirB, 0o755); errMkdirB != nil {
		t.Fatalf("failed to create auth dir B: %v", errMkdirB)
	}

	lateAPath := filepath.Join(dirA, "late-a.json")
	if errWriteLateA := os.WriteFile(lateAPath, []byte(`{"type":"codex","email":"late-a@example.com"}`), 0o600); errWriteLateA != nil {
		t.Fatalf("failed to write late-a.json: %v", errWriteLateA)
	}
	credBPath := filepath.Join(dirB, "cred-b.json")
	if errWriteB := os.WriteFile(credBPath, []byte(`{"type":"codex","email":"b@example.com"}`), 0o600); errWriteB != nil {
		t.Fatalf("failed to write cred-b.json: %v", errWriteB)
	}

	parser := &blockingAuthDirSwitchParser{
		blockPath: lateAPath,
		entered:   make(chan struct{}),
		release:   make(chan struct{}),
	}

	queue := make(chan AuthUpdateBatch, 16)
	w := &Watcher{
		authDir:          dirA,
		lastAuthHashes:   make(map[string]string),
		fileAuthsByPath:  make(map[string]map[string]*coreauth.Auth),
		pluginAuthParser: parser,
	}
	w.SetConfig(&config.Config{AuthDir: dirA})
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()

	done := make(chan struct{})
	go func() {
		w.addOrUpdateClientLocked(lateAPath)
		close(done)
	}()

	select {
	case <-parser.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for addOrUpdateClientLocked to reach synthesis")
	}

	// Switch auth-dir to dirB while the dirA event is paused mid-synthesis.
	w.SetConfig(&config.Config{AuthDir: dirB})
	w.refreshAuthState(false)

	select {
	case batch := <-queue:
		if len(batch.Updates) != 1 {
			t.Fatalf("expected one auth update, got %+v", batch.Updates)
		}
		update := batch.Updates[0]
		if update.Action != AuthUpdateActionAdd || update.ID != "cred-b.json" {
			t.Fatalf("expected add update for cred-b.json, got %+v", update)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cred-b.json update")
	}

	close(parser.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for paused addOrUpdateClientLocked to finish")
	}

	w.clientsMutex.RLock()
	currentCount := len(w.currentAuths)
	_, hasCredB := w.currentAuths["cred-b.json"]
	_, hasLateA := w.lastAuthHashes[w.normalizeAuthPath(lateAPath)]
	w.clientsMutex.RUnlock()
	if currentCount != 1 || !hasCredB || hasLateA {
		t.Fatalf("stale dirA event polluted state after switch: currentCount=%d, hasCredB=%v, hasLateA=%v", currentCount, hasCredB, hasLateA)
	}
}

func TestConcurrentSetConfigKeepsWatchedAuthDirOnLatestConfig(t *testing.T) {
	tmpDir := t.TempDir()
	dirA := filepath.Join(tmpDir, "auth-a")
	dirB := filepath.Join(tmpDir, "auth-b")
	dirC := filepath.Join(tmpDir, "auth-c")
	for _, dir := range []string{dirA, dirB, dirC} {
		if errMkdir := os.MkdirAll(dir, 0o755); errMkdir != nil {
			t.Fatalf("failed to create auth dir %s: %v", dir, errMkdir)
		}
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if errWriteCfg := os.WriteFile(configPath, []byte("auth_dir: "+dirA+"\n"), 0o600); errWriteCfg != nil {
		t.Fatalf("failed to write config: %v", errWriteCfg)
	}

	w, errNewWatcher := NewWatcher(configPath, dirA, nil)
	if errNewWatcher != nil {
		t.Fatalf("NewWatcher failed: %v", errNewWatcher)
	}
	defer func() {
		if errStop := w.Stop(); errStop != nil {
			t.Errorf("failed to stop watcher: %v", errStop)
		}
	}()

	queue := make(chan AuthUpdateBatch, 16)
	w.SetAuthUpdateQueue(queue)
	w.SetConfig(&config.Config{AuthDir: dirA})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if errStart := w.Start(ctx); errStart != nil {
		t.Fatalf("Start failed: %v", errStart)
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			w.SetConfig(&config.Config{AuthDir: dirB})
		}()
		go func() {
			defer wg.Done()
			w.SetConfig(&config.Config{AuthDir: dirC})
		}()
	}
	wg.Wait()
	w.SetConfig(&config.Config{AuthDir: dirC})

	w.watchMu.Lock()
	watched := w.watchedAuthDir
	w.watchMu.Unlock()
	if !w.sameNormalizedAuthPath(watched, dirC) || !w.sameNormalizedAuthPath(w.currentAuthDir(), dirC) {
		t.Fatalf("expected watchedAuthDir=%q and currentAuthDir=%q to match dirC=%q", watched, w.currentAuthDir(), dirC)
	}

	credCPath := filepath.Join(dirC, "cred-c.json")
	if errWriteC := os.WriteFile(credCPath, []byte(`{"type":"codex","email":"c@example.com"}`), 0o600); errWriteC != nil {
		t.Fatalf("failed to write cred-c.json: %v", errWriteC)
	}
	select {
	case batch := <-queue:
		if len(batch.Updates) != 1 {
			t.Fatalf("expected one auth update, got %+v", batch.Updates)
		}
		update := batch.Updates[0]
		if update.Action != AuthUpdateActionAdd || update.ID != "cred-c.json" {
			t.Fatalf("expected add update for cred-c.json, got %+v", update)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fsnotify update from dirC")
	}
}

func TestProgressiveAuthDirSwitchPreservesReplacementWithSameID(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	mustWriteFile(t, filepath.Join(dirA, "shared.json"), []byte(`{"type":"codex","email":"old@example.com"}`))
	mustWriteFile(t, filepath.Join(dirB, "shared.json"), []byte(`{"type":"codex","email":"new@example.com"}`))
	queue := make(chan AuthUpdateBatch, 4)
	w := newInitialLoadTestWatcher(t, dirA, queue)
	acknowledgeInitialLoadUntilDone(t, queue, w.StartInitialAuthLoad(context.Background(), 1))

	w.SetConfig(&config.Config{AuthDir: dirB})
	acknowledgeInitialLoadUntilDone(t, queue, w.StartInitialAuthLoad(context.Background(), 1))

	snapshot := w.SnapshotCoreAuths()
	if len(snapshot) != 1 || snapshot[0].ID != "shared.json" || snapshot[0].Label != "new@example.com" {
		t.Fatalf("expected replacement from new auth directory, got %+v", snapshot)
	}
	w.clientsMutex.RLock()
	current := w.currentAuths["shared.json"]
	w.clientsMutex.RUnlock()
	if current == nil || current.Label != "new@example.com" {
		t.Fatalf("new directory replacement removed during reconciliation: %+v", current)
	}
}

func TestProgressiveAuthDirSwitchCancelsInFlightScan(t *testing.T) {
	dirA := t.TempDir()
	dirB := t.TempDir()
	path := filepath.Join(dirA, "stale.json")
	mustWriteFile(t, path, []byte(`{"type":"xai"}`))
	readCaptured := make(chan struct{})
	releaseRead := make(chan struct{})
	originalRead := readInitialAuthFile
	defer func() { readInitialAuthFile = originalRead }()
	readInitialAuthFile = func(path string) ([]byte, error) {
		data, errRead := os.ReadFile(path)
		close(readCaptured)
		<-releaseRead
		return data, errRead
	}
	queue := make(chan AuthUpdateBatch, 4)
	w := newInitialLoadTestWatcher(t, dirA, queue)
	done := w.StartInitialAuthLoad(context.Background(), 1)
	<-readCaptured
	w.SetConfig(&config.Config{AuthDir: dirB})
	close(releaseRead)
	<-done

	select {
	case batch := <-queue:
		t.Fatalf("old auth directory scan emitted batch: %+v", batch.Updates)
	default:
	}
	if snapshot := w.SnapshotCoreAuths(); len(snapshot) != 0 {
		t.Fatalf("old auth directory scan polluted snapshot: %+v", snapshot)
	}
	if status := w.AuthLoadStatus(); status.State == AuthLoadStateReady {
		t.Fatalf("old auth directory scan published ready status: %+v", status)
	}
}
