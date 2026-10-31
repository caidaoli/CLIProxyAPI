package cliproxy

import (
	"context"
	"net/http"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executionregistry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

type staleStateRawJSONStorage struct {
	data []byte
}

func (s *staleStateRawJSONStorage) RawJSON() []byte {
	if s == nil {
		return nil
	}
	return append([]byte(nil), s.data...)
}

func (*staleStateRawJSONStorage) SaveTokenToFile(string) error {
	return nil
}

func TestServiceApplyCoreAuthAddOrUpdate_DeleteReAddDoesNotInheritStaleRuntimeState(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	authID := "service-stale-state-auth"
	modelID := "stale-model"
	lastRefreshedAt := time.Date(2026, time.March, 1, 8, 0, 0, 0, time.UTC)
	nextRefreshAfter := lastRefreshedAt.Add(30 * time.Minute)

	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:               authID,
		Provider:         "claude",
		Status:           coreauth.StatusActive,
		LastRefreshedAt:  lastRefreshedAt,
		NextRefreshAfter: nextRefreshAfter,
		ModelStates: map[string]*coreauth.ModelState{
			modelID: {
				Quota: coreauth.QuotaState{BackoffLevel: 7},
			},
		},
	})

	service.applyCoreAuthRemoval(context.Background(), authID)

	if _, ok := service.coreManager.GetByID(authID); ok {
		t.Fatalf("expected auth %q to be removed from runtime state", authID)
	}

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	})

	updated, ok := service.coreManager.GetByID(authID)
	if !ok || updated == nil {
		t.Fatalf("expected re-added auth to be present")
	}
	if updated.Disabled {
		t.Fatalf("expected re-added auth to be active")
	}
	if !updated.LastRefreshedAt.IsZero() {
		t.Fatalf("expected LastRefreshedAt to reset on delete -> re-add, got %v", updated.LastRefreshedAt)
	}
	if !updated.NextRefreshAfter.IsZero() {
		t.Fatalf("expected NextRefreshAfter to reset on delete -> re-add, got %v", updated.NextRefreshAfter)
	}
	if len(updated.ModelStates) != 0 {
		t.Fatalf("expected ModelStates to reset on delete -> re-add, got %d entries", len(updated.ModelStates))
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) == 0 {
		t.Fatalf("expected re-added auth to re-register models in global registry")
	}
}

func TestHandleAuthUpdate_ReplaceMaterialClearsRuntimeErrorState(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	ctx := context.Background()
	authID := "codex-user@example.com.json"
	modelID := "gpt-5"

	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	if _, errRegister := service.coreManager.Register(ctx, &coreauth.Auth{
		ID:       authID,
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"access_token": "invalid-token"},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	service.coreManager.MarkResult(ctx, coreauth.Result{
		AuthID:   authID,
		Provider: "codex",
		Model:    modelID,
		Success:  false,
		Error: &coreauth.Error{
			Code:       "unauthorized",
			Message:    "401 Your authentication token has been invalidated",
			HTTPStatus: http.StatusUnauthorized,
		},
	})

	failedAuth, ok := service.coreManager.GetByID(authID)
	if !ok || failedAuth == nil {
		t.Fatalf("expected auth %q to exist", authID)
	}
	if len(failedAuth.ModelStates) == 0 || failedAuth.LastError == nil {
		t.Fatalf("expected runtime error state before replacement, got %+v", failedAuth)
	}

	service.handleAuthUpdate(ctx, watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionModify,
		ID:     authID,
		Auth: &coreauth.Auth{
			ID:       authID,
			Provider: "codex",
			Status:   coreauth.StatusActive,
			Metadata: map[string]any{"access_token": "replacement-token"},
		},
		ReplaceMaterial: true,
	})

	updated, ok := service.coreManager.GetByID(authID)
	if !ok || updated == nil {
		t.Fatalf("expected updated auth %q to exist", authID)
	}
	if updated.Status != coreauth.StatusActive {
		t.Fatalf("status = %q, want %q", updated.Status, coreauth.StatusActive)
	}
	if updated.LastError != nil || updated.StatusMessage != "" || updated.Unavailable || !updated.NextRetryAfter.IsZero() {
		t.Fatalf("runtime error state survived replacement: status_message=%q unavailable=%v next=%v last_error=%+v",
			updated.StatusMessage, updated.Unavailable, updated.NextRetryAfter, updated.LastError)
	}
	if len(updated.ModelStates) != 0 {
		t.Fatalf("model states survived replacement: %+v", updated.ModelStates)
	}
}

func TestHandleAuthUpdate_ReplacePluginStorageClearsRuntimeErrorState(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	ctx := context.Background()
	authID := "plugin-storage.json"
	modelID := "plugin-model"
	metadata := map[string]any{"type": "plugin-provider"}

	t.Cleanup(func() {
		GlobalModelRegistry().UnregisterClient(authID)
	})

	if _, errRegister := service.coreManager.Register(ctx, &coreauth.Auth{
		ID:       authID,
		Provider: "plugin-provider",
		Status:   coreauth.StatusActive,
		Metadata: metadata,
		Storage:  &staleStateRawJSONStorage{data: []byte(`{"type":"plugin-provider","token":"invalid-token"}`)},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	service.coreManager.MarkResult(ctx, coreauth.Result{
		AuthID:   authID,
		Provider: "plugin-provider",
		Model:    modelID,
		Success:  false,
		Error: &coreauth.Error{
			Code:       "unauthorized",
			Message:    "401 plugin token invalidated",
			HTTPStatus: http.StatusUnauthorized,
		},
	})

	service.handleAuthUpdate(ctx, watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionModify,
		ID:     authID,
		Auth: &coreauth.Auth{
			ID:       authID,
			Provider: "plugin-provider",
			Status:   coreauth.StatusActive,
			Metadata: metadata,
			Storage:  &staleStateRawJSONStorage{data: []byte(`{"token":"replacement-token","type":"plugin-provider"}`)},
		},
		ReplaceMaterial: true,
	})

	updated, ok := service.coreManager.GetByID(authID)
	if !ok || updated == nil {
		t.Fatalf("expected updated auth %q to exist", authID)
	}
	if updated.LastError != nil || updated.Unavailable || len(updated.ModelStates) != 0 {
		t.Fatalf("runtime error state survived plugin storage replacement: %+v", updated)
	}
}

func TestHandleAuthUpdate_RefreshFileEchoPreservesQuotaCooldown(t *testing.T) {
	tests := []struct {
		name             string
		persistCooldowns bool
	}{
		{name: "in_memory"},
		{name: "persisted", persistCooldowns: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			manager := coreauth.NewManager(nil, nil, nil)
			var cooldownStore *coreauth.FileCooldownStateStore
			if tc.persistCooldowns {
				cooldownStore = coreauth.NewFileCooldownStateStore(t.TempDir())
				manager.SetCooldownStateStore(cooldownStore)
			}
			service := &Service{
				cfg:         &config.Config{},
				coreManager: manager,
			}

			ctx := context.Background()
			authID := "codex-quota-" + tc.name + ".json"
			modelID := "gpt-5.6-sol"
			retryAfter := 6 * time.Hour

			t.Cleanup(func() {
				GlobalModelRegistry().UnregisterClient(authID)
			})

			if _, errRegister := manager.Register(ctx, &coreauth.Auth{
				ID:       authID,
				Provider: "codex",
				Status:   coreauth.StatusActive,
				Metadata: map[string]any{"access_token": "initial-token", "email": "quota@example.com"},
				Storage:  &staleStateRawJSONStorage{data: []byte(`{"type":"codex","token":"initial-token"}`)},
			}); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}

			manager.MarkResult(ctx, coreauth.Result{
				AuthID:     authID,
				Provider:   "codex",
				Model:      modelID,
				Success:    false,
				RetryAfter: &retryAfter,
				Error: &coreauth.Error{
					Code:       "usage_limit_reached",
					Message:    "usage limit reached",
					HTTPStatus: http.StatusTooManyRequests,
				},
			})
			if cooldownStore != nil {
				if errFlush := manager.FlushCooldownStates(ctx); errFlush != nil {
					t.Fatalf("flush cooldown before replacement: %v", errFlush)
				}
			}

			failedAuth, ok := manager.GetByID(authID)
			if !ok || failedAuth == nil {
				t.Fatalf("expected auth %q to exist", authID)
			}
			failedState := failedAuth.ModelStates[modelID]
			if failedState == nil || !failedState.Unavailable || !failedState.Quota.Exceeded {
				t.Fatalf("expected quota cooldown before replacement, got %+v", failedState)
			}

			refreshedAuth := failedAuth.Clone()
			refreshedAuth.Metadata["access_token"] = "refreshed-token"
			refreshedAuth.Metadata["expires_in"] = int64(3600)
			refreshedAuth.Metadata["limits"] = map[string]any{"requests": int64(2)}
			refreshedAuth.Storage = &staleStateRawJSONStorage{data: []byte(`{"type":"codex","token":"refreshed-token","limits":{"requests":2}}`)}
			if _, errUpdate := manager.Update(ctx, refreshedAuth); errUpdate != nil {
				t.Fatalf("apply refreshed credential: %v", errUpdate)
			}
			refreshed, ok := manager.GetByID(authID)
			if !ok || refreshed.ModelStates[modelID] == nil || !refreshed.ModelStates[modelID].Quota.Exceeded {
				t.Fatalf("internal refresh cleared quota cooldown before file echo: %+v", refreshed)
			}

			service.handleAuthUpdate(ctx, watcher.AuthUpdate{
				Action: watcher.AuthUpdateActionModify,
				ID:     authID,
				Auth: &coreauth.Auth{
					ID:       authID,
					Provider: "codex",
					Status:   coreauth.StatusActive,
					Metadata: map[string]any{
						"access_token": "refreshed-token",
						"disabled":     false,
						"email":        "quota@example.com",
						"expires_in":   float64(3600),
						"limits":       map[string]any{"requests": float64(2)},
					},
					Storage: &staleStateRawJSONStorage{data: []byte(`{
						"limits":{"requests":2.0},
						"token":"refreshed-token",
						"type":"codex"
					}`)},
				},
				ReplaceMaterial: true,
			})

			updated, ok := manager.GetByID(authID)
			if !ok || updated == nil {
				t.Fatalf("expected updated auth %q to exist", authID)
			}
			updatedState := updated.ModelStates[modelID]
			if updatedState == nil || !updatedState.Unavailable || !updatedState.Quota.Exceeded {
				t.Fatalf("quota cooldown was cleared by material replacement: %+v", updatedState)
			}
			if !updatedState.NextRetryAfter.Equal(failedState.NextRetryAfter) {
				t.Fatalf("next retry changed from %v to %v", failedState.NextRetryAfter, updatedState.NextRetryAfter)
			}

			if cooldownStore != nil {
				if errFlush := manager.FlushCooldownStates(ctx); errFlush != nil {
					t.Fatalf("flush cooldown after replacement: %v", errFlush)
				}
				snapshots, errLoad := cooldownStore.Load(ctx)
				if errLoad != nil {
					t.Fatalf("load persisted cooldown: %v", errLoad)
				}
				if len(snapshots) != 1 || len(snapshots[0].Records) != 1 || snapshots[0].Records[0].Model != modelID {
					t.Fatalf("persisted cooldown was cleared by material replacement: %+v", snapshots)
				}
			}
		})
	}
}

func TestForceHomeRuntimeConfigEnablesUsageStatistics(t *testing.T) {
	cfg := &config.Config{
		UsageStatisticsEnabled: false,
		DisableCooling:         false,
		SaveCooldownStatus:     true,
		WebsocketAuth:          false,
	}

	forceHomeRuntimeConfig(cfg)

	if !cfg.UsageStatisticsEnabled {
		t.Fatal("expected home runtime config to force usage statistics enabled")
	}
	if !cfg.DisableCooling {
		t.Fatal("expected home runtime config to force cooling disabled")
	}
	if cfg.SaveCooldownStatus {
		t.Fatal("expected home runtime config to force cooldown status persistence disabled")
	}
	if !cfg.WebsocketAuth {
		t.Fatal("expected home runtime config to force websocket authentication enabled")
	}
}

func TestLifetimeRegistryObservesBarrierFromAppliedHomeConfig(t *testing.T) {
	registry := executionregistry.New()
	manager := coreauth.NewManager(nil, nil, nil)
	cfg := internalconfig.DefaultCredentialInFlightConfig()
	cfg.SnapshotInterval = "30ms"

	if errApply := applyHomeInFlightPublisherConfig(manager, cfg); errApply != nil {
		t.Fatal(errApply)
	}
	applyHomeObservationBarrier(registry, 14)

	if freeze := registry.FreezeInFlight(time.Now().UTC()); freeze.BarrierRevision != 14 {
		t.Fatalf("barrier revision = %d, want 14", freeze.BarrierRevision)
	}
	if got := manager.HomeInFlightPublisherConfig(); got.SnapshotInterval != 30*time.Millisecond {
		t.Fatalf("publisher interval = %v, want 30ms", got.SnapshotInterval)
	}
}

func TestApplyHomeOverlayDoesNotApplyWithoutReadyClient(t *testing.T) {
	baseCfg := &config.Config{UsageStatisticsEnabled: false, SaveCooldownStatus: true}
	baseCfg.Home.Enabled = true
	service := &Service{cfg: baseCfg}

	service.applyHomeOverlay(&config.Config{
		UsageStatisticsEnabled: false,
		SaveCooldownStatus:     true,
	})

	if service.cfg == nil || service.cfg.UsageStatisticsEnabled {
		t.Fatal("unready home overlay changed usage statistics")
	}
	if !service.cfg.Home.Enabled {
		t.Fatal("unready home overlay changed local home settings")
	}
	if !service.cfg.SaveCooldownStatus {
		t.Fatal("unready home overlay changed cooldown status persistence")
	}
}
