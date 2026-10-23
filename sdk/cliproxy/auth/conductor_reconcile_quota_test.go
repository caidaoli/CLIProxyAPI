package auth

import (
	"context"
	"net/http"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// ReconcileRegistryModelStates must preserve active quota cooldown state.
// Previously it reset all non-clean model states unconditionally, which cleared
// quota cooldowns set by MarkResult and allowed the scheduler to re-pick
// exhausted credentials.
func TestReconcileRegistryModelStates_PreservesActiveQuotaCooldown(t *testing.T) {
	prev := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prev) })

	model := "gpt-5.5"
	registerSchedulerModels(t, "codex", model, "quota-preserve-1")

	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.executors["codex"] = schedulerTestExecutor{}
	if _, err := m.Register(context.Background(), &Auth{ID: "quota-preserve-1", Provider: "codex"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	retryAfter := 3 * time.Hour
	m.MarkResult(context.Background(), Result{
		AuthID:     "quota-preserve-1",
		Provider:   "codex",
		Model:      model,
		Success:    false,
		Error:      &Error{Code: "quota", Message: "usage limit reached", HTTPStatus: http.StatusTooManyRequests},
		RetryAfter: &retryAfter,
	})

	auth, ok := m.GetByID("quota-preserve-1")
	if !ok || auth == nil {
		t.Fatal("auth missing after MarkResult")
	}
	state := auth.ModelStates[model]
	if state == nil || !state.Quota.Exceeded {
		t.Fatalf("expected quota exceeded state after MarkResult, got %+v", state)
	}

	m.ReconcileRegistryModelStates(context.Background(), "quota-preserve-1")

	auth, ok = m.GetByID("quota-preserve-1")
	if !ok || auth == nil {
		t.Fatal("auth missing after reconcile")
	}
	state = auth.ModelStates[model]
	if state == nil {
		t.Fatalf("model state %q was deleted by reconcile", model)
	}
	if !state.Quota.Exceeded {
		t.Fatalf("ReconcileRegistryModelStates cleared active quota cooldown; want Exceeded=true, got %+v", state.Quota)
	}
	if state.Quota.NextRecoverAt.IsZero() {
		t.Fatalf("ReconcileRegistryModelStates cleared NextRecoverAt; want non-zero")
	}

	// Verify the scheduler still blocks the auth.
	for i := range 5 {
		picked, _, err := m.pickNext(context.Background(), "codex", model, cliproxyexecutor.Options{}, map[string]struct{}{})
		if err == nil && picked != nil && picked.ID == "quota-preserve-1" {
			t.Fatalf("pickNext #%d returned quota-exhausted auth after reconcile", i)
		}
	}
}

// ReconcileRegistryModelStates must preserve active credential cooldowns even
// when they are not quota failures. Model re-registration must not make an
// exhausted credential immediately selectable again.
func TestReconcileRegistryModelStates_PreservesActivePaymentCooldown(t *testing.T) {
	model := "grok-4.5"
	authID := "payment-cooldown-1"
	registerSchedulerModels(t, "xai", model, authID)

	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.RegisterExecutor(schedulerProviderTestExecutor{provider: "xai"})
	if _, err := m.Register(context.Background(), &Auth{ID: authID, Provider: "xai"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	m.MarkResult(context.Background(), Result{
		AuthID:   authID,
		Provider: "xai",
		Model:    model,
		Success:  false,
		Error:    &Error{Message: `{"error":"Grok Build usage balance exhausted"}`, HTTPStatus: http.StatusPaymentRequired},
	})

	m.ReconcileRegistryModelStates(context.Background(), authID)

	auth, ok := m.GetByID(authID)
	if !ok || auth == nil {
		t.Fatal("auth missing")
	}
	state := auth.ModelStates[model]
	if state == nil || !state.Unavailable || !state.NextRetryAfter.After(time.Now()) {
		t.Fatalf("active payment cooldown was cleared by reconcile: %+v", state)
	}

	picked, errSelect := m.SelectAuth(context.Background(), "xai", model, cliproxyexecutor.Options{})
	if errSelect == nil || picked != nil {
		t.Fatalf("SelectAuth returned cooling auth after reconcile: auth=%+v err=%v", picked, errSelect)
	}
}

// Expired quota cooldown should be reset by reconcile.
func TestReconcileRegistryModelStates_ResetsExpiredQuotaCooldown(t *testing.T) {
	model := "gpt-5.5"
	registerSchedulerModels(t, "codex", model, "expired-quota-1")

	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.executors["codex"] = schedulerTestExecutor{}
	if _, err := m.Register(context.Background(), &Auth{ID: "expired-quota-1", Provider: "codex"}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Manually set an expired quota cooldown.
	m.mu.Lock()
	auth := m.auths["expired-quota-1"]
	if auth.ModelStates == nil {
		auth.ModelStates = make(map[string]*ModelState)
	}
	auth.ModelStates[model] = &ModelState{
		Unavailable:    true,
		Status:         StatusError,
		NextRetryAfter: time.Now().Add(-1 * time.Hour),
		Quota: QuotaState{
			Exceeded:      true,
			Reason:        "quota",
			NextRecoverAt: time.Now().Add(-1 * time.Hour),
		},
	}
	m.mu.Unlock()

	m.ReconcileRegistryModelStates(context.Background(), "expired-quota-1")

	auth, ok := m.GetByID("expired-quota-1")
	if !ok || auth == nil {
		t.Fatal("auth missing")
	}
	state := auth.ModelStates[model]
	if state != nil && state.Quota.Exceeded {
		t.Fatalf("expected expired quota to be reset by reconcile, got %+v", state.Quota)
	}
}

func TestReconcileRegistryModelStates_PreservesIndependentAuthCooldown(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "active"
		if expired {
			name = "expired"
		}
		t.Run(name, func(t *testing.T) {
			ctx := WithSkipPersist(context.Background())
			authID := "independent-auth-cooldown-" + name
			registerSchedulerModels(t, "codex", "gpt-5.5", authID)
			m := NewManager(nil, nil, nil)
			retry := time.Now().Add(time.Hour)
			if expired {
				retry = time.Now().Add(-time.Hour)
			}
			if _, errRegister := m.Register(ctx, &Auth{
				ID: authID, Provider: "codex", Unavailable: true, Status: StatusError,
				NextRetryAfter: retry, LastError: &Error{HTTPStatus: http.StatusTooManyRequests, Message: "quota exhausted"},
				Quota: QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: retry},
			}); errRegister != nil {
				t.Fatal(errRegister)
			}
			m.ReconcileRegistryModelStates(ctx, authID)
			got, ok := m.GetByID(authID)
			if !ok {
				t.Fatal("credential missing after reconciliation")
			}
			if expired {
				if got.Unavailable || got.Quota.Exceeded || !got.NextRetryAfter.IsZero() {
					t.Fatalf("expired auth cooldown retained after reconciliation: %+v", got)
				}
			} else if !got.Unavailable || !got.Quota.Exceeded || !got.NextRetryAfter.Equal(retry) || got.LastError == nil {
				t.Fatalf("independent auth cooldown cleared by model reconciliation: %+v", got)
			}
		})
	}
}
