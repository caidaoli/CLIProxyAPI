package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalusage "github.com/router-for-me/CLIProxyAPI/v8/internal/usage"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

type deleteTrackingStore struct {
	mu         sync.Mutex
	deletedIDs []string
}

func (s *deleteTrackingStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *deleteTrackingStore) Save(context.Context, *Auth) (string, error) { return "", nil }

func (s *deleteTrackingStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedIDs = append(s.deletedIDs, id)
	return nil
}

func (s *deleteTrackingStore) deleted() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.deletedIDs...)
}

type blockingDeleteStore struct {
	mu            sync.Mutex
	auths         map[string]*Auth
	deleteStarted chan struct{}
	allowDelete   chan struct{}
	deleteOnce    sync.Once
}

func newBlockingDeleteStore() *blockingDeleteStore {
	return &blockingDeleteStore{
		auths:         make(map[string]*Auth),
		deleteStarted: make(chan struct{}),
		allowDelete:   make(chan struct{}),
	}
}

func (s *blockingDeleteStore) List(context.Context) ([]*Auth, error) { return nil, nil }

func (s *blockingDeleteStore) Save(_ context.Context, auth *Auth) (string, error) {
	s.mu.Lock()
	s.auths[auth.ID] = auth.Clone()
	s.mu.Unlock()
	return auth.ID, nil
}

func (s *blockingDeleteStore) Delete(_ context.Context, id string) error {
	s.deleteOnce.Do(func() { close(s.deleteStarted) })
	<-s.allowDelete
	s.mu.Lock()
	delete(s.auths, id)
	s.mu.Unlock()
	return nil
}

func (s *blockingDeleteStore) auth(id string) (*Auth, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	auth, ok := s.auths[id]
	return auth.Clone(), ok
}

type refreshOAuthError struct {
	status  int
	code    string
	message string
}

func (e *refreshOAuthError) Error() string {
	return fmt.Sprintf("token refresh failed with status %d: %s", e.status, e.message)
}

func (e *refreshOAuthError) StatusCode() int { return e.status }

func (e *refreshOAuthError) OAuthErrorCode() string { return e.code }

type oauthRefreshFailureExecutor struct {
	schedulerProviderTestExecutor
	err   error
	calls atomic.Int32
}

func (e *oauthRefreshFailureExecutor) Refresh(context.Context, *Auth) (*Auth, error) {
	e.calls.Add(1)
	return nil, e.err
}

func registerExpiredRefreshAuth(t *testing.T, manager *Manager, id, provider string) {
	t.Helper()
	auth := &Auth{
		ID:       id,
		Provider: provider,
		Metadata: map[string]any{
			"access_token":             "expired-access-token",
			"refresh_token":            "refresh-token",
			"expired":                  time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
			"refresh_interval_seconds": 1,
		},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
}

func waitForAuthCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestManagerMarkResultKeepsUnauthorizedAuthWhenDeleteDisabled(t *testing.T) {
	SetDeleteUnauthorizedAuth(false)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := &deleteTrackingStore{}
	manager := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex"},
	}
	if _, err := manager.Register(WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	manager.MarkResult(context.Background(), Result{
		AuthID:  "auth-1",
		Model:   "gpt-5",
		Success: false,
		Error:   &Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
	})

	if _, ok := manager.GetByID("auth-1"); !ok {
		t.Fatal("expected auth to remain registered")
	}
	if got := store.deleted(); len(got) != 0 {
		t.Fatalf("expected no deleted auths, got %v", got)
	}
}

func TestManagerMarkResultDeletesUnauthorizedAuthWhenEnabled(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := &deleteTrackingStore{}
	manager := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex"},
	}
	if _, err := manager.Register(WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	manager.MarkResult(context.Background(), Result{
		AuthID:  "auth-1",
		Model:   "gpt-5",
		Success: false,
		Error:   &Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
	})

	if _, ok := manager.GetByID("auth-1"); ok {
		t.Fatal("expected auth to be removed from manager")
	}
	if got := store.deleted(); len(got) != 1 || got[0] != "auth-1" {
		t.Fatalf("expected auth-1 to be deleted, got %v", got)
	}
}

func TestManagerMarkResultDeletesUsageForUnauthorizedAuth(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	oldStatisticsEnabled := internalusage.StatisticsEnabled()
	internalusage.SetStatisticsEnabled(true)
	t.Cleanup(func() { internalusage.SetStatisticsEnabled(oldStatisticsEnabled) })

	store := &deleteTrackingStore{}
	manager := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "usage-delete-auth",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	authIndex := auth.EnsureIndex()
	stats := internalusage.GetRequestStatistics()
	t.Cleanup(func() { stats.DeleteAuthUsage([]string{authIndex}) })
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "usage-delete-test",
		Model:       "gpt-5",
		AuthIndex:   authIndex,
		RequestedAt: time.Now(),
		Detail:      coreusage.Detail{TotalTokens: 1},
	})

	manager.MarkResult(context.Background(), Result{
		AuthID:  auth.ID,
		Model:   "gpt-5",
		Success: false,
		Error:   &Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
	})

	for _, apiStats := range stats.Snapshot().APIs {
		for _, modelStats := range apiStats.Models {
			for _, detail := range modelStats.Details {
				if detail.AuthIndex == authIndex {
					t.Fatalf("usage for deleted auth index %q was retained", authIndex)
				}
			}
		}
	}
}

func TestManagerMarkResultKeepsXAICredentialFailures(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	for _, tc := range []struct {
		name       string
		statusCode int
		message    string
	}{
		{name: "permission denied", statusCode: http.StatusForbidden, message: `{"code":"permission-denied","error":"Access denied."}`},
		{name: "unauthorized", statusCode: http.StatusUnauthorized, message: "unauthorized"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &deleteTrackingStore{}
			manager := NewManager(store, nil, nil)
			manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "xai"})
			auth := &Auth{
				ID:       "xai-" + strings.ReplaceAll(tc.name, " ", "-"),
				Provider: "xai",
				Metadata: map[string]any{"type": "xai", "refresh_token": "refresh-token"},
			}
			if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}

			manager.MarkResult(context.Background(), Result{
				AuthID:   auth.ID,
				Provider: "xai",
				Model:    "grok-4.5",
				Success:  false,
				Error:    &Error{HTTPStatus: tc.statusCode, Message: tc.message},
			})

			if _, ok := manager.GetByID(auth.ID); !ok {
				t.Fatal("expected xai OAuth auth to remain registered")
			}
			if got := store.deleted(); len(got) != 0 {
				t.Fatalf("deleted auths = %v, want none", got)
			}
		})
	}
}

func TestManagerMarkResultKeepsNonXAIPermissionDenied(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := &deleteTrackingStore{}
	manager := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "codex-permission-denied",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	manager.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: "codex",
		Model:    "gpt-5",
		Success:  false,
		Error: &Error{
			HTTPStatus: http.StatusForbidden,
			Message:    `{"code":"permission-denied","error":"Access denied."}`,
		},
	})

	if _, ok := manager.GetByID(auth.ID); !ok {
		t.Fatal("expected non-xai auth to remain registered")
	}
	if got := store.deleted(); len(got) != 0 {
		t.Fatalf("deleted auths = %v, want none", got)
	}
}

func TestManagerMarkResultKeepsXAIPermissionDeniedWhenDeleteDisabled(t *testing.T) {
	SetDeleteUnauthorizedAuth(false)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := &deleteTrackingStore{}
	manager := NewManager(store, nil, nil)
	auth := &Auth{
		ID:       "xai-permission-denied-retained",
		Provider: "xai",
		Metadata: map[string]any{"type": "xai"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	manager.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: "xai",
		Model:    "grok-4.5",
		Success:  false,
		Error: &Error{
			HTTPStatus: http.StatusForbidden,
			Message:    `{"code":"permission-denied","error":"Access denied."}`,
		},
	})

	if _, ok := manager.GetByID(auth.ID); !ok {
		t.Fatal("expected xai auth to remain when deletion is disabled")
	}
	if got := store.deleted(); len(got) != 0 {
		t.Fatalf("deleted auths = %v, want none", got)
	}
}

func TestManagerAutoRefreshKeepsXAIInvalidGrantWithUpstreamLifecycle(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := &deleteTrackingStore{}
	manager := NewManager(store, nil, nil)
	executor := &oauthRefreshFailureExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "xai"},
		err:                           fmt.Errorf(`xai token request failed with status 400: {"error":"invalid_grant"}`),
	}
	manager.RegisterExecutor(executor)
	registerExpiredRefreshAuth(t, manager, "xai-retained", "xai")

	manager.StartAutoRefresh(context.Background(), time.Millisecond)
	t.Cleanup(manager.StopAutoRefresh)
	waitForAuthCondition(t, func() bool {
		auth, ok := manager.GetByID("xai-retained")
		return ok && auth.LastError != nil && !auth.NextRefreshAfter.IsZero()
	}, "xai invalid_grant backoff")

	retained, ok := manager.GetByID("xai-retained")
	if !ok || retained == nil {
		t.Fatal("expected invalid auth to remain registered")
	}
	if !retained.Unavailable || retained.Status != StatusError || retained.StatusMessage != "invalid grant (retrying)" {
		t.Fatalf("retained auth state = unavailable:%v status:%s message:%q, want upstream invalid grant backoff", retained.Unavailable, retained.Status, retained.StatusMessage)
	}
	if retained.LastError.Code != "" || retained.LastError.HTTPStatus != 0 {
		t.Fatalf("LastError = %#v, want unclassified upstream refresh error", retained.LastError)
	}
	if _, shouldSchedule := nextRefreshCheckAt(time.Now(), retained, time.Second); !shouldSchedule {
		t.Fatal("expected xai invalid_grant to remain on the upstream refresh schedule")
	}
	if got := store.deleted(); len(got) != 0 {
		t.Fatalf("deleted auths = %v, want none", got)
	}
}

func TestManagerRefreshXAIOAuthFailureFollowsUpstreamLifecycle(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	tests := []struct {
		name              string
		err               error
		failedAccessToken string
		wantMessage       string
		wantScheduled     bool
	}{
		{
			name:          "expired token transient failure",
			err:           &refreshOAuthError{status: http.StatusInternalServerError, message: "upstream unavailable"},
			wantMessage:   "token expired",
			wantScheduled: true,
		},
		{
			name:              "rejected token invalid grant",
			err:               fmt.Errorf(`xai token request failed with status 400: {"error":"invalid_grant"}`),
			failedAccessToken: "expired-access-token",
			wantMessage:       "unauthorized (refresh token invalid)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &deleteTrackingStore{}
			manager := NewManager(store, nil, nil)
			manager.RegisterExecutor(&oauthRefreshFailureExecutor{
				schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "xai"},
				err:                           tt.err,
			})
			registerExpiredRefreshAuth(t, manager, "xai-upstream", "xai")

			if _, errRefresh := manager.refreshAuthForRequest(context.Background(), "xai-upstream", tt.failedAccessToken); errRefresh == nil {
				t.Fatal("refreshAuthForRequest() error = nil, want refresh failure")
			}

			updated, ok := manager.GetByID("xai-upstream")
			if !ok {
				t.Fatal("expected xai auth to remain registered")
			}
			if !updated.Unavailable || updated.Status != StatusError || updated.StatusMessage != tt.wantMessage {
				t.Fatalf("auth state = unavailable:%v status:%s message:%q, want unavailable error %q", updated.Unavailable, updated.Status, updated.StatusMessage, tt.wantMessage)
			}
			if _, scheduled := nextRefreshCheckAt(time.Now(), updated, time.Second); scheduled != tt.wantScheduled {
				t.Fatalf("refresh scheduled = %v, want %v", scheduled, tt.wantScheduled)
			}
			if got := store.deleted(); len(got) != 0 {
				t.Fatalf("deleted auths = %v, want none", got)
			}
		})
	}
}

func TestManagerAutoRefreshKeepsNonInvalidGrantBadRequest(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := &deleteTrackingStore{}
	manager := NewManager(store, nil, nil)
	executor := &oauthRefreshFailureExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "refresh-test"},
		err:                           &refreshOAuthError{status: http.StatusBadRequest, code: "invalid_request", message: "malformed request"},
	}
	manager.RegisterExecutor(executor)
	registerExpiredRefreshAuth(t, manager, "xai-bad-request", "refresh-test")

	refreshStartedAt := time.Now()
	manager.StartAutoRefresh(context.Background(), time.Millisecond)
	t.Cleanup(manager.StopAutoRefresh)
	waitForAuthCondition(t, func() bool {
		auth, ok := manager.GetByID("xai-bad-request")
		return ok && auth.LastError != nil && !auth.NextRefreshAfter.IsZero()
	}, "transient bad-request backoff")

	retained, ok := manager.GetByID("xai-bad-request")
	if !ok || retained.LastError.StatusCode() != http.StatusBadRequest {
		t.Fatalf("expected retained HTTP 400 auth, got %#v", retained)
	}
	if got := store.deleted(); len(got) != 0 {
		t.Fatalf("deleted auths = %v, want none", got)
	}
	if got := retained.NextRefreshAfter; got.Before(refreshStartedAt.Add(4*time.Minute+59*time.Second)) || got.After(time.Now().Add(5*time.Minute+time.Second)) {
		t.Fatalf("NextRefreshAfter = %s, want approximately five-minute backoff", got)
	}
}

func TestManagerAutoRefreshKeepsStructuredBadRequestContainingUnauthorizedText(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	for _, message := range []string{"upstream status 401", "upstream 401 unauthorized"} {
		t.Run(message, func(t *testing.T) {
			store := &deleteTrackingStore{}
			manager := NewManager(store, nil, nil)
			executor := &oauthRefreshFailureExecutor{
				schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "refresh-test"},
				err:                           &refreshOAuthError{status: http.StatusBadRequest, code: "invalid_request", message: message},
			}
			manager.RegisterExecutor(executor)
			registerExpiredRefreshAuth(t, manager, "xai-structured-bad-request", "refresh-test")

			refreshStartedAt := time.Now()
			manager.StartAutoRefresh(context.Background(), time.Millisecond)
			t.Cleanup(manager.StopAutoRefresh)
			waitForAuthCondition(t, func() bool {
				auth, ok := manager.GetByID("xai-structured-bad-request")
				return ok && auth.LastError != nil && !auth.NextRefreshAfter.IsZero()
			}, "structured bad-request backoff")

			retained, ok := manager.GetByID("xai-structured-bad-request")
			if !ok || retained.LastError.StatusCode() != http.StatusBadRequest {
				t.Fatalf("expected retained HTTP 400 auth, got %#v", retained)
			}
			if got := store.deleted(); len(got) != 0 {
				t.Fatalf("deleted auths = %v, want none", got)
			}
			if got := retained.NextRefreshAfter; got.Before(refreshStartedAt.Add(4*time.Minute+59*time.Second)) || got.After(time.Now().Add(5*time.Minute+time.Second)) {
				t.Fatalf("NextRefreshAfter = %s, want approximately five-minute backoff", got)
			}
		})
	}
}

func TestManagerAutoRefreshDeletesUnauthorizedWhenEnabled(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := &deleteTrackingStore{}
	manager := NewManager(store, nil, nil)
	executor := &oauthRefreshFailureExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "refresh-test"},
		err:                           &refreshOAuthError{status: http.StatusUnauthorized, message: "expired credentials"},
	}
	manager.RegisterExecutor(executor)
	registerExpiredRefreshAuth(t, manager, "xai-unauthorized-delete", "refresh-test")

	manager.StartAutoRefresh(context.Background(), time.Millisecond)
	t.Cleanup(manager.StopAutoRefresh)
	waitForAuthCondition(t, func() bool { return len(store.deleted()) == 1 }, "unauthorized auth deletion")

	if _, ok := manager.GetByID("xai-unauthorized-delete"); ok {
		t.Fatal("expected unauthorized auth to be removed from manager")
	}
}

func TestManagerAutoRefreshKeepsXAIUnauthorizedWithUpstreamLifecycle(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := &deleteTrackingStore{}
	manager := NewManager(store, nil, nil)
	executor := &oauthRefreshFailureExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "xai"},
		err:                           &refreshOAuthError{status: http.StatusUnauthorized, message: "expired credentials"},
	}
	manager.RegisterExecutor(executor)
	registerExpiredRefreshAuth(t, manager, "xai-unauthorized-retain", "xai")

	manager.StartAutoRefresh(context.Background(), time.Millisecond)
	t.Cleanup(manager.StopAutoRefresh)
	waitForAuthCondition(t, func() bool {
		auth, ok := manager.GetByID("xai-unauthorized-retain")
		return ok && auth.LastError != nil && auth.LastError.Code == "unauthorized"
	}, "retained unauthorized state")

	retained, ok := manager.GetByID("xai-unauthorized-retain")
	if !ok || !retained.Unavailable || retained.Status != StatusError || !retained.NextRefreshAfter.IsZero() {
		t.Fatalf("retained unauthorized auth = %#v", retained)
	}
	if got := store.deleted(); len(got) != 0 {
		t.Fatalf("deleted auths = %v, want none", got)
	}
}

func TestManagerAutoRefreshUsesUpstreamXAIUnauthorizedTextDetection(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := &deleteTrackingStore{}
	manager := NewManager(store, nil, nil)
	executor := &oauthRefreshFailureExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "xai"},
		err:                           &refreshOAuthError{status: http.StatusBadRequest, code: "invalid_request", message: "upstream status 401"},
	}
	manager.RegisterExecutor(executor)
	registerExpiredRefreshAuth(t, manager, "xai-unauthorized-text", "xai")

	manager.StartAutoRefresh(context.Background(), time.Millisecond)
	t.Cleanup(manager.StopAutoRefresh)
	waitForAuthCondition(t, func() bool {
		auth, ok := manager.GetByID("xai-unauthorized-text")
		return ok && auth.Unavailable && auth.Status == StatusError
	}, "upstream xai unauthorized text state")

	retained, ok := manager.GetByID("xai-unauthorized-text")
	if !ok || retained.StatusMessage != "unauthorized" {
		t.Fatalf("retained auth = %#v, want upstream unauthorized state", retained)
	}
	if got := store.deleted(); len(got) != 0 {
		t.Fatalf("deleted auths = %v, want none", got)
	}
}

func TestManagerAutoRefreshDeleteDoesNotRemoveReplacementAuth(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := newBlockingDeleteStore()
	manager := NewManager(store, nil, nil)
	executor := &oauthRefreshFailureExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "refresh-test"},
		err:                           &refreshOAuthError{status: http.StatusBadRequest, code: "invalid_grant", message: "revoked"},
	}
	manager.RegisterExecutor(executor)
	registerExpiredRefreshAuth(t, manager, "xai-replaced", "refresh-test")

	manager.StartAutoRefresh(context.Background(), time.Millisecond)
	t.Cleanup(manager.StopAutoRefresh)
	select {
	case <-store.deleteStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for terminal deletion")
	}

	replacement := &Auth{
		ID:       "xai-replaced",
		Provider: "refresh-test",
		Metadata: map[string]any{
			"access_token":  "replacement-access-token",
			"refresh_token": "replacement-refresh-token",
		},
	}
	registered := make(chan error, 1)
	go func() {
		_, errRegister := manager.Register(context.Background(), replacement)
		registered <- errRegister
	}()

	select {
	case errRegister := <-registered:
		t.Fatalf("replacement registration completed before terminal deletion: %v", errRegister)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.allowDelete)
	if errRegister := <-registered; errRegister != nil {
		t.Fatalf("register replacement auth: %v", errRegister)
	}

	registeredAuth, ok := manager.GetByID("xai-replaced")
	if !ok || authAccessToken(registeredAuth) != "replacement-access-token" {
		t.Fatalf("manager auth = %#v, want replacement", registeredAuth)
	}
	persisted, ok := store.auth("xai-replaced")
	if !ok || authAccessToken(persisted) != "replacement-access-token" {
		t.Fatalf("stored auth = %#v, want replacement", persisted)
	}
	selected, errSelect := manager.SelectAuth(context.Background(), "refresh-test", "", cliproxyexecutor.Options{})
	if errSelect != nil || selected == nil || selected.ID != "xai-replaced" {
		t.Fatalf("selected auth = %#v, err = %v", selected, errSelect)
	}
}

func TestManagerMarkResultDeleteDoesNotRemoveReplacementAuth(t *testing.T) {
	SetDeleteUnauthorizedAuth(true)
	t.Cleanup(func() { SetDeleteUnauthorizedAuth(false) })

	store := newBlockingDeleteStore()
	manager := NewManager(store, nil, nil)
	manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "refresh-test"})
	initial := &Auth{
		ID:       "xai-result-replaced",
		Provider: "refresh-test",
		Metadata: map[string]any{"access_token": "initial-access-token", "refresh_token": "initial-refresh-token"},
	}
	if _, errRegister := manager.Register(context.Background(), initial); errRegister != nil {
		t.Fatalf("register initial auth: %v", errRegister)
	}

	resultDone := make(chan struct{})
	go func() {
		manager.MarkResult(context.Background(), Result{
			AuthID:   initial.ID,
			Provider: "refresh-test",
			Success:  false,
			Error:    &Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
		})
		close(resultDone)
	}()
	select {
	case <-store.deleteStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result deletion")
	}

	replacement := &Auth{
		ID:       initial.ID,
		Provider: "refresh-test",
		Metadata: map[string]any{"access_token": "replacement-access-token", "refresh_token": "replacement-refresh-token"},
	}
	registered := make(chan error, 1)
	go func() {
		_, errRegister := manager.Register(context.Background(), replacement)
		registered <- errRegister
	}()

	select {
	case errRegister := <-registered:
		t.Fatalf("replacement registration completed before result deletion: %v", errRegister)
	case <-time.After(20 * time.Millisecond):
	}
	close(store.allowDelete)
	<-resultDone
	if errRegister := <-registered; errRegister != nil {
		t.Fatalf("register replacement auth: %v", errRegister)
	}

	registeredAuth, ok := manager.GetByID(initial.ID)
	if !ok || authAccessToken(registeredAuth) != "replacement-access-token" {
		t.Fatalf("manager auth = %#v, want replacement", registeredAuth)
	}
	persisted, ok := store.auth(initial.ID)
	if !ok || authAccessToken(persisted) != "replacement-access-token" {
		t.Fatalf("stored auth = %#v, want replacement", persisted)
	}
	selected, errSelect := manager.SelectAuth(context.Background(), "refresh-test", "", cliproxyexecutor.Options{})
	if errSelect != nil || selected == nil || selected.ID != initial.ID {
		t.Fatalf("selected auth = %#v, err = %v", selected, errSelect)
	}
}
