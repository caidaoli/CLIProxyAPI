package management

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth/xai"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestAPICallUsesRequestProxyURL(t *testing.T) {
	t.Parallel()

	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("proxied"))
	}))
	defer proxyServer.Close()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://127.0.0.1:1"},
		},
	}
	router := gin.New()
	router.POST("/", h.APICall)

	body := `{"method":"GET","url":"http://upstream.invalid/test","proxy_url":"` + proxyServer.URL + `"}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response apiCallResponse
	if errDecode := json.NewDecoder(recorder.Body).Decode(&response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("upstream status code = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	if response.Body != "proxied" {
		t.Fatalf("upstream body = %q, want %q", response.Body, "proxied")
	}
}

func TestAPICallUsesRequestProxyURLWithProviderAuthentication(t *testing.T) {
	t.Parallel()

	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer request-token" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer request-token")
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("authenticated proxy"))
	}))
	defer proxyServer.Close()

	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://127.0.0.1:1"}}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewCodexExecutor(cfg))
	auth := &coreauth.Auth{
		ID:       "request-proxy-auth",
		Index:    "request-proxy-index",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		ProxyURL: "http://127.0.0.1:2",
		Metadata: map[string]any{"access_token": "request-token"},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(cfg, manager)
	router := gin.New()
	router.POST("/", h.APICall)

	body := `{"auth_index":"request-proxy-index","method":"GET","url":"http://upstream.invalid/test","proxy_url":"` + proxyServer.URL + `"}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response apiCallResponse
	if errDecode := json.NewDecoder(recorder.Body).Decode(&response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("upstream status code = %d, want %d", response.StatusCode, http.StatusCreated)
	}
	if response.Body != "authenticated proxy" {
		t.Fatalf("upstream body = %q, want %q", response.Body, "authenticated proxy")
	}
}

func TestAPICallTransportDirectBypassesGlobalProxy(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
		},
	}

	transport := h.apiCallTransport(&coreauth.Auth{ProxyURL: "direct"}, "")
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}
	if httpTransport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
	}
}

// TestAPICallRejectsCredentialWithoutToken pins the failure mode behind the
// misleading upstream error "x-api-key header is required": when a credential
// holds no access token, the $TOKEN$ header is dropped and the request used to
// reach the upstream with no authentication at all. It must fail here instead.
func TestAPICallRejectsCredentialWithoutToken(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewClaudeExecutor(&config.Config{}))
	auth := &coreauth.Auth{
		ID:       "claude-oauth",
		Index:    "claude-auth-index",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"type":          "claude",
			"email":         "user@example.com",
			"refresh_token": "sk-ant-ort01-refresh",
		},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)

	payload := fmt.Sprintf(`{"authIndex":"claude-auth-index","method":"POST","url":%q,"header":{"Authorization":"Bearer $TOKEN$"},"data":"{}"}`, upstream.URL)
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", strings.NewReader(payload))
	c.Request.Header.Set("Content-Type", "application/json")

	h.APICall(c)

	if got := upstreamCalls.Load(); got != 0 {
		t.Errorf("upstream calls = %d, want 0: unauthenticated request must not be forwarded", got)
	}
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("APICall status = %d, want %d; body=%s", recorder.Code, http.StatusUnauthorized, recorder.Body.String())
	}
}

func TestAPICallAllowsEmptyAPIKeyCredential(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization = %q, want empty for empty API key", got)
		}
		if got := r.Header.Get("Custom-Token"); got != "custom-secret" {
			t.Errorf("Custom-Token = %q, want custom-secret", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewClaudeExecutor(&config.Config{}))
	auth := &coreauth.Auth{
		ID:       "claude-apikey",
		Index:    "claude-apikey-index",
		Provider: "claude",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.AttributeAuthKind: coreauth.AuthKindAPIKey,
			"base_url":                 upstream.URL,
			"header:Custom-Token":      "custom-secret",
		},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)

	payload := fmt.Sprintf(`{"authIndex":"claude-apikey-index","method":"POST","url":%q,"header":{"Authorization":"Bearer $TOKEN$"},"data":"{}"}`, upstream.URL)
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", strings.NewReader(payload))
	c.Request.Header.Set("Content-Type", "application/json")

	h.APICall(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("APICall status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1 for base_URL-only API key", got)
	}
}

func TestAPICallBatchReturnsOrderedPartialResults(t *testing.T) {
	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		w.Header().Set("X-Test-Path", r.URL.Path)
		if r.URL.Path == "/denied" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"denied"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	payload := fmt.Sprintf(`{"requests":[
		{"id":"ok","method":"GET","url":%q},
		{"id":"invalid","method":"GET","url":"not-a-url"},
		{"id":"denied","method":"GET","url":%q}
	]}`, upstream.URL+"/ok", upstream.URL+"/denied")

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/custom/api-call/batch", strings.NewReader(payload))
	c.Request.Header.Set("Content-Type", "application/json")

	h.APICallBatch(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("APICallBatch status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if got := upstreamCalls.Load(); got != 2 {
		t.Fatalf("upstream calls = %d, want 2", got)
	}

	var response apiCallBatchResponse
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode APICallBatch response: %v", errDecode)
	}
	if len(response.Results) != 3 {
		t.Fatalf("results length = %d, want 3", len(response.Results))
	}
	if got := response.Results[0]; got.ID != "ok" || got.StatusCode != http.StatusOK || got.Error != "" {
		t.Errorf("first result = %+v, want successful ok result", got)
	}
	if got := response.Results[1]; got.ID != "invalid" || got.ErrorStatus != http.StatusBadRequest || got.Error != "invalid url" {
		t.Errorf("second result = %+v, want invalid-url item error", got)
	}
	if got := response.Results[2]; got.ID != "denied" || got.StatusCode != http.StatusForbidden || got.Error != "" {
		t.Errorf("third result = %+v, want upstream forbidden response", got)
	}
}

func TestAPICallBatchRejectsAmbiguousRequestSet(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "empty", payload: `{"requests":[]}`},
		{name: "missing id", payload: `{"requests":[{"method":"GET","url":"https://example.com"}]}`},
		{name: "duplicate id", payload: `{"requests":[{"id":"same","method":"GET","url":"https://example.com"},{"id":"same","method":"GET","url":"https://example.com"}]}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/custom/api-call/batch", strings.NewReader(tc.payload))
			c.Request.Header.Set("Content-Type", "application/json")

			h.APICallBatch(c)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
		})
	}
}

func TestAPICallBatchRejectsTooManyRequests(t *testing.T) {
	requests := make([]apiCallRequest, maxAPICallBatchSize+1)
	for i := range requests {
		requests[i] = apiCallRequest{
			ID:     fmt.Sprintf("request-%d", i),
			Method: http.MethodGet,
			URL:    "https://example.com",
		}
	}
	payload, errMarshal := json.Marshal(apiCallBatchRequest{Requests: requests})
	if errMarshal != nil {
		t.Fatalf("marshal request: %v", errMarshal)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/custom/api-call/batch", bytes.NewReader(payload))
	c.Request.Header.Set("Content-Type", "application/json")

	h.APICallBatch(c)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusRequestEntityTooLarge, recorder.Body.String())
	}
}

func TestRemoveCodexAuthRemovesRuntimeAuth(t *testing.T) {
	t.Parallel()

	authDir := t.TempDir()
	fileName := "cleanup-user.json"
	filePath := filepath.Join(authDir, fileName)
	if err := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "cleanup-runtime-auth",
		FileName: fileName,
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path": filePath,
		},
	}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}
	var notifiedPath string
	h.SetAuthFileMutationHook(func(path string) {
		notifiedPath = path
		if _, ok := manager.GetByID(auth.ID); !ok {
			t.Error("runtime auth changed before mutation hook")
		}
		if _, errStat := os.Stat(filePath); !os.IsNotExist(errStat) {
			t.Errorf("auth file still exists when mutation hook ran: %v", errStat)
		}
	})

	if err := h.removeCodexAuth(context.Background(), auth); err != nil {
		t.Fatalf("removeCodexAuth returned error: %v", err)
	}
	normalizedAuthDir, errEval := filepath.EvalSymlinks(authDir)
	if errEval != nil {
		t.Fatalf("resolve auth dir: %v", errEval)
	}
	wantPath := filepath.Join(normalizedAuthDir, fileName)
	if notifiedPath != wantPath {
		t.Fatalf("mutation path = %q, want %q", notifiedPath, wantPath)
	}
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("expected auth file to be removed, stat err: %v", err)
	}
	if _, ok := manager.GetByID(auth.ID); ok {
		t.Fatalf("expected runtime auth %q to be removed", auth.ID)
	}
}

func TestRemoveCodexAuthNotifiesMutationWhenFileAlreadyMissing(t *testing.T) {
	authDir := t.TempDir()
	fileName := "already-missing.json"
	filePath := filepath.Join(authDir, fileName)
	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "already-missing-runtime-auth",
		FileName: fileName,
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path": filePath,
		},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}
	var notifiedPath string
	h.SetAuthFileMutationHook(func(path string) {
		notifiedPath = path
		if _, ok := manager.GetByID(auth.ID); !ok {
			t.Error("runtime auth changed before mutation hook")
		}
	})

	if errRemove := h.removeCodexAuth(context.Background(), auth); errRemove != nil {
		t.Fatalf("removeCodexAuth returned error: %v", errRemove)
	}
	normalizedAuthDir, errEval := filepath.EvalSymlinks(authDir)
	if errEval != nil {
		t.Fatalf("resolve auth dir: %v", errEval)
	}
	wantPath := filepath.Join(normalizedAuthDir, fileName)
	if notifiedPath != wantPath {
		t.Fatalf("mutation path = %q, want %q", notifiedPath, wantPath)
	}
	if _, ok := manager.GetByID(auth.ID); ok {
		t.Fatalf("expected runtime auth %q to be removed", auth.ID)
	}
}

func TestCleanupCodexAuthDeletesClientErrorsOnly(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		body       string
		wantDelete bool
	}{
		{
			name:       "deactivated_workspace",
			statusCode: http.StatusPaymentRequired,
			body:       `{"detail":{"code":"deactivated_workspace"}}`,
			wantDelete: true,
		},
		{
			name:       "server_error",
			statusCode: http.StatusInternalServerError,
			body:       `{"error":"temporary upstream failure"}`,
			wantDelete: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			authDir := t.TempDir()
			fileName := tc.name + ".json"
			filePath := filepath.Join(authDir, fileName)
			if err := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); err != nil {
				t.Fatalf("write auth file: %v", err)
			}

			var verifyRequests atomic.Int32
			verifyServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				verifyRequests.Add(1)
				if got := r.URL.Path; got != "/backend-api/wham/usage" {
					t.Errorf("verify path = %q, want /backend-api/wham/usage", got)
				}
				if got := r.Header.Get("Authorization"); got != "Bearer codex-token" {
					t.Errorf("authorization header = %q, want Bearer codex-token", got)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer verifyServer.Close()

			oldTransport := http.DefaultTransport
			http.DefaultTransport = &http.Transport{
				DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
					var dialer net.Dialer
					return dialer.DialContext(ctx, network, verifyServer.Listener.Addr().String())
				},
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			}
			t.Cleanup(func() {
				http.DefaultTransport = oldTransport
			})

			manager := coreauth.NewManager(nil, nil, nil)
			auth := &coreauth.Auth{
				ID:       "cleanup-" + tc.name,
				FileName: fileName,
				Provider: "codex",
				Status:   coreauth.StatusActive,
				Attributes: map[string]string{
					"path": filePath,
				},
				Metadata: map[string]any{
					"access_token": "codex-token",
				},
			}
			if _, err := manager.Register(context.Background(), auth); err != nil {
				t.Fatalf("register auth: %v", err)
			}

			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
			h.tokenStore = &memoryAuthStore{}

			gin.SetMode(gin.TestMode)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-tools/codex/cleanup", nil)

			h.CleanupCodexAuth(c)

			if recorder.Code != http.StatusOK {
				t.Fatalf("cleanup status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
			}
			if got := verifyRequests.Load(); got != 1 {
				t.Fatalf("verify requests = %d, want 1", got)
			}
			_, statErr := os.Stat(filePath)
			_, stillRegistered := manager.GetByID(auth.ID)
			if tc.wantDelete {
				if !os.IsNotExist(statErr) {
					t.Fatalf("expected 4xx auth file to be removed, stat err: %v", statErr)
				}
				if stillRegistered {
					t.Fatalf("expected runtime auth %q to be removed", auth.ID)
				}
			} else {
				if statErr != nil {
					t.Fatalf("expected 5xx auth file to remain, stat err: %v", statErr)
				}
				if !stillRegistered {
					t.Fatalf("expected runtime auth %q to remain", auth.ID)
				}
			}
		})
	}
}

func TestCleanupCodexAuthVerifiesCredentialsConcurrently(t *testing.T) {
	const authCount = 4

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	for i := 0; i < authCount; i++ {
		fileName := fmt.Sprintf("concurrent-%d.json", i)
		filePath := filepath.Join(authDir, fileName)
		if err := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); err != nil {
			t.Fatalf("write auth file %d: %v", i, err)
		}
		auth := &coreauth.Auth{
			ID:       fmt.Sprintf("cleanup-concurrent-%d", i),
			FileName: fileName,
			Provider: "codex",
			Status:   coreauth.StatusActive,
			Attributes: map[string]string{
				"path": filePath,
			},
			Metadata: map[string]any{
				"access_token": fmt.Sprintf("codex-token-%d", i),
			},
		}
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("register auth %d: %v", i, err)
		}
	}

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	verifyServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := inFlight.Add(1)
		for {
			previous := maxInFlight.Load()
			if current <= previous || maxInFlight.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		inFlight.Add(-1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer verifyServer.Close()

	oldTransport := http.DefaultTransport
	http.DefaultTransport = &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, verifyServer.Listener.Addr().String())
		},
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	t.Cleanup(func() {
		http.DefaultTransport = oldTransport
	})

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-tools/codex/cleanup", nil)

	h.CleanupCodexAuth(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("cleanup status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if got := maxInFlight.Load(); got < 2 {
		t.Fatalf("max concurrent verify requests = %d, want at least 2", got)
	}
}

func TestCleanupAuthRejectsUnsupportedProvider(t *testing.T) {
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, coreauth.NewManager(nil, nil, nil))

	for _, provider := range []string{"qwen", "xai"} {
		t.Run(provider, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(
				http.MethodPost,
				"/v0/management/custom/codex-cleanup",
				strings.NewReader(`{"provider":"`+provider+`"}`),
			)
			c.Request.Header.Set("Content-Type", "application/json")

			h.CleanupCodexAuth(c)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("cleanup status = %d, want %d; body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			if !strings.Contains(recorder.Body.String(), "unsupported cleanup provider: "+provider) {
				t.Fatalf("expected unsupported provider message, body=%s", recorder.Body.String())
			}
		})
	}
}

func TestAPICallTransportInvalidAuthFallsBackToGlobalProxy(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
		},
	}

	transport := h.apiCallTransport(&coreauth.Auth{ProxyURL: "bad-value"}, "")
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}

	req, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("http.NewRequest returned error: %v", errRequest)
	}

	proxyURL, errProxy := httpTransport.Proxy(req)
	if errProxy != nil {
		t.Fatalf("httpTransport.Proxy returned error: %v", errProxy)
	}
	if proxyURL == nil || proxyURL.String() != "http://global-proxy.example.com:8080" {
		t.Fatalf("proxy URL = %v, want http://global-proxy.example.com:8080", proxyURL)
	}
}

func TestAPICallTransportRequestProxyOverridesCredentialAndGlobalProxy(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
		},
	}
	auth := &coreauth.Auth{ProxyURL: "http://credential-proxy.example.com:8080"}

	transport := h.apiCallTransport(auth, " http://request-proxy.example.com:8080 ")
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}

	req, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errRequest != nil {
		t.Fatalf("http.NewRequest returned error: %v", errRequest)
	}

	proxyURL, errProxy := httpTransport.Proxy(req)
	if errProxy != nil {
		t.Fatalf("httpTransport.Proxy returned error: %v", errProxy)
	}
	if proxyURL == nil || proxyURL.String() != "http://request-proxy.example.com:8080" {
		t.Fatalf("proxy URL = %v, want http://request-proxy.example.com:8080", proxyURL)
	}
}

func TestAPICallTransportInvalidRequestProxyDoesNotFallBack(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
		},
	}
	auth := &coreauth.Auth{ProxyURL: "http://credential-proxy.example.com:8080"}

	transport := h.apiCallTransport(auth, "bad-value")
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", transport)
	}
	if httpTransport.Proxy != nil {
		t.Fatal("expected invalid request proxy to avoid lower-priority proxy settings")
	}
}

func TestAPICallTransportAPIKeyAuthFallsBackToConfigProxyURL(t *testing.T) {
	t.Parallel()

	h := &Handler{
		cfg: &config.Config{
			SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"},
			GeminiKey: []config.GeminiKey{{
				APIKey:   "gemini-key",
				ProxyURL: "http://gemini-proxy.example.com:8080",
			}},
			ClaudeKey: []config.ClaudeKey{{
				APIKey:   "claude-key",
				ProxyURL: "http://claude-proxy.example.com:8080",
			}},
			CodexKey: []config.CodexKey{{
				APIKey:   "codex-key",
				ProxyURL: "http://codex-proxy.example.com:8080",
			}},
			XAIKey: []config.XAIKey{{
				APIKey:   "xai-key",
				ProxyURL: "http://xai-proxy.example.com:8080",
			}},
			MetaKey: []config.MetaKey{{
				APIKey:   "meta-key",
				ProxyURL: "http://meta-proxy.example.com:8080",
			}},
			OpenAICompatibility: []config.OpenAICompatibility{{
				Name:    "bohe",
				BaseURL: "https://bohe.example.com",
				APIKeyEntries: []config.OpenAICompatibilityAPIKey{{
					APIKey:   "compat-key",
					ProxyURL: "http://compat-proxy.example.com:8080",
				}},
			}},
		},
	}

	cases := []struct {
		name      string
		auth      *coreauth.Auth
		wantProxy string
	}{
		{
			name: "gemini",
			auth: &coreauth.Auth{
				Provider:   "gemini",
				Attributes: map[string]string{"api_key": "gemini-key"},
			},
			wantProxy: "http://gemini-proxy.example.com:8080",
		},
		{
			name: "claude",
			auth: &coreauth.Auth{
				Provider:   "claude",
				Attributes: map[string]string{"api_key": "claude-key"},
			},
			wantProxy: "http://claude-proxy.example.com:8080",
		},
		{
			name: "codex",
			auth: &coreauth.Auth{
				Provider:   "codex",
				Attributes: map[string]string{"api_key": "codex-key"},
			},
			wantProxy: "http://codex-proxy.example.com:8080",
		},
		{
			name: "xai",
			auth: &coreauth.Auth{
				Provider:   "xai",
				Attributes: map[string]string{"api_key": "xai-key"},
			},
			wantProxy: "http://xai-proxy.example.com:8080",
		},
		{
			name: "meta",
			auth: &coreauth.Auth{
				Provider:   "meta",
				Attributes: map[string]string{"api_key": "meta-key"},
			},
			wantProxy: "http://meta-proxy.example.com:8080",
		},
		{
			name: "openai-compatibility",
			auth: &coreauth.Auth{
				Provider: "bohe",
				Attributes: map[string]string{
					"api_key":      "compat-key",
					"compat_name":  "bohe",
					"provider_key": "bohe",
				},
			},
			wantProxy: "http://compat-proxy.example.com:8080",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			transport := h.apiCallTransport(tc.auth, "")
			httpTransport, ok := transport.(*http.Transport)
			if !ok {
				t.Fatalf("transport type = %T, want *http.Transport", transport)
			}

			req, errRequest := http.NewRequest(http.MethodGet, "https://example.com", nil)
			if errRequest != nil {
				t.Fatalf("http.NewRequest returned error: %v", errRequest)
			}

			proxyURL, errProxy := httpTransport.Proxy(req)
			if errProxy != nil {
				t.Fatalf("httpTransport.Proxy returned error: %v", errProxy)
			}
			if proxyURL == nil || proxyURL.String() != tc.wantProxy {
				t.Fatalf("proxy URL = %v, want %s", proxyURL, tc.wantProxy)
			}
		})
	}
}

func TestAuthByIndexDistinguishesSharedAPIKeysAcrossProviders(t *testing.T) {
	t.Parallel()

	manager := coreauth.NewManager(nil, nil, nil)
	geminiAuth := &coreauth.Auth{
		ID:       "gemini:apikey:123",
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key": "shared-key",
		},
	}
	compatAuth := &coreauth.Auth{
		ID:       "openai-compatibility:bohe:456",
		Provider: "bohe",
		Label:    "bohe",
		Attributes: map[string]string{
			"api_key":      "shared-key",
			"compat_name":  "bohe",
			"provider_key": "bohe",
		},
	}

	if _, errRegister := manager.Register(context.Background(), geminiAuth); errRegister != nil {
		t.Fatalf("register gemini auth: %v", errRegister)
	}
	if _, errRegister := manager.Register(context.Background(), compatAuth); errRegister != nil {
		t.Fatalf("register compat auth: %v", errRegister)
	}

	geminiIndex := geminiAuth.EnsureIndex()
	compatIndex := compatAuth.EnsureIndex()
	if geminiIndex == compatIndex {
		t.Fatalf("shared api key produced duplicate auth_index %q", geminiIndex)
	}

	h := &Handler{authManager: manager}

	gotGemini := h.authByIndex(geminiIndex)
	if gotGemini == nil {
		t.Fatal("expected gemini auth by index")
	}
	if gotGemini.ID != geminiAuth.ID {
		t.Fatalf("authByIndex(gemini) returned %q, want %q", gotGemini.ID, geminiAuth.ID)
	}

	gotCompat := h.authByIndex(compatIndex)
	if gotCompat == nil {
		t.Fatal("expected compat auth by index")
	}
	if gotCompat.ID != compatAuth.ID {
		t.Fatalf("authByIndex(compat) returned %q, want %q", gotCompat.ID, compatAuth.ID)
	}
}

func TestAPICallReplacesTokenInBodyData(t *testing.T) {
	t.Parallel()

	var receivedBody string
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		receivedBody = string(b)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstreamServer.Close()

	manager := coreauth.NewManager(nil, &coreauth.RoundRobinSelector{}, nil)
	manager.RegisterExecutor(runtimeexecutor.NewDevinExecutor(&config.Config{}))
	devinAuth := &coreauth.Auth{
		ID:       "devin-test.json",
		Provider: "devin",
		Attributes: map[string]string{
			"api_key": "secret-session-token-xyz",
		},
		Metadata: map[string]any{
			"type":    "devin",
			"api_key": "secret-session-token-xyz",
		},
	}
	if _, errRegister := manager.Register(context.Background(), devinAuth); errRegister != nil {
		t.Fatalf("register devin auth: %v", errRegister)
	}
	authIndex := devinAuth.EnsureIndex()

	h := &Handler{
		cfg:         &config.Config{},
		authManager: manager,
	}
	router := gin.New()
	router.POST("/", h.APICall)

	reqPayload := map[string]any{
		"method":     "POST",
		"url":        upstreamServer.URL,
		"auth_index": authIndex,
		"header": map[string]string{
			"Content-Type": "application/json",
		},
		"data": `{"metadata":{"apiKey":"$TOKEN$","ideName":"chisel"}}`,
	}
	reqBytes, _ := json.Marshal(reqPayload)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(reqBytes)))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	expectedBody := `{"metadata":{"apiKey":"secret-session-token-xyz","ideName":"chisel"}}`
	if receivedBody != expectedBody {
		t.Fatalf("received body = %q, want %q", receivedBody, expectedBody)
	}
}

func TestAPICallResolvesMetaToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/key" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{
				"api_key": "LLM|api-call-minted-key",
			})
			return
		}
		if r.URL.Path == "/test" {
			if got := r.Header.Get("X-Client-Id"); got != "tbh:tui" {
				t.Errorf("X-Client-Id = %q, want tbh:tui", got)
			}
			authHeader := r.Header.Get("Authorization")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{
				"auth_header": authHeader,
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	t.Setenv("META_MINT_URL", server.URL+"/key")

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewMetaExecutor(&config.Config{}))
	metaAuth := &coreauth.Auth{
		ID:       "meta:oauth:user1",
		Provider: "meta",
		Metadata: map[string]any{
			"dca_token": "dca:tool-test-token",
		},
	}
	if _, err := manager.Register(context.Background(), metaAuth); err != nil {
		t.Fatalf("register meta auth: %v", err)
	}
	authIndex := metaAuth.EnsureIndex()

	h := &Handler{
		cfg:         &config.Config{},
		authManager: manager,
		tokenStore:  &memoryAuthStore{},
	}
	router := gin.New()
	router.POST("/api/call", h.APICall)

	body := `{"auth_index":"` + authIndex + `","method":"GET","url":"` + server.URL + `/test","header":{"Authorization":"Bearer $TOKEN$"}}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/call", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var response apiCallResponse
	if errDecode := json.NewDecoder(recorder.Body).Decode(&response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}

	var upstreamBody map[string]string
	if err := json.Unmarshal([]byte(response.Body), &upstreamBody); err != nil {
		t.Fatalf("decode upstream response: %v", err)
	}
	if upstreamBody["auth_header"] != "Bearer LLM|api-call-minted-key" {
		t.Errorf("expected header 'Bearer LLM|api-call-minted-key', got %q", upstreamBody["auth_header"])
	}
}

func TestResolveMetaTokenUpdatesManagerAndStore(t *testing.T) {
	mints := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mints++
		_ = json.NewEncoder(w).Encode(map[string]string{"api_key": "LLM|minted", "base_url": " https://regional.meta.example/v1 "})
	}))
	defer server.Close()
	t.Setenv("META_MINT_URL", server.URL)
	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	auth, err := manager.Register(coreauth.WithSkipPersist(context.Background()), &coreauth.Auth{ID: "meta-management", Provider: "meta", Metadata: map[string]any{"dca_token": "dca:test", "base_url": "https://previous.meta.example/v1"}, Attributes: map[string]string{"base_url": "https://previous.meta.example/v1"}})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: &config.Config{}, authManager: manager, tokenStore: store}
	for i := 0; i < 2; i++ {
		token, err := h.resolveTokenForAuth(context.Background(), h.authByIndex(auth.Index), "")
		if err != nil {
			t.Fatal(err)
		}
		if token != "LLM|minted" {
			t.Fatalf("token = %q", token)
		}
	}
	if mints != 1 {
		t.Errorf("minted %d times for sequential calls, want 1", mints)
	}
	live := h.authByIndex(auth.Index)
	if live.Metadata["api_key"] != "LLM|minted" {
		t.Error("live manager did not retain minted key")
	}
	if live.Metadata["base_url"] != "https://regional.meta.example/v1" || live.Attributes["base_url"] != "https://regional.meta.example/v1" {
		t.Error("live manager did not retain minted base URL")
	}
	saved, err := store.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(saved) != 1 || saved[0].Metadata["api_key"] != "LLM|minted" || saved[0].Metadata["base_url"] != "https://regional.meta.example/v1" {
		t.Fatalf("configured store did not retain minted credentials: %#v", saved)
	}
}

type failingMetaTokenStore struct {
	memoryAuthStore
	err error
}

func (s *failingMetaTokenStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", s.err
}

func TestResolveMetaTokenPropagatesStoreFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"api_key":"LLM|minted"}`))
	}))
	defer server.Close()
	t.Setenv("META_MINT_URL", server.URL)
	saveErr := errors.New("test store unavailable")
	store := &failingMetaTokenStore{err: saveErr}
	manager := coreauth.NewManager(store, nil, nil)
	auth, err := manager.Register(coreauth.WithSkipPersist(context.Background()), &coreauth.Auth{ID: "meta-save-failure", Provider: "meta", Metadata: map[string]any{"dca_token": "dca:test"}})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: &config.Config{}, authManager: manager, tokenStore: store}
	token, err := h.resolveTokenForAuth(context.Background(), h.authByIndex(auth.Index), "")
	if !errors.Is(err, saveErr) {
		t.Fatalf("error = %v, want store failure", err)
	}
	if token != "" {
		t.Error("returned a token despite failed persistence")
	}
	if h.authByIndex(auth.Index).Metadata["api_key"] != nil {
		t.Error("failed save installed a token in the manager")
	}
}

func TestResolveMetaToken_ConcurrentSingleflight(t *testing.T) {
	var mints int64
	serverStarted := make(chan struct{})
	releaseServer := make(chan struct{})
	var startOnce sync.Once

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&mints, 1)
		startOnce.Do(func() { close(serverStarted) })
		<-releaseServer
		_ = json.NewEncoder(w).Encode(map[string]string{
			"api_key":  "LLM|minted-concurrent",
			"base_url": "https://regional.meta.example/v1",
		})
	}))
	defer server.Close()

	t.Setenv("META_MINT_URL", server.URL)
	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	auth, err := manager.Register(coreauth.WithSkipPersist(context.Background()), &coreauth.Auth{
		ID:       "meta-mgmt-concurrent",
		Provider: "meta",
		Metadata: map[string]any{"dca_token": "dca:concurrent-test"},
	})
	if err != nil {
		t.Fatal(err)
	}

	h := &Handler{cfg: &config.Config{}, authManager: manager, tokenStore: store}

	const callers = 5
	var entryWg sync.WaitGroup
	entryWg.Add(callers)
	ready := make(chan struct{})
	var inFlight sync.WaitGroup
	inFlight.Add(callers)
	var doneWg sync.WaitGroup
	doneWg.Add(callers)

	for i := 0; i < callers; i++ {
		go func() {
			defer doneWg.Done()
			entryWg.Done()
			<-ready
			inFlight.Done()
			var token string
			var errToken error
			if i%2 == 0 {
				prepared, err := manager.PrepareRequestAuth(context.Background(), runtimeexecutor.NewMetaExecutor(h.cfg), auth.Clone())
				errToken = err
				token = metaTokenFromAuth(prepared)
			} else {
				token, errToken = h.resolveTokenForAuth(context.Background(), auth.Clone(), "")
			}
			if errToken != nil {
				t.Errorf("resolveTokenForAuth error: %v", errToken)
			}
			if token != "LLM|minted-concurrent" {
				t.Errorf("expected LLM|minted-concurrent, got %q", token)
			}
		}()
	}

	entryWg.Wait()
	close(ready)

	<-serverStarted
	inFlight.Wait()
	close(releaseServer)
	doneWg.Wait()

	if totalMints := atomic.LoadInt64(&mints); totalMints != 1 {
		t.Errorf("expected 1 mint request, got %d", totalMints)
	}
	live := h.authByIndex(auth.Index)
	if live.Metadata["api_key"] != "LLM|minted-concurrent" {
		t.Error("live manager did not retain minted key")
	}
	if live.Attributes["api_key"] != "LLM|minted-concurrent" {
		t.Error("live manager did not retain minted key in attributes")
	}
}

func TestResolveMetaTokenUsesRequestProxyWithoutSavingOverride(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer dca:proxy-test" {
			t.Error("mint request did not carry the DCA token")
		}
		_, _ = w.Write([]byte(`{"api_key":"LLM|proxied"}`))
	}))
	defer proxy.Close()
	t.Setenv("META_MINT_URL", "http://meta.invalid/key")
	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	auth, err := manager.Register(coreauth.WithSkipPersist(context.Background()), &coreauth.Auth{
		ID: "meta-proxy.json", Provider: "meta", ProxyURL: "http://127.0.0.1:1",
		Metadata: map[string]any{"dca_token": "dca:proxy-test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: &config.Config{}, authManager: manager, tokenStore: store}
	token, err := h.resolveMetaToken(context.Background(), auth.Clone(), proxy.URL)
	if err != nil || token != "LLM|proxied" {
		t.Fatalf("resolve via request proxy: token=%q, err=%v", token, err)
	}
	live, _ := manager.GetByID(auth.ID)
	if live.ProxyURL != auth.ProxyURL {
		t.Fatal("request proxy override changed the credential's configured proxy")
	}
}

func TestAPICallRejectsUnresolvedTokenPlaceholder(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		body    map[string]any
		wantErr []string
	}{
		{
			name: "omitted auth_index in header",
			body: map[string]any{
				"method": "GET",
				"header": map[string]string{"Authorization": "Bearer $TOKEN$"},
			},
			wantErr: []string{"auth token not found"},
		},
		{
			name: "omitted auth_index in data",
			body: map[string]any{
				"method": "POST",
				"data":   `{"apiKey":"$TOKEN$"}`,
			},
			wantErr: []string{"auth token not found"},
		},
		{
			name: "unmatched auth_index",
			body: map[string]any{
				"auth_index": "missing-auth-index",
				"authIndex":  "missing-auth-index",
				"method":     "GET",
				"header":     map[string]string{"Authorization": "Bearer $TOKEN$"},
			},
			wantErr: []string{"auth token not found", "auth credential not found for auth_index"},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var upstreamHit atomic.Bool
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				upstreamHit.Store(true)
				w.WriteHeader(http.StatusOK)
			}))
			defer upstream.Close()

			manager := coreauth.NewManager(nil, nil, nil)
			h := &Handler{cfg: &config.Config{}, authManager: manager}
			router := gin.New()
			router.POST("/", h.APICall)

			payload := map[string]any{}
			for k, v := range tc.body {
				payload[k] = v
			}
			payload["url"] = upstream.URL + "/test"
			bodyBytes, errMarshal := json.Marshal(payload)
			if errMarshal != nil {
				t.Fatalf("marshal request: %v", errMarshal)
			}

			recorder := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(bodyBytes)))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, req)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			var errBody map[string]any
			if errDecode := json.Unmarshal(recorder.Body.Bytes(), &errBody); errDecode != nil {
				t.Fatalf("decode error body: %v; body = %s", errDecode, recorder.Body.String())
			}
			gotErr, _ := errBody["error"].(string)
			matched := false
			for _, want := range tc.wantErr {
				if gotErr == want {
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("error = %q, want one of %q", gotErr, tc.wantErr)
			}
			if upstreamHit.Load() {
				t.Fatal("upstream must not receive a request when $TOKEN$ cannot be resolved")
			}
		})
	}
}

func TestAPICallReplacesXAIOAuthAccessToken(t *testing.T) {
	t.Parallel()

	var receivedAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewXAIExecutor(&config.Config{}))
	xaiAuth := &coreauth.Auth{
		ID:       "xai-user@example.com.json",
		FileName: "xai-user@example.com.json",
		Provider: "xai",
		Attributes: map[string]string{
			"auth_kind": "oauth",
			"base_url":  "https://api.x.ai/v1",
		},
		Metadata: map[string]any{
			"type":          "xai",
			"auth_kind":     "oauth",
			"access_token":  "xai-oauth-access-token",
			"refresh_token": "xai-refresh-token",
			"base_url":      "https://api.x.ai/v1",
			"expired":       time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	if _, errRegister := manager.Register(context.Background(), xaiAuth); errRegister != nil {
		t.Fatalf("register xai auth: %v", errRegister)
	}
	authIndex := xaiAuth.EnsureIndex()

	h := &Handler{cfg: &config.Config{}, authManager: manager}
	router := gin.New()
	router.POST("/", h.APICall)

	body := `{"authIndex":"` + authIndex + `","method":"GET","url":"` + upstream.URL + `/billing","header":{"Authorization":"Bearer $TOKEN$","x-xai-token-auth":"xai-grok-cli"}}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if receivedAuth != "Bearer xai-oauth-access-token" {
		t.Fatalf("Authorization = %q, want %q", receivedAuth, "Bearer xai-oauth-access-token")
	}
}

func TestAPICallRefreshesExpiredXAIOAuthToken(t *testing.T) {
	t.Parallel()

	var refreshCalls atomic.Int32
	var refreshMethod, refreshGrantType, refreshToken string
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshCalls.Add(1)
		refreshMethod = r.Method
		_ = r.ParseForm()
		refreshGrantType = r.PostForm.Get("grant_type")
		refreshToken = r.PostForm.Get("refresh_token")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "xai-access-fresh",
			"refresh_token": "xai-refresh-new",
			"token_type":    "Bearer",
			"expires_in":    3600,
		})
	}))
	defer tokenServer.Close()

	var receivedAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewXAIExecutor(&config.Config{}))
	xaiAuth := &coreauth.Auth{
		ID:       "xai-expired@example.com.json",
		FileName: "xai-expired@example.com.json",
		Provider: "xai",
		Attributes: map[string]string{
			"auth_kind": "oauth",
			"base_url":  "https://api.x.ai/v1",
		},
		Metadata: map[string]any{
			"type":           "xai",
			"auth_kind":      "oauth",
			"access_token":   "xai-access-stale",
			"refresh_token":  "xai-refresh-old",
			"token_endpoint": tokenServer.URL,
			"base_url":       "https://api.x.ai/v1",
			"expired":        time.Now().Add(-time.Hour).Format(time.RFC3339),
		},
	}
	if _, errRegister := manager.Register(context.Background(), xaiAuth); errRegister != nil {
		t.Fatalf("register xai auth: %v", errRegister)
	}
	authIndex := xaiAuth.EnsureIndex()

	h := &Handler{cfg: &config.Config{}, authManager: manager, tokenStore: store}
	router := gin.New()
	router.POST("/", h.APICall)

	body := `{"auth_index":"` + authIndex + `","method":"GET","url":"` + upstream.URL + `/billing","header":{"Authorization":"Bearer $TOKEN$"}}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if receivedAuth != "Bearer xai-access-fresh" {
		t.Fatalf("Authorization = %q, want %q", receivedAuth, "Bearer xai-access-fresh")
	}
	if refreshCalls.Load() != 1 {
		t.Fatalf("refresh calls = %d, want 1", refreshCalls.Load())
	}
	if refreshMethod != http.MethodPost {
		t.Fatalf("refresh method = %s, want POST", refreshMethod)
	}
	if refreshGrantType != "refresh_token" {
		t.Fatalf("grant_type = %q, want refresh_token", refreshGrantType)
	}
	if refreshToken != "xai-refresh-old" {
		t.Fatalf("refresh_token = %q, want xai-refresh-old", refreshToken)
	}
	live := h.authByIndex(authIndex)
	if live == nil {
		t.Fatal("expected refreshed auth to remain addressable by auth_index")
	}
	if got, _ := live.Metadata["access_token"].(string); got != "xai-access-fresh" {
		t.Fatalf("manager access_token = %q, want xai-access-fresh", got)
	}
	if got, _ := live.Metadata["refresh_token"].(string); got != "xai-refresh-new" {
		t.Fatalf("manager refresh_token = %q, want xai-refresh-new", got)
	}
	saved := store.items[xaiAuth.ID]
	if saved == nil {
		t.Fatal("expected refreshed auth to be persisted to token store")
	}
	if got, _ := saved.Metadata["access_token"].(string); got != "xai-access-fresh" {
		t.Fatalf("persisted access_token = %q, want xai-access-fresh", got)
	}
}

func TestAPICallRefreshesExpiredXAIOAuthToken_RefreshFailure(t *testing.T) {
	t.Parallel()

	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer tokenServer.Close()

	var upstreamHit atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHit.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewXAIExecutor(&config.Config{}))
	xaiAuth := &coreauth.Auth{
		ID:       "xai-failed-refresh@example.com.json",
		FileName: "xai-failed-refresh@example.com.json",
		Provider: "xai",
		Attributes: map[string]string{
			"auth_kind": "oauth",
			"base_url":  "https://api.x.ai/v1",
		},
		Metadata: map[string]any{
			"type":           "xai",
			"auth_kind":      "oauth",
			"access_token":   "xai-expired-token",
			"refresh_token":  "xai-bad-refresh",
			"token_endpoint": tokenServer.URL,
			"expired":        time.Now().Add(-time.Hour).Format(time.RFC3339),
		},
	}
	if _, errRegister := manager.Register(context.Background(), xaiAuth); errRegister != nil {
		t.Fatalf("register xai auth: %v", errRegister)
	}
	authIndex := xaiAuth.EnsureIndex()

	h := &Handler{cfg: &config.Config{}, authManager: manager}
	router := gin.New()
	router.POST("/", h.APICall)

	body := `{"auth_index":"` + authIndex + `","method":"GET","url":"` + upstream.URL + `/billing","header":{"Authorization":"Bearer $TOKEN$"}}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	var respBody map[string]any
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &respBody); errDecode != nil {
		t.Fatalf("decode error body: %v", errDecode)
	}
	if got, _ := respBody["error"].(string); got != "auth token refresh failed" {
		t.Fatalf("error = %q, want 'auth token refresh failed'", got)
	}
	if upstreamHit.Load() {
		t.Fatal("upstream must not be hit when token refresh fails")
	}
}

func TestAPICallEndToEndWithAuthFilesList(t *testing.T) {
	tempDir, errTemp := os.MkdirTemp("", "xai-auth-e2e-*")
	if errTemp != nil {
		t.Fatal(errTemp)
	}
	defer func() {
		_ = os.RemoveAll(tempDir)
	}()

	authJSON := `{
  "type": "xai",
  "auth_kind": "oauth",
  "base_url": "https://api.x.ai/v1",
  "access_token": "xai-e2e-access-token",
  "refresh_token": "xai-e2e-refresh-token",
  "id_token": "xai-e2e-id-token",
  "email": "user@example.com",
  "disabled": false
}`
	filePath := filepath.Join(tempDir, "xai-user.json")
	if errWrite := os.WriteFile(filePath, []byte(authJSON), 0600); errWrite != nil {
		t.Fatal(errWrite)
	}

	store := sdkAuth.NewFileTokenStore()
	store.SetBaseDir(tempDir)

	manager := coreauth.NewManager(store, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewXAIExecutor(&config.Config{}))
	if errLoad := manager.Load(context.Background()); errLoad != nil {
		t.Fatal(errLoad)
	}

	cfg := &config.Config{AuthDir: tempDir}
	h := &Handler{cfg: cfg, authManager: manager, tokenStore: store}
	router := gin.New()
	router.GET("/v0/management/auth-files", h.ListAuthFiles)
	router.POST("/v0/management/api-call", h.APICall)

	recList := httptest.NewRecorder()
	reqList := httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	router.ServeHTTP(recList, reqList)

	if recList.Code != http.StatusOK {
		t.Fatalf("ListAuthFiles status = %d, want 200; body = %s", recList.Code, recList.Body.String())
	}

	var listData struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(recList.Body.Bytes(), &listData); errDecode != nil {
		t.Fatalf("decode auth-files: %v", errDecode)
	}
	if len(listData.Files) == 0 {
		t.Fatal("expected at least one auth file in list")
	}

	authIndex, _ := listData.Files[0]["auth_index"].(string)
	if strings.TrimSpace(authIndex) == "" {
		t.Fatal("expected non-empty auth_index from GET /v0/management/auth-files")
	}

	var receivedAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"billing":"ok"}`))
	}))
	defer upstream.Close()

	callPayload := map[string]any{
		"authIndex": authIndex,
		"method":    "GET",
		"url":       upstream.URL + "/v1/billing?format=credits",
		"header": map[string]string{
			"Authorization":    "Bearer $TOKEN$",
			"x-xai-token-auth": "xai-grok-cli",
		},
	}
	callBytes, _ := json.Marshal(callPayload)

	recCall := httptest.NewRecorder()
	reqCall := httptest.NewRequest(http.MethodPost, "/v0/management/api-call", strings.NewReader(string(callBytes)))
	reqCall.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recCall, reqCall)

	if recCall.Code != http.StatusOK {
		t.Fatalf("APICall status = %d, want 200; body = %s", recCall.Code, recCall.Body.String())
	}
	if receivedAuth != "Bearer xai-e2e-access-token" {
		t.Fatalf("upstream Authorization = %q, want 'Bearer xai-e2e-access-token'", receivedAuth)
	}
}

func TestAPICallConcurrentXAITokenRefresh(t *testing.T) {
	t.Parallel()

	var refreshCount atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "xai-fresh-concurrent",
			"refresh_token": "xai-new-refresh",
			"token_type":    "Bearer",
			"expires_in":    3600,
		})
	}))
	defer tokenServer.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader != "Bearer xai-fresh-concurrent" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`invalid auth`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewXAIExecutor(&config.Config{}))
	xaiAuth := &coreauth.Auth{
		ID:       "xai-concurrent@example.com.json",
		FileName: "xai-concurrent@example.com.json",
		Provider: "xai",
		Attributes: map[string]string{
			"auth_kind": "oauth",
			"base_url":  "https://api.x.ai/v1",
		},
		Metadata: map[string]any{
			"type":           "xai",
			"auth_kind":      "oauth",
			"access_token":   "xai-stale-concurrent",
			"refresh_token":  "xai-refresh-shared",
			"token_endpoint": tokenServer.URL,
			"expired":        time.Now().Add(-time.Hour).Format(time.RFC3339),
		},
		Storage: &xaiauth.TokenStorage{
			Type:          "xai",
			AuthKind:      "oauth",
			AccessToken:   "xai-stale-concurrent",
			RefreshToken:  "xai-refresh-shared",
			TokenEndpoint: tokenServer.URL,
			Expire:        time.Now().Add(-time.Hour).Format(time.RFC3339),
		},
	}
	if _, errRegister := manager.Register(context.Background(), xaiAuth); errRegister != nil {
		t.Fatalf("register xai auth: %v", errRegister)
	}
	authIndex := xaiAuth.EnsureIndex()

	h := &Handler{cfg: &config.Config{}, authManager: manager, tokenStore: store}
	router := gin.New()
	router.POST("/", h.APICall)

	const concurrency = 8
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := `{"authIndex":"` + authIndex + `","method":"GET","url":"` + upstream.URL + `/billing","header":{"Authorization":"Bearer $TOKEN$"}}`
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				errCh <- fmt.Errorf("unexpected status %d: %s", rec.Code, rec.Body.String())
				return
			}
			var resp apiCallResponse
			if errDecode := json.Unmarshal(rec.Body.Bytes(), &resp); errDecode != nil {
				errCh <- fmt.Errorf("decode response: %w", errDecode)
				return
			}
			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("unexpected upstream status %d: body=%s", resp.StatusCode, resp.Body)
				return
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatal(err)
	}
	if count := refreshCount.Load(); count == 0 {
		t.Fatal("expected at least one refresh call")
	}
}

func TestAPICallReplacesXAIOAuthAccessTokenInData(t *testing.T) {
	t.Parallel()

	var receivedBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		receivedBody = string(b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewXAIExecutor(&config.Config{}))
	xaiAuth := &coreauth.Auth{
		ID:       "xai-body-data@example.com.json",
		FileName: "xai-body-data@example.com.json",
		Provider: "xai",
		Attributes: map[string]string{
			"auth_kind": "oauth",
			"base_url":  "https://api.x.ai/v1",
		},
		Metadata: map[string]any{
			"type":          "xai",
			"auth_kind":     "oauth",
			"access_token":  "xai-body-data-token",
			"refresh_token": "xai-refresh-token",
			"base_url":      "https://api.x.ai/v1",
			"expired":       time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	if _, errRegister := manager.Register(context.Background(), xaiAuth); errRegister != nil {
		t.Fatalf("register xai auth: %v", errRegister)
	}
	authIndex := xaiAuth.EnsureIndex()

	h := &Handler{cfg: &config.Config{}, authManager: manager}
	router := gin.New()
	router.POST("/", h.APICall)

	body := `{"authIndex":"` + authIndex + `","method":"POST","url":"` + upstream.URL + `/probe","data":"{\"token\":\"$TOKEN$\",\"client\":\"grok\"}"}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	wantBody := `{"token":"xai-body-data-token","client":"grok"}`
	if receivedBody != wantBody {
		t.Fatalf("receivedBody = %q, want %q", receivedBody, wantBody)
	}
}

func TestAPICallRefreshesWhenOnlyIDTokenPresentWithRefreshToken(t *testing.T) {
	t.Parallel()

	var refreshCalls atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "xai-from-id-refresh",
			"refresh_token": "xai-new-refresh-token",
			"token_type":    "Bearer",
			"expires_in":    3600,
		})
	}))
	defer tokenServer.Close()

	var receivedAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewXAIExecutor(&config.Config{}))
	xaiAuth := &coreauth.Auth{
		ID:       "xai-id-only@example.com.json",
		FileName: "xai-id-only@example.com.json",
		Provider: "xai",
		Attributes: map[string]string{
			"auth_kind": "oauth",
			"base_url":  "https://api.x.ai/v1",
		},
		Metadata: map[string]any{
			"type":           "xai",
			"auth_kind":      "oauth",
			"id_token":       "xai-raw-id-token",
			"refresh_token":  "xai-refresh-token",
			"token_endpoint": tokenServer.URL,
			"base_url":       "https://api.x.ai/v1",
		},
	}
	if _, errRegister := manager.Register(context.Background(), xaiAuth); errRegister != nil {
		t.Fatalf("register xai auth: %v", errRegister)
	}
	authIndex := xaiAuth.EnsureIndex()

	h := &Handler{cfg: &config.Config{}, authManager: manager, tokenStore: store}
	router := gin.New()
	router.POST("/", h.APICall)

	body := `{"authIndex":"` + authIndex + `","method":"GET","url":"` + upstream.URL + `/billing","header":{"Authorization":"Bearer $TOKEN$"}}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if receivedAuth != "Bearer xai-from-id-refresh" {
		t.Fatalf("Authorization = %q, want 'Bearer xai-from-id-refresh'", receivedAuth)
	}
	if refreshCalls.Load() != 1 {
		t.Fatalf("refreshCalls = %d, want 1", refreshCalls.Load())
	}
}

func TestAPICallRejectsWhenOnlyIDTokenPresentWithoutRefreshToken(t *testing.T) {
	t.Parallel()

	var upstreamHit atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamHit.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewXAIExecutor(&config.Config{}))
	xaiAuth := &coreauth.Auth{
		ID:       "xai-id-no-refresh@example.com.json",
		FileName: "xai-id-no-refresh@example.com.json",
		Provider: "xai",
		Attributes: map[string]string{
			"auth_kind": "oauth",
			"base_url":  "https://api.x.ai/v1",
		},
		Metadata: map[string]any{
			"type":      "xai",
			"auth_kind": "oauth",
			"id_token":  "xai-raw-id-token",
			"base_url":  "https://api.x.ai/v1",
		},
	}
	if _, errRegister := manager.Register(context.Background(), xaiAuth); errRegister != nil {
		t.Fatalf("register xai auth: %v", errRegister)
	}
	authIndex := xaiAuth.EnsureIndex()

	h := &Handler{cfg: &config.Config{}, authManager: manager}
	router := gin.New()
	router.POST("/", h.APICall)

	body := `{"authIndex":"` + authIndex + `","method":"GET","url":"` + upstream.URL + `/billing","header":{"Authorization":"Bearer $TOKEN$"}}`
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, http.StatusUnauthorized, recorder.Body.String())
	}
	var respBody map[string]any
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &respBody); errDecode != nil {
		t.Fatalf("decode error body: %v", errDecode)
	}
	if got, _ := respBody["error"].(string); got != "missing access token" {
		t.Fatalf("error = %q, want 'missing access token'", got)
	}
	if upstreamHit.Load() {
		t.Fatal("upstream must not be hit when access token cannot be resolved")
	}
}
