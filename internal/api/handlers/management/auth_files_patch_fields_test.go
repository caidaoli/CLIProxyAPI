package management

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	fileauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestSyncAuthFilePriorityAttributeTracksFileSource(t *testing.T) {
	auth := &coreauth.Auth{
		Attributes: map[string]string{coreauth.AttributeSourceBackend: coreauth.AuthSourceFile},
		Metadata:   map[string]any{"priority": float64(1)},
	}
	syncAuthFilePriorityAttribute(auth)
	if auth.Attributes[coreauth.AttributeFilePriority] != "true" {
		t.Fatal("added file priority not marked as inherited")
	}
	delete(auth.Metadata, "priority")
	syncAuthFilePriorityAttribute(auth)
	if _, inherited := auth.Attributes[coreauth.AttributeFilePriority]; inherited {
		t.Fatal("removed file priority still marked as inherited")
	}
}

func TestPatchAuthFileFields_MergeHeadersAndDeleteEmptyValues(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:       "test.json",
		FileName: "test.json",
		Provider: "claude",
		Attributes: map[string]string{
			"path":            "/tmp/test.json",
			"header:X-Old":    "old",
			"header:X-Remove": "gone",
		},
		Metadata: map[string]any{
			"type": "claude",
			"headers": map[string]any{
				"X-Old":    "old",
				"X-Remove": "gone",
			},
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	body := `{"name":"test.json","prefix":"p1","proxy_url":"http://proxy.local","headers":{"X-Old":"new","X-New":"v","X-Remove":"  ","X-Nope":""}}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	updated, ok := manager.GetByID("test.json")
	if !ok || updated == nil {
		t.Fatalf("expected auth record to exist after patch")
	}

	if updated.Prefix != "p1" {
		t.Fatalf("prefix = %q, want %q", updated.Prefix, "p1")
	}
	if updated.ProxyURL != "http://proxy.local" {
		t.Fatalf("proxy_url = %q, want %q", updated.ProxyURL, "http://proxy.local")
	}

	if updated.Metadata == nil {
		t.Fatalf("expected metadata to be non-nil")
	}
	if got, _ := updated.Metadata["prefix"].(string); got != "p1" {
		t.Fatalf("metadata.prefix = %q, want %q", got, "p1")
	}
	if got, _ := updated.Metadata["proxy_url"].(string); got != "http://proxy.local" {
		t.Fatalf("metadata.proxy_url = %q, want %q", got, "http://proxy.local")
	}

	headersMeta, ok := updated.Metadata["headers"].(map[string]any)
	if !ok {
		raw, _ := json.Marshal(updated.Metadata["headers"])
		t.Fatalf("metadata.headers = %T (%s), want map[string]any", updated.Metadata["headers"], string(raw))
	}
	if got := headersMeta["X-Old"]; got != "new" {
		t.Fatalf("metadata.headers.X-Old = %#v, want %q", got, "new")
	}
	if got := headersMeta["X-New"]; got != "v" {
		t.Fatalf("metadata.headers.X-New = %#v, want %q", got, "v")
	}
	if _, ok := headersMeta["X-Remove"]; ok {
		t.Fatalf("expected metadata.headers.X-Remove to be deleted")
	}
	if _, ok := headersMeta["X-Nope"]; ok {
		t.Fatalf("expected metadata.headers.X-Nope to be absent")
	}

	if got := updated.Attributes["header:X-Old"]; got != "new" {
		t.Fatalf("attrs header:X-Old = %q, want %q", got, "new")
	}
	if got := updated.Attributes["header:X-New"]; got != "v" {
		t.Fatalf("attrs header:X-New = %q, want %q", got, "v")
	}
	if _, ok := updated.Attributes["header:X-Remove"]; ok {
		t.Fatalf("expected attrs header:X-Remove to be deleted")
	}
	if _, ok := updated.Attributes["header:X-Nope"]; ok {
		t.Fatalf("expected attrs header:X-Nope to be absent")
	}
}

func TestPatchAuthFileFields_HeadersEmptyMapIsNoop(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:       "noop.json",
		FileName: "noop.json",
		Provider: "claude",
		Attributes: map[string]string{
			"path":         "/tmp/noop.json",
			"header:X-Kee": "1",
		},
		Metadata: map[string]any{
			"type": "claude",
			"headers": map[string]any{
				"X-Kee": "1",
			},
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	body := `{"name":"noop.json","note":"hello","headers":{}}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	updated, ok := manager.GetByID("noop.json")
	if !ok || updated == nil {
		t.Fatalf("expected auth record to exist after patch")
	}
	if got := updated.Attributes["header:X-Kee"]; got != "1" {
		t.Fatalf("attrs header:X-Kee = %q, want %q", got, "1")
	}
	headersMeta, ok := updated.Metadata["headers"].(map[string]any)
	if !ok {
		t.Fatalf("expected metadata.headers to remain a map, got %T", updated.Metadata["headers"])
	}
	if got := headersMeta["X-Kee"]; got != "1" {
		t.Fatalf("metadata.headers.X-Kee = %#v, want %q", got, "1")
	}
}

func TestPatchAuthFileFields_WebsocketsFalseIsUpdate(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:       "codex.json",
		FileName: "codex.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path":       "/tmp/codex.json",
			"websockets": "true",
		},
		Metadata: map[string]any{
			"type":       "codex",
			"websockets": true,
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	body := `{"name":"codex.json","websockets":false}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	updated, ok := manager.GetByID("codex.json")
	if !ok || updated == nil {
		t.Fatalf("expected auth record to exist after patch")
	}
	if got := updated.Attributes["websockets"]; got != "false" {
		t.Fatalf("attrs websockets = %q, want %q", got, "false")
	}
	if got, ok := updated.Metadata["websockets"].(bool); !ok || got {
		t.Fatalf("metadata.websockets = %#v, want false", updated.Metadata["websockets"])
	}
}

func TestPatchAuthFileFields_ArbitraryFieldsPersistToFile(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	fileName := "generic.json"
	filePath := filepath.Join(authDir, fileName)
	store := fileauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "codex",
		Attributes: map[string]string{
			"path": filePath,
		},
		Metadata: map[string]any{
			"type": "codex",
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	body := `{"name":"generic.json","abc":true,"nested.cde":true,"fgh":{"ijk":true}}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFields(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	raw, errRead := os.ReadFile(filePath)
	if errRead != nil {
		t.Fatalf("failed to read updated auth file: %v", errRead)
	}
	var data map[string]any
	if errUnmarshal := json.Unmarshal(raw, &data); errUnmarshal != nil {
		t.Fatalf("failed to unmarshal updated auth file: %v", errUnmarshal)
	}
	if got := data["abc"]; got != true {
		t.Fatalf("abc = %#v, want true", got)
	}
	nested, ok := data["nested"].(map[string]any)
	if !ok {
		t.Fatalf("nested = %#v, want object", data["nested"])
	}
	if got := nested["cde"]; got != true {
		t.Fatalf("nested.cde = %#v, want true", got)
	}
	fgh, ok := data["fgh"].(map[string]any)
	if !ok {
		t.Fatalf("fgh = %#v, want object", data["fgh"])
	}
	if got := fgh["ijk"]; got != true {
		t.Fatalf("fgh.ijk = %#v, want true", got)
	}
}

func TestPatchAuthFileFieldsBatch_UpdatesValidItemsAndReportsFailures(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	for _, record := range []*coreauth.Auth{
		{
			ID:       "first.json",
			FileName: "first.json",
			Provider: "claude",
			Metadata: map[string]any{"type": "claude"},
		},
		{
			ID:       "second.json",
			FileName: "second.json",
			Provider: "codex",
			Metadata: map[string]any{"type": "codex", "websockets": true},
		},
	} {
		if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
			t.Fatalf("failed to register auth record: %v", errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	body := `{"updates":[{"name":"first.json","fields":{"note":"updated"}},{"name":"missing.json","fields":{"note":"ignored"}},{"name":"second.json","fields":{"websockets":false}}]}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields/batch", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req
	h.PatchAuthFileFieldsBatch(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var response struct {
		Updated int `json:"updated"`
		Failed  []struct {
			Name  string `json:"name"`
			Error string `json:"error"`
		} `json:"failed"`
	}
	if errDecode := json.NewDecoder(rec.Body).Decode(&response); errDecode != nil {
		t.Fatalf("failed to decode response: %v", errDecode)
	}
	if response.Updated != 2 {
		t.Fatalf("updated = %d, want 2", response.Updated)
	}
	if len(response.Failed) != 1 {
		t.Fatalf("failed = %#v, want one item", response.Failed)
	}
	if response.Failed[0].Name != "missing.json" || response.Failed[0].Error != "auth file not found" {
		t.Fatalf("failed item = %#v", response.Failed[0])
	}

	first, ok := manager.GetByID("first.json")
	if !ok || first == nil {
		t.Fatal("first auth record not found")
	}
	if got := first.Metadata["note"]; got != "updated" {
		t.Fatalf("first note = %#v, want updated", got)
	}

	second, ok := manager.GetByID("second.json")
	if !ok || second == nil {
		t.Fatal("second auth record not found")
	}
	if got := second.Metadata["websockets"]; got != false {
		t.Fatalf("second websockets = %#v, want false", got)
	}
}

func TestPatchAuthFileFieldsBatch_PriorityZeroIsImmediatelyListed(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	for _, record := range []*coreauth.Auth{
		{
			ID:         "selected.json",
			FileName:   "selected.json",
			Provider:   "codex",
			Attributes: map[string]string{"path": "/tmp/selected.json", "priority": "1"},
			Metadata:   map[string]any{"type": "codex", "priority": float64(1)},
		},
		{
			ID:         "untouched.json",
			FileName:   "untouched.json",
			Provider:   "codex",
			Attributes: map[string]string{"path": "/tmp/untouched.json", "priority": "7"},
			Metadata:   map[string]any{"type": "codex", "priority": float64(7)},
		},
	} {
		if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
			t.Fatalf("failed to register auth record: %v", errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	patchRecorder := httptest.NewRecorder()
	patchContext, _ := gin.CreateTestContext(patchRecorder)
	patchRequest := httptest.NewRequest(
		http.MethodPatch,
		"/v0/management/auth-files/fields/batch",
		strings.NewReader(`{"updates":[{"name":"selected.json","fields":{"priority":0}}]}`),
	)
	patchRequest.Header.Set("Content-Type", "application/json")
	patchContext.Request = patchRequest
	h.PatchAuthFileFieldsBatch(patchContext)

	if patchRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d with body %s", http.StatusOK, patchRecorder.Code, patchRecorder.Body.String())
	}

	listRecorder := httptest.NewRecorder()
	listContext, _ := gin.CreateTestContext(listRecorder)
	listContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	h.ListAuthFiles(listContext)

	if listRecorder.Code != http.StatusOK {
		t.Fatalf("expected list status %d, got %d with body %s", http.StatusOK, listRecorder.Code, listRecorder.Body.String())
	}
	var response struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.NewDecoder(listRecorder.Body).Decode(&response); errDecode != nil {
		t.Fatalf("failed to decode list response: %v", errDecode)
	}

	priorities := make(map[string]any, len(response.Files))
	for _, file := range response.Files {
		name, _ := file["name"].(string)
		priorities[name] = file["priority"]
	}
	if got := priorities["selected.json"]; got != float64(0) {
		t.Fatalf("selected priority = %#v, want 0", got)
	}
	if got := priorities["untouched.json"]; got != float64(7) {
		t.Fatalf("untouched priority = %#v, want 7", got)
	}
}

func TestPatchAuthFileFields_WeightPersistsAndSyncsRuntime(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	fileName := "weighted.json"
	filePath := filepath.Join(authDir, fileName)
	store := fileauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:         fileName,
		FileName:   fileName,
		Provider:   "codex",
		Attributes: map[string]string{"path": filePath},
		Metadata:   map[string]any{"type": "codex"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	patch := func(weight string) *httptest.ResponseRecorder {
		t.Helper()
		rec := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(rec)
		body := `{"name":"weighted.json","weight":` + weight + `}`
		ctx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
		ctx.Request.Header.Set("Content-Type", "application/json")
		h.PatchAuthFileFields(ctx)
		return rec
	}

	if rec := patch("7"); rec.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	updated, ok := manager.GetByID(fileName)
	if !ok || updated.Attributes[coreauth.AttributeWeight] != "7" {
		t.Fatalf("runtime weight = %#v, want 7", updated)
	}
	raw, errRead := os.ReadFile(filePath)
	if errRead != nil {
		t.Fatalf("ReadFile() error = %v", errRead)
	}
	var persisted map[string]any
	if errUnmarshal := json.Unmarshal(raw, &persisted); errUnmarshal != nil {
		t.Fatalf("Unmarshal() error = %v", errUnmarshal)
	}
	if persisted["weight"] != float64(7) {
		t.Fatalf("persisted weight = %#v, want 7", persisted["weight"])
	}

	if rec := patch("null"); rec.Code != http.StatusOK {
		t.Fatalf("reset status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	updated, _ = manager.GetByID(fileName)
	if _, exists := updated.Attributes[coreauth.AttributeWeight]; exists {
		t.Fatal("runtime weight remains after reset")
	}
	raw, errRead = os.ReadFile(filePath)
	if errRead != nil {
		t.Fatalf("ReadFile() after reset error = %v", errRead)
	}
	persisted = nil
	if errUnmarshal := json.Unmarshal(raw, &persisted); errUnmarshal != nil {
		t.Fatalf("Unmarshal() after reset error = %v", errUnmarshal)
	}
	if _, exists := persisted["weight"]; exists {
		t.Fatal("persisted weight remains after reset")
	}
}

func TestPatchAuthFileFields_RejectsInvalidWeights(t *testing.T) {
	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{ID: "auth.json", FileName: "auth.json", Provider: "codex", Metadata: map[string]any{"type": "codex"}}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)

	for _, weight := range []string{"1.5", "1000001", "9223372036854775808", `"7"`} {
		rec := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(rec)
		body := `{"name":"auth.json","weight":` + weight + `}`
		ctx.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
		ctx.Request.Header.Set("Content-Type", "application/json")
		h.PatchAuthFileFields(ctx)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("weight %s status = %d, want 400; body=%s", weight, rec.Code, rec.Body.String())
		}
	}
}

func TestPatchAuthFileFields_RequestRetryRoundTrip(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	fileName := "request-retry.json"
	store := fileauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := coreauth.NewManager(store, nil, nil)
	record := &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "codex",
		Attributes: map[string]string{
			"path": filepath.Join(authDir, fileName),
		},
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	handler := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	engine := gin.New()
	engine.GET("/auth-files", handler.ListAuthFiles)
	engine.PATCH("/auth-files/fields", handler.PatchAuthFileFields)

	patch := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPatch, "/auth-files/fields", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		engine.ServeHTTP(response, request)
		return response
	}
	getRequestRetry := func() *int {
		t.Helper()
		response := httptest.NewRecorder()
		engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/auth-files", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("GET status = %d body=%s", response.Code, response.Body.String())
		}
		var payload struct {
			Files []struct {
				Name         string `json:"name"`
				RequestRetry *int   `json:"request_retry"`
			} `json:"files"`
		}
		if errDecode := json.Unmarshal(response.Body.Bytes(), &payload); errDecode != nil {
			t.Fatalf("decode GET response: %v", errDecode)
		}
		if len(payload.Files) != 1 || payload.Files[0].Name != fileName {
			t.Fatalf("GET files = %#v", payload.Files)
		}
		return payload.Files[0].RequestRetry
	}

	if response := patch(`{"name":"request-retry.json","request-retry":2}`); response.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d body=%s", response.Code, response.Body.String())
	}
	updated, ok := manager.GetByID(fileName)
	if !ok {
		t.Fatal("updated auth is missing")
	}
	if retry, okRetry := updated.RequestRetryOverride(); !okRetry || retry != 2 {
		t.Fatalf("RequestRetryOverride() = (%d, %t), want (2, true)", retry, okRetry)
	}
	if _, exists := updated.Metadata["request-retry"]; exists {
		t.Fatalf("legacy request-retry metadata remains: %#v", updated.Metadata)
	}
	persistedData, errRead := os.ReadFile(filepath.Join(authDir, fileName))
	if errRead != nil {
		t.Fatalf("read persisted auth: %v", errRead)
	}
	var persisted map[string]any
	if errUnmarshal := json.Unmarshal(persistedData, &persisted); errUnmarshal != nil {
		t.Fatalf("decode persisted auth: %v", errUnmarshal)
	}
	if persisted["request_retry"] != float64(2) {
		t.Fatalf("persisted request_retry = %#v, want 2", persisted["request_retry"])
	}
	if _, exists := persisted["request-retry"]; exists {
		t.Fatalf("persisted legacy request-retry remains: %#v", persisted)
	}
	if retry := getRequestRetry(); retry == nil || *retry != 2 {
		t.Fatalf("GET request_retry = %#v, want 2", retry)
	}

	if response := patch(`{"name":"request-retry.json","request_retry":0}`); response.Code != http.StatusOK {
		t.Fatalf("PATCH underscore status = %d body=%s", response.Code, response.Body.String())
	}
	if retry := getRequestRetry(); retry == nil || *retry != 0 {
		t.Fatalf("GET request_retry = %#v, want explicit 0", retry)
	}

	if response := patch(`{"name":"request-retry.json","request-retry":-1}`); response.Code != http.StatusOK {
		t.Fatalf("PATCH negative status = %d body=%s", response.Code, response.Body.String())
	}
	if retry := getRequestRetry(); retry != nil {
		t.Fatalf("GET request_retry after negative clear = %#v, want omitted", retry)
	}

	if response := patch(`{"name":"request-retry.json","request_retry":2}`); response.Code != http.StatusOK {
		t.Fatalf("PATCH reset status = %d body=%s", response.Code, response.Body.String())
	}
	if response := patch(`{"name":"request-retry.json","request-retry":2,"request_retry":3}`); response.Code != http.StatusOK {
		t.Fatalf("PATCH canonical precedence status = %d body=%s", response.Code, response.Body.String())
	}
	if retry := getRequestRetry(); retry == nil || *retry != 3 {
		t.Fatalf("GET request_retry after alias conflict = %#v, want canonical 3", retry)
	}
	if response := patch(`{"name":"request-retry.json","request_retry":2}`); response.Code != http.StatusOK {
		t.Fatalf("PATCH second reset status = %d body=%s", response.Code, response.Body.String())
	}
	for _, body := range []string{
		`{"name":"request-retry.json","request-retry":"2"}`,
		`{"name":"request-retry.json","request-retry":1.5}`,
		`{"name":"request-retry.json","request_retry.child":2}`,
		`{"name":"request-retry.json","request_retry .child":2}`,
		`{"name":"request-retry.json","request-retry .child":2}`,
	} {
		if response := patch(body); response.Code != http.StatusBadRequest {
			t.Fatalf("PATCH %s status = %d, want 400 body=%s", body, response.Code, response.Body.String())
		}
		if retry := getRequestRetry(); retry == nil || *retry != 2 {
			t.Fatalf("invalid PATCH changed request_retry to %#v", retry)
		}
	}

	if response := patch(`{"name":"request-retry.json","request_retry":null}`); response.Code != http.StatusOK {
		t.Fatalf("PATCH null status = %d body=%s", response.Code, response.Body.String())
	}
	if retry := getRequestRetry(); retry != nil {
		t.Fatalf("GET request_retry after null clear = %#v, want omitted", retry)
	}
}

func TestAuthFileRequestRetryFromJSON(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
		ok   bool
	}{
		{name: "canonical", raw: `{"request_retry":2}`, want: 2, ok: true},
		{name: "legacy", raw: `{"request-retry":2}`, want: 2, ok: true},
		{name: "negative inherits", raw: `{"request_retry":-1}`},
		{name: "string integer compatibility", raw: `{"request_retry":"2"}`, want: 2, ok: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := authFileRequestRetryFromJSON([]byte(test.raw))
			if got != test.want || ok != test.ok {
				t.Fatalf("authFileRequestRetryFromJSON(%s) = (%d, %t), want (%d, %t)", test.raw, got, ok, test.want, test.ok)
			}
		})
	}
}

func TestNormalizeAuthFilePatchFieldsCanonicalizesLegacyRoots(t *testing.T) {
	fields := map[string]json.RawMessage{
		"request-retry":             json.RawMessage(`2`),
		" disable-cooling ":         json.RawMessage(`true`),
		"fingerprint-profile.value": json.RawMessage(`"x"`),
		"provider-specific":         json.RawMessage(`"preserved"`),
	}

	normalized, errNormalize := normalizeAuthFilePatchFields(fields)
	if errNormalize != nil {
		t.Fatalf("normalizeAuthFilePatchFields() error = %v", errNormalize)
	}
	for _, key := range []string{"request_retry", "disable_cooling", "fingerprint_profile.value", "provider-specific"} {
		if _, exists := normalized[key]; !exists {
			t.Fatalf("normalized fields missing %q: %#v", key, normalized)
		}
	}

	canonicalWins, errCanonicalWins := normalizeAuthFilePatchFields(map[string]json.RawMessage{
		"request-retry": json.RawMessage(`2`),
		"request_retry": json.RawMessage(`3`),
	})
	if errCanonicalWins != nil {
		t.Fatalf("normalizeAuthFilePatchFields() canonical precedence error = %v", errCanonicalWins)
	}
	if got := string(canonicalWins["request_retry"]); got != "3" {
		t.Fatalf("normalized request_retry = %s, want canonical value 3", got)
	}

	_, errNestedDuplicate := normalizeAuthFilePatchFields(map[string]json.RawMessage{
		"disable_cooling.value":   json.RawMessage(`true`),
		"disable_cooling . value": json.RawMessage(`false`),
	})
	if errNestedDuplicate == nil {
		t.Fatal("normalizeAuthFilePatchFields() accepted equivalent nested paths")
	}
}

func TestSetSourceAuthFileDisabledNormalizesLegacyMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","request-retry":2,"disable-cooling":true}`), 0o600); errWrite != nil {
		t.Fatalf("write legacy auth file: %v", errWrite)
	}

	if errDisable := setSourceAuthFileDisabled(path, true); errDisable != nil {
		t.Fatalf("setSourceAuthFileDisabled() error = %v", errDisable)
	}
	persistedData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read persisted auth file: %v", errRead)
	}
	var persisted map[string]any
	if errUnmarshal := json.Unmarshal(persistedData, &persisted); errUnmarshal != nil {
		t.Fatalf("decode persisted auth file: %v", errUnmarshal)
	}
	if got := persisted["request_retry"]; got != float64(2) {
		t.Fatalf("persisted request_retry = %#v, want 2", got)
	}
	if got := persisted["disable_cooling"]; got != true {
		t.Fatalf("persisted disable_cooling = %#v, want true", got)
	}
	if got := persisted["disabled"]; got != true {
		t.Fatalf("persisted disabled = %#v, want true", got)
	}
	for _, legacy := range []string{"request-retry", "disable-cooling"} {
		if _, exists := persisted[legacy]; exists {
			t.Fatalf("persisted metadata retained %q: %#v", legacy, persisted)
		}
	}
}

func TestPatchAuthFileFields_SyncsPlanTypeAndInvokesHook(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	fileName := "codex-plan-patch.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "codex",
		Attributes: map[string]string{
			"path":      filePath,
			"plan_type": "pro",
		},
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	handler := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	var hookedAuth *coreauth.Auth
	handler.SetPostAuthPersistHook(func(_ context.Context, auth *coreauth.Auth) error {
		if auth != nil {
			hookedAuth = auth.Clone()
		}
		return nil
	})

	teamIDToken := "eyJhbGciOiJub25lIn0.eyJlbWFpbCI6ICJ1c2VyQGV4YW1wbGUuY29tIiwgImh0dHBzOi8vYXBpLm9wZW5haS5jb20vYXV0aCI6IHsiY2hhdGdwdF9wbGFuX3R5cGUiOiAidGVhbSIsICJjaGF0Z3B0X2FjY291bnRfaWQiOiAiYWNjLTEyMyJ9fQ.sig"
	body := fmt.Sprintf(`{"name":%q,"id_token":%q}`, fileName, teamIDToken)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	handler.PatchAuthFileFields(ctx)
	if rec.Code != http.StatusOK {
		t.Fatalf("PatchAuthFileFields status = %d, body = %s", rec.Code, rec.Body.String())
	}

	auth, ok := manager.GetByID(fileName)
	if !ok || auth == nil {
		t.Fatal("auth not found after patch")
	}
	if got := auth.Attributes["plan_type"]; got != "team" {
		t.Fatalf("auth plan_type attribute = %q, want team", got)
	}
	if hookedAuth == nil {
		t.Fatal("expected postAuthPersistHook to be invoked on PatchAuthFileFields")
	}
	if got := hookedAuth.Attributes["plan_type"]; got != "team" {
		t.Fatalf("hooked auth plan_type attribute = %q, want team", got)
	}
}

func TestPatchAuthFileFields_ClearsPlanTypeWhenRemoved(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	fileName := "codex-clear-plan.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex","plan_type":"free"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "codex",
		Attributes: map[string]string{
			"path":      filePath,
			"plan_type": "free",
		},
		Metadata: map[string]any{
			"type":      "codex",
			"plan_type": "free",
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	handler := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	body := fmt.Sprintf(`{"name":%q,"plan_type":null}`, fileName)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	handler.PatchAuthFileFields(ctx)
	if rec.Code != http.StatusOK {
		t.Fatalf("PatchAuthFileFields status = %d, body = %s", rec.Code, rec.Body.String())
	}

	auth, ok := manager.GetByID(fileName)
	if !ok || auth == nil {
		t.Fatal("auth not found after patch")
	}
	if _, exists := auth.Attributes["plan_type"]; exists {
		t.Fatalf("expected plan_type attribute to be deleted after clearing plan_type, got %q", auth.Attributes["plan_type"])
	}
}

func TestPatchAuthFileFields_IDTokenMissingPlanTypeDefaultsToFree(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	tempDir := t.TempDir()
	fileName := "codex-test-patch-idtoken.json"
	filePath := filepath.Join(tempDir, fileName)

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	claimsMap := map[string]any{
		"email": "user@example.com",
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "acc-12345",
		},
	}
	payloadBytes, _ := json.Marshal(claimsMap)
	claims := base64.RawURLEncoding.EncodeToString(payloadBytes)
	testIDToken := header + "." + claims + "."

	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("failed to write auth file: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "codex",
		Attributes: map[string]string{
			"path": filePath,
		},
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	handler := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: tempDir}, manager)

	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)

	body := fmt.Sprintf(`{"name":%q,"id_token":%q}`, fileName, testIDToken)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	handler.PatchAuthFileFields(ctx)
	if rec.Code != http.StatusOK {
		t.Fatalf("PatchAuthFileFields status = %d, body = %s", rec.Code, rec.Body.String())
	}

	auth, ok := manager.GetByID(fileName)
	if !ok || auth == nil {
		t.Fatal("auth not found after patch")
	}
	if got := auth.Attributes["plan_type"]; got != "free" {
		t.Fatalf("auth plan_type attribute = %q, want free", got)
	}
}

func TestPatchAuthFileFieldsBatch_ExcludedModelsValidation(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)
	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	for _, name := range []string{"valid.json", "invalid.json"} {
		if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
			ID: name, FileName: name, Provider: "codex",
			Metadata:   map[string]any{"type": "codex", "excluded_models": []string{"old-*"}},
			Attributes: map[string]string{"auth_kind": "oauth", "excluded_models": "old-*"},
		}); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	body := `{"updates":[{"name":"invalid.json","fields":{"excluded_models":[42]}},{"name":"valid.json","fields":{"excluded-models":[" Local-* "]}}]}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodPatch, "/v8/management/credentials/fields/batch", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	h.PatchAuthFileFieldsBatch(ctx)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var result struct {
		Updated int `json:"updated"`
		Failed  []struct {
			Name  string `json:"name"`
			Error string `json:"error"`
		} `json:"failed"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &result); errDecode != nil {
		t.Fatal(errDecode)
	}
	if result.Updated != 1 || len(result.Failed) != 1 || result.Failed[0].Name != "invalid.json" || result.Failed[0].Error != "excluded_models must be a string array or null" {
		t.Fatalf("unexpected batch result: %+v", result)
	}
	valid, _ := manager.GetByID("valid.json")
	if got := valid.Attributes["excluded_models"]; got != "local-*" {
		t.Fatalf("valid credential exclusions = %q, want local-*", got)
	}
	invalid, _ := manager.GetByID("invalid.json")
	if got := invalid.Attributes["excluded_models"]; got != "old-*" {
		t.Fatalf("rejected credential exclusions changed: %q", got)
	}
	rules, ok := invalid.Metadata["excluded_models"].([]string)
	if !ok || len(rules) != 1 || rules[0] != "old-*" {
		t.Fatalf("rejected credential metadata changed: %#v", invalid.Metadata)
	}
}
