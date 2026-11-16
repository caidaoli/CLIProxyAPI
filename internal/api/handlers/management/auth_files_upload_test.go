package management

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/MoeForge/moego/json"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func uploadMultipartAuthFile(t *testing.T, h *Handler, name string, content string) {
	t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", name)
	if err != nil {
		t.Fatalf("failed to create multipart file: %v", err)
	}
	if _, err = part.Write([]byte(content)); err != nil {
		t.Fatalf("failed to write multipart content: %v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}
}

func TestUploadAuthFileNotifiesMutationBeforeRuntimeAuthUpdate(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	name := "codex-user@example.com.json"
	var notifiedPath string
	h.SetAuthFileMutationHook(func(path string) {
		notifiedPath = path
		if _, ok := manager.GetByID(name); ok {
			t.Error("runtime auth changed before mutation hook")
		}
	})

	uploadMultipartAuthFile(t, h, name, `{"type":"codex","email":"user@example.com"}`)

	normalizedAuthDir, errEval := filepath.EvalSymlinks(authDir)
	if errEval != nil {
		t.Fatalf("resolve auth dir: %v", errEval)
	}
	wantPath := filepath.Join(normalizedAuthDir, name)
	if got := notifiedPath; got != wantPath {
		t.Fatalf("mutation path = %q, want %q", got, wantPath)
	}
	if _, ok := manager.GetByID(name); !ok {
		t.Fatalf("expected uploaded auth record %q to exist", name)
	}
}

func TestUploadAuthFile_PreservesPriorityAttributes(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	content := `{"type":"codex","email":"midai0530@gmail.com","priority":98}`

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "codex-midai0530@gmail.com-plus.json")
	if err != nil {
		t.Fatalf("failed to create multipart file: %v", err)
	}
	if _, err = part.Write([]byte(content)); err != nil {
		t.Fatalf("failed to write multipart content: %v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var payload map[string]any
	if err = json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if status, _ := payload["status"].(string); status != "ok" {
		t.Fatalf("expected status ok, got %#v", payload["status"])
	}

	auth, ok := manager.GetByID("codex-midai0530@gmail.com-plus.json")
	if !ok || auth == nil {
		t.Fatalf("expected uploaded auth record to exist")
	}
	if got := auth.Attributes["priority"]; got != "98" {
		t.Fatalf("priority attribute = %q, want %q", got, "98")
	}
	if got := auth.Metadata["priority"]; got != float64(98) {
		t.Fatalf("priority metadata = %#v, want 98", got)
	}
}

func TestUploadAuthFile_FillsMissingEmailFromMultipartFileName(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "codex-midai0530@gmail.com-plus.json")
	if err != nil {
		t.Fatalf("failed to create multipart file: %v", err)
	}
	if _, err = part.Write([]byte(`{"type":"codex","priority":98}`)); err != nil {
		t.Fatalf("failed to write multipart content: %v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	data, err := os.ReadFile(filepath.Join(authDir, "codex-midai0530@gmail.com-plus.json"))
	if err != nil {
		t.Fatalf("expected uploaded auth file to exist: %v", err)
	}
	var saved map[string]any
	if err = json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("failed to decode saved auth file: %v", err)
	}
	if got := saved["email"]; got != "midai0530@gmail.com" {
		t.Fatalf("saved email = %#v, want %q", got, "midai0530@gmail.com")
	}

	auth, ok := manager.GetByID("codex-midai0530@gmail.com-plus.json")
	if !ok || auth == nil {
		t.Fatalf("expected uploaded auth record to exist")
	}
	if got := auth.Metadata["email"]; got != "midai0530@gmail.com" {
		t.Fatalf("auth metadata email = %#v, want %q", got, "midai0530@gmail.com")
	}
	if got := auth.Label; got != "midai0530@gmail.com" {
		t.Fatalf("auth label = %q, want %q", got, "midai0530@gmail.com")
	}
}

func TestUploadAuthFile_PreservesExistingEmailWhenFileNameHasDifferentEmail(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	content := `{"type":"claude","email":"real@example.com"}`
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "claude-wrong@example.com.json")
	if err != nil {
		t.Fatalf("failed to create multipart file: %v", err)
	}
	if _, err = part.Write([]byte(content)); err != nil {
		t.Fatalf("failed to write multipart content: %v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	data, err := os.ReadFile(filepath.Join(authDir, "claude-wrong@example.com.json"))
	if err != nil {
		t.Fatalf("expected uploaded auth file to exist: %v", err)
	}
	if string(data) != content {
		t.Fatalf("expected existing email payload to stay unchanged, got %q", string(data))
	}
}

func TestUploadAuthFile_FillsMissingEmailFromRawUploadName(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	name := "xai-user@example.com.json"
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPost,
		"/v0/management/auth-files?name="+url.QueryEscape(name),
		bytes.NewBufferString(`{"type":"xai"}`),
	)
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	data, err := os.ReadFile(filepath.Join(authDir, name))
	if err != nil {
		t.Fatalf("expected uploaded auth file to exist: %v", err)
	}
	var saved map[string]any
	if err = json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("failed to decode saved auth file: %v", err)
	}
	if got := saved["email"]; got != "xai-user@example.com" {
		t.Fatalf("saved email = %#v, want %q", got, "xai-user@example.com")
	}
}

func TestUploadAuthFile_ReplacingSameNameClearsRuntimeErrorState(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	name := "codex-user@example.com.json"
	uploadMultipartAuthFile(t, h, name, `{"type":"codex","email":"user@example.com","access_token":"old"}`)

	manager.MarkResult(context.Background(), coreauth.Result{
		AuthID:   name,
		Provider: "codex",
		Model:    "gpt-5",
		Success:  false,
		Error: &coreauth.Error{
			Code:       "unauthorized",
			Message:    "401 Your authentication token has been invalidated",
			HTTPStatus: http.StatusUnauthorized,
		},
	})

	failedAuth, ok := manager.GetByID(name)
	if !ok || failedAuth == nil {
		t.Fatalf("expected auth record %s to exist", name)
	}
	if len(failedAuth.ModelStates) == 0 || failedAuth.LastError == nil {
		t.Fatalf("expected 401 model state before replacement, got %+v", failedAuth)
	}

	uploadMultipartAuthFile(t, h, name, `{"type":"codex","email":"user@example.com","access_token":"new"}`)

	replacedAuth, ok := manager.GetByID(name)
	if !ok || replacedAuth == nil {
		t.Fatalf("expected replaced auth record %s to exist", name)
	}
	if replacedAuth.Status != coreauth.StatusActive {
		t.Fatalf("status = %q, want %q", replacedAuth.Status, coreauth.StatusActive)
	}
	if replacedAuth.LastError != nil || replacedAuth.StatusMessage != "" || replacedAuth.Unavailable || !replacedAuth.NextRetryAfter.IsZero() {
		t.Fatalf("runtime error state survived replacement: status_message=%q unavailable=%v next=%v last_error=%+v",
			replacedAuth.StatusMessage, replacedAuth.Unavailable, replacedAuth.NextRetryAfter, replacedAuth.LastError)
	}
	if len(replacedAuth.ModelStates) != 0 {
		t.Fatalf("model states survived same-name replacement: %+v", replacedAuth.ModelStates)
	}
}

func TestUploadAuthFile_TxtMultipartRewritesToJSON(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	uploadMultipartAuthFile(t, h, "codex-user@example.com.txt", `{"type":"codex","email":"user@example.com"}`)

	storedName := "codex-user@example.com.json"
	if _, err := os.Stat(filepath.Join(authDir, storedName)); err != nil {
		t.Fatalf("expected rewritten auth file %q: %v", storedName, err)
	}
	if _, err := os.Stat(filepath.Join(authDir, "codex-user@example.com.txt")); !os.IsNotExist(err) {
		t.Fatalf("did not expect .txt file on disk, err=%v", err)
	}
	if _, ok := manager.GetByID(storedName); !ok {
		t.Fatalf("expected uploaded auth record %q to exist", storedName)
	}
}

func TestUploadAuthFile_TxtRawBodyRewritesToJSON(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	content := `{"type":"codex","email":"raw@example.com"}`
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPost,
		"/v0/management/auth-files?name="+url.QueryEscape("codex-raw@example.com.txt"),
		bytes.NewReader([]byte(content)),
	)
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	storedName := "codex-raw@example.com.json"
	data, err := os.ReadFile(filepath.Join(authDir, storedName))
	if err != nil {
		t.Fatalf("expected rewritten auth file %q: %v", storedName, err)
	}
	var saved map[string]any
	if err = json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("failed to decode saved auth file: %v", err)
	}
	if got := saved["email"]; got != "raw@example.com" {
		t.Fatalf("saved email = %#v, want %q", got, "raw@example.com")
	}
	if _, ok := manager.GetByID(storedName); !ok {
		t.Fatalf("expected uploaded auth record %q to exist", storedName)
	}
}
func TestUploadAuthFile_InvokesPostAuthPersistHook(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	var hookedAuth *coreauth.Auth
	h.SetPostAuthPersistHook(func(_ context.Context, auth *coreauth.Auth) error {
		if auth != nil {
			hookedAuth = auth.Clone()
		}
		return nil
	})

	content := `{"type":"codex","email":"user@example.com","access_token":"token-123"}`

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "codex-user@example.com.json")
	if err != nil {
		t.Fatalf("failed to create multipart file: %v", err)
	}
	if _, err = part.Write([]byte(content)); err != nil {
		t.Fatalf("failed to write multipart content: %v", err)
	}
	if err = writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}
	if hookedAuth == nil {
		t.Fatal("expected postAuthPersistHook to be invoked on auth file upload")
	}
	if hookedAuth.ID != "codex-user@example.com.json" {
		t.Fatalf("hooked auth ID = %q, want codex-user@example.com.json", hookedAuth.ID)
	}
}
