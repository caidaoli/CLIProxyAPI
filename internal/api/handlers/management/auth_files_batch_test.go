package management

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/base64"
	"github.com/MoeForge/moego/json"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func buildAuthZipArchive(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	archive := zip.NewWriter(&buf)
	for name, content := range files {
		writer, errCreate := archive.Create(name)
		if errCreate != nil {
			t.Fatalf("create zip entry %s: %v", name, errCreate)
		}
		if _, errWrite := writer.Write([]byte(content)); errWrite != nil {
			t.Fatalf("write zip entry %s: %v", name, errWrite)
		}
	}
	if errClose := archive.Close(); errClose != nil {
		t.Fatalf("close zip archive: %v", errClose)
	}
	return buf.Bytes()
}

func buildAuthTarArchive(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	archive := tar.NewWriter(&buf)
	for name, content := range files {
		payload := []byte(content)
		header := &tar.Header{
			Name: name,
			Mode: 0o600,
			Size: int64(len(payload)),
		}
		if errWrite := archive.WriteHeader(header); errWrite != nil {
			t.Fatalf("write tar header %s: %v", name, errWrite)
		}
		if _, errWrite := archive.Write(payload); errWrite != nil {
			t.Fatalf("write tar entry %s: %v", name, errWrite)
		}
	}
	if errClose := archive.Close(); errClose != nil {
		t.Fatalf("close tar archive: %v", errClose)
	}
	return buf.Bytes()
}

func buildAuthTarGzArchive(t *testing.T, files map[string]string) []byte {
	t.Helper()
	tarBytes := buildAuthTarArchive(t, files)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, errWrite := gz.Write(tarBytes); errWrite != nil {
		t.Fatalf("write gzip payload: %v", errWrite)
	}
	if errClose := gz.Close(); errClose != nil {
		t.Fatalf("close gzip writer: %v", errClose)
	}
	return buf.Bytes()
}

func buildAuthTestJWT(t *testing.T, claims map[string]any) string {
	t.Helper()
	header, errHeader := json.Marshal(map[string]string{"alg": "none", "typ": "JWT"})
	if errHeader != nil {
		t.Fatalf("marshal JWT header: %v", errHeader)
	}
	payload, errPayload := json.Marshal(claims)
	if errPayload != nil {
		t.Fatalf("marshal JWT claims: %v", errPayload)
	}
	return base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload) + ".test"
}

func uploadRawAuthPayload(t *testing.T, h *Handler, name string, payload []byte) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPost,
		"/v0/management/auth-files?name="+url.QueryEscape(name),
		bytes.NewReader(payload),
	)
	ctx.Request = req
	h.UploadAuthFile(ctx)
	return rec
}

func TestDetectAuthArchiveFormat(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		filename    string
		contentType string
		want        authArchiveFormat
	}{
		{name: "zip-ext", filename: "bundle.zip", want: authArchiveZip},
		{name: "tar-ext", filename: "bundle.tar", want: authArchiveTar},
		{name: "tar-gz-ext", filename: "bundle.tar.gz", want: authArchiveTarGz},
		{name: "tgz-ext", filename: "bundle.tgz", want: authArchiveTarGz},
		{name: "json-ext", filename: "auth.json", want: authArchiveUnknown},
		{name: "zip-ct", contentType: "application/zip", want: authArchiveZip},
		{name: "tar-ct", contentType: "application/x-tar", want: authArchiveTar},
		{name: "gzip-ct", contentType: "application/gzip", want: authArchiveTarGz},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := detectAuthArchiveFormat(tc.filename, tc.contentType); got != tc.want {
				t.Fatalf("detectAuthArchiveFormat(%q, %q) = %v, want %v", tc.filename, tc.contentType, got, tc.want)
			}
		})
	}
}

func TestUploadAuthFile_ZipMultipart(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	zipBytes := buildAuthZipArchive(t, map[string]string{
		"nested/xai-alpha@example.com.json": `{"type":"xai","email":"alpha@example.com","access_token":"a"}`,
		"xai-beta@example.com.json":         `{"type":"xai","email":"beta@example.com","access_token":"b"}`,
		"readme.txt":                        "ignore me",
		"__MACOSX/._xai-skip.json":          `{"type":"xai"}`,
	})

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, errCreate := writer.CreateFormFile("file", "auths.zip")
	if errCreate != nil {
		t.Fatalf("create multipart file: %v", errCreate)
	}
	if _, errWrite := part.Write(zipBytes); errWrite != nil {
		t.Fatalf("write multipart zip: %v", errWrite)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var payload map[string]any
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if got := int(payload["uploaded"].(float64)); got != 2 {
		t.Fatalf("uploaded = %d, want 2; body = %s", got, rec.Body.String())
	}

	for _, name := range []string{"xai-alpha@example.com.json", "xai-beta@example.com.json"} {
		if _, errStat := os.Stat(filepath.Join(authDir, name)); errStat != nil {
			t.Fatalf("expected extracted auth file %s: %v", name, errStat)
		}
	}
	if got := len(manager.List()); got != 2 {
		t.Fatalf("registered auth count = %d, want 2", got)
	}
}

func TestUploadAuthFile_ZipRawBody(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	zipBytes := buildAuthZipArchive(t, map[string]string{
		"xai-one@example.com.json": `{"type":"xai","email":"one@example.com","access_token":"1"}`,
		"xai-two@example.com.json": `{"type":"xai","email":"two@example.com","access_token":"2"}`,
	})

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPost,
		"/v0/management/auth-files?name="+url.QueryEscape("bundle.zip"),
		bytes.NewReader(zipBytes),
	)
	req.Header.Set("Content-Type", "application/zip")
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var payload map[string]any
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if got := int(payload["uploaded"].(float64)); got != 2 {
		t.Fatalf("uploaded = %d, want 2; body = %s", got, rec.Body.String())
	}
	if got := len(manager.List()); got != 2 {
		t.Fatalf("registered auth count = %d, want 2", got)
	}
}

func TestUploadAuthFile_ZipRejectsTraversal(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	// Entry path looks nested/absolute-ish; we only persist basename and reject unsafe basenames.
	zipBytes := buildAuthZipArchive(t, map[string]string{
		"../escape.json":               `{"type":"xai","access_token":"bad"}`,
		"ok/xai-safe@example.com.json": `{"type":"xai","email":"safe@example.com","access_token":"ok"}`,
	})

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name=bundle.zip", bytes.NewReader(zipBytes))
	req.Header.Set("Content-Type", "application/zip")
	ctx.Request = req

	h.UploadAuthFile(ctx)

	// escape.json basename is safe after filepath.Base, so both may import — ensure no file lands outside authDir.
	entries, errRead := os.ReadDir(authDir)
	if errRead != nil {
		t.Fatalf("read auth dir: %v", errRead)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "..") || strings.ContainsAny(entry.Name(), `/\`) {
			t.Fatalf("unsafe file name written: %q", entry.Name())
		}
	}
	// Parent of authDir must not gain escape.json
	if _, errStat := os.Stat(filepath.Join(filepath.Dir(authDir), "escape.json")); errStat == nil {
		t.Fatalf("zip slip wrote outside auth dir")
	}
	_ = rec
}

func TestUploadAuthFile_TarMultipart(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	tarBytes := buildAuthTarArchive(t, map[string]string{
		"nested/xai-alpha@example.com.json": `{"type":"xai","email":"alpha@example.com","access_token":"a"}`,
		"xai-beta@example.com.json":         `{"type":"xai","email":"beta@example.com","access_token":"b"}`,
		"readme.txt":                        "ignore me",
	})

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, errCreate := writer.CreateFormFile("file", "auths.tar")
	if errCreate != nil {
		t.Fatalf("create multipart file: %v", errCreate)
	}
	if _, errWrite := part.Write(tarBytes); errWrite != nil {
		t.Fatalf("write multipart tar: %v", errWrite)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var payload map[string]any
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if got := int(payload["uploaded"].(float64)); got != 2 {
		t.Fatalf("uploaded = %d, want 2; body = %s", got, rec.Body.String())
	}
	for _, name := range []string{"xai-alpha@example.com.json", "xai-beta@example.com.json"} {
		if _, errStat := os.Stat(filepath.Join(authDir, name)); errStat != nil {
			t.Fatalf("expected extracted auth file %s: %v", name, errStat)
		}
	}
	if got := len(manager.List()); got != 2 {
		t.Fatalf("registered auth count = %d, want 2", got)
	}
}

func TestUploadAuthFile_TarGzRawBody(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	archiveBytes := buildAuthTarGzArchive(t, map[string]string{
		"nested/xai-one@example.com.json": `{"type":"xai","email":"one@example.com","access_token":"1"}`,
		"xai-two@example.com.json":        `{"type":"xai","email":"two@example.com","access_token":"2"}`,
		"notes.md":                        "skip",
	})

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPost,
		"/v0/management/auth-files?name="+url.QueryEscape("bundle.tgz"),
		bytes.NewReader(archiveBytes),
	)
	req.Header.Set("Content-Type", "application/gzip")
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var payload map[string]any
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if got := int(payload["uploaded"].(float64)); got != 2 {
		t.Fatalf("uploaded = %d, want 2; body = %s", got, rec.Body.String())
	}
	if got := len(manager.List()); got != 2 {
		t.Fatalf("registered auth count = %d, want 2", got)
	}
}

func TestUploadAuthFile_TarRejectsTraversal(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	tarBytes := buildAuthTarArchive(t, map[string]string{
		"../escape.json":               `{"type":"xai","access_token":"bad"}`,
		"ok/xai-safe@example.com.json": `{"type":"xai","email":"safe@example.com","access_token":"ok"}`,
	})

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name=bundle.tar", bytes.NewReader(tarBytes))
	req.Header.Set("Content-Type", "application/x-tar")
	ctx.Request = req

	h.UploadAuthFile(ctx)

	entries, errRead := os.ReadDir(authDir)
	if errRead != nil {
		t.Fatalf("read auth dir: %v", errRead)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "..") || strings.ContainsAny(entry.Name(), `/\`) {
			t.Fatalf("unsafe file name written: %q", entry.Name())
		}
	}
	if _, errStat := os.Stat(filepath.Join(filepath.Dir(authDir), "escape.json")); errStat == nil {
		t.Fatalf("tar slip wrote outside auth dir")
	}
	_ = rec
}

func TestUploadAuthFile_BatchMultipartExceedsDefaultPartLimit(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	store := sdkAuth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := coreauth.NewManager(store, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	const fileCount = 2465
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for fileIndex := 0; fileIndex < fileCount; fileIndex++ {
		part, errCreate := writer.CreateFormFile("file", fmt.Sprintf("xai-%04d.json", fileIndex))
		if errCreate != nil {
			t.Fatalf("create multipart file: %v", errCreate)
		}
		if _, errWrite := part.Write([]byte(`{"type":"xai","access_token":"token"}`)); errWrite != nil {
			t.Fatalf("write multipart file: %v", errWrite)
		}
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := len(manager.List()); got != fileCount {
		t.Fatalf("registered auth count = %d, want %d", got, fileCount)
	}
}

func TestUploadAuthFile_BatchMultipart(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	files := []struct {
		name    string
		content string
	}{
		{name: "alpha.json", content: `{"type":"codex","email":"alpha@example.com"}`},
		{name: "beta.json", content: `{"type":"claude","email":"beta@example.com"}`},
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, file := range files {
		part, err := writer.CreateFormFile("file", file.name)
		if err != nil {
			t.Fatalf("failed to create multipart file: %v", err)
		}
		if _, err = part.Write([]byte(file.content)); err != nil {
			t.Fatalf("failed to write multipart content: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
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
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if got, ok := payload["uploaded"].(float64); !ok || int(got) != len(files) {
		t.Fatalf("expected uploaded=%d, got %#v", len(files), payload["uploaded"])
	}

	for _, file := range files {
		fullPath := filepath.Join(authDir, file.name)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			t.Fatalf("expected uploaded file %s to exist: %v", file.name, err)
		}
		if string(data) != file.content {
			t.Fatalf("expected file %s content %q, got %q", file.name, file.content, string(data))
		}
	}

	auths := manager.List()
	if len(auths) != len(files) {
		t.Fatalf("expected %d auth entries, got %d", len(files), len(auths))
	}
}

func TestUploadAuthFile_WatcherBackedBatchDefersRuntimeLoading(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	for _, version := range []string{"v0", "v8"} {
		t.Run(version, func(t *testing.T) {
			authDir := t.TempDir()
			manager := coreauth.NewManager(nil, nil, nil)
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
			h.SetAuthLoadStatusProvider(func() watcher.AuthLoadStatus {
				return watcher.AuthLoadStatus{State: watcher.AuthLoadStateReady}
			})

			var body bytes.Buffer
			writer := multipart.NewWriter(&body)
			for _, name := range []string{"alpha.json", "beta.json"} {
				part, errCreate := writer.CreateFormFile("file", name)
				if errCreate != nil {
					t.Fatalf("create multipart file: %v", errCreate)
				}
				if _, errWrite := part.Write([]byte(`{"type":"codex","access_token":"token"}`)); errWrite != nil {
					t.Fatalf("write multipart file: %v", errWrite)
				}
			}
			if errClose := writer.Close(); errClose != nil {
				t.Fatalf("close multipart writer: %v", errClose)
			}

			rec := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rec)
			req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
			req.Header.Set("Content-Type", writer.FormDataContentType())
			ctx.Request = req

			if version == "v8" {
				h.UploadAuthFileV8(ctx)
			} else {
				h.UploadAuthFile(ctx)
			}

			if rec.Code != http.StatusOK {
				t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			if got := len(manager.List()); got != 0 {
				t.Fatalf("synchronously registered auth count = %d, want 0", got)
			}
			for _, name := range []string{"alpha.json", "beta.json"} {
				if _, errStat := os.Stat(filepath.Join(authDir, name)); errStat != nil {
					t.Fatalf("uploaded file %s was not persisted: %v", name, errStat)
				}
			}
			var payload map[string]any
			if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
				t.Fatalf("decode response: %v", errDecode)
			}
			if got := int(payload["uploaded"].(float64)); got != 2 {
				t.Fatalf("uploaded count = %d, want 2", got)
			}
			if _, returnedFiles := payload["files"]; returnedFiles {
				t.Fatalf("response returned uploaded file details: %s", rec.Body.String())
			}
		})
	}
}

func TestUploadAuthFile_BatchMultipart_InvalidJSONDoesNotOverwriteExistingFile(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	existingName := "alpha.json"
	existingContent := `{"type":"codex","email":"alpha@example.com"}`
	if err := os.WriteFile(filepath.Join(authDir, existingName), []byte(existingContent), 0o600); err != nil {
		t.Fatalf("failed to seed existing auth file: %v", err)
	}

	files := []struct {
		name    string
		content string
	}{
		{name: existingName, content: `{"type":"codex"`},
		{name: "beta.json", content: `{"type":"claude","email":"beta@example.com"}`},
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, file := range files {
		part, err := writer.CreateFormFile("file", file.name)
		if err != nil {
			t.Fatalf("failed to create multipart file: %v", err)
		}
		if _, err = part.Write([]byte(file.content)); err != nil {
			t.Fatalf("failed to write multipart content: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusMultiStatus, rec.Code, rec.Body.String())
	}

	data, err := os.ReadFile(filepath.Join(authDir, existingName))
	if err != nil {
		t.Fatalf("expected existing auth file to remain readable: %v", err)
	}
	if string(data) != existingContent {
		t.Fatalf("expected existing auth file to remain %q, got %q", existingContent, string(data))
	}

	betaData, err := os.ReadFile(filepath.Join(authDir, "beta.json"))
	if err != nil {
		t.Fatalf("expected valid auth file to be created: %v", err)
	}
	if string(betaData) != files[1].content {
		t.Fatalf("expected beta auth file content %q, got %q", files[1].content, string(betaData))
	}
}

func TestUploadAuthFile_Sub2APIDataRawBody(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	payload := `{
		"type":"sub2api-data",
		"version":1,
		"exported_at":"2026-07-18T09:57:29Z",
		"accounts":[
			{
				"name":"alpha+team@example.com",
				"platform":"openai",
				"type":"oauth",
				"credentials":{
					"access_token":"access-alpha",
					"refresh_token":"refresh-alpha",
					"chatgpt_account_id":"account-team",
					"client_id":"client-1",
					"expires_at":1800000000,
					"expires_in":3600,
					"plan_type":"team",
					"model_mapping":{"client-alias":"upstream-model","identity":"identity"}
				},
				"priority":-5
			},
			{
				"name":"beta@example.com",
				"platform":"openai",
				"type":"oauth",
				"credentials":{
					"access_token":"access-beta",
					"chatgpt_account_id":"account-team",
					"expires_at":1800007200,
					"expires_in":7200,
					"plan_type":"team"
				},
				"priority":1
			}
		]
	}`

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPost,
		"/v0/management/auth-files?name="+url.QueryEscape("sub2api-export.json"),
		bytes.NewBufferString(payload),
	)
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response struct {
		Uploaded int `json:"uploaded"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if response.Uploaded != 2 {
		t.Fatalf("uploaded = %d, want 2; body = %s", response.Uploaded, rec.Body.String())
	}

	alphaPath := filepath.Join(authDir, "codex-alpha+team@example.com.json")
	alphaData, errRead := os.ReadFile(alphaPath)
	if errRead != nil {
		t.Fatalf("read converted auth file: %v", errRead)
	}
	var alpha map[string]any
	if errDecode := json.Unmarshal(alphaData, &alpha); errDecode != nil {
		t.Fatalf("decode converted auth file: %v", errDecode)
	}
	assertStringField := func(key, want string) {
		t.Helper()
		if got, _ := alpha[key].(string); got != want {
			t.Fatalf("converted %s = %q, want %q", key, got, want)
		}
	}
	assertStringField("type", "codex")
	assertStringField("email", "alpha+team@example.com")
	assertStringField("access_token", "access-alpha")
	assertStringField("refresh_token", "refresh-alpha")
	assertStringField("account_id", "account-team")
	assertStringField("chatgpt_account_id", "account-team")
	assertStringField("workspace_id", "account-team")
	assertStringField("client_id", "client-1")
	assertStringField("plan_type", "team")
	assertStringField("chatgpt_plan_type", "team")
	assertStringField("expired", time.Unix(1800000000, 0).UTC().Format(time.RFC3339))
	assertStringField("last_refresh", time.Unix(1800000000-3600, 0).UTC().Format(time.RFC3339))
	if got, _ := alpha["disabled"].(bool); got {
		t.Fatalf("converted disabled = true, want false")
	}
	if got := int(alpha["priority"].(float64)); got != -5 {
		t.Fatalf("converted priority = %d, want -5", got)
	}
	if _, exists := alpha["model_aliases"]; exists {
		t.Fatalf("converted auth must not invent model_aliases from Sub2API model_mapping")
	}

	if runtime.GOOS != "windows" {
		info, errStat := os.Stat(alphaPath)
		if errStat != nil {
			t.Fatalf("stat converted auth file: %v", errStat)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("converted auth file mode = %o, want 600", got)
		}
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "sub2api-export.json")); !os.IsNotExist(errStat) {
		t.Fatalf("aggregate source file should not be persisted, stat err = %v", errStat)
	}
	if got := len(manager.List()); got != 2 {
		t.Fatalf("registered auth count = %d, want 2", got)
	}
	betaData, errReadBeta := os.ReadFile(filepath.Join(authDir, "codex-beta@example.com.json"))
	if errReadBeta != nil {
		t.Fatalf("read converted auth without refresh token: %v", errReadBeta)
	}
	var beta map[string]any
	if errDecode := json.Unmarshal(betaData, &beta); errDecode != nil {
		t.Fatalf("decode converted auth without refresh token: %v", errDecode)
	}
	if refreshToken, exists := beta["refresh_token"]; !exists || refreshToken != "" {
		t.Fatalf("converted refresh_token = %#v, want explicit empty string", refreshToken)
	}
	alphaAuth, okAuth := manager.GetByID("codex-alpha+team@example.com.json")
	if !okAuth || alphaAuth.Provider != "codex" {
		t.Fatalf("converted runtime auth = %#v, want codex", alphaAuth)
	}
}

func TestUploadAuthFile_CLIProxyAPIAuthBundleMultipart(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	payload := []byte(`{
		"type":"cliproxyapi-auth-bundle",
		"version":1,
		"source":"test-exporter",
		"exported_at":"2026-07-19T08:48:52Z",
		"accounts":[
			{
				"type":"xai",
				"auth_kind":"oauth",
				"email":"native-one@example.com",
				"sub":"native-sub-1",
				"local_account_id":"local-1",
				"access_token":"bundle-access-1",
				"refresh_token":"bundle-refresh-1",
				"base_url":"https://api.x.ai/v1",
				"disabled":false,
				"headers":{"X-Native-Test":"one"}
			},
			{
				"type":"xai",
				"auth_kind":"oauth",
				"email":"native-two@example.com",
				"sub":"native-sub-2",
				"local_account_id":"local-2",
				"access_token":"bundle-access-2",
				"refresh_token":"bundle-refresh-2",
				"base_url":"https://api.x.ai/v1",
				"disabled":true
			}
		]
	}`)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, errCreate := writer.CreateFormFile("file", "native-bundle.json")
	if errCreate != nil {
		t.Fatalf("create multipart file: %v", errCreate)
	}
	if _, errWrite := part.Write(payload); errWrite != nil {
		t.Fatalf("write multipart file: %v", errWrite)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req
	h.UploadAuthFile(ctx)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var response struct {
		Uploaded int `json:"uploaded"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if response.Uploaded != 2 {
		t.Fatalf("uploaded = %d, want 2; body = %s", response.Uploaded, rec.Body.String())
	}

	firstPath := filepath.Join(authDir, "xai-native-one@example.com.json")
	firstData, errRead := os.ReadFile(firstPath)
	if errRead != nil {
		t.Fatalf("read imported native auth file: %v", errRead)
	}
	var first map[string]any
	if errDecode := json.Unmarshal(firstData, &first); errDecode != nil {
		t.Fatalf("decode imported native auth file: %v", errDecode)
	}
	if got, _ := first["type"].(string); got != "xai" {
		t.Fatalf("imported type = %q, want xai", got)
	}
	if got, _ := first["local_account_id"].(string); got != "local-1" {
		t.Fatalf("imported local_account_id = %q, want local-1", got)
	}
	headers, _ := first["headers"].(map[string]any)
	if got, _ := headers["X-Native-Test"].(string); got != "one" {
		t.Fatalf("imported header = %q, want one", got)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "native-bundle.json")); !os.IsNotExist(errStat) {
		t.Fatalf("native bundle container should not be persisted, stat err = %v", errStat)
	}
	if got := len(manager.List()); got != 2 {
		t.Fatalf("registered auth count = %d, want 2", got)
	}
}

func TestUploadAuthFile_Sub2APICompatibleDocumentShapes(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	cases := []struct {
		name       string
		email      string
		payloadFor func(string) string
	}{
		{
			name:  "accounts container without type",
			email: "container@example.com",
			payloadFor: func(email string) string {
				return fmt.Sprintf(`{"accounts":[{"name":%q,"platform":"openai","type":"oauth","credentials":{"access_token":"access-container"}}]}`, email)
			},
		},
		{
			name:  "account array",
			email: "array@example.com",
			payloadFor: func(email string) string {
				return fmt.Sprintf(`[{"name":%q,"platform":"openai","type":"oauth","credentials":{"access_token":"access-array"}}]`, email)
			},
		},
		{
			name:  "single account",
			email: "single@example.com",
			payloadFor: func(email string) string {
				return fmt.Sprintf(`{"name":%q,"platform":"openai","type":"oauth","credentials":{"access_token":"access-single"}}`, email)
			},
		},
		{
			name:  "nested account",
			email: "nested@example.com",
			payloadFor: func(email string) string {
				return fmt.Sprintf(`{"backup":{"records":[{"name":%q,"platform":"openai","type":"oauth","credentials":{"access_token":"access-nested"}}]}}`, email)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			authDir := t.TempDir()
			manager := coreauth.NewManager(nil, nil, nil)
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

			rec := uploadRawAuthPayload(t, h, "source.json", []byte(tc.payloadFor(tc.email)))
			if rec.Code != http.StatusOK {
				t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			path := filepath.Join(authDir, "codex-"+tc.email+".json")
			data, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatalf("read converted auth file: %v", errRead)
			}
			var document map[string]any
			if errDecode := json.Unmarshal(data, &document); errDecode != nil {
				t.Fatalf("decode converted auth file: %v", errDecode)
			}
			if got, _ := document["type"].(string); got != "codex" {
				t.Fatalf("converted type = %q, want codex", got)
			}
			if _, errStat := os.Stat(filepath.Join(authDir, "source.json")); !os.IsNotExist(errStat) {
				t.Fatalf("source container should not be persisted, stat err = %v", errStat)
			}
		})
	}
}

func TestUploadAuthFile_Sub2APICodexUsesAliasesJWTAndFlexibleTimestamps(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	accessToken := buildAuthTestJWT(t, map[string]any{
		"client_id": "jwt-client-id",
		"email":     "jwt-codex@example.com",
		"exp":       1800000000,
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": "jwt-account-id",
			"chatgpt_plan_type":  "pro",
		},
	})
	payload, errMarshal := json.Marshal(map[string]any{
		"accounts": []any{
			map[string]any{
				"name":     "Imported Codex Account",
				"platform": "openai",
				"type":     "oauth",
				"credentials": map[string]any{
					"accessToken":  accessToken,
					"refreshToken": "refresh-jwt",
				},
				"extra": map[string]any{
					"last_refresh": "1799996400000",
				},
				"disabled":   true,
				"priority":   -3,
				"note":       "imported",
				"prefix":     "team",
				"proxy_url":  "http://127.0.0.1:18080",
				"websockets": true,
				"headers": map[string]any{
					"X-Import-Test": "yes",
				},
			},
		},
	})
	if errMarshal != nil {
		t.Fatalf("marshal Sub2API payload: %v", errMarshal)
	}

	rec := uploadRawAuthPayload(t, h, "aliases.json", payload)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	path := filepath.Join(authDir, "codex-jwt-codex@example.com.json")
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read converted auth file: %v", errRead)
	}
	var document map[string]any
	if errDecode := json.Unmarshal(data, &document); errDecode != nil {
		t.Fatalf("decode converted auth file: %v", errDecode)
	}
	wantStrings := map[string]string{
		"type":               "codex",
		"email":              "jwt-codex@example.com",
		"refresh_token":      "refresh-jwt",
		"account_id":         "jwt-account-id",
		"chatgpt_account_id": "jwt-account-id",
		"workspace_id":       "jwt-account-id",
		"client_id":          "jwt-client-id",
		"plan_type":          "pro",
		"chatgpt_plan_type":  "pro",
		"expired":            time.Unix(1800000000, 0).UTC().Format(time.RFC3339),
		"last_refresh":       time.UnixMilli(1799996400000).UTC().Format(time.RFC3339),
		"note":               "imported",
		"prefix":             "team",
		"proxy_url":          "http://127.0.0.1:18080",
	}
	for key, want := range wantStrings {
		if got, _ := document[key].(string); got != want {
			t.Fatalf("converted %s = %q, want %q", key, got, want)
		}
	}
	if _, exists := document["id_token"]; exists {
		t.Fatalf("converted auth must not synthesize id_token")
	}
	if got, _ := document["disabled"].(bool); !got {
		t.Fatalf("converted disabled = false, want true")
	}
	if got := int(document["priority"].(float64)); got != -3 {
		t.Fatalf("converted priority = %d, want -3", got)
	}
	if got, _ := document["websockets"].(bool); !got {
		t.Fatalf("converted websockets = false, want true")
	}
	headers, _ := document["headers"].(map[string]any)
	if got, _ := headers["X-Import-Test"].(string); got != "yes" {
		t.Fatalf("converted header = %q, want yes", got)
	}
}

func TestUploadAuthFile_Sub2APIConvertsAnthropicOAuthToClaude(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	payload, errMarshal := json.Marshal(map[string]any{
		"exported_at": "2026-07-19T08:09:38Z",
		"proxies":     []any{},
		"accounts": []any{
			map[string]any{
				"name":     "A",
				"platform": "anthropic",
				"type":     "oauth",
				"credentials": map[string]any{
					"access_token":  "anthropic-access",
					"account_uuid":  "11111111-1111-1111-1111-111111111111",
					"email_address": "claude@example.com",
					"expires_at":    1784477337,
					"expires_in":    28800,
					"org_uuid":      "22222222-2222-2222-2222-222222222222",
					"refresh_token": "anthropic-refresh",
					"scope":         "user:profile user:inference user:sessions:claude_code",
					"token_type":    "Bearer",
				},
				"extra": map[string]any{
					"account_uuid":               "11111111-1111-1111-1111-111111111111",
					"email_address":              "claude@example.com",
					"org_uuid":                   "22222222-2222-2222-2222-222222222222",
					"session_window_utilization": 1,
				},
				"priority":              1,
				"auto_pause_on_expired": true,
			},
		},
	})
	if errMarshal != nil {
		t.Fatalf("marshal Sub2API payload: %v", errMarshal)
	}

	rec := uploadRawAuthPayload(t, h, "anthropic.json", payload)
	if rec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	path := filepath.Join(authDir, "claude-claude@example.com.json")
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read converted Claude auth file: %v", errRead)
	}
	var document map[string]any
	if errDecode := json.Unmarshal(data, &document); errDecode != nil {
		t.Fatalf("decode converted Claude auth file: %v", errDecode)
	}
	wantStrings := map[string]string{
		"type":          "claude",
		"email":         "claude@example.com",
		"access_token":  "anthropic-access",
		"refresh_token": "anthropic-refresh",
		"expired":       time.Unix(1784477337, 0).UTC().Format(time.RFC3339),
		"last_refresh":  time.Unix(1784477337-28800, 0).UTC().Format(time.RFC3339),
	}
	for key, want := range wantStrings {
		if got, _ := document[key].(string); got != want {
			t.Fatalf("converted %s = %q, want %q", key, got, want)
		}
	}
	if got := int(document["priority"].(float64)); got != 1 {
		t.Fatalf("converted priority = %d, want 1", got)
	}
	if got, _ := document["disabled"].(bool); got {
		t.Fatalf("converted disabled = true, want false")
	}
	if _, exists := document["session_window_utilization"]; exists {
		t.Fatalf("converted Claude auth must not persist Sub2API usage snapshots")
	}
	auth, okAuth := manager.GetByID("claude-claude@example.com.json")
	if !okAuth || auth.Provider != "claude" {
		t.Fatalf("converted runtime auth = %#v, want claude", auth)
	}
	if got, _ := auth.Metadata["refresh_token"].(string); got != "anthropic-refresh" {
		t.Fatalf("converted runtime refresh token = %q, want synthetic refresh token", got)
	}
}

func TestUploadAuthFile_Sub2APIArchiveEntriesAreConverted(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	files := map[string]string{
		"sub2api.json": `{"accounts":[{"name":"archive@example.com","platform":"openai","type":"oauth","credentials":{"access_token":"archive-access"}}]}`,
		"native.json":  `{"type":"codex","email":"native@example.com","access_token":"native-access","refresh_token":""}`,
	}
	cases := []struct {
		name        string
		filename    string
		contentType string
		build       func(*testing.T, map[string]string) []byte
	}{
		{name: "zip", filename: "bundle.zip", contentType: "application/zip", build: buildAuthZipArchive},
		{name: "tar", filename: "bundle.tar", contentType: "application/x-tar", build: buildAuthTarArchive},
		{name: "tar gzip", filename: "bundle.tar.gz", contentType: "application/gzip", build: buildAuthTarGzArchive},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			authDir := t.TempDir()
			manager := coreauth.NewManager(nil, nil, nil)
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
			archive := tc.build(t, files)

			rec := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rec)
			req := httptest.NewRequest(
				http.MethodPost,
				"/v0/management/auth-files?name="+url.QueryEscape(tc.filename),
				bytes.NewReader(archive),
			)
			req.Header.Set("Content-Type", tc.contentType)
			ctx.Request = req
			h.UploadAuthFile(ctx)

			if rec.Code != http.StatusOK {
				t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			if _, errStat := os.Stat(filepath.Join(authDir, "codex-archive@example.com.json")); errStat != nil {
				t.Fatalf("converted archive auth file missing: %v", errStat)
			}
			if _, errStat := os.Stat(filepath.Join(authDir, "native.json")); errStat != nil {
				t.Fatalf("native archive auth file missing: %v", errStat)
			}
			if _, errStat := os.Stat(filepath.Join(authDir, "sub2api.json")); !os.IsNotExist(errStat) {
				t.Fatalf("Sub2API archive container should not be persisted, stat err = %v", errStat)
			}
			if got := len(manager.List()); got != 2 {
				t.Fatalf("registered auth count = %d, want 2", got)
			}
		})
	}
}

func TestUploadAuthFile_Sub2APIGeneratedNameDoesNotOverwriteExistingAuth(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	for _, endpoint := range []string{"/v0/management/auth-files", "/v8/management/credentials"} {
		t.Run(endpoint, func(t *testing.T) {
			authDir := t.TempDir()
			existingPath := filepath.Join(authDir, "codex-conflict@example.com.json")
			existing := []byte(`{"type":"codex","email":"conflict@example.com","access_token":"old-access","refresh_token":""}`)
			if errWrite := os.WriteFile(existingPath, existing, 0o600); errWrite != nil {
				t.Fatalf("write existing auth file: %v", errWrite)
			}
			manager := coreauth.NewManager(nil, nil, nil)
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
			payload := []byte(`{"accounts":[{"name":"conflict@example.com","platform":"openai","type":"oauth","credentials":{"access_token":"new-access"}}]}`)

			rec := performUploadRequest(t, newV8AndV0AuthRouter(h), endpoint, false,
				map[string]string{"conflict-import.json": string(payload)}, []string{"conflict-import.json"})
			if rec.Code != http.StatusOK {
				t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
			}
			gotExisting, errReadExisting := os.ReadFile(existingPath)
			if errReadExisting != nil {
				t.Fatalf("read existing auth file: %v", errReadExisting)
			}
			if string(gotExisting) != string(existing) {
				t.Fatalf("existing auth file was overwritten")
			}
			newPath := filepath.Join(authDir, "codex-conflict@example.com-2.json")
			newData, errReadNew := os.ReadFile(newPath)
			if errReadNew != nil {
				t.Fatalf("read suffixed converted auth file: %v", errReadNew)
			}
			var document map[string]any
			if errDecode := json.Unmarshal(newData, &document); errDecode != nil {
				t.Fatalf("decode suffixed converted auth file: %v", errDecode)
			}
			if got, _ := document["access_token"].(string); got != "new-access" {
				t.Fatalf("converted access_token = %q, want new-access", got)
			}
		})
	}
}

func TestUploadAuthFile_Sub2APIDuplicateCredentialIsReported(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	payload := []byte(`{"accounts":[
		{"name":"first@example.com","platform":"openai","type":"oauth","credentials":{"access_token":"same-access","chatgpt_account_id":"same-account"}},
		{"name":"second@example.com","platform":"openai","type":"oauth","credentials":{"access_token":"same-access","chatgpt_account_id":"same-account"}}
	]}`)

	rec := uploadRawAuthPayload(t, h, "duplicates.json", payload)
	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusMultiStatus, rec.Body.String())
	}
	var response struct {
		Uploaded int `json:"uploaded"`
		Failed   []struct {
			Error string `json:"error"`
		} `json:"failed"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if response.Uploaded != 1 || len(response.Failed) != 1 || !strings.Contains(response.Failed[0].Error, "duplicate credential record") {
		t.Fatalf("response = %#v, want one upload and one duplicate failure", response)
	}
	if got := len(manager.List()); got != 1 {
		t.Fatalf("registered auth count = %d, want 1", got)
	}
}

func TestUploadAuthFile_Sub2APIDataMultipartReportsUnsupportedAccount(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	payload := `{
		"type":"sub2api-data",
		"version":1,
		"accounts":[
			{"name":"valid@example.com","platform":"openai","type":"oauth","credentials":{"access_token":"access","refresh_token":"refresh","chatgpt_account_id":"account"}},
			{"name":"unsupported@example.com","platform":"grok","type":"oauth","credentials":{"access_token":"access","refresh_token":"refresh"}}
		]
	}`

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, errCreate := writer.CreateFormFile("file", "sub2api-export.json")
	if errCreate != nil {
		t.Fatalf("create multipart file: %v", errCreate)
	}
	if _, errWrite := part.Write([]byte(payload)); errWrite != nil {
		t.Fatalf("write multipart file: %v", errWrite)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusMultiStatus, rec.Body.String())
	}
	var response struct {
		Uploaded int `json:"uploaded"`
		Failed   []struct {
			Name  string `json:"name"`
			Error string `json:"error"`
		} `json:"failed"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if response.Uploaded != 1 || len(response.Failed) != 1 {
		t.Fatalf("response = %#v, want one upload and one failure", response)
	}
	if !strings.Contains(response.Failed[0].Error, "unsupported account platform/type") {
		t.Fatalf("failure error = %q, want unsupported platform/type", response.Failed[0].Error)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "codex-valid@example.com.json")); errStat != nil {
		t.Fatalf("expected valid converted auth file: %v", errStat)
	}
	if got := len(manager.List()); got != 1 {
		t.Fatalf("registered auth count = %d, want 1", got)
	}
}

func TestUploadAuthFile_Sub2APIDataRejectsUnsupportedVersion(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodPost,
		"/v0/management/auth-files?name="+url.QueryEscape("sub2api-export.json"),
		bytes.NewBufferString(`{"type":"sub2api-data","version":2,"accounts":[]}`),
	)
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unsupported sub2api-data version") {
		t.Fatalf("response body = %s, want unsupported version error", rec.Body.String())
	}
	entries, errRead := os.ReadDir(authDir)
	if errRead != nil {
		t.Fatalf("read auth dir: %v", errRead)
	}
	if len(entries) != 0 {
		t.Fatalf("auth dir entries = %d, want 0", len(entries))
	}
}

func TestUploadAuthFile_Sub2APIDataRejectsInvalidExportTimestamp(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	payload := []byte(`{"exported_at":"not-a-timestamp","accounts":[{"name":"invalid-time@example.com","platform":"openai","type":"oauth","credentials":{"access_token":"access"}}]}`)

	rec := uploadRawAuthPayload(t, h, "invalid-time.json", payload)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "invalid sub2api exported_at") {
		t.Fatalf("response body = %s, want invalid exported_at error", rec.Body.String())
	}
	entries, errRead := os.ReadDir(authDir)
	if errRead != nil {
		t.Fatalf("read auth dir: %v", errRead)
	}
	if len(entries) != 0 {
		t.Fatalf("auth dir entries = %d, want 0", len(entries))
	}
}

func TestDeleteAuthFile_BatchQuery(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	files := []string{"alpha.json", "beta.json"}
	for _, name := range files {
		if err := os.WriteFile(filepath.Join(authDir, name), []byte(`{"type":"codex"}`), 0o600); err != nil {
			t.Fatalf("failed to write auth file %s: %v", name, err)
		}
	}

	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodDelete,
		"/v0/management/auth-files?name="+url.QueryEscape(files[0])+"&name="+url.QueryEscape(files[1]),
		nil,
	)
	ctx.Request = req

	h.DeleteAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected delete status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if got, ok := payload["deleted"].(float64); !ok || int(got) != len(files) {
		t.Fatalf("expected deleted=%d, got %#v", len(files), payload["deleted"])
	}

	for _, name := range files {
		if _, err := os.Stat(filepath.Join(authDir, name)); !os.IsNotExist(err) {
			t.Fatalf("expected auth file %s to be removed, stat err: %v", name, err)
		}
	}
}

func TestUploadAuthFile_BodyLimitExceededReturns413(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	gin.SetMode(gin.TestMode)

	zipBytes := buildAuthZipArchive(t, map[string]string{
		"xai-alpha@example.com.json": `{"type":"xai","email":"alpha@example.com","access_token":"` + strings.Repeat("a", 4096) + `"}`,
	})

	var multipartBody bytes.Buffer
	writer := multipart.NewWriter(&multipartBody)
	part, errCreate := writer.CreateFormFile("file", "auths.zip")
	if errCreate != nil {
		t.Fatalf("create multipart file: %v", errCreate)
	}
	if _, errWrite := part.Write(zipBytes); errWrite != nil {
		t.Fatalf("write multipart zip: %v", errWrite)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}

	tests := []struct {
		name        string
		target      string
		contentType string
		body        []byte
	}{
		{name: "multipart", target: "/v0/management/auth-files", contentType: writer.FormDataContentType(), body: multipartBody.Bytes()},
		{name: "raw archive", target: "/v0/management/auth-files?name=auths.zip", contentType: "application/zip", body: zipBytes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authDir := t.TempDir()
			manager := coreauth.NewManager(nil, nil, nil)
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

			rec := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rec)
			req := httptest.NewRequest(http.MethodPost, tt.target, bytes.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			req.Body = http.MaxBytesReader(rec, req.Body, int64(len(tt.body)/2))
			ctx.Request = req

			h.UploadAuthFile(ctx)

			if rec.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("upload status = %d, want %d; body = %s", rec.Code, http.StatusRequestEntityTooLarge, rec.Body.String())
			}
			if got := len(manager.List()); got != 0 {
				t.Fatalf("registered auth count = %d, want 0", got)
			}
		})
	}
}
