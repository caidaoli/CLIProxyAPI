package management

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestGetAuthFileLoadStatus(t *testing.T) {
	t.Run("idle without provider", func(t *testing.T) {
		h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, nil)
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/load-status", nil)
		h.GetAuthFileLoadStatus(ctx)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
		}
		var got watcher.AuthLoadStatus
		if errDecode := json.Unmarshal(recorder.Body.Bytes(), &got); errDecode != nil {
			t.Fatalf("decode response: %v", errDecode)
		}
		if got.State != watcher.AuthLoadStateIdle {
			t.Fatalf("state = %q, want %q", got.State, watcher.AuthLoadStateIdle)
		}
	})

	completed := time.Date(2026, 7, 14, 3, 9, 1, 0, time.UTC)
	tests := []watcher.AuthLoadStatus{
		{State: watcher.AuthLoadStateLoading, FilesDiscovered: 3, FilesProcessed: 1, AuthsLoaded: 1},
		{State: watcher.AuthLoadStateReady, FilesDiscovered: 3, FilesProcessed: 3, AuthsLoaded: 3, ScanComplete: true, CompletedAt: &completed},
		{State: watcher.AuthLoadStateDegraded, FilesDiscovered: 3, FilesProcessed: 3, AuthsLoaded: 2, FilesFailed: 1, ScanComplete: true, CompletedAt: &completed},
	}
	for _, want := range tests {
		t.Run(string(want.State), func(t *testing.T) {
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, nil)
			h.SetAuthLoadStatusProvider(func() watcher.AuthLoadStatus { return want })
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/load-status", nil)
			h.GetAuthFileLoadStatus(ctx)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
			}
			var got watcher.AuthLoadStatus
			if errDecode := json.Unmarshal(recorder.Body.Bytes(), &got); errDecode != nil {
				t.Fatalf("decode response: %v", errDecode)
			}
			if got.State != want.State || got.FilesProcessed != want.FilesProcessed || got.FilesFailed != want.FilesFailed || got.ScanComplete != want.ScanComplete {
				t.Fatalf("response = %+v, want %+v", got, want)
			}
		})
	}
}

func TestParseAuthFileListQuery(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		paginated bool
		want      authFileListQuery
		wantErr   bool
	}{
		{
			name: "without pagination parameters keeps full list mode",
			path: "/v0/management/auth-files",
			want: authFileListQuery{
				Page: 1, PageSize: 12, Sort: authFileSortDefault,
			},
		},
		{
			name:      "page selects paginated mode with default page size",
			path:      "/v0/management/auth-files?page=2&type=%20CoDeX%20&search=*DEX-A*&sort=priority&problem_only=true&disabled_only=false&enabled_only=true",
			paginated: true,
			want: authFileListQuery{
				Page: 2, PageSize: 12, Type: "codex", Search: "*DEX-A*", Sort: authFileSortPriority,
				ProblemOnly: true, EnabledOnly: true,
			},
		},
		{
			name:      "page size above forty selects paginated mode with default page",
			path:      "/v0/management/auth-files?page_size=500&sort=az",
			paginated: true,
			want:      authFileListQuery{Page: 1, PageSize: 500, Sort: authFileSortAZ},
		},
		{name: "rejects page below one", path: "/v0/management/auth-files?page=0", wantErr: true},
		{name: "rejects page size below lower bound", path: "/v0/management/auth-files?page_size=2", wantErr: true},
		{name: "rejects unknown sort", path: "/v0/management/auth-files?page=1&sort=recent", wantErr: true},
		{name: "rejects malformed boolean", path: "/v0/management/auth-files?page=1&problem_only=maybe", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ginCtx, _ := gin.CreateTestContext(recorder)
			ginCtx.Request = httptest.NewRequest(http.MethodGet, tt.path, nil)

			got, paginated, errQuery := parseAuthFileListQuery(ginCtx)
			if tt.wantErr {
				if errQuery == nil {
					t.Fatal("expected query error")
				}
				return
			}
			if errQuery != nil {
				t.Fatalf("parseAuthFileListQuery() error = %v", errQuery)
			}
			if paginated != tt.paginated {
				t.Fatalf("paginated = %v, want %v", paginated, tt.paginated)
			}
			if got != tt.want {
				t.Fatalf("query = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestBuildAuthFileListPage(t *testing.T) {
	auths := []*coreauth.Auth{
		{ID: "codex-b.json", FileName: "codex-b.json", Provider: "codex", Attributes: map[string]string{"path": "/auth/codex-b.json", "priority": "1"}},
		{ID: "codex-a.json", FileName: "codex-a.json", Provider: "codex", Disabled: true, StatusMessage: "expired", Attributes: map[string]string{"path": "/auth/codex-a.json", "priority": "20"}},
		{ID: "claude-a.json", FileName: "claude-a.json", Provider: "claude", Attributes: map[string]string{"path": "/auth/claude-a.json", "priority": "5"}},
	}

	t.Run("sorts priorities and reports counts", func(t *testing.T) {
		query := authFileListQuery{Page: 1, PageSize: 2, Sort: authFileSortPriority}
		page := buildAuthFileListPage(auths, query)

		assertAuthNames(t, page.Auths, "codex-a.json", "claude-a.json")
		if page.Total != 3 || page.TypeCounts["all"] != 3 || page.EnabledTypeCounts["codex"] != 1 {
			t.Fatalf("unexpected page metadata: %#v", page)
		}
		if page.EnabledTypeCounts["all"] != 2 || page.EnabledTypeCounts["claude"] != 1 {
			t.Fatalf("unexpected enabled counts: %#v", page.EnabledTypeCounts)
		}
		if wantTypes := []string{"all", "claude", "codex"}; !reflect.DeepEqual(page.Types, wantTypes) {
			t.Fatalf("types = %#v, want %#v", page.Types, wantTypes)
		}
	})

	t.Run("default sort follows descending priority", func(t *testing.T) {
		page := buildAuthFileListPage(auths, authFileListQuery{Page: 1, PageSize: 2, Sort: authFileSortDefault})
		assertAuthNames(t, page.Auths, "codex-a.json", "claude-a.json")
	})

	t.Run("applies status filters before type counts", func(t *testing.T) {
		page := buildAuthFileListPage(auths, authFileListQuery{Page: 1, PageSize: 12, Sort: authFileSortDefault, ProblemOnly: true})
		assertAuthNames(t, page.Auths, "codex-a.json")
		if page.Total != 1 || page.TypeCounts["all"] != 1 || page.TypeCounts["codex"] != 1 {
			t.Fatalf("unexpected problem page: %#v", page)
		}
	})

	t.Run("matches case insensitive wildcard search without regular expressions", func(t *testing.T) {
		page := buildAuthFileListPage(auths, authFileListQuery{Page: 1, PageSize: 12, Type: "codex", Search: "*DEX-B*", Sort: authFileSortAZ})
		assertAuthNames(t, page.Auths, "codex-b.json")
		if page.Total != 1 || page.TypeCounts["all"] != 3 {
			t.Fatalf("unexpected type/search page: %#v", page)
		}

		literal := buildAuthFileListPage(auths, authFileListQuery{Page: 1, PageSize: 12, Search: "codex-[a].json", Sort: authFileSortAZ})
		if literal.Total != 0 {
			t.Fatalf("raw regular expression search must not match: %#v", literal)
		}
	})

	t.Run("searches name type and provider", func(t *testing.T) {
		providerOnly := &coreauth.Auth{
			ID:       "account-a.json",
			FileName: "account-a.json",
			Provider: "codex",
			Attributes: map[string]string{
				"path": "/auth/account-a.json",
			},
		}
		page := buildAuthFileListPage([]*coreauth.Auth{providerOnly}, authFileListQuery{Page: 1, PageSize: 12, Search: "CODEX", Sort: authFileSortAZ})
		assertAuthNames(t, page.Auths, "account-a.json")
	})

	t.Run("excludes auth records that list entries omit", func(t *testing.T) {
		visible := &coreauth.Auth{ID: "visible.json", FileName: "visible.json", Provider: "codex", Attributes: map[string]string{"path": "/auth/visible.json"}}
		hiddenNoPath := &coreauth.Auth{ID: "hidden.json", FileName: "hidden.json", Provider: "codex"}
		hiddenDisabledRuntime := &coreauth.Auth{ID: "disabled-runtime.json", FileName: "disabled-runtime.json", Provider: "codex", Disabled: true, Attributes: map[string]string{"runtime_only": "true"}}
		page := buildAuthFileListPage([]*coreauth.Auth{visible, hiddenNoPath, hiddenDisabledRuntime}, authFileListQuery{Page: 1, PageSize: 12, Sort: authFileSortDefault})
		assertAuthNames(t, page.Auths, "visible.json")
		if page.Total != 1 || page.TypeCounts["all"] != 1 || page.EnabledTypeCounts["all"] != 1 {
			t.Fatalf("invisible auths leaked into page metadata: %#v", page)
		}
	})

	t.Run("combining enabled and disabled filters matches nothing", func(t *testing.T) {
		page := buildAuthFileListPage(auths, authFileListQuery{Page: 1, PageSize: 12, Sort: authFileSortDefault, DisabledOnly: true, EnabledOnly: true})
		if page.Total != 0 || len(page.Auths) != 0 || page.TypeCounts["all"] != 0 {
			t.Fatalf("unexpected mutually exclusive filter result: %#v", page)
		}
	})
}

func TestBuildAuthFileListPage_MaxIntPageReturnsEmpty(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	auth := &coreauth.Auth{
		ID:       "visible.json",
		FileName: "visible.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": "/auth/visible.json",
		},
	}
	page := buildAuthFileListPage([]*coreauth.Auth{auth}, authFileListQuery{
		Page: maxInt, PageSize: 40, Sort: authFileSortDefault,
	})
	if page.Total != 1 || len(page.Auths) != 0 || page.Page != maxInt || page.PageSize != 40 {
		t.Fatalf("unexpected oversized page: %#v", page)
	}
}

func TestListAuthFiles_Paginated(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	authDir := t.TempDir()
	for _, auth := range authFileListTestAuths() {
		auth.Attributes["path"] = filepath.Join(authDir, auth.FileName)
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files?page=1&page_size=3", nil)
	h.ListAuthFiles(ginCtx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Files    []map[string]any `json:"files"`
		Total    int              `json:"total"`
		Page     int              `json:"page"`
		PageSize int              `json:"page_size"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatal(errDecode)
	}
	if len(response.Files) != 3 || response.Total != 4 || response.Page != 1 || response.PageSize != 3 {
		t.Fatalf("unexpected paginated response: %#v", response)
	}
	for _, file := range response.Files {
		if _, ok := file["success"]; !ok {
			t.Fatalf("expected full auth file entry, got %#v", file)
		}
	}
}

func TestListAuthFiles_ReportsActiveCooldowns(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	authDir := t.TempDir()
	now := time.Now()
	auth := &coreauth.Auth{
		ID:       "cooling.json",
		FileName: "cooling.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": filepath.Join(authDir, "cooling.json"),
		},
		ModelStates: map[string]*coreauth.ModelState{
			"gpt-active": {
				Status:         coreauth.StatusError,
				StatusMessage:  "quota exhausted",
				Unavailable:    true,
				NextRetryAfter: now.Add(time.Hour),
				Quota:          coreauth.QuotaState{Exceeded: true, Reason: "quota", NextRecoverAt: now.Add(time.Hour)},
			},
			"gpt-expired": {
				Status:         coreauth.StatusError,
				Unavailable:    true,
				NextRetryAfter: now.Add(-time.Hour),
			},
			"gpt-ready": {
				Status:      coreauth.StatusActive,
				Unavailable: false,
			},
		},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	readyAuth := &coreauth.Auth{
		ID:       "ready.json",
		FileName: "ready.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": filepath.Join(authDir, "ready.json"),
		},
	}
	if _, errRegister := manager.Register(context.Background(), readyAuth); errRegister != nil {
		t.Fatal(errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files?page=1&page_size=12", nil)
	h.ListAuthFiles(ginCtx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Files []struct {
			Name      string `json:"name"`
			Cooldowns []struct {
				Model          string    `json:"model"`
				Status         string    `json:"status"`
				Reason         string    `json:"reason"`
				NextRetryAfter time.Time `json:"next_retry_after"`
			} `json:"cooldowns"`
		} `json:"files"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatal(errDecode)
	}
	if len(response.Files) != 2 {
		t.Fatalf("unexpected cooldown response: %s", recorder.Body.String())
	}
	filesByName := make(map[string]int, len(response.Files))
	for i := range response.Files {
		filesByName[response.Files[i].Name] = i
	}
	coolingIndex, okCooling := filesByName["cooling.json"]
	readyIndex, okReady := filesByName["ready.json"]
	if !okCooling || !okReady {
		t.Fatalf("missing auth file entry: %s", recorder.Body.String())
	}
	cooling := response.Files[coolingIndex]
	ready := response.Files[readyIndex]
	if len(cooling.Cooldowns) != 1 || ready.Cooldowns == nil || len(ready.Cooldowns) != 0 {
		t.Fatalf("unexpected cooldown response: %s", recorder.Body.String())
	}
	cooldown := cooling.Cooldowns[0]
	if cooldown.Model != "gpt-active" || cooldown.Status != "cooling" || cooldown.Reason != "quota" {
		t.Fatalf("cooldown = %#v", cooldown)
	}
	if !cooldown.NextRetryAfter.After(now) {
		t.Fatalf("next retry = %v, want after %v", cooldown.NextRetryAfter, now)
	}
}

func TestListAuthFiles_PaginatedMaxIntPage(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "visible.json",
		FileName: "visible.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": filepath.Join(t.TempDir(), "visible.json"),
		},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/v0/management/auth-files?page=%d&page_size=40", maxInt), nil)
	h.ListAuthFiles(ginCtx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Files    []map[string]any `json:"files"`
		Total    int              `json:"total"`
		Page     int              `json:"page"`
		PageSize int              `json:"page_size"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatal(errDecode)
	}
	if response.Total != 1 || len(response.Files) != 0 || response.Page != maxInt || response.PageSize != 40 {
		t.Fatalf("unexpected oversized page response: %#v", response)
	}
}

func TestListAuthFiles_PaginatedSearchesProvider(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "account-a.json",
		FileName: "account-a.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": filepath.Join(t.TempDir(), "account-a.json"),
		},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files?page=1&page_size=3&search=codex", nil)
	h.ListAuthFiles(ginCtx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Files []map[string]any `json:"files"`
		Total int              `json:"total"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatal(errDecode)
	}
	if response.Total != 1 || len(response.Files) != 1 || response.Files[0]["name"] != "account-a.json" {
		t.Fatalf("provider search response = %#v", response)
	}
}

func TestListAuthFiles_PaginatedFromDisk(t *testing.T) {
	authDir := t.TempDir()
	for _, auth := range authFileListTestAuths() {
		contents := fmt.Sprintf(`{"type":%q,"priority":%q}`, auth.Provider, auth.Attributes["priority"])
		if errWrite := os.WriteFile(filepath.Join(authDir, auth.FileName), []byte(contents), 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files?page=1&page_size=3", nil)
	h.ListAuthFiles(ginCtx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Files    []map[string]any `json:"files"`
		Total    int              `json:"total"`
		Page     int              `json:"page"`
		PageSize int              `json:"page_size"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatal(errDecode)
	}
	if len(response.Files) != 3 || response.Total != 4 || response.Page != 1 || response.PageSize != 3 {
		t.Fatalf("unexpected disk-backed paginated response: %#v", response)
	}
}

func BenchmarkListAuthFilesPaginated1037(b *testing.B) {
	h := benchmarkAuthFileListHandler(b)
	req := httptest.NewRequest(http.MethodGet, "/v0/management/auth-files?page=1&page_size=12", nil)

	b.ReportAllocs()
	b.ResetTimer()
	responseBytes := 0
	for i := 0; i < b.N; i++ {
		recorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(recorder)
		ginCtx.Request = req.Clone(context.Background())
		h.ListAuthFiles(ginCtx)
		responseBytes = recorder.Body.Len()
	}
	b.ReportMetric(float64(responseBytes), "response_B")
}

func authFileListTestAuths() []*coreauth.Auth {
	return []*coreauth.Auth{
		{ID: "codex-b.json", FileName: "codex-b.json", Provider: "codex", Attributes: map[string]string{"priority": "1"}},
		{ID: "codex-a.json", FileName: "codex-a.json", Provider: "codex", Disabled: true, StatusMessage: "expired", Attributes: map[string]string{"priority": "20"}},
		{ID: "claude-a.json", FileName: "claude-a.json", Provider: "claude", Attributes: map[string]string{"priority": "5"}},
		{ID: "gemini-a.json", FileName: "gemini-a.json", Provider: "gemini", Attributes: map[string]string{"priority": "3"}},
	}
}

func assertAuthNames(t *testing.T, auths []*coreauth.Auth, want ...string) {
	t.Helper()
	got := make([]string, 0, len(auths))
	for _, auth := range auths {
		got = append(got, auth.FileName)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("auth names = %#v, want %#v", got, want)
	}
}

func benchmarkAuthFileListHandler(b *testing.B) *Handler {
	b.Helper()
	manager := coreauth.NewManager(nil, nil, nil)
	for i := 0; i < 1037; i++ {
		auth := &coreauth.Auth{
			ID:       fmt.Sprintf("codex-%04d.json", i),
			FileName: fmt.Sprintf("codex-%04d.json", i),
			Provider: "codex",
			Attributes: map[string]string{
				"runtime_only": "true",
				"priority":     fmt.Sprintf("%d", i%10),
			},
		}
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			b.Fatal(errRegister)
		}
	}
	return NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: b.TempDir()}, manager)
}
