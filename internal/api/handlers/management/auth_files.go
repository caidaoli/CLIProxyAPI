package management

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/antigravity"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/credentialweight"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

var lastRefreshKeys = []string{"last_refresh", "lastRefresh", "last_refreshed_at", "lastRefreshedAt"}

type authFileCooldown struct {
	Model          string    `json:"model,omitempty"`
	Status         string    `json:"status"`
	Reason         string    `json:"reason,omitempty"`
	NextRetryAfter time.Time `json:"next_retry_after"`
}

var (
	callbackForwardersMu       sync.Mutex
	callbackForwarders         = make(map[int]*callbackForwarder)
	authFileEntryMu            sync.Mutex
	errAuthFileMustBeJSON      = errors.New("auth file must be .json")
	errInvalidAuthFile         = errors.New("invalid auth file")
	errAuthFileNotFound        = errors.New("auth file not found")
	errPluginVirtualAuth       = errors.New("plugin virtual auth cannot be modified directly; edit or delete the source auth file")
	newCodexOAuthService       = func(cfg *config.Config) codexOAuthService { return codex.NewCodexAuth(cfg) }
	newAntigravityOAuthService = func(cfg *config.Config) antigravityOAuthService { return antigravity.NewAntigravityAuth(cfg, nil) }
)

var (
	authFileEmailNamePrefixes = []string{
		"antigravity-",
		"gemini-cli-",
		"claude-",
		"codex-",
		"gemini-",
		"iflow-",
		"kimi-",
		"qwen-",
		"vertex-",
	}
	authFileEmailNamePattern = regexp.MustCompile(`(?i)(?:^|[-_])([a-z0-9._%+\-]+@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)*\.[a-z]{2,})(?:[-_+]|$)`)
)

func extractLastRefreshTimestamp(meta map[string]any) (time.Time, bool) {
	if len(meta) == 0 {
		return time.Time{}, false
	}
	for _, key := range lastRefreshKeys {
		if val, ok := meta[key]; ok {
			if ts, ok1 := parseLastRefreshValue(val); ok1 {
				return ts, true
			}
		}
	}
	return time.Time{}, false
}

func parseLastRefreshValue(v any) (time.Time, bool) {
	switch val := v.(type) {
	case string:
		s := strings.TrimSpace(val)
		if s == "" {
			return time.Time{}, false
		}
		layouts := []string{time.RFC3339, time.RFC3339Nano, "2006-01-02 15:04:05", "2006-01-02T15:04:05Z07:00"}
		for _, layout := range layouts {
			if ts, err := time.Parse(layout, s); err == nil {
				return ts.UTC(), true
			}
		}
		if unix, err := strconv.ParseInt(s, 10, 64); err == nil && unix > 0 {
			return time.Unix(unix, 0).UTC(), true
		}
	case float64:
		if val <= 0 {
			return time.Time{}, false
		}
		return time.Unix(int64(val), 0).UTC(), true
	case int64:
		if val <= 0 {
			return time.Time{}, false
		}
		return time.Unix(val, 0).UTC(), true
	case int:
		if val <= 0 {
			return time.Time{}, false
		}
		return time.Unix(int64(val), 0).UTC(), true
	case json.Number:
		if i, err := val.Int64(); err == nil && i > 0 {
			return time.Unix(i, 0).UTC(), true
		}
	}
	return time.Time{}, false
}

func (h *Handler) ListAuthFiles(c *gin.Context) {
	if h == nil {
		c.JSON(500, gin.H{"error": "handler not initialized"})
		return
	}
	query, paginated, errQuery := parseAuthFileListQuery(c)
	if errQuery != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errQuery.Error()})
		return
	}
	if h.authManager == nil {
		h.listAuthFilesFromDisk(c, query, paginated)
		return
	}
	nameFilter := strings.TrimSpace(c.Query("name"))
	authIndexFilter := strings.TrimSpace(c.Query("auth_index"))
	h.mu.Lock()
	host := h.pluginHost
	h.mu.Unlock()
	quota := authFileQuotaContext{v8: c.GetBool(ConfigV8ContextKey)}
	if host != nil {
		quota.pluginIDs = host.QuotaProviderPluginIDs(c.Request.Context())
	}
	auths := h.authManager.List()
	observedAt := time.Now().UTC()
	cooldownsKnown := !h.authManager.HomeEnabled()
	if nameFilter != "" || authIndexFilter != "" {
		filtered := make([]*coreauth.Auth, 0, len(auths))
		for _, auth := range auths {
			if matchesAuthFileLookup(auth, nameFilter, authIndexFilter) {
				filtered = append(filtered, auth)
			}
		}
		auths = filtered
	}
	if !paginated {
		h.writeFullAuthFileList(c, auths, quota, observedAt, cooldownsKnown)
		return
	}
	page := buildAuthFileListPage(auths, query)
	files := make([]gin.H, 0, len(page.Auths))
	for _, auth := range page.Auths {
		if entry := h.buildAuthFileEntry(auth, quota); entry != nil {
			entry["cooldowns"] = nil
			if cooldownsKnown {
				entry["cooldowns"] = coreauth.CooldownSnapshotForAuth(auth, observedAt)
			}
			files = append(files, entry)
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"observed_at":         observedAt,
		"files":               files,
		"total":               page.Total,
		"page":                page.Page,
		"page_size":           page.PageSize,
		"types":               page.Types,
		"type_counts":         page.TypeCounts,
		"enabled_type_counts": page.EnabledTypeCounts,
	})
}

func lockedAuthIndex(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	authFileEntryMu.Lock()
	defer authFileEntryMu.Unlock()
	return strings.TrimSpace(auth.EnsureIndex())
}

func matchesAuthFileLookup(auth *coreauth.Auth, name string, authIndex string) bool {
	if auth == nil {
		return false
	}
	if name != "" && strings.TrimSpace(auth.ID) != name && strings.TrimSpace(auth.FileName) != name {
		return false
	}
	if authIndex != "" && lockedAuthIndex(auth) != authIndex {
		return false
	}
	return true
}

func (h *Handler) lookupAuthFile(name string, authIndex string) (*coreauth.Auth, bool) {
	name = strings.TrimSpace(name)
	authIndex = strings.TrimSpace(authIndex)
	if h == nil || h.authManager == nil || name == "" {
		return nil, false
	}
	if authIndex == "" {
		if auth, ok := h.authManager.GetByID(name); ok {
			return auth, true
		}
		auths := h.authManager.List()
		for _, auth := range auths {
			if auth != nil && strings.TrimSpace(auth.FileName) == name {
				return auth, true
			}
		}
		return nil, false
	}
	auths := h.authManager.List()
	for _, auth := range auths {
		if matchesAuthFileLookup(auth, name, authIndex) {
			return auth, true
		}
	}
	return nil, false
}

// GetAuthFileModels returns the models supported by a specific auth file
func (h *Handler) GetAuthFileModels(c *gin.Context) {
	name := c.Query("name")
	if name == "" {
		c.JSON(400, gin.H{"error": "name is required"})
		return
	}

	// Try to find auth ID via authManager
	var authID string
	if h.authManager != nil {
		auths := h.authManager.List()
		for _, auth := range auths {
			if auth.FileName == name || auth.ID == name {
				authID = auth.ID
				break
			}
		}
	}

	if authID == "" {
		authID = name // fallback to filename as ID
	}

	// Get models from registry
	reg := registry.GetGlobalRegistry()
	models := reg.GetModelsForClient(authID)

	result := make([]gin.H, 0, len(models))
	for _, m := range models {
		entry := gin.H{
			"id": m.ID,
		}
		if m.DisplayName != "" {
			entry["display_name"] = m.DisplayName
		}
		if m.Type != "" {
			entry["type"] = m.Type
		}
		if m.OwnedBy != "" {
			entry["owned_by"] = m.OwnedBy
		}
		result = append(result, entry)
	}

	c.JSON(200, gin.H{"models": result})
}

// GetAuthFileLoadStatus returns the current progressive auth-file load status.
func (h *Handler) GetAuthFileLoadStatus(c *gin.Context) {
	h.mu.Lock()
	provider := h.authLoadStatusProvider
	h.mu.Unlock()
	if provider == nil {
		c.JSON(http.StatusOK, watcher.AuthLoadStatus{State: watcher.AuthLoadStateIdle})
		return
	}
	c.JSON(http.StatusOK, provider())
}

// List auth files from disk when the auth manager is unavailable.
func (h *Handler) listAuthFilesFromDisk(c *gin.Context, query authFileListQuery, paginated bool) {
	observedAt := time.Now().UTC()
	nameFilter := strings.TrimSpace(c.Query("name"))
	authIndexFilter := strings.TrimSpace(c.Query("auth_index"))
	entries, err := os.ReadDir(h.cfg.AuthDir)
	if err != nil {
		c.JSON(500, gin.H{"error": fmt.Sprintf("failed to read auth dir: %v", err)})
		return
	}
	files := make([]gin.H, 0)
	diskAuths := make([]*coreauth.Auth, 0)
	diskEntries := make(map[*coreauth.Auth]gin.H)
	// Disk fallback has no stable auth_index identity; reject that filter.
	if authIndexFilter != "" {
		if !paginated {
			c.JSON(200, gin.H{"observed_at": observedAt, "files": files})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"observed_at":         observedAt,
			"files":               files,
			"total":               0,
			"page":                query.Page,
			"page_size":           query.PageSize,
			"types":               []string{},
			"type_counts":         map[string]int{"all": 0},
			"enabled_type_counts": map[string]int{"all": 0},
		})
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if nameFilter != "" && name != nameFilter {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		if info, errInfo := e.Info(); errInfo == nil {
			fileData := gin.H{"name": name, "size": info.Size(), "modtime": info.ModTime(), "cooldowns": nil}

			// Read file to get type field
			full := filepath.Join(h.cfg.AuthDir, name)
			diskAuth := &coreauth.Auth{
				ID:       name,
				FileName: name,
				Attributes: map[string]string{
					"path": full,
				},
			}
			if data, errRead := os.ReadFile(full); errRead == nil {
				var fieldBuf [10]ojson.Result
				fields := ojson.GetManyBytesNoCopy(fieldBuf[:0], data, "type", "email", "project_id", "priority", coreauth.AttributeWeight, "note", "websockets", "disabled", "status_message", "status")
				typeValue := strings.Clone(fields[0].String())
				emailValue := strings.Clone(fields[1].String())
				fileData["type"] = typeValue
				fileData["email"] = emailValue
				diskAuth.Provider = typeValue
				diskAuth.Disabled = fields[7].Bool()
				diskAuth.StatusMessage = strings.Clone(strings.TrimSpace(fields[8].String()))
				if strings.EqualFold(strings.TrimSpace(fields[9].String()), string(coreauth.StatusDisabled)) {
					diskAuth.Status = coreauth.StatusDisabled
				}
				if projectID := strings.TrimSpace(fields[2].String()); projectID != "" {
					fileData["project_id"] = strings.Clone(projectID)
				}
				if pv := fields[3]; pv.Exists() {
					switch pv.Type {
					case ojson.Number:
						fileData["priority"] = int(pv.Int())
					case ojson.String:
						if parsed, errAtoi := strconv.Atoi(strings.TrimSpace(pv.String())); errAtoi == nil {
							fileData["priority"] = parsed
						}
					}
				}
				if priority, ok := fileData["priority"].(int); ok {
					diskAuth.Attributes["priority"] = strconv.Itoa(priority)
				}
				if wv := fields[4]; wv.Exists() {
					var rawWeight string
					switch wv.Type {
					case ojson.Number:
						rawWeight = wv.Raw
					case ojson.String:
						rawWeight = wv.String()
					}
					if rawWeight != "" {
						if weight, errWeight := credentialweight.ParseString(rawWeight); errWeight == nil {
							fileData[coreauth.AttributeWeight] = weight
						}
					}
				}
				if nv := fields[5]; nv.Exists() && nv.Type == ojson.String {
					if trimmed := strings.TrimSpace(nv.String()); trimmed != "" {
						fileData["note"] = strings.Clone(trimmed)
					}
				}
				if wv := fields[6]; wv.Exists() {
					switch wv.Type {
					case ojson.True:
						fileData["websockets"] = true
					case ojson.False:
						fileData["websockets"] = false
					case ojson.String:
						if parsed, errParse := strconv.ParseBool(strings.TrimSpace(wv.String())); errParse == nil {
							fileData["websockets"] = parsed
						}
					}
				}
				if requestRetry, okRetry := authFileRequestRetryFromJSON(data); okRetry {
					fileData["request_retry"] = requestRetry
				}
			}

			files = append(files, fileData)
			diskAuths = append(diskAuths, diskAuth)
			diskEntries[diskAuth] = fileData
		}
	}
	if !paginated {
		c.JSON(200, gin.H{"observed_at": observedAt, "files": files})
		return
	}
	page := buildAuthFileListPage(diskAuths, query)
	pageFiles := make([]gin.H, 0, len(page.Auths))
	for _, auth := range page.Auths {
		pageFiles = append(pageFiles, diskEntries[auth])
	}
	c.JSON(http.StatusOK, gin.H{
		"observed_at":         observedAt,
		"files":               pageFiles,
		"total":               page.Total,
		"page":                page.Page,
		"page_size":           page.PageSize,
		"types":               page.Types,
		"type_counts":         page.TypeCounts,
		"enabled_type_counts": page.EnabledTypeCounts,
	})
}

// authFileQuotaContext carries request-scoped quota capability data for auth file entries.
type authFileQuotaContext struct {
	// pluginIDs maps normalized provider keys to the quota plugin selected for them.
	// A nil map means no quota plugins are available.
	pluginIDs map[string]string
	// v8 hides declarative quota_probe support because v8 only serves plugin quota routes.
	v8 bool
}

func (h *Handler) buildAuthFileEntry(auth *coreauth.Auth, quota authFileQuotaContext) gin.H {
	authFileEntryMu.Lock()
	defer authFileEntryMu.Unlock()
	return h.buildAuthFileEntryLocked(auth, quota)
}

func isPersistentAuthFailure(auth *coreauth.Auth, now time.Time) bool {
	if auth == nil {
		return false
	}
	// Terminal unauthorized failure with no refresh scheduled.
	if coreauth.HasUnauthorizedAuthFailure(auth) {
		return true
	}
	// An OAuth credential whose access token is expired cannot be used to serve requests.
	if exp, ok := auth.AccessTokenExpirationTime(); ok && !exp.IsZero() && !exp.After(now) {
		return true
	}
	// An explicit token expiration status.
	if strings.EqualFold(strings.TrimSpace(auth.StatusMessage), "token expired") {
		return true
	}
	return false
}

func isModelStateBlocked(state *coreauth.ModelState, now time.Time) bool {
	if state == nil {
		return false
	}
	if state.Status == coreauth.StatusDisabled {
		return true
	}
	if !state.Unavailable && !state.Quota.Exceeded {
		return false
	}
	hasRecoveryTime := !state.NextRetryAfter.IsZero() || (!state.Quota.NextRecoverAt.IsZero() && state.Quota.Exceeded)
	if !state.NextRetryAfter.IsZero() && state.NextRetryAfter.After(now) {
		return true
	}
	if state.Quota.Exceeded && !state.Quota.NextRecoverAt.IsZero() && state.Quota.NextRecoverAt.After(now) {
		return true
	}
	if hasRecoveryTime {
		return false
	}
	return true
}

func reconcileAuthFileCooldownState(auth *coreauth.Auth, now time.Time) (unavailable bool, status coreauth.Status, statusMessage string, nextRetry time.Time) {
	if auth == nil {
		return false, coreauth.StatusActive, "", time.Time{}
	}
	unavailable = auth.Unavailable
	status = auth.Status
	statusMessage = auth.StatusMessage
	if !auth.NextRetryAfter.IsZero() {
		nextRetry = auth.NextRetryAfter
	}

	if auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return unavailable, coreauth.StatusDisabled, statusMessage, nextRetry
	}

	// Never reconcile an active authentication or token failure to active.
	if isPersistentAuthFailure(auth, now) {
		if !nextRetry.IsZero() && !nextRetry.After(now) {
			nextRetry = time.Time{}
		}
		if statusMessage == "" && auth.RequiresSignIn {
			statusMessage = "unauthorized (refresh token invalid)"
		}
		return true, coreauth.StatusError, statusMessage, nextRetry
	}

	// Check if there is an active credential-level cooldown.
	// Matching selector.availabilityBlock: if neither Unavailable nor Quota.Exceeded is true,
	// an inactive timestamp does not block the credential.
	hasActiveCredCooldown := false
	if auth.Unavailable || auth.Quota.Exceeded {
		if !auth.NextRetryAfter.IsZero() && auth.NextRetryAfter.After(now) {
			hasActiveCredCooldown = true
		}
		if auth.Quota.Exceeded && auth.Quota.Reason == "credential_quota" && auth.Quota.NextRecoverAt.After(now) {
			hasActiveCredCooldown = true
			if nextRetry.IsZero() || auth.Quota.NextRecoverAt.After(nextRetry) {
				nextRetry = auth.Quota.NextRecoverAt
			}
		}
	}

	// Check per-model states.
	hasSchedulableModels := false
	allSchedulableBlocked := true
	hasActiveModelCooldown := false
	hadAnyModelCooldown := false
	for _, state := range auth.ModelStates {
		if state == nil {
			continue
		}
		if state.Status == coreauth.StatusDisabled {
			continue
		}
		hasSchedulableModels = true
		if !state.NextRetryAfter.IsZero() || (state.Quota.Exceeded && !state.Quota.NextRecoverAt.IsZero()) {
			hadAnyModelCooldown = true
		}
		if (!state.NextRetryAfter.IsZero() && state.NextRetryAfter.After(now)) ||
			(state.Quota.Exceeded && !state.Quota.NextRecoverAt.IsZero() && state.Quota.NextRecoverAt.After(now)) {
			hasActiveModelCooldown = true
		}
		if !isModelStateBlocked(state, now) {
			allSchedulableBlocked = false
		}
	}

	hadCooldown := !auth.NextRetryAfter.IsZero() ||
		(auth.Quota.Exceeded && !auth.Quota.NextRecoverAt.IsZero()) ||
		hadAnyModelCooldown

	// If there is an active credential cooldown, keep unavailable/error.
	// If all recorded models are blocked and the credential itself was marked unavailable, keep unavailable/error.
	if hasActiveCredCooldown || (hasSchedulableModels && allSchedulableBlocked && auth.Unavailable) {
		if !nextRetry.IsZero() && !nextRetry.After(now) {
			nextRetry = time.Time{}
		}
		return true, coreauth.StatusError, statusMessage, nextRetry
	}

	// If the credential was not marked unavailable and has no active credential cooldown, keep unavailable=false.
	if !auth.Unavailable && !hasActiveCredCooldown {
		if status == coreauth.StatusError && hasSchedulableModels && !allSchedulableBlocked {
			status = coreauth.StatusActive
			statusMessage = ""
		}
		return false, status, statusMessage, time.Time{}
	}

	// If a cooldown was recorded but has expired (and no active model cooldown blocks all models):
	if hadCooldown && !hasActiveCredCooldown && !hasActiveModelCooldown {
		return false, coreauth.StatusActive, "", time.Time{}
	}

	// If partial models are still cooling, the credential as a whole remains available for other models.
	if hadCooldown && hasSchedulableModels && !allSchedulableBlocked {
		if status == coreauth.StatusError && !hasActiveCredCooldown {
			status = coreauth.StatusActive
			statusMessage = ""
		}
		return false, status, statusMessage, time.Time{}
	}

	// If nextRetry is in the past, do not expose a past retry deadline.
	if !nextRetry.IsZero() && !nextRetry.After(now) {
		nextRetry = time.Time{}
	}

	return unavailable, status, statusMessage, nextRetry
}

func (h *Handler) buildAuthFileEntryLocked(auth *coreauth.Auth, quota authFileQuotaContext) gin.H {
	if auth == nil {
		return nil
	}
	auth.EnsureIndex()
	if !authFileListVisible(auth) {
		return nil
	}
	runtimeOnly := isRuntimeOnlyAuth(auth)
	path := strings.TrimSpace(authAttribute(auth, "path"))
	name := strings.TrimSpace(auth.FileName)
	if name == "" {
		name = auth.ID
	}
	unavailable, status, statusMessage, nextRetryAfter := reconcileAuthFileCooldownState(auth, time.Now().UTC())
	entry := gin.H{
		"id":             auth.ID,
		"auth_index":     auth.Index,
		"name":           name,
		"type":           strings.TrimSpace(auth.Provider),
		"provider":       strings.TrimSpace(auth.Provider),
		"label":          auth.Label,
		"status":         status,
		"status_message": statusMessage,
		"disabled":       auth.Disabled,
		"unavailable":    unavailable,
		"runtime_only":   runtimeOnly,
		"source":         "memory",
	}
	entry["success"] = auth.Success
	entry["failed"] = auth.Failed
	entry["quota"] = quotaObservationPayloadForProvider(auth.Provider, auth.Quota)
	if modelQuotas := modelQuotaObservationPayload(auth.Provider, auth.ModelStates); len(modelQuotas) > 0 {
		entry["model_quotas"] = modelQuotas
	}
	if auth.LastRequestError != nil {
		entry["last_request_error"] = auth.LastRequestError
	}
	if pluginID := quota.pluginIDs[strings.ToLower(strings.TrimSpace(auth.Provider))]; pluginID != "" {
		entry["supports_quota"] = true
		entry["quota_provider"] = auth.Provider
		if quota.v8 {
			entry["quota_plugin_id"] = pluginID
		}
	}
	// Declarative quota_probe is only executed by the v0 quota/fetch route.
	if !quota.v8 && auth.Metadata != nil {
		if probe, okProbe := auth.Metadata["quota_probe"]; okProbe && probe != nil {
			entry["supports_quota"] = true
			entry["quota_probe"] = probe
		}
	}
	if email := authEmail(auth); email != "" {
		entry["email"] = email
	}
	if projectID := authProjectID(auth); projectID != "" {
		entry["project_id"] = projectID
	}
	if accountType, account := auth.AccountInfo(); accountType != "" || account != "" {
		if accountType != "" {
			entry["account_type"] = accountType
		}
		if account != "" {
			entry["account"] = account
		}
	}
	if !auth.CreatedAt.IsZero() {
		entry["created_at"] = auth.CreatedAt
	}
	if !auth.UpdatedAt.IsZero() {
		entry["modtime"] = auth.UpdatedAt
		entry["updated_at"] = auth.UpdatedAt
	}
	if !auth.LastRefreshedAt.IsZero() {
		entry["last_refresh"] = auth.LastRefreshedAt
	}
	if !nextRetryAfter.IsZero() {
		entry["next_retry_after"] = nextRetryAfter
	}
	entry["cooldowns"] = activeAuthFileCooldowns(auth, time.Now())
	if path != "" {
		entry["path"] = path
		entry["source"] = "file"
	}
	if claims := extractCodexIDTokenClaims(auth); claims != nil {
		entry["id_token"] = claims
	}
	if priority, ok := authFilePriority(auth); ok {
		entry["priority"] = priority
	}
	// Expose note from Attributes (set by synthesizer from JSON "note" field).
	// Fall back to Metadata for auths registered via UploadAuthFile (no synthesizer).
	if note := strings.TrimSpace(authAttribute(auth, "note")); note != "" {
		entry["note"] = note
	} else if auth.Metadata != nil {
		if rawNote, ok := auth.Metadata["note"].(string); ok {
			if trimmed := strings.TrimSpace(rawNote); trimmed != "" {
				entry["note"] = trimmed
			}
		}
	}
	if weight, ok := authWeightValue(auth); ok {
		entry[coreauth.AttributeWeight] = weight
	}
	if websockets, ok := authWebsocketsValue(auth); ok {
		entry["websockets"] = websockets
	}
	if requestRetry, ok := auth.RequestRetryOverride(); ok {
		entry["request_retry"] = requestRetry
	}
	if quota.v8 {
		nowUTC := time.Now().UTC()
		if exp, okExp := auth.AccessTokenExpirationTime(); okExp && !exp.IsZero() && exp.Unix() > 0 {
			entry["access_token_expires_at"] = exp.UTC().Format(time.RFC3339)
		}
		if !auth.NextRefreshAfter.IsZero() && auth.NextRefreshAfter.After(nowUTC) {
			entry["next_refresh_after"] = auth.NextRefreshAfter.UTC().Format(time.RFC3339)
		}
		entry["requires_sign_in"] = auth.RequiresSignIn
		if statusMessage != "" {
			entry["status_message"] = coreauth.SanitizeRenewalMessage(statusMessage)
		}
		entry["renewal_state"] = auth.RenewalStateAt(nowUTC)
		recoveryStage := auth.RecoveryStageAt(nowUTC)
		entry["recovery_stage"] = recoveryStage
		entry["quota_blocked"] = recoveryStage == coreauth.RecoveryStageQuotaBlocked
		entry["adopted"] = !auth.AdoptedAt.IsZero() || auth.CredentialVersion > 0
		if !auth.AdoptedAt.IsZero() {
			entry["adopted_at"] = auth.AdoptedAt.UTC().Format(time.RFC3339)
		}
		entry["inference_verified"] = auth.InferenceVerified
		if !auth.LastInferenceVerifiedAt.IsZero() {
			entry["last_inference_verified_at"] = auth.LastInferenceVerifiedAt.UTC().Format(time.RFC3339)
		}
		entry["renewal_verified"] = auth.RenewalVerified
		if !auth.LastRenewalVerifiedAt.IsZero() {
			entry["last_renewal_verified_at"] = auth.LastRenewalVerifiedAt.UTC().Format(time.RFC3339)
		}
		if auth.RefreshError != nil {
			entry["refresh_error"] = gin.H{
				"code":        auth.RefreshError.Code,
				"message":     auth.RefreshError.Message,
				"http_status": auth.RefreshError.HTTPStatus,
			}
		}
		if auth.RequiresSignIn {
			entry["next_action"] = "sign_in"
			entry["recovery_hint"] = "Re-authenticate this account on this proxy instance via OAuth login; signing in on another machine or a client's separate direct login does not update this instance."
			event := gin.H{
				"type":        "renewal_sign_in_required",
				"auth_id":     auth.ID,
				"provider":    strings.TrimSpace(auth.Provider),
				"next_action": "sign_in",
			}
			if !auth.RenewalEventAt.IsZero() {
				event["occurred_at"] = auth.RenewalEventAt.UTC().Format(time.RFC3339)
			}
			if email := authEmail(auth); email != "" {
				event["account"] = email
			}
			if exp, okExp := auth.AccessTokenExpirationTime(); okExp && !exp.IsZero() && exp.Unix() > 0 {
				event["access_token_expires_at"] = exp.UTC().Format(time.RFC3339)
			}
			entry["renewal_event"] = event
		}
	}
	if quota.v8 && strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		entry["siwc_bind_supported"] = siwcBindEligible(auth)
		entry["use_siwc"] = coreauth.IsCodexUseSIWC(auth)
		entry["siwc_status"] = string(auth.CodexSIWCStatus(time.Now().UTC()))
		section, _ := auth.Metadata["siwc"].(map[string]any)
		accessToken, _ := section["access_token"].(string)
		refreshToken, _ := section["refresh_token"].(string)
		clientID, _ := section["client_id"].(string)
		entry["siwc_bound"] = strings.TrimSpace(accessToken) != "" && strings.TrimSpace(refreshToken) != "" && strings.TrimSpace(clientID) != ""
		if expiry, okExpiry := auth.SIWCExpirationTime(); okExpiry {
			entry["siwc_expires_at"] = expiry.UTC().Format(time.RFC3339)
		}
	}
	return entry
}

func activeAuthFileCooldowns(auth *coreauth.Auth, now time.Time) []authFileCooldown {
	if auth == nil {
		return nil
	}
	cooldowns := make([]authFileCooldown, 0, len(auth.ModelStates))
	for model, state := range auth.ModelStates {
		if state == nil || state.Status == coreauth.StatusDisabled || !state.Unavailable || !state.NextRetryAfter.After(now) {
			continue
		}
		cooldowns = append(cooldowns, authFileCooldown{
			Model:          strings.TrimSpace(model),
			Status:         "cooling",
			Reason:         authFileCooldownReason(state.Quota, state.LastError),
			NextRetryAfter: state.NextRetryAfter,
		})
	}
	if len(cooldowns) == 0 && auth.Status != coreauth.StatusDisabled && auth.Unavailable && auth.NextRetryAfter.After(now) {
		cooldowns = append(cooldowns, authFileCooldown{
			Status:         "cooling",
			Reason:         authFileCooldownReason(auth.Quota, auth.LastError),
			NextRetryAfter: auth.NextRetryAfter,
		})
	}
	sort.Slice(cooldowns, func(i, j int) bool {
		if cooldowns[i].NextRetryAfter.Equal(cooldowns[j].NextRetryAfter) {
			return cooldowns[i].Model < cooldowns[j].Model
		}
		return cooldowns[i].NextRetryAfter.Before(cooldowns[j].NextRetryAfter)
	})
	return cooldowns
}

func authFileCooldownReason(quota coreauth.QuotaState, lastErr *coreauth.Error) string {
	if reason := strings.TrimSpace(quota.Reason); reason != "" {
		return reason
	}
	if lastErr != nil {
		return strings.TrimSpace(lastErr.Code)
	}
	return ""
}

func authFileRequestRetryFromJSON(data []byte) (int, bool) {
	var metadata map[string]any
	if errUnmarshal := json.Unmarshal(data, &metadata); errUnmarshal != nil {
		return 0, false
	}
	return (&coreauth.Auth{Metadata: metadata}).RequestRetryOverride()
}

// quotaObservationPayload exposes only passive provider observations. Cooldown
// fields are intentionally excluded so this management response cannot be
// mistaken for scheduler state or influence scheduling behavior.
func quotaObservationPayloadForProvider(provider string, quota coreauth.QuotaState) gin.H {
	if !coreauth.ProviderSupportsQuotaObservation(provider) {
		return quotaObservationPayload(coreauth.QuotaState{})
	}
	return quotaObservationPayload(quota)
}

func quotaObservationPayload(quota coreauth.QuotaState) gin.H {
	observed := gin.H{}
	if !quota.ObservedAt.IsZero() {
		observed["observed_at"] = quota.ObservedAt
	}
	signals := make(map[string]string, len(quota.Signals))
	for key, value := range quota.Signals {
		signals[key] = value
	}
	observed["signals"] = signals
	return observed
}

func modelQuotaObservationPayload(provider string, states map[string]*coreauth.ModelState) gin.H {
	if !coreauth.ProviderSupportsQuotaObservation(provider) {
		return gin.H{}
	}
	observations := gin.H{}
	for model, state := range states {
		if state == nil {
			continue
		}
		if state.Quota.ObservedAt.IsZero() && len(state.Quota.Signals) == 0 {
			continue
		}
		observations[model] = quotaObservationPayloadForProvider(provider, state.Quota)
	}
	return observations
}

func authWeightValue(auth *coreauth.Auth) (int64, bool) {
	if auth == nil {
		return 0, false
	}
	if rawWeight := strings.TrimSpace(authAttribute(auth, coreauth.AttributeWeight)); rawWeight != "" {
		weight, errWeight := credentialweight.ParseString(rawWeight)
		return weight, errWeight == nil
	}
	if auth.Metadata == nil {
		return 0, false
	}
	rawWeight, ok := auth.Metadata[coreauth.AttributeWeight]
	if !ok || rawWeight == nil {
		return 0, false
	}
	weight, errWeight := credentialweight.ParseValue(rawWeight)
	return weight, errWeight == nil
}

func authWebsocketsValue(auth *coreauth.Auth) (bool, bool) {
	if auth == nil {
		return false, false
	}
	if auth.Attributes != nil {
		if raw := strings.TrimSpace(auth.Attributes["websockets"]); raw != "" {
			parsed, errParse := strconv.ParseBool(raw)
			if errParse == nil {
				return parsed, true
			}
		}
	}
	if auth.Metadata == nil {
		return false, false
	}
	raw, ok := auth.Metadata["websockets"]
	if !ok || raw == nil {
		return false, false
	}
	switch v := raw.(type) {
	case bool:
		return v, true
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(v))
		if errParse == nil {
			return parsed, true
		}
	}
	return false, false
}

func authProjectID(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Metadata != nil {
		if v, ok := auth.Metadata["project_id"].(string); ok {
			if projectID := strings.TrimSpace(v); projectID != "" {
				return projectID
			}
		}
	}
	if auth.Attributes != nil {
		if projectID := strings.TrimSpace(auth.Attributes["project_id"]); projectID != "" {
			return projectID
		}
	}
	return ""
}

func extractCodexIDTokenClaims(auth *coreauth.Auth) gin.H {
	if auth == nil || auth.Metadata == nil {
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return nil
	}
	idTokenRaw, ok := auth.Metadata["id_token"].(string)
	if !ok {
		return nil
	}
	idToken := strings.TrimSpace(idTokenRaw)
	if idToken == "" {
		return nil
	}
	claims, err := codex.ParseJWTToken(idToken)
	if err != nil || claims == nil {
		return nil
	}

	result := gin.H{}
	if v := strings.TrimSpace(claims.CodexAuthInfo.ChatgptAccountID); v != "" {
		result["chatgpt_account_id"] = v
	}
	if v := strings.TrimSpace(claims.CodexAuthInfo.ChatgptPlanType); v != "" {
		result["plan_type"] = v
	}
	if v := claims.CodexAuthInfo.ChatgptSubscriptionActiveStart; v != nil {
		result["chatgpt_subscription_active_start"] = v
	}
	if v := claims.CodexAuthInfo.ChatgptSubscriptionActiveUntil; v != nil {
		result["chatgpt_subscription_active_until"] = v
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

func authEmail(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Metadata != nil {
		if v, ok := auth.Metadata["email"].(string); ok {
			return strings.TrimSpace(v)
		}
	}
	if auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes["email"]); v != "" {
			return v
		}
		if v := strings.TrimSpace(auth.Attributes["account_email"]); v != "" {
			return v
		}
	}
	return ""
}

func authAttribute(auth *coreauth.Auth, key string) string {
	if auth == nil || len(auth.Attributes) == 0 {
		return ""
	}
	return auth.Attributes[key]
}

func isRuntimeOnlyAuth(auth *coreauth.Auth) bool {
	if auth == nil || len(auth.Attributes) == 0 {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(auth.Attributes["runtime_only"]), "true")
}

func isUnsafeAuthFileName(name string) bool {
	if strings.TrimSpace(name) == "" {
		return true
	}
	if strings.ContainsAny(name, "/\\") {
		return true
	}
	if filepath.VolumeName(name) != "" {
		return true
	}
	return false
}
