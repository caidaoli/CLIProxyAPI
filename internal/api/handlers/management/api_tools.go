package management

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/MoeForge/moego/json"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth/xai"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/clienterror"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

const (
	defaultAPICallTimeout = 60 * time.Second
	maxAPICallBatchSize   = 256
	apiCallBatchWorkers   = 8
)

const (
	antigravityOAuthClientID     = "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com"
	antigravityOAuthClientSecret = "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"
)

var antigravityOAuthTokenURL = "https://oauth2.googleapis.com/token"

type apiCallRequest struct {
	ID              string            `json:"id,omitempty"`
	AuthIndexSnake  *string           `json:"auth_index"`
	AuthIndexCamel  *string           `json:"authIndex"`
	AuthIndexPascal *string           `json:"AuthIndex"`
	Method          string            `json:"method"`
	URL             string            `json:"url"`
	ProxyURL        string            `json:"proxy_url"`
	Header          map[string]string `json:"header"`
	Data            string            `json:"data"`
}

type apiCallResponse struct {
	ID          string              `json:"id,omitempty"`
	StatusCode  int                 `json:"status_code"`
	Header      map[string][]string `json:"header"`
	Body        string              `json:"body"`
	Error       string              `json:"error,omitempty"`
	ErrorStatus int                 `json:"error_status,omitempty"`
}

type apiCallBatchRequest struct {
	Requests []apiCallRequest `json:"requests"`
}

type apiCallBatchResponse struct {
	Results []apiCallResponse `json:"results"`
}

type apiCallExecutionError struct {
	status  int
	message string
}

// APICall makes a generic HTTP request on behalf of the management API caller.
// It is protected by the management middleware.
//
// Endpoint:
//
//	POST /v0/management/api-call
//
// Authentication:
//
//	Same as other management APIs (requires a management key and remote-management rules).
//	You can provide the key via:
//	- Authorization: Bearer <key>
//	- X-Management-Key: <key>
//
// Request JSON:
//   - auth_index / authIndex / AuthIndex (optional):
//     The credential "auth_index" from GET /v0/management/auth-files (or other endpoints returning it).
//     If omitted, credential-specific proxy selection is skipped.
//     If "$TOKEN$" is present and the credential or token cannot be resolved, the request fails with HTTP 400.
//   - method (required): HTTP method, e.g. GET, POST, PUT, PATCH, DELETE.
//   - url (required): Absolute URL including scheme and host, e.g. "https://api.example.com/v1/ping".
//   - proxy_url (optional): Proxy used for this request. Supports HTTP, HTTPS, SOCKS5, SOCKS5H,
//     and "direct"/"none" to explicitly bypass proxies. When set, credential and global proxies are ignored.
//   - header (optional): Request headers map.
//     Supports magic variable "$TOKEN$" which is replaced using the selected credential:
//     1) metadata.access_token
//     2) attributes.api_key
//     3) metadata.token / metadata.id_token / metadata.cookie
//     Example: {"Authorization":"Bearer $TOKEN$"}.
//     Note: if you need to override the HTTP Host header, set header["Host"].
//   - data (optional): Raw request body as string (useful for POST/PUT/PATCH).
//
// Proxy selection (highest priority first):
//  1. Request proxy_url (when set, lower-priority proxy settings are ignored)
//  2. Selected credential proxy_url
//  3. Global config proxy-url
//  4. Direct connect (environment proxies are not used)
//
// Response JSON (returned with HTTP 200 when the APICall itself succeeds):
//   - status_code: Upstream HTTP status code.
//   - header: Upstream response headers.
//   - body: Upstream response body as string.
//
// Example:
//
//	curl -sS -X POST "http://127.0.0.1:8317/v0/management/api-call" \
//	  -H "Authorization: Bearer <MANAGEMENT_KEY>" \
//	  -H "Content-Type: application/json" \
//	  -d '{"auth_index":"<AUTH_INDEX>","method":"GET","url":"https://api.example.com/v1/ping","header":{"Authorization":"Bearer $TOKEN$"}}'
//
//	curl -sS -X POST "http://127.0.0.1:8317/v0/management/api-call" \
//	  -H "Authorization: Bearer 831227" \
//	  -H "Content-Type: application/json" \
//	  -d '{"auth_index":"<AUTH_INDEX>","method":"POST","url":"https://api.example.com/v1/fetchAvailableModels","header":{"Authorization":"Bearer $TOKEN$","Content-Type":"application/json","User-Agent":"cliproxyapi"},"data":"{}"}'
func (h *Handler) APICall(c *gin.Context) {
	var body apiCallRequest
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil {
		c.JSON(clienterror.HTTPStatusFromErrorOr(errBindJSON, http.StatusBadRequest), gin.H{"error": "invalid body"})
		return
	}

	response, callErr := h.executeAPICall(c.Request.Context(), body)
	if callErr != nil {
		c.JSON(callErr.status, gin.H{"error": callErr.message})
		return
	}

	c.JSON(http.StatusOK, response)
}

// APICallBatch executes independent management proxy calls with bounded concurrency.
// Every item keeps its own status so one bad credential does not discard the rest.
func (h *Handler) APICallBatch(c *gin.Context) {
	var body apiCallBatchRequest
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if len(body.Requests) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "requests must not be empty"})
		return
	}
	if len(body.Requests) > maxAPICallBatchSize {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "too many requests"})
		return
	}

	seenIDs := make(map[string]struct{}, len(body.Requests))
	for i := range body.Requests {
		id := strings.TrimSpace(body.Requests[i].ID)
		if id == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing request id"})
			return
		}
		if _, exists := seenIDs[id]; exists {
			c.JSON(http.StatusBadRequest, gin.H{"error": "duplicate request id"})
			return
		}
		seenIDs[id] = struct{}{}
		body.Requests[i].ID = id
	}

	results := make([]apiCallResponse, len(body.Requests))
	requestContext := c.Request.Context()
	workerCount := min(apiCallBatchWorkers, len(body.Requests))
	jobs := make(chan int)
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				request := body.Requests[index]
				response, callErr := h.executeAPICall(requestContext, request)
				response.ID = request.ID
				if callErr != nil {
					response.Error = callErr.message
					response.ErrorStatus = callErr.status
				}
				results[index] = response
			}
		}()
	}
	for index := range body.Requests {
		jobs <- index
	}
	close(jobs)
	workers.Wait()

	c.JSON(http.StatusOK, apiCallBatchResponse{Results: results})
}

func (h *Handler) executeAPICall(ctx context.Context, body apiCallRequest) (apiCallResponse, *apiCallExecutionError) {
	fail := func(status int, message string) (apiCallResponse, *apiCallExecutionError) {
		return apiCallResponse{}, &apiCallExecutionError{status: status, message: message}
	}

	method := strings.ToUpper(strings.TrimSpace(body.Method))
	if method == "" {
		return fail(http.StatusBadRequest, "missing method")
	}

	urlStr := strings.TrimSpace(body.URL)
	if urlStr == "" {
		return fail(http.StatusBadRequest, "missing url")
	}
	parsedURL, errParseURL := url.Parse(urlStr)
	if errParseURL != nil || parsedURL.Scheme == "" || parsedURL.Host == "" {
		return fail(http.StatusBadRequest, "invalid url")
	}

	requestProxyURL := strings.TrimSpace(body.ProxyURL)
	if requestProxyURL != "" {
		if _, errParseProxy := proxyutil.Parse(requestProxyURL); errParseProxy != nil {
			return fail(http.StatusBadRequest, "invalid proxy_url")
		}
	}

	authIndex := firstNonEmptyString(body.AuthIndexSnake, body.AuthIndexCamel, body.AuthIndexPascal)
	auth := h.authByIndex(authIndex)

	reqHeaders := make(map[string]string, len(body.Header))
	for key, value := range body.Header {
		reqHeaders[key] = value
	}

	var hostOverride string
	var token string
	var tokenResolved bool
	var tokenErr error

	resolveToken := func() *apiCallExecutionError {
		if !tokenResolved {
			token, tokenErr = h.resolveTokenForAuth(ctx, auth, requestProxyURL)
			tokenResolved = true
		}
		if token == "" {
			if tokenErr != nil {
				return &apiCallExecutionError{status: http.StatusBadRequest, message: "auth token refresh failed"}
			}
			if auth == nil {
				if authIndex != "" {
					return &apiCallExecutionError{status: http.StatusBadRequest, message: "auth credential not found for auth_index"}
				}
				return &apiCallExecutionError{status: http.StatusBadRequest, message: "auth token not found"}
			}
			// Base-URL-only API keys have no bearer token; their executor omits stale auth headers.
			// OAuth/file credentials without a usable token must fail here so the
			// management proxy does not forward an unauthenticated request.
			if auth.AuthKind() != coreauth.AuthKindAPIKey {
				return &apiCallExecutionError{status: http.StatusUnauthorized, message: "missing access token"}
			}
		}
		return nil
	}

	for key, value := range reqHeaders {
		if !strings.Contains(value, "$TOKEN$") {
			continue
		}
		if tokenFailure := resolveToken(); tokenFailure != nil {
			return fail(tokenFailure.status, tokenFailure.message)
		}
		if token == "" {
			delete(reqHeaders, key)
			continue
		}
		reqHeaders[key] = strings.ReplaceAll(value, "$TOKEN$", token)
	}

	if strings.Contains(body.Data, "$TOKEN$") {
		if tokenFailure := resolveToken(); tokenFailure != nil {
			return fail(tokenFailure.status, tokenFailure.message)
		}
		replacement := token
		if json.Valid([]byte(body.Data)) && strings.ContainsAny(token, "\"\\\r\n\t") {
			if b, errMarshal := json.Marshal(token); errMarshal == nil && len(b) >= 2 {
				replacement = string(b[1 : len(b)-1])
			}
		}
		body.Data = strings.ReplaceAll(body.Data, "$TOKEN$", replacement)
	}

	var requestBody io.Reader
	if body.Data != "" {
		requestBody = strings.NewReader(body.Data)
	}

	req, errNewRequest := http.NewRequestWithContext(ctx, method, urlStr, requestBody)
	if errNewRequest != nil {
		return fail(http.StatusBadRequest, "failed to build request")
	}

	for key, value := range reqHeaders {
		if strings.EqualFold(key, "host") {
			hostOverride = strings.TrimSpace(value)
			continue
		}
		req.Header.Set(key, value)
	}
	if hostOverride != "" {
		req.Host = hostOverride
	}

	var resp *http.Response
	var errDo error
	if auth != nil && h != nil && h.authManager != nil {
		requestAuth := auth
		if requestProxyURL != "" {
			// Keep the request-scoped proxy off the shared credential.
			requestAuthCopy := *auth
			requestAuthCopy.ProxyURL = requestProxyURL
			requestAuth = &requestAuthCopy
		}
		requestContext, cancelRequest := context.WithTimeout(ctx, defaultAPICallTimeout)
		defer cancelRequest()
		resp, errDo = h.authManager.HttpRequest(requestContext, requestAuth, req.WithContext(requestContext))
	} else {
		httpClient := &http.Client{
			Timeout: defaultAPICallTimeout,
		}
		httpClient.Transport = h.apiCallTransport(auth, requestProxyURL)
		resp, errDo = httpClient.Do(req)
	}
	if errDo != nil {
		log.WithError(errDo).Debug("management APICall request failed")
		// A credential that cannot authenticate must not look like a transport
		// failure: surface the executor's own status so callers can tell an
		// unusable credential apart from an unreachable upstream.
		var statusAware interface{ StatusCode() int }
		if errors.As(errDo, &statusAware) && statusAware.StatusCode() == http.StatusUnauthorized {
			return fail(http.StatusUnauthorized, errDo.Error())
		}
		return fail(http.StatusBadGateway, "request failed")
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Errorf("response body close error: %v", errClose)
		}
	}()

	respBody, errReadAll := io.ReadAll(resp.Body)
	if errReadAll != nil {
		return fail(http.StatusBadGateway, "failed to read response")
	}

	return apiCallResponse{
		StatusCode: resp.StatusCode,
		Header:     resp.Header,
		Body:       string(respBody),
	}, nil
}

func firstNonEmptyString(values ...*string) string {
	for _, v := range values {
		if v == nil {
			continue
		}
		if out := strings.TrimSpace(*v); out != "" {
			return out
		}
	}
	return ""
}

func tokenValueForAuth(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if v := tokenValueFromMetadata(auth.Metadata); v != "" {
		return v
	}
	if auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes["api_key"]); v != "" {
			return v
		}
		if v := strings.TrimSpace(auth.Attributes["session_token"]); v != "" {
			return v
		}
	}
	return ""
}

func (h *Handler) resolveTokenForAuth(ctx context.Context, auth *coreauth.Auth, requestProxyURL string) (string, error) {
	if auth == nil {
		return "", nil
	}

	if strings.EqualFold(strings.TrimSpace(auth.Provider), "antigravity") {
		token, errToken := h.refreshAntigravityOAuthAccessToken(ctx, auth, requestProxyURL)
		return token, errToken
	}

	if strings.EqualFold(strings.TrimSpace(auth.Provider), "meta") {
		token, errToken := h.resolveMetaToken(ctx, auth, requestProxyURL)
		return token, errToken
	}

	if strings.EqualFold(strings.TrimSpace(auth.Provider), "xai") {
		token, errToken := h.resolveXAIToken(ctx, auth, requestProxyURL)
		return token, errToken
	}

	return tokenValueForAuth(auth), nil
}

func (h *Handler) resolveXAIToken(ctx context.Context, auth *coreauth.Auth, requestProxyURL string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if auth == nil {
		return "", nil
	}

	current := xaiTokenValue(auth)
	if current != "" && !xaiOAuthTokenNeedsRefresh(auth) {
		return current, nil
	}

	refreshToken := xaiRefreshToken(auth)
	if refreshToken == "" {
		return current, nil
	}

	proxyURL := strings.TrimSpace(requestProxyURL)
	if proxyURL == "" {
		proxyURL = strings.TrimSpace(auth.ProxyURL)
	}
	var cfg *config.Config
	if h != nil {
		cfg = h.cfg
	}
	svc := xaiauth.NewXAIAuthWithProxyURL(cfg, proxyURL)
	tokenEndpoint := xaiTokenEndpoint(auth)
	td, errRefresh := svc.RefreshTokens(ctx, refreshToken, tokenEndpoint)
	if errRefresh != nil {
		return "", errRefresh
	}
	if td == nil || strings.TrimSpace(td.AccessToken) == "" {
		return "", fmt.Errorf("xai oauth token refresh returned empty access_token")
	}

	now := time.Now()
	metadata := make(map[string]any, len(auth.Metadata)+10)
	if auth.Metadata != nil {
		for k, v := range auth.Metadata {
			metadata[k] = v
		}
	}
	metadata["type"] = "xai"
	metadata["auth_kind"] = "oauth"
	metadata["access_token"] = strings.TrimSpace(td.AccessToken)
	if strings.TrimSpace(td.RefreshToken) != "" {
		metadata["refresh_token"] = strings.TrimSpace(td.RefreshToken)
	}
	if strings.TrimSpace(td.IDToken) != "" {
		metadata["id_token"] = strings.TrimSpace(td.IDToken)
	}
	if strings.TrimSpace(td.TokenType) != "" {
		metadata["token_type"] = strings.TrimSpace(td.TokenType)
	}
	if td.ExpiresIn > 0 {
		metadata["expires_in"] = td.ExpiresIn
	}
	if strings.TrimSpace(td.Expire) != "" {
		metadata["expired"] = strings.TrimSpace(td.Expire)
	}
	if strings.TrimSpace(td.Email) != "" {
		metadata["email"] = strings.TrimSpace(td.Email)
	}
	if strings.TrimSpace(td.Subject) != "" {
		metadata["sub"] = strings.TrimSpace(td.Subject)
	}
	if tokenEndpoint != "" {
		metadata["token_endpoint"] = tokenEndpoint
	}
	if stringValue(metadata, "base_url") == "" {
		metadata["base_url"] = xaiauth.DefaultAPIBaseURL
	}
	metadata["last_refresh"] = now.UTC().Format(time.RFC3339)
	auth.Metadata = metadata

	attributes := make(map[string]string, len(auth.Attributes)+2)
	if auth.Attributes != nil {
		for k, v := range auth.Attributes {
			attributes[k] = v
		}
	}
	attributes["auth_kind"] = "oauth"
	if strings.TrimSpace(attributes["base_url"]) == "" {
		attributes["base_url"] = xaiauth.DefaultAPIBaseURL
	}
	auth.Attributes = attributes

	if ts := xaiTokenStorage(auth); ts != nil {
		copyStorage := *ts
		copyStorage.AccessToken = strings.TrimSpace(td.AccessToken)
		if strings.TrimSpace(td.RefreshToken) != "" {
			copyStorage.RefreshToken = strings.TrimSpace(td.RefreshToken)
		}
		if strings.TrimSpace(td.IDToken) != "" {
			copyStorage.IDToken = strings.TrimSpace(td.IDToken)
		}
		if strings.TrimSpace(td.TokenType) != "" {
			copyStorage.TokenType = strings.TrimSpace(td.TokenType)
		}
		if td.ExpiresIn > 0 {
			copyStorage.ExpiresIn = td.ExpiresIn
		}
		if strings.TrimSpace(td.Expire) != "" {
			copyStorage.Expire = strings.TrimSpace(td.Expire)
		}
		if strings.TrimSpace(td.Email) != "" {
			copyStorage.Email = strings.TrimSpace(td.Email)
		}
		if strings.TrimSpace(td.Subject) != "" {
			copyStorage.Subject = strings.TrimSpace(td.Subject)
		}
		copyStorage.LastRefresh = now.UTC().Format(time.RFC3339)
		auth.Storage = &copyStorage
	}

	if store := h.tokenStoreWithBaseDir(); store != nil && !coreauth.IsConfigAPIKeyAuth(auth) {
		if _, errSave := store.Save(ctx, auth); errSave != nil {
			log.WithError(errSave).Warn("management APICall: failed to persist refreshed xai oauth token")
		}
	}
	if h != nil && h.authManager != nil {
		auth.LastRefreshedAt = now
		auth.UpdatedAt = now
		if _, errUpdate := h.authManager.Update(ctx, auth); errUpdate != nil {
			log.WithError(errUpdate).Warn("management APICall: failed to update refreshed xai oauth credential in manager")
		}
	}

	return strings.TrimSpace(td.AccessToken), nil
}

func xaiTokenValue(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes["api_key"]); v != "" {
			return v
		}
	}
	if auth.Metadata != nil {
		if v := stringValue(auth.Metadata, "api_key"); v != "" {
			return v
		}
		if v := stringValue(auth.Metadata, "access_token"); v != "" {
			return v
		}
		if v := stringValue(auth.Metadata, "accessToken"); v != "" {
			return v
		}
	}
	if ts := xaiTokenStorage(auth); ts != nil {
		if v := strings.TrimSpace(ts.AccessToken); v != "" {
			return v
		}
	}
	return ""
}

func xaiRefreshToken(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if v := stringValue(auth.Metadata, "refresh_token"); v != "" {
		return v
	}
	if ts := xaiTokenStorage(auth); ts != nil {
		return strings.TrimSpace(ts.RefreshToken)
	}
	return ""
}

func xaiTokenEndpoint(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if v := stringValue(auth.Metadata, "token_endpoint"); v != "" {
		return v
	}
	if ts := xaiTokenStorage(auth); ts != nil {
		return strings.TrimSpace(ts.TokenEndpoint)
	}
	return ""
}

func xaiTokenStorage(auth *coreauth.Auth) *xaiauth.TokenStorage {
	if auth == nil {
		return nil
	}
	ts, ok := auth.Storage.(*xaiauth.TokenStorage)
	if !ok {
		return nil
	}
	return ts
}

func xaiOAuthTokenNeedsRefresh(auth *coreauth.Auth) bool {
	if auth == nil {
		return true
	}
	tokenStr := xaiTokenValue(auth)
	if tokenStr == "" {
		return true
	}

	lead := xaiauth.RefreshLead()
	now := time.Now()
	if lead > 0 {
		now = now.Add(lead)
	}

	temp := auth.Clone()
	if temp.Metadata == nil {
		temp.Metadata = make(map[string]any)
	}
	if stringValue(temp.Metadata, "access_token") == "" {
		temp.Metadata["access_token"] = tokenStr
	}
	if ts := xaiTokenStorage(auth); ts != nil {
		if stringValue(temp.Metadata, "expired") == "" && strings.TrimSpace(ts.Expire) != "" {
			temp.Metadata["expired"] = strings.TrimSpace(ts.Expire)
		}
		if _, ok := temp.Metadata["expires_in"]; !ok && ts.ExpiresIn > 0 {
			temp.Metadata["expires_in"] = ts.ExpiresIn
		}
		if stringValue(temp.Metadata, "last_refresh") == "" && strings.TrimSpace(ts.LastRefresh) != "" {
			temp.Metadata["last_refresh"] = strings.TrimSpace(ts.LastRefresh)
		}
	}

	return !temp.HasValidAccessToken(now)
}

func (h *Handler) refreshAntigravityOAuthAccessToken(ctx context.Context, auth *coreauth.Auth, requestProxyURL string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if auth == nil {
		return "", nil
	}

	metadata := auth.Metadata
	if len(metadata) == 0 {
		return "", fmt.Errorf("antigravity oauth metadata missing")
	}

	current := strings.TrimSpace(tokenValueFromMetadata(metadata))
	if current != "" && !antigravityTokenNeedsRefresh(metadata) {
		return current, nil
	}

	refreshToken := stringValue(metadata, "refresh_token")
	if refreshToken == "" {
		return "", fmt.Errorf("antigravity refresh token missing")
	}

	tokenURL := strings.TrimSpace(antigravityOAuthTokenURL)
	if tokenURL == "" {
		tokenURL = "https://oauth2.googleapis.com/token"
	}
	form := url.Values{}
	form.Set("client_id", antigravityOAuthClientID)
	form.Set("client_secret", antigravityOAuthClientSecret)
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)

	req, errReq := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if errReq != nil {
		return "", errReq
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	httpClient := &http.Client{
		Timeout:   defaultAPICallTimeout,
		Transport: h.apiCallTransport(auth, requestProxyURL),
	}
	resp, errDo := httpClient.Do(req)
	if errDo != nil {
		return "", errDo
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Errorf("response body close error: %v", errClose)
		}
	}()

	bodyBytes, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return "", errRead
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("antigravity oauth token refresh failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		TokenType    string `json:"token_type"`
	}
	if errUnmarshal := json.Unmarshal(bodyBytes, &tokenResp); errUnmarshal != nil {
		return "", errUnmarshal
	}

	if strings.TrimSpace(tokenResp.AccessToken) == "" {
		return "", fmt.Errorf("antigravity oauth token refresh returned empty access_token")
	}

	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	now := time.Now()
	auth.Metadata["access_token"] = strings.TrimSpace(tokenResp.AccessToken)
	if strings.TrimSpace(tokenResp.RefreshToken) != "" {
		auth.Metadata["refresh_token"] = strings.TrimSpace(tokenResp.RefreshToken)
	}
	if tokenResp.ExpiresIn > 0 {
		auth.Metadata["expires_in"] = tokenResp.ExpiresIn
		auth.Metadata["timestamp"] = now.UnixMilli()
		auth.Metadata["expired"] = now.Add(time.Duration(tokenResp.ExpiresIn) * time.Second).Format(time.RFC3339)
	}
	auth.Metadata["type"] = "antigravity"

	if h != nil && h.authManager != nil {
		auth.LastRefreshedAt = now
		auth.UpdatedAt = now
		_, _ = h.authManager.Update(ctx, auth)
	}

	return strings.TrimSpace(tokenResp.AccessToken), nil
}

func metaTokenFromAuth(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Metadata != nil {
		if k, ok := auth.Metadata["api_key"].(string); ok && strings.TrimSpace(k) != "" && !strings.HasPrefix(strings.TrimSpace(k), "dca:") {
			return strings.TrimSpace(k)
		}
		if t, ok := auth.Metadata["access_token"].(string); ok && strings.TrimSpace(t) != "" && !strings.HasPrefix(strings.TrimSpace(t), "dca:") {
			return strings.TrimSpace(t)
		}
	}
	if auth.Attributes != nil {
		if k := strings.TrimSpace(auth.Attributes["api_key"]); k != "" && !strings.HasPrefix(k, "dca:") {
			return k
		}
		if t := strings.TrimSpace(auth.Attributes["access_token"]); t != "" && !strings.HasPrefix(t, "dca:") {
			return t
		}
	}
	return ""
}

// metaManagementPreparer applies the tool's proxy override only to acquisition.
// The saved credential retains its configured proxy.
type metaManagementPreparer struct {
	executor *executor.MetaExecutor
	proxyURL string
}

func (p metaManagementPreparer) ShouldPrepareRequestAuth(auth *coreauth.Auth) bool {
	return p.executor.ShouldPrepareRequestAuth(auth)
}

func (p metaManagementPreparer) PrepareRequestAuth(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	proxyURL := auth.ProxyURL
	if strings.TrimSpace(p.proxyURL) != "" {
		auth.ProxyURL = p.proxyURL
	}
	updated, err := p.executor.PrepareRequestAuth(ctx, auth)
	if updated != nil {
		updated.ProxyURL = proxyURL
	}
	return updated, err
}

func (h *Handler) resolveMetaToken(ctx context.Context, auth *coreauth.Auth, requestProxyURL string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if auth == nil {
		return "", nil
	}
	if token := metaTokenFromAuth(auth); token != "" {
		return token, nil
	}
	var cfg *config.Config
	if h != nil {
		cfg = h.cfg
	}
	preparer := metaManagementPreparer{executor: executor.NewMetaExecutor(cfg), proxyURL: requestProxyURL}
	if !preparer.ShouldPrepareRequestAuth(auth) {
		return "", nil
	}
	if h == nil || h.authManager == nil || auth.ID == "" {
		return "", fmt.Errorf("meta token mint requires a registered credential")
	}
	updated, err := h.authManager.PrepareRequestAuth(ctx, preparer, auth)
	if err != nil {
		return "", err
	}
	return metaTokenFromAuth(updated), nil
}

func antigravityTokenNeedsRefresh(metadata map[string]any) bool {
	// Refresh a bit early to avoid requests racing token expiry.
	const skew = 30 * time.Second

	if metadata == nil {
		return true
	}
	if expStr, ok := metadata["expired"].(string); ok {
		if ts, errParse := time.Parse(time.RFC3339, strings.TrimSpace(expStr)); errParse == nil {
			return !ts.After(time.Now().Add(skew))
		}
	}
	expiresIn := int64Value(metadata["expires_in"])
	timestampMs := int64Value(metadata["timestamp"])
	if expiresIn > 0 && timestampMs > 0 {
		exp := time.UnixMilli(timestampMs).Add(time.Duration(expiresIn) * time.Second)
		return !exp.After(time.Now().Add(skew))
	}
	return true
}

func int64Value(raw any) int64 {
	switch typed := raw.(type) {
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case uint:
		return int64(typed)
	case uint32:
		return int64(typed)
	case uint64:
		if typed > uint64(^uint64(0)>>1) {
			return 0
		}
		return int64(typed)
	case float32:
		return int64(typed)
	case float64:
		return int64(typed)
	case json.Number:
		if i, errParse := typed.Int64(); errParse == nil {
			return i
		}
	case string:
		if s := strings.TrimSpace(typed); s != "" {
			if i, errParse := json.Number(s).Int64(); errParse == nil {
				return i
			}
		}
	}
	return 0
}

func stringValue(metadata map[string]any, key string) string {
	if len(metadata) == 0 || key == "" {
		return ""
	}
	if v, ok := metadata[key].(string); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func tokenValueFromMetadata(metadata map[string]any) string {
	if len(metadata) == 0 {
		return ""
	}
	if v, ok := metadata["accessToken"].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if v, ok := metadata["access_token"].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if tokenRaw, ok := metadata["token"]; ok && tokenRaw != nil {
		switch typed := tokenRaw.(type) {
		case string:
			if v := strings.TrimSpace(typed); v != "" {
				return v
			}
		case map[string]any:
			if v, ok := typed["access_token"].(string); ok && strings.TrimSpace(v) != "" {
				return strings.TrimSpace(v)
			}
			if v, ok := typed["accessToken"].(string); ok && strings.TrimSpace(v) != "" {
				return strings.TrimSpace(v)
			}
		case map[string]string:
			if v := strings.TrimSpace(typed["access_token"]); v != "" {
				return v
			}
			if v := strings.TrimSpace(typed["accessToken"]); v != "" {
				return v
			}
		}
	}
	if v, ok := metadata["token"].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if v, ok := metadata["id_token"].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if v, ok := metadata["api_key"].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if v, ok := metadata["session_token"].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if v, ok := metadata["cookie"].(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return ""
}

func (h *Handler) authByIndex(authIndex string) *coreauth.Auth {
	authIndex = strings.TrimSpace(authIndex)
	if authIndex == "" || h == nil || h.authManager == nil {
		return nil
	}
	auths := h.authManager.List()
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		auth.EnsureIndex()
		if auth.Index == authIndex {
			return auth
		}
	}
	return nil
}

func (h *Handler) apiCallTransport(auth *coreauth.Auth, requestProxyURL string) http.RoundTripper {
	if proxyStr := strings.TrimSpace(requestProxyURL); proxyStr != "" {
		if transport := buildProxyTransport(proxyStr); transport != nil {
			return transport
		}
		return directAPICallTransport()
	}

	var proxyCandidates []string
	if auth != nil {
		if proxyStr := strings.TrimSpace(auth.ProxyURL); proxyStr != "" {
			proxyCandidates = append(proxyCandidates, proxyStr)
		}
		if h != nil && h.cfg != nil {
			if proxyStr := strings.TrimSpace(proxyURLFromAPIKeyConfig(h.cfg, auth)); proxyStr != "" {
				proxyCandidates = append(proxyCandidates, proxyStr)
			}
		}
	}
	if h != nil && h.cfg != nil {
		if proxyStr := strings.TrimSpace(h.cfg.ProxyURL); proxyStr != "" {
			proxyCandidates = append(proxyCandidates, proxyStr)
		}
	}

	for _, proxyStr := range proxyCandidates {
		if transport := buildProxyTransport(proxyStr); transport != nil {
			return transport
		}
	}

	return directAPICallTransport()
}

func directAPICallTransport() http.RoundTripper {
	transport, ok := http.DefaultTransport.(*http.Transport)
	if !ok || transport == nil {
		return &http.Transport{Proxy: nil}
	}
	clone := transport.Clone()
	clone.Proxy = nil
	return clone
}

type apiKeyConfigEntry interface {
	GetAPIKey() string
	GetBaseURL() string
}

func resolveAPIKeyConfig[T apiKeyConfigEntry](entries []T, auth *coreauth.Auth) *T {
	if auth == nil || len(entries) == 0 {
		return nil
	}
	attrKey, attrBase := "", ""
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range entries {
		entry := &entries[i]
		cfgKey := strings.TrimSpace((*entry).GetAPIKey())
		cfgBase := strings.TrimSpace((*entry).GetBaseURL())
		if attrKey != "" && attrBase != "" {
			if strings.EqualFold(cfgKey, attrKey) && strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
			continue
		}
		if attrKey != "" && strings.EqualFold(cfgKey, attrKey) {
			if cfgBase == "" || strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
		}
		if attrKey == "" && attrBase != "" && strings.EqualFold(cfgBase, attrBase) {
			return entry
		}
	}
	if attrKey != "" {
		for i := range entries {
			entry := &entries[i]
			if strings.EqualFold(strings.TrimSpace((*entry).GetAPIKey()), attrKey) {
				return entry
			}
		}
	}
	return nil
}

func proxyURLFromAPIKeyConfig(cfg *config.Config, auth *coreauth.Auth) string {
	if cfg == nil || auth == nil {
		return ""
	}
	authKind, authAccount := auth.AccountInfo()
	if !strings.EqualFold(strings.TrimSpace(authKind), "api_key") {
		return ""
	}

	attrs := auth.Attributes
	compatName := ""
	providerKey := ""
	if len(attrs) > 0 {
		compatName = strings.TrimSpace(attrs["compat_name"])
		providerKey = strings.TrimSpace(attrs["provider_key"])
	}
	if compatName != "" || strings.EqualFold(strings.TrimSpace(auth.Provider), "openai-compatibility") {
		return resolveOpenAICompatAPIKeyProxyURL(cfg, auth, strings.TrimSpace(authAccount), providerKey, compatName)
	}

	switch strings.ToLower(strings.TrimSpace(auth.Provider)) {
	case "gemini":
		if entry := resolveAPIKeyConfig(cfg.GeminiKey, auth); entry != nil {
			return strings.TrimSpace(entry.ProxyURL)
		}
	case "gemini-interactions":
		if entry := resolveAPIKeyConfig(cfg.InteractionsKey, auth); entry != nil {
			return strings.TrimSpace(entry.ProxyURL)
		}
	case "claude":
		if entry := resolveAPIKeyConfig(cfg.ClaudeKey, auth); entry != nil {
			return strings.TrimSpace(entry.ProxyURL)
		}
	case "codex":
		if entry := resolveAPIKeyConfig(cfg.CodexKey, auth); entry != nil {
			return strings.TrimSpace(entry.ProxyURL)
		}
	case "xai":
		if entry := resolveAPIKeyConfig(cfg.XAIKey, auth); entry != nil {
			return strings.TrimSpace(entry.ProxyURL)
		}
	case "meta":
		if entry := resolveAPIKeyConfig(cfg.MetaKey, auth); entry != nil {
			return strings.TrimSpace(entry.ProxyURL)
		}
	}
	return ""
}

func resolveOpenAICompatAPIKeyProxyURL(cfg *config.Config, auth *coreauth.Auth, apiKey, providerKey, compatName string) string {
	if cfg == nil || auth == nil {
		return ""
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return ""
	}
	candidates := make([]string, 0, 3)
	if v := strings.TrimSpace(compatName); v != "" {
		candidates = append(candidates, v)
	}
	if v := strings.TrimSpace(providerKey); v != "" {
		candidates = append(candidates, v)
	}
	if v := strings.TrimSpace(auth.Provider); v != "" {
		candidates = append(candidates, v)
	}

	for i := range cfg.OpenAICompatibility {
		compat := &cfg.OpenAICompatibility[i]
		if compat.Disabled {
			continue
		}
		for _, candidate := range candidates {
			if candidate != "" && strings.EqualFold(strings.TrimSpace(candidate), compat.Name) {
				for j := range compat.APIKeyEntries {
					entry := &compat.APIKeyEntries[j]
					if strings.EqualFold(strings.TrimSpace(entry.APIKey), apiKey) {
						return strings.TrimSpace(entry.ProxyURL)
					}
				}
				return ""
			}
		}
	}
	return ""
}

func buildProxyTransport(proxyStr string) *http.Transport {
	transport, _, errBuild := proxyutil.BuildHTTPTransport(proxyStr)
	if errBuild != nil {
		log.WithError(errBuild).Debug("build proxy transport failed")
		return nil
	}
	return transport
}

const (
	codexVerifyURL       = "https://chatgpt.com/backend-api/wham/usage"
	codexVerifyUserAgent = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
)

const authCleanupMaxConcurrency = 8

type authCleanupRequest struct {
	Provider string `json:"provider"`
}

// isAuthCleanupProviderSupported reports whether the management API can probe
// credentials of this provider for invalid-token cleanup.
func isAuthCleanupProviderSupported(provider string) bool {
	return normalizeAuthCleanupProvider(provider) == "codex"
}

// authCleanupUnsupportedMessage returns a stable user-facing error for providers
// that are intentionally not cleaned up by this endpoint.
func authCleanupUnsupportedMessage(provider string) string {
	return fmt.Sprintf("unsupported cleanup provider: %s", provider)
}

func normalizeAuthCleanupProvider(provider string) string {
	return strings.ToLower(strings.TrimSpace(provider))
}

// parseAuthCleanupProvider resolves the target provider from query/body.
// Empty input defaults to "codex" for backward compatibility with older clients.
func parseAuthCleanupProvider(c *gin.Context) string {
	if c == nil {
		return "codex"
	}
	provider := normalizeAuthCleanupProvider(c.Query("provider"))
	if provider == "" {
		var body authCleanupRequest
		if err := c.ShouldBindJSON(&body); err == nil {
			provider = normalizeAuthCleanupProvider(body.Provider)
		}
	}
	if provider == "" {
		return "codex"
	}
	return provider
}

// CleanupCodexAuth verifies provider credentials and removes invalid ones.
//
// Endpoint:
//
//	POST /v0/management/custom/codex-cleanup
//
// Request body (optional):
//
//	{"provider":"codex"}
//
// Query (optional):
//
//	?provider=codex
//
// Provider defaults to "codex" when omitted. Codex credentials are deleted on
// 4xx verification responses. Other responses and request errors keep the credential.
//
// Response: NDJSON stream (application/x-ndjson), one JSON object per line:
//
//	{"type":"start","total":N,"provider":"codex"}
//	{"type":"progress","index":1,"total":N,"name":"...","auth_index":"...","status_code":200,"deleted":false}
//	{"type":"done","total":N,"deleted":M,"provider":"codex"}
func (h *Handler) CleanupCodexAuth(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	provider := parseAuthCleanupProvider(c)
	if !isAuthCleanupProviderSupported(provider) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": authCleanupUnsupportedMessage(provider),
		})
		return
	}

	ctx := c.Request.Context()
	auths := h.authManager.List()

	// Collect matching enabled auths first to know total count.
	var targetAuths []*coreauth.Auth
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(auth.Provider), provider) {
			continue
		}
		if auth.Disabled {
			continue
		}
		if !isAuthCleanupCandidate(auth) {
			continue
		}
		auth.EnsureIndex()
		targetAuths = append(targetAuths, auth)
	}

	total := len(targetAuths)
	log.Infof("[auth-cleanup] starting cleanup provider=%s total=%d", provider, total)

	c.Writer.Header().Set("Content-Type", "application/x-ndjson")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)

	writeEvent := func(v any) {
		data, _ := json.Marshal(v)
		data = append(data, '\n')
		_, _ = c.Writer.Write(data)
		c.Writer.Flush()
	}

	writeEvent(gin.H{"type": "start", "total": total, "provider": provider})

	deleted := 0
	for result := range h.verifyAuthsForCleanup(ctx, provider, targetAuths) {
		ev := result.event
		if result.verifyErr != nil {
			writeEvent(ev)
			continue
		}

		if result.shouldDelete {
			log.Infof("[auth-cleanup] provider=%s %s: token invalid (status %d), removing", provider, result.name, result.statusCode)
			removed, delErr := h.removeVerifiedCleanupAuth(ctx, result)
			if delErr != nil {
				log.Errorf("[auth-cleanup] provider=%s %s: delete failed: %v", provider, result.name, delErr)
				ev["deleted"] = false
				ev["error"] = delErr.Error()
			} else if !removed {
				log.Infof("[auth-cleanup] provider=%s %s: credential changed during verification, keeping it", provider, result.name)
				ev["deleted"] = false
				ev["skipped"] = "credential_changed"
			} else {
				log.Infof("[auth-cleanup] provider=%s %s: deleted successfully", provider, result.name)
				ev["deleted"] = true
				deleted++
			}
		} else {
			ev["deleted"] = false
			if result.statusCode >= http.StatusBadRequest {
				log.Warnf("[auth-cleanup] provider=%s %s: verify returned status %d, keeping credential", provider, result.name, result.statusCode)
			} else {
				log.Debugf("[auth-cleanup] provider=%s %s: valid (status %d)", provider, result.name, result.statusCode)
			}
		}
		writeEvent(ev)
	}

	writeEvent(gin.H{"type": "done", "total": total, "deleted": deleted, "provider": provider})
	log.Infof("[auth-cleanup] finished provider=%s checked=%d deleted=%d", provider, total, deleted)
}

type authCleanupJob struct {
	index    int
	total    int
	provider string
	auth     *coreauth.Auth
	name     string
}

type authCleanupVerifyResult struct {
	auth         *coreauth.Auth
	name         string
	statusCode   int
	shouldDelete bool
	tokenHash    [sha256.Size]byte
	verifyErr    error
	event        gin.H
}

func (h *Handler) verifyAuthsForCleanup(ctx context.Context, provider string, auths []*coreauth.Auth) <-chan authCleanupVerifyResult {
	results := make(chan authCleanupVerifyResult, len(auths))
	total := len(auths)
	if total == 0 {
		close(results)
		return results
	}

	workers := authCleanupWorkerCount(total)
	jobs := make(chan authCleanupJob)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- h.verifyAuthForCleanup(ctx, job)
			}
		}()
	}

	go func() {
		for i, auth := range auths {
			name := strings.TrimSpace(auth.FileName)
			if name == "" {
				name = auth.ID
			}
			jobs <- authCleanupJob{
				index:    i + 1,
				total:    total,
				provider: provider,
				auth:     auth,
				name:     name,
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	return results
}

func authCleanupWorkerCount(total int) int {
	if total <= 0 {
		return 0
	}
	if total < authCleanupMaxConcurrency {
		return total
	}
	return authCleanupMaxConcurrency
}

func isAuthCleanupCandidate(auth *coreauth.Auth) bool {
	if auth == nil {
		return false
	}
	fileBacked := strings.TrimSpace(authAttribute(auth, "path")) != "" || strings.TrimSpace(auth.FileName) != ""
	return fileBacked
}

func (h *Handler) verifyAuthForCleanup(ctx context.Context, job authCleanupJob) authCleanupVerifyResult {
	ev := gin.H{
		"type":       "progress",
		"index":      job.index,
		"total":      job.total,
		"name":       job.name,
		"auth_index": job.auth.Index,
		"provider":   job.provider,
	}
	result := authCleanupVerifyResult{
		auth:  job.auth,
		name:  job.name,
		event: ev,
	}

	token, tokenErr := h.resolveTokenForAuth(ctx, job.auth, "")
	if tokenErr != nil || token == "" {
		errMsg := "token not available"
		if tokenErr != nil {
			errMsg = tokenErr.Error()
		}
		log.Warnf("[auth-cleanup] provider=%s %s: token resolve failed: %s", job.provider, job.name, errMsg)
		ev["error"] = errMsg
		result.verifyErr = fmt.Errorf("%s", errMsg)
		return result
	}
	result.tokenHash = sha256.Sum256([]byte(token))

	statusCode, verifyErr := h.verifyProviderToken(ctx, job.provider, job.auth, token)
	ev["status_code"] = statusCode
	result.statusCode = statusCode
	if verifyErr != nil {
		log.Warnf("[auth-cleanup] provider=%s %s: verify request failed: %v", job.provider, job.name, verifyErr)
		ev["error"] = verifyErr.Error()
		result.verifyErr = verifyErr
		return result
	}

	result.shouldDelete = shouldDeleteCleanupAuth(job.provider, statusCode)
	return result
}

func shouldDeleteCleanupAuth(provider string, statusCode int) bool {
	switch normalizeAuthCleanupProvider(provider) {
	case "codex":
		return statusCode >= http.StatusBadRequest && statusCode < http.StatusInternalServerError
	default:
		return false
	}
}

func (h *Handler) verifyProviderToken(ctx context.Context, provider string, auth *coreauth.Auth, token string) (int, error) {
	switch normalizeAuthCleanupProvider(provider) {
	case "codex":
		return h.verifyCodexToken(ctx, auth, token, extractCodexAccountID(auth))
	default:
		return 0, fmt.Errorf("unsupported cleanup provider: %s", provider)
	}
}

func (h *Handler) verifyCodexToken(ctx context.Context, auth *coreauth.Auth, token, accountID string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, codexVerifyURL, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", codexVerifyUserAgent)
	if accountID != "" {
		req.Header.Set("Chatgpt-Account-Id", accountID)
	}

	client := &http.Client{
		Timeout:   defaultAPICallTimeout,
		Transport: h.apiCallTransport(auth, ""),
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Warnf("[auth-cleanup] codex verify response close failed: %v", errClose)
		}
	}()

	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, nil
}

func (h *Handler) removeVerifiedCleanupAuth(ctx context.Context, result authCleanupVerifyResult) (bool, error) {
	if h == nil || h.authManager == nil || result.auth == nil {
		return false, nil
	}
	authID := strings.TrimSpace(result.auth.ID)
	if authID == "" {
		return false, nil
	}
	expectedPath := h.cleanupAuthPath(result.auth)
	var matchErr error
	removed, errRemove := h.authManager.RemoveIf(ctx, authID, func(current *coreauth.Auth) bool {
		if current == nil || !strings.EqualFold(strings.TrimSpace(current.Provider), strings.TrimSpace(result.auth.Provider)) {
			return false
		}
		if !sameAuthFilePath(h.cleanupAuthPath(current), expectedPath) {
			return false
		}
		currentToken, errToken := h.resolveTokenForAuth(ctx, current, "")
		if errToken != nil || currentToken == "" {
			if errToken != nil {
				matchErr = fmt.Errorf("failed to re-resolve current token: %w", errToken)
			}
			return false
		}
		return sha256.Sum256([]byte(currentToken)) == result.tokenHash
	}, func(current *coreauth.Auth) error {
		return h.deleteAuthBacking(ctx, current)
	})
	if errRemove != nil {
		return false, errRemove
	}
	if matchErr != nil {
		return false, matchErr
	}
	return removed, nil
}

// removeCodexAuth deletes a credential file and its runtime auth record.
// Name kept for historical call sites; works for any provider auth file.
func (h *Handler) removeCodexAuth(ctx context.Context, auth *coreauth.Auth) error {
	if errDelete := h.deleteAuthBacking(ctx, auth); errDelete != nil {
		return errDelete
	}
	id := strings.TrimSpace(auth.ID)
	if id == "" {
		id = h.cleanupAuthPath(auth)
	}
	h.removeAuth(ctx, id)
	return nil
}

func (h *Handler) deleteAuthBacking(ctx context.Context, auth *coreauth.Auth) error {
	path := h.cleanupAuthPath(auth)
	if path == "" {
		return fmt.Errorf("auth path is empty")
	}
	if errUsage := h.deleteAuthUsage(ctx, auth); errUsage != nil {
		return fmt.Errorf("failed to delete usage statistics: %w", errUsage)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove file: %w", err)
	}
	h.notifyAuthFileMutation(path)
	if err := h.deleteTokenRecord(ctx, path); err != nil {
		return fmt.Errorf("failed to delete token record: %w", err)
	}
	return nil
}

func (h *Handler) cleanupAuthPath(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	path := strings.TrimSpace(authAttribute(auth, "path"))
	if path == "" && h != nil && h.cfg != nil && strings.TrimSpace(auth.FileName) != "" {
		path = filepath.Join(h.cfg.AuthDir, auth.FileName)
	}
	if path != "" && !filepath.IsAbs(path) {
		if abs, errAbs := filepath.Abs(path); errAbs == nil {
			path = abs
		}
	}
	return path
}

func extractCodexAccountID(auth *coreauth.Auth) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	idTokenRaw, ok := auth.Metadata["id_token"].(string)
	if !ok {
		return ""
	}
	claims, err := codex.ParseJWTToken(strings.TrimSpace(idTokenRaw))
	if err != nil || claims == nil {
		return ""
	}
	return strings.TrimSpace(claims.GetAccountID())
}
