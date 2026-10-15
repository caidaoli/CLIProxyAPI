package synthesizer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	kimiauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth/kimi"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	log "github.com/sirupsen/logrus"
)

// FileSynthesizer generates Auth entries from OAuth JSON files.
// It handles file-based authentication.
type FileSynthesizer struct{}

// NewFileSynthesizer creates a new FileSynthesizer instance.
func NewFileSynthesizer() *FileSynthesizer {
	return &FileSynthesizer{}
}

// Synthesize generates Auth entries from auth files in the auth directory.
func (s *FileSynthesizer) Synthesize(ctx *SynthesisContext) ([]*coreauth.Auth, error) {
	out := make([]*coreauth.Auth, 0, 16)
	if ctx == nil || ctx.AuthDir == "" {
		return out, nil
	}

	entries, err := os.ReadDir(ctx.AuthDir)
	if err != nil {
		// Not an error if directory doesn't exist
		return out, nil
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		full := filepath.Join(ctx.AuthDir, name)
		data, errRead := os.ReadFile(full)
		if errRead != nil || len(data) == 0 {
			continue
		}
		auths, errSynthesize := SynthesizeAuthFile(ctx, full, data)
		if errSynthesize != nil {
			log.WithError(errSynthesize).Warnf("skipping auth file %s", name)
			continue
		}
		if len(auths) == 0 {
			continue
		}
		out = append(out, auths...)
	}
	return out, nil
}

// NativeAuthFileResult contains native auth synthesis results and the source provider.
type NativeAuthFileResult struct {
	Provider string
	Auths    []*coreauth.Auth
}

// SynthesizeAuthFile generates Auth entries for one auth JSON file payload.
// It shares exactly the same mapping behavior as FileSynthesizer.Synthesize.
func SynthesizeAuthFile(ctx *SynthesisContext, fullPath string, data []byte) ([]*coreauth.Auth, error) {
	auths, handled, errPlugin := SynthesizePluginAuthFile(ctx, fullPath, data)
	if errPlugin != nil && ctx.StrictFileParsing {
		return nil, errPlugin
	}
	if errPlugin == nil && handled {
		return auths, nil
	}
	result, errNative := SynthesizeNativeAuthFile(ctx, fullPath, data)
	if errNative != nil {
		return nil, errNative
	}
	return result.Auths, nil
}

// resolveFileAuthKind decides the shared auth_kind for a synthesized file credential.
func resolveFileAuthKind(metadata map[string]any) string {
	if kind, ok := metadata["auth_kind"].(string); ok {
		if normalized := strings.ToLower(strings.TrimSpace(kind)); normalized != "" {
			switch normalized {
			case coreauth.AuthKindAPIKey, "api_key", "api-key":
				return coreauth.AuthKindAPIKey
			case coreauth.AuthKindOAuth, "oauth2":
				return coreauth.AuthKindOAuth
			}
		}
	}
	return coreauth.AuthKindOAuth
}

// SynthesizeNativeAuthFile generates native auth entries without invoking plugin parsing.
func SynthesizeNativeAuthFile(ctx *SynthesisContext, fullPath string, data []byte) (NativeAuthFileResult, error) {
	if ctx == nil || len(data) == 0 {
		return NativeAuthFileResult{}, fmt.Errorf("auth file payload is empty")
	}
	var metadata map[string]any
	if errUnmarshal := json.Unmarshal(data, &metadata); errUnmarshal != nil {
		return NativeAuthFileResult{}, fmt.Errorf("parse auth file: %w", errUnmarshal)
	}
	coreauth.NormalizeCredentialMetadata(metadata)
	if errWeight := coreauth.ValidateAuthWeight(&coreauth.Auth{Metadata: metadata}); errWeight != nil {
		return NativeAuthFileResult{}, fmt.Errorf("invalid weight in %s: %w", filepath.Base(fullPath), errWeight)
	}
	t, _ := metadata["type"].(string)
	provider := strings.ToLower(strings.TrimSpace(t))
	if provider == "gemini" {
		provider = "gemini-cli"
	}
	auths, errSynthesize := synthesizeNativeFileAuths(ctx, fullPath, metadata, provider)
	if errSynthesize != nil {
		return NativeAuthFileResult{}, errSynthesize
	}
	return NativeAuthFileResult{Provider: provider, Auths: auths}, nil
}

// SynthesizePluginAuthFile generates plugin auth entries when a plugin handles the file.
func SynthesizePluginAuthFile(ctx *SynthesisContext, fullPath string, data []byte) ([]*coreauth.Auth, bool, error) {
	if ctx == nil || ctx.PluginAuthParser == nil {
		return nil, false, nil
	}
	var metadata map[string]any
	if errUnmarshal := json.Unmarshal(data, &metadata); errUnmarshal != nil {
		return nil, false, fmt.Errorf("parse plugin auth file: %w", errUnmarshal)
	}
	coreauth.NormalizeCredentialMetadata(metadata)
	if errWeight := coreauth.ValidateAuthWeight(&coreauth.Auth{Metadata: metadata}); errWeight != nil {
		return nil, false, fmt.Errorf("invalid weight in %s: %w", filepath.Base(fullPath), errWeight)
	}
	t, _ := metadata["type"].(string)
	provider := strings.ToLower(strings.TrimSpace(t))
	if provider == "gemini" {
		provider = "gemini-cli"
	}
	auths, handled, errParse := parsePluginFileAuths(ctx.PluginAuthParser, pluginapi.AuthParseRequest{
		Provider: provider,
		Path:     fullPath,
		FileName: filepath.Base(fullPath),
		RawJSON:  data,
	})
	if errParse != nil || !handled {
		return nil, handled, errParse
	}
	decorated, errDecorate := decoratePluginFileAuths(ctx, fullPath, metadata, compactPluginAuths(auths))
	if errDecorate != nil {
		return nil, true, errDecorate
	}
	return decorated, true, nil
}

func decoratePluginFileAuths(ctx *SynthesisContext, fullPath string, metadata map[string]any, auths []*coreauth.Auth) ([]*coreauth.Auth, error) {
	if len(auths) == 0 {
		return nil, nil
	}
	perAccountExcluded := extractExcludedModelsFromMetadata(metadata)
	perAccountModelAliases := extractOAuthModelAliasesFromMetadata(metadata)
	disabled, _ := metadata["disabled"].(bool)
	for index, auth := range auths {
		if auth == nil {
			continue
		}
		coreauth.NormalizeCredentialMetadata(auth.Metadata)
		if len(auths) > 1 {
			coreauth.MarkPluginVirtualAuth(auth, fullPath, index)
		}
		auth.CreatedAt = ctx.Now
		auth.UpdatedAt = ctx.Now
		if auth.Attributes == nil {
			auth.Attributes = make(map[string]string)
		}
		auth.Attributes[coreauth.AttributePath] = fullPath
		auth.Attributes[coreauth.AttributeSource] = fullPath
		auth.Attributes[coreauth.AttributeSourceBackend] = coreauth.AuthSourceFile
		if disabled {
			auth.Disabled = true
			auth.Status = coreauth.StatusDisabled
			if auth.Metadata == nil {
				auth.Metadata = make(map[string]any)
			}
			auth.Metadata["disabled"] = true
		}
		if p, ok := metadata["proxy_url"].(string); ok && auth.ProxyURL == "" {
			auth.ProxyURL = strings.TrimSpace(p)
		}
		if pref, ok := metadata["prefix"].(string); ok && auth.Prefix == "" {
			auth.Prefix = strings.Trim(strings.TrimSpace(pref), "/")
		}
		if errWeight := coreauth.ApplyAuthWeightMetadata(auth, metadata); errWeight != nil {
			return nil, fmt.Errorf("invalid plugin auth weight in %s: %w", filepath.Base(fullPath), errWeight)
		}
		coreauth.ApplyAuthPriorityMetadata(auth, metadata)
		if _, inherited := auth.Attributes[coreauth.AttributeFilePriority]; inherited {
			if setter, ok := auth.Storage.(interface{ SetMetadata(map[string]any) }); ok {
				setter.SetMetadata(auth.Metadata)
			}
		}
		coreauth.SetOAuthModelAliasesAttribute(auth, perAccountModelAliases)
		ApplyAuthExcludedModelsMeta(auth, ctx.Config, perAccountExcluded, "oauth")
		coreauth.ApplyCustomHeadersFromMetadata(auth)
		applyFingerprintProfileAttribute(auth, metadata)
	}
	return auths, nil
}

func synthesizeNativeFileAuths(ctx *SynthesisContext, fullPath string, metadata map[string]any, provider string) ([]*coreauth.Auth, error) {
	if provider == "" || provider == "gemini-cli" {
		return nil, nil
	}
	now := ctx.Now
	cfg := ctx.Config
	label := provider
	if email, _ := metadata["email"].(string); email != "" {
		label = email
	}
	// Use relative path under authDir as ID to stay consistent with the file-based token store.
	id := fullPath
	if strings.TrimSpace(ctx.AuthDir) != "" {
		if rel, errRel := filepath.Rel(ctx.AuthDir, fullPath); errRel == nil && rel != "" {
			id = rel
		}
	}
	if runtime.GOOS == "windows" {
		id = strings.ToLower(id)
	}

	proxyURL := ""
	if p, ok := metadata["proxy_url"].(string); ok {
		proxyURL = p
	}

	prefix := ""
	if rawPrefix, ok := metadata["prefix"].(string); ok {
		trimmed := strings.TrimSpace(rawPrefix)
		trimmed = strings.Trim(trimmed, "/")
		if trimmed != "" && !strings.Contains(trimmed, "/") {
			prefix = trimmed
		}
	}

	disabled, _ := metadata["disabled"].(bool)
	status := coreauth.StatusActive
	if disabled {
		status = coreauth.StatusDisabled
	}

	// Read per-account excluded models from the OAuth JSON file.
	perAccountExcluded := extractExcludedModelsFromMetadata(metadata)
	perAccountModelAliases := extractOAuthModelAliasesFromMetadata(metadata)

	a := &coreauth.Auth{
		ID:       id,
		FileName: filepath.Base(fullPath),
		Provider: provider,
		Label:    label,
		Prefix:   prefix,
		Status:   status,
		Disabled: disabled,
		Attributes: map[string]string{
			coreauth.AttributeSource:        fullPath,
			coreauth.AttributePath:          fullPath,
			coreauth.AttributeSourceBackend: coreauth.AuthSourceFile,
		},
		ProxyURL:  proxyURL,
		Metadata:  metadata,
		CreatedAt: now,
		UpdatedAt: now,
	}
	// Read priority from auth file.
	coreauth.ApplyAuthPriorityMetadata(a, metadata)
	if errWeight := coreauth.ApplyAuthWeightMetadata(a, metadata); errWeight != nil {
		return nil, fmt.Errorf("invalid auth weight in %s: %w", filepath.Base(fullPath), errWeight)
	}
	// Read note from auth file.
	if rawNote, ok := metadata["note"]; ok {
		if note, isStr := rawNote.(string); isStr {
			if trimmed := strings.TrimSpace(note); trimmed != "" {
				a.Attributes["note"] = trimmed
			}
		}
	}
	coreauth.ApplyCustomHeadersFromMetadata(a)
	coreauth.SetOAuthModelAliasesAttribute(a, perAccountModelAliases)
	authKind := resolveFileAuthKind(metadata)
	ApplyAuthExcludedModelsMeta(a, cfg, perAccountExcluded, authKind)
	applyFingerprintProfileAttribute(a, metadata)
	// For Kimi auth files, preserve domain and base_url attributes.
	if provider == "kimi" || provider == "kimi-ai" || provider == "kimi.ai" || provider == "kimi.com" {
		if bu, ok := metadata["base_url"].(string); ok && strings.TrimSpace(bu) != "" {
			a.Attributes["base_url"] = strings.TrimSpace(bu)
		}
		if dom, ok := metadata["domain"].(string); ok && strings.TrimSpace(dom) != "" {
			a.Attributes["domain"] = strings.TrimSpace(dom)
		}
		resolvedDomain := kimiauth.ResolveKimiDomainFromAuth(a)
		if a.Attributes["domain"] == "" {
			a.Attributes["domain"] = resolvedDomain
		} else {
			a.Attributes["domain"] = kimiauth.NormalizeKimiDomain(a.Attributes["domain"])
		}
		if a.Attributes["base_url"] == "" {
			a.Attributes["base_url"] = kimiauth.ResolveKimiAPIBaseURL(resolvedDomain)
		}
	}
	// Explicit metadata wins over the JWT claim so user edits and imports land
	// as written; "chatgpt_plan_type" stays supported for sub2api imports.
	if provider == "codex" {
		planType := ""
		for _, key := range []string{"plan_type", "chatgpt_plan_type"} {
			if value, ok := metadata[key].(string); ok {
				planType = strings.TrimSpace(value)
				if planType != "" {
					break
				}
			}
		}
		if planType == "" {
			if idTokenRaw, ok := metadata["id_token"].(string); ok && strings.TrimSpace(idTokenRaw) != "" {
				planType = codex.DefaultPlanType
				if claims, errParse := codex.ParseJWTToken(idTokenRaw); errParse == nil && claims != nil {
					planType = claims.GetPlanType()
				}
			}
		}
		if planType != "" {
			a.Attributes["plan_type"] = planType
		}
	}
	return []*coreauth.Auth{a}, nil
}

func parsePluginFileAuths(parser PluginAuthParser, req pluginapi.AuthParseRequest) ([]*coreauth.Auth, bool, error) {
	if parser == nil {
		return nil, false, nil
	}
	if multiParser, ok := parser.(PluginMultiAuthParser); ok {
		return multiParser.ParseAuths(context.Background(), req)
	}
	auth, handled, errParse := parser.ParseAuth(context.Background(), req)
	if errParse != nil || !handled || auth == nil {
		return nil, handled, errParse
	}
	return []*coreauth.Auth{auth}, true, nil
}

func compactPluginAuths(auths []*coreauth.Auth) []*coreauth.Auth {
	if len(auths) == 0 {
		return nil
	}
	out := auths[:0]
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if errWeight := coreauth.ValidateAuthWeight(auth); errWeight != nil {
			continue
		}
		out = append(out, auth)
	}
	return out
}

// extractOAuthModelAliasesFromMetadata reads per-account model aliases from OAuth JSON metadata.
// "model_aliases" is canonical; "model-aliases" remains a legacy alias.
func extractOAuthModelAliasesFromMetadata(metadata map[string]any) []config.OAuthModelAlias {
	if metadata == nil {
		return nil
	}
	raw, ok := metadata["model_aliases"]
	if !ok {
		raw, ok = metadata["model-aliases"]
	}
	if !ok || raw == nil {
		return nil
	}
	data, errMarshal := json.Marshal(raw)
	if errMarshal != nil {
		return nil
	}
	var aliases []config.OAuthModelAlias
	if errUnmarshal := json.Unmarshal(data, &aliases); errUnmarshal != nil {
		return nil
	}
	cfg := config.Config{
		OAuthModelAlias: map[string][]config.OAuthModelAlias{
			"auth": aliases,
		},
	}
	cfg.SanitizeOAuthModelAlias()
	return cfg.OAuthModelAlias["auth"]
}

// extractExcludedModelsFromMetadata reads per-account excluded models from the OAuth JSON metadata.
// "excluded_models" is canonical; "excluded-models" remains a legacy alias.
func extractExcludedModelsFromMetadata(metadata map[string]any) []string {
	if metadata == nil {
		return nil
	}
	raw, ok := metadata["excluded_models"]
	if !ok {
		raw, ok = metadata["excluded-models"]
	}
	if !ok || raw == nil {
		return nil
	}
	var stringSlice []string
	switch v := raw.(type) {
	case []string:
		stringSlice = v
	case []interface{}:
		stringSlice = make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				stringSlice = append(stringSlice, s)
			}
		}
	default:
		return nil
	}
	result := make([]string, 0, len(stringSlice))
	for _, s := range stringSlice {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
