package cliproxy

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	log "github.com/sirupsen/logrus"
)

func (s *Service) currentConfig() *config.Config {
	if s == nil {
		return nil
	}
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	return cfg
}

func (s *Service) currentWatcher() *WatcherWrapper {
	if s == nil {
		return nil
	}
	s.cfgMu.RLock()
	w := s.watcher
	s.cfgMu.RUnlock()
	return w
}

func (s *Service) setWatcher(w *WatcherWrapper) {
	if s == nil {
		return
	}
	s.cfgMu.Lock()
	s.watcher = w
	s.cfgMu.Unlock()
}

func (s *Service) applyConfigUpdate(newCfg *config.Config) {
	s.applyConfigUpdateWithAuthSynthesis(context.Background(), newCfg, true)
}

func (s *Service) applyWatcherConfigUpdate(newCfg *config.Config) {
	s.applyConfigUpdateWithAuthSynthesis(context.Background(), newCfg, false)
}

type configCommit struct {
	cfg      *config.Config
	sequence uint64
}

type routingRuntimeState struct {
	strategy                 string
	sessionAffinity          bool
	sessionAffinityTTL       time.Duration
	sessionAffinitySubagents bool
	sessionAffinityRebindOn  string
}

func normalizedRoutingRuntimeState(cfg *config.Config) routingRuntimeState {
	state := routingRuntimeState{
		strategy:                 coreauth.RoutingStrategyRoundRobin,
		sessionAffinityTTL:       time.Hour,
		sessionAffinitySubagents: true,
		sessionAffinityRebindOn:  coreauth.SessionAffinityRebindOnAny,
	}
	if cfg == nil {
		return state
	}

	// Reuse the shared normalizer so aliases (rr/ff/sf) stay in lockstep with
	// SelectorForRoutingStrategy and the management API.
	if normalized, ok := coreauth.NormalizeRoutingStrategy(cfg.Routing.Strategy); ok {
		state.strategy = normalized
	}
	state.sessionAffinity = cfg.Routing.SessionAffinity
	if ttl := strings.TrimSpace(cfg.Routing.SessionAffinityTTL); ttl != "" {
		if parsed, errParse := time.ParseDuration(ttl); errParse == nil && parsed > 0 {
			if parsed < time.Second {
				parsed = time.Second
			}
			state.sessionAffinityTTL = parsed
		}
	}
	if state.sessionAffinity && cfg.Routing.SessionAffinitySubagents != nil {
		state.sessionAffinitySubagents = *cfg.Routing.SessionAffinitySubagents
	}
	if state.sessionAffinity {
		switch strings.ToLower(strings.TrimSpace(cfg.Routing.SessionAffinityRebindOn)) {
		case coreauth.SessionAffinityRebindOnQuota:
			state.sessionAffinityRebindOn = coreauth.SessionAffinityRebindOnQuota
		case coreauth.SessionAffinityRebindOnNever:
			state.sessionAffinityRebindOn = coreauth.SessionAffinityRebindOnNever
		}
	}
	return state
}

func newRoutingSelector(state routingRuntimeState) coreauth.Selector {
	return newRoutingSelectorWithExisting(state, nil)
}

func newRoutingSelectorWithExisting(state routingRuntimeState, existing coreauth.Selector) coreauth.Selector {
	selector := coreauth.SelectorForRoutingStrategy(state.strategy)
	if state.strategy == coreauth.RoutingStrategyWeightedRoundRobin {
		if existingWeighted := coreauth.UnwrapWeightedRoundRobinSelector(existing); existingWeighted != nil {
			selector = existingWeighted
		}
	}
	if state.sessionAffinity {
		subagents := state.sessionAffinitySubagents
		selector = coreauth.NewSessionAffinitySelectorWithConfig(coreauth.SessionAffinityConfig{
			Fallback:         selector,
			TTL:              state.sessionAffinityTTL,
			SubagentAffinity: &subagents,
			RebindOn:         state.sessionAffinityRebindOn,
		})
	}
	return selector
}

func (s *Service) applyConfigUpdateWithAuthSynthesis(ctx context.Context, newCfg *config.Config, synthesizeConfigAuths bool) bool {
	commit := s.commitConfigUpdate(newCfg)
	if commit.cfg == nil {
		return false
	}
	return s.applyConfigRuntime(ctx, commit, synthesizeConfigAuths)
}

// commitConfigUpdate applies only in-memory configuration state. Runtime work that
// may block on plugins, storage, or networking is deliberately deferred. Catalog
// source generations change here so an older commit cannot restart stale readers.
func (s *Service) commitConfigUpdate(newCfg *config.Config) configCommit {
	if s == nil {
		return configCommit{}
	}

	s.configUpdateMu.Lock()
	defer s.configUpdateMu.Unlock()

	if newCfg == nil {
		s.cfgMu.RLock()
		newCfg = s.cfg
		s.cfgMu.RUnlock()
	}
	if newCfg == nil {
		return configCommit{}
	}
	if errValidate := newCfg.ValidateCredentialWeights(); errValidate != nil {
		log.WithError(errValidate).Warn("rejected config update with invalid credential weights")
		return configCommit{}
	}

	if errValidate := newCfg.Models.Validate(); errValidate != nil {
		log.WithError(errValidate).Warn("rejected invalid model catalog sources")
		return configCommit{}
	}
	// Clone before assigning the generation: callers may still hold the input snapshot.
	newCfg = newCfg.CloneForRuntime()
	helps.AdvanceProxyPoolGeneration(s.proxyPoolPublisher, newCfg)
	s.cfgMu.Lock()
	s.cfg = newCfg
	s.cfgMu.Unlock()
	s.cancelStaleAntigravityProbes("")
	s.configSequence++
	registry.UpdateModelCatalogSources(newCfg.Models, newCfg.Home.Enabled)
	return configCommit{cfg: newCfg, sequence: s.configSequence}
}

func (s *Service) configCommitCurrent(commit configCommit) bool {
	if s == nil || commit.sequence == 0 {
		return false
	}
	s.configUpdateMu.Lock()
	current := s.configSequence == commit.sequence
	s.configUpdateMu.Unlock()
	return current
}

func (s *Service) applyConfigRuntime(ctx context.Context, commit configCommit, synthesizeConfigAuths bool) bool {
	cfg := commit.cfg
	if s == nil || cfg == nil {
		return false
	}
	s.configRuntimeMu.Lock()
	defer s.configRuntimeMu.Unlock()
	if !s.configCommitCurrent(commit) {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return false
	}

	if !s.applyManagerConfig(ctx, commit) {
		return false
	}
	if errContext := ctx.Err(); errContext != nil {
		return false
	}
	if !s.applyPprofConfigContext(ctx, cfg) {
		return false
	}
	s.applyDiscoveryConfigContext(ctx, cfg)
	if s.kvStoreManaged.Load() {
		s.applyKVStoreConfig(cfg)
	}
	if errContext := ctx.Err(); errContext != nil {
		return false
	}
	if !s.updateServerClientsContext(ctx, cfg) {
		return false
	}
	if errContext := ctx.Err(); errContext != nil {
		return false
	}

	registrationCtx := coreauth.WithSkipPersist(ctx)
	s.syncPluginRuntimeConfigForConfig(registrationCtx, cfg)
	if errContext := ctx.Err(); errContext != nil {
		return false
	}
	currentWatcher := s.currentWatcher()
	progressiveFullScanPending := !synthesizeConfigAuths && s.progressiveFileAuth && currentWatcher != nil &&
		currentWatcher.AuthLoadStatus().State == watcher.AuthLoadStateLoading
	var auths []*coreauth.Auth
	if s.coreManager != nil {
		auths = s.coreManager.List()
	}
	s.registerAvailableExecutors(registrationCtx, executorRegistrationOptions{
		includeBaseline:   cfg.Home.Enabled,
		forceReplaceAuths: true,
		auths:             auths,
	})
	if errContext := ctx.Err(); errContext != nil {
		return false
	}
	if synthesizeConfigAuths {
		s.registerConfigAPIKeyAuths(registrationCtx, cfg)
	}
	if errContext := ctx.Err(); errContext != nil {
		return false
	}
	if !progressiveFullScanPending && s.coreManager != nil && !cfg.Home.Enabled && cfg.SaveCooldownStatus {
		if errRestoreCooldown := s.coreManager.RestoreCooldownStates(registrationCtx); errRestoreCooldown != nil && ctx.Err() == nil {
			log.Warnf("failed to restore cooldown state after config update: %v", errRestoreCooldown)
		}
	}
	if errContext := ctx.Err(); errContext != nil {
		return false
	}
	if !progressiveFullScanPending {
		s.syncPluginModelRuntime(registrationCtx)
	}
	if ctx.Err() == nil && s.hooks.OnConfigApplied != nil {
		s.hooks.OnConfigApplied(s, cfg)
	}
	return ctx.Err() == nil
}

func (s *Service) applyManagerConfig(ctx context.Context, commit configCommit) bool {
	if s == nil || s.coreManager == nil || commit.cfg == nil {
		return s != nil && commit.cfg != nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return false
	}
	routingState := normalizedRoutingRuntimeState(commit.cfg)
	if s.appliedRoutingState == nil || *s.appliedRoutingState != routingState {
		s.coreManager.SetSelector(newRoutingSelectorWithExisting(routingState, s.coreManager.Selector()))
		s.appliedRoutingState = &routingState
	}
	s.applyRetryConfig(commit.cfg)
	store := s.resolveCooldownStateStore(commit.cfg)
	if !s.coreManager.ApplyConfigWithCooldownStateStore(ctx, commit.cfg, store) {
		return false
	}
	s.coreManager.SetOAuthModelAlias(commit.cfg.OAuthModelAlias)
	return true
}

func (s *Service) updateServerClientsContext(ctx context.Context, cfg *config.Config) bool {
	if s == nil || cfg == nil || (ctx != nil && ctx.Err() != nil) {
		return false
	}
	if s.updateServerClientsContextFn != nil {
		return s.updateServerClientsContextFn(ctx, cfg)
	}
	if s.server == nil {
		return true
	}
	return s.server.UpdateClientsContext(ctx, cfg)
}

func (s *Service) reloadConfigFromWatcher() bool {
	w := s.currentWatcher()
	if w == nil {
		return false
	}
	return w.ReloadConfigIfChanged()
}

func (s *Service) registerConfigAPIKeyAuths(ctx context.Context, cfg *config.Config) {
	if s == nil || s.coreManager == nil || cfg == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	configSynth := synthesizer.NewConfigSynthesizer()
	auths, errSynthesize := configSynth.Synthesize(&synthesizer.SynthesisContext{
		Config:      cfg,
		Now:         time.Now(),
		IDGenerator: synthesizer.NewStableIDGenerator(),
	})
	if errSynthesize != nil {
		log.Warnf("failed to synthesize config API key auths: %v", errSynthesize)
		return
	}

	registrationCtx := coreauth.WithDeferredAPIKeyModelAliasRebuild(ctx)
	tasks := make([]modelRegistrationTask, 0, len(auths))
	needsAliasRebuild := false
	for _, auth := range auths {
		if !coreauth.IsConfigAPIKeyAuth(auth) {
			continue
		}
		prepared := s.prepareCoreAuthForModelRegistration(registrationCtx, auth)
		if prepared == nil {
			continue
		}
		needsAliasRebuild = true
		authForRegistration := prepared
		tasks = append(tasks, modelRegistrationTask{
			phase:    modelRegistrationPhaseConfigAPIKey,
			category: modelRegistrationCategory(authForRegistration),
			run: func(compatCache *openAICompatibilityRegistrationCache) {
				s.completeModelRegistrationForAuthWithCache(registrationCtx, authForRegistration, compatCache)
			},
		})
	}
	if needsAliasRebuild {
		s.coreManager.RefreshAPIKeyModelAlias()
	}
	s.runModelRegistrationTasks(registrationCtx, tasks)
}

func (s *Service) beforeProgressiveAuthLoad(ctx context.Context) error {
	cfg := s.currentConfig()
	if s == nil || s.coreManager == nil || cfg == nil {
		return nil
	}
	if cfg.SaveCooldownStatus {
		if errPrepare := s.coreManager.PrepareCooldownRestore(ctx); errPrepare != nil {
			return fmt.Errorf("prepare cooldown restore: %w", errPrepare)
		}
	}
	s.registerConfigAPIKeyAuths(coreauth.WithSkipPersist(ctx), cfg)
	return nil
}

func (s *Service) afterProgressiveAuthLoad(ctx context.Context) error {
	if s == nil || s.coreManager == nil {
		return nil
	}
	var errComplete error
	if cfg := s.currentConfig(); cfg != nil && cfg.SaveCooldownStatus {
		if errCooldown := s.coreManager.CompleteCooldownRestore(ctx); errCooldown != nil {
			errComplete = fmt.Errorf("complete cooldown restore: %w", errCooldown)
		}
	}
	s.configRuntimeMu.Lock()
	s.syncPluginModelRuntime(ctx)
	if ctx.Err() == nil && s.hooks.OnConfigApplied != nil {
		s.hooks.OnConfigApplied(s, s.currentConfig())
	}
	s.configRuntimeMu.Unlock()
	s.startCoreAuthAutoRefresh(ctx)
	return errComplete
}

func (s *Service) startCoreAuthAutoRefresh(ctx context.Context) {
	if s == nil || s.coreManager == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.autoRefreshOnce.Do(func() {
		interval := 15 * time.Minute
		s.coreManager.StartAutoRefresh(ctx, interval)
		log.Infof("core auth auto-refresh started (interval=%s)", interval)
	})
}

func (s *Service) setupFileWatcher(ctx context.Context) (*WatcherWrapper, error) {
	cfg := s.currentConfig()
	if s == nil || cfg == nil || s.watcherFactory == nil {
		return nil, fmt.Errorf("create watcher: service is not configured")
	}
	watcherWrapper, errCreate := s.watcherFactory(s.configPath, cfg.AuthDir, func(newCfg *config.Config) {
		s.applyWatcherConfigUpdate(newCfg)
	})
	if errCreate != nil {
		return nil, fmt.Errorf("create watcher: %w", errCreate)
	}
	s.setWatcher(watcherWrapper)
	s.ensureAuthUpdateQueue(ctx)
	watcherWrapper.SetAuthUpdateQueue(s.authUpdates)
	watcherWrapper.SetConfig(s.currentConfig())
	s.registerPluginAuthParser()
	watcherWrapper.SetFileAuthLoadingEnabled(s.progressiveFileAuth)
	if s.progressiveFileAuth {
		watcherWrapper.SetAuthLoadHooks(watcher.AuthLoadHooks{
			Before: s.beforeProgressiveAuthLoad,
			After:  s.afterProgressiveAuthLoad,
		})
	}
	watcherCtx, watcherCancel := context.WithCancel(ctx)
	s.cfgMu.Lock()
	s.watcherCancel = watcherCancel
	s.cfgMu.Unlock()
	if errStart := watcherWrapper.Start(watcherCtx); errStart != nil {
		watcherCancel()
		return nil, fmt.Errorf("start watcher: %w", errStart)
	}
	return watcherWrapper, nil
}

func forceHomeRuntimeConfig(cfg *config.Config) {
	if cfg == nil {
		return
	}
	cfg.APIKeys = nil
	cfg.UsageStatisticsEnabled = true
	cfg.DisableCooling = true
	cfg.SaveCooldownStatus = false
	cfg.WebsocketAuth = true
	cfg.RemoteManagement.AllowRemote = false
	cfg.RemoteManagement.DisableControlPanel = true
	cfg.Plugins.StoreAuth = nil
	cfg.KVStore.Disabled = true
}
