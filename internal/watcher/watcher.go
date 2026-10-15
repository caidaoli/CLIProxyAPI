// Package watcher watches config/auth files and triggers hot reloads.
// It supports cross-platform fsnotify event handling.
package watcher

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher/synthesizer"
	"gopkg.in/yaml.v3"

	sdkAuth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// storePersister captures persistence-capable token store methods used by the watcher.
type storePersister interface {
	PersistConfig(ctx context.Context) error
	PersistAuthFiles(ctx context.Context, message string, paths ...string) error
}

type authDirProvider interface {
	AuthDir() string
}

// Watcher manages file watching for configuration and authentication files
type Watcher struct {
	configPath             string
	authDir                string
	config                 *config.Config
	clientsMutex           sync.RWMutex
	authRescanMu           sync.Mutex
	configReloadMu         sync.Mutex
	configReloadTimer      *time.Timer
	configApplyMu          sync.Mutex // Serializes config file reloads so they apply in file order.
	serverUpdateMu         sync.Mutex
	serverUpdateTimer      *time.Timer
	serverUpdateLast       time.Time
	serverUpdatePend       bool
	stopped                atomic.Bool
	reloadCallback         func(*config.Config)
	watcher                *fsnotify.Watcher
	lastAuthHashes         map[string]string
	lastAuthContents       map[string]*coreauth.Auth
	fileAuthsByPath        map[string]map[string]*coreauth.Auth
	lastRemoveTimes        map[string]time.Time
	lastConfigHash         string
	authQueue              chan<- AuthUpdateBatch
	currentAuths           map[string]*coreauth.Auth
	authRevisions          map[string]uint64 // Includes deletion tombstones; guarded by clientsMutex.
	fileObservations       map[string]uint64 // Tracks file events even when content is unchanged.
	activeAuthScans        int               // Guarded by clientsMutex.
	runtimeAuths           map[string]*coreauth.Auth
	dispatchMu             sync.Mutex
	dispatchCond           *sync.Cond
	pendingUpdates         map[string]AuthUpdate
	pendingOrder           []string
	dispatchCancel         context.CancelFunc
	storePersister         storePersister
	pluginAuthParser       synthesizer.PluginAuthParser
	mirroredAuthDir        string
	oldConfigYaml          []byte
	authLoadMu             sync.Mutex
	authLoadCancel         context.CancelFunc
	authLoadDone           chan struct{}
	authLoadStatus         atomic.Value
	pathGenerations        map[string]uint64
	nextPathGeneration     uint64
	authLoadSequence       uint64
	authLoadHooks          AuthLoadHooks
	fileAuthLoadingEnabled bool
	lifecycleCtx           context.Context
	authEventGateArmed     bool
	authEventGateOpen      bool
	pendingConfigEvent     bool
}

// AuthUpdateAction represents the type of change detected in auth sources.
type AuthUpdateAction string

const (
	AuthUpdateActionAdd    AuthUpdateAction = "add"
	AuthUpdateActionModify AuthUpdateAction = "modify"
	AuthUpdateActionDelete AuthUpdateAction = "delete"
)

// AuthUpdate describes an incremental change to auth configuration.
type AuthUpdate struct {
	Action AuthUpdateAction
	ID     string
	Auth   *coreauth.Auth
	// ReplaceMaterial means the persisted credential material changed and stale runtime errors must be cleared.
	ReplaceMaterial bool
	revision        uint64 // Watcher-local ordering, independent of runtime auth generations.
	fileRemoval     bool   // The source file was removed, not just an ID from its content.
}

// IsFileRemoval reports whether this delete was observed as a whole-file removal.
func (u AuthUpdate) IsFileRemoval() bool {
	return u.fileRemoval
}

// SetFileRemoval marks a delete as a whole-file removal rather than a per-ID diff.
func (u *AuthUpdate) SetFileRemoval(fileRemoval bool) {
	if u != nil {
		u.fileRemoval = fileRemoval
	}
}

type AuthUpdateResult struct {
	ID     string
	Loaded bool
}

type AuthUpdateBatch struct {
	Updates []AuthUpdate
	Result  chan<- []AuthUpdateResult
	Initial bool
}

type AuthLoadHooks struct {
	Before func(context.Context) error
	After  func(context.Context) error
}

// Revision returns the monotonic watcher revision assigned to this update.
func (u AuthUpdate) Revision() uint64 {
	return u.revision
}

// SetRevision updates the revision counter for this update.
func (u *AuthUpdate) SetRevision(rev uint64) {
	if u != nil {
		u.revision = rev
	}
}

const (
	// replaceCheckDelay is a short delay to allow atomic replace (rename) to settle
	// before deciding whether a Remove event indicates a real deletion.
	replaceCheckDelay        = 50 * time.Millisecond
	configReloadDebounce     = 150 * time.Millisecond
	authRemoveDebounceWindow = 1 * time.Second
	serverUpdateDebounce     = 1 * time.Second
)

// NewWatcher creates a new file watcher instance
func NewWatcher(configPath, authDir string, reloadCallback func(*config.Config)) (*Watcher, error) {
	watcher, errNewWatcher := fsnotify.NewWatcher()
	if errNewWatcher != nil {
		return nil, errNewWatcher
	}
	w := &Watcher{
		configPath:             configPath,
		authDir:                authDir,
		reloadCallback:         reloadCallback,
		watcher:                watcher,
		lastAuthHashes:         make(map[string]string),
		fileAuthsByPath:        make(map[string]map[string]*coreauth.Auth),
		pathGenerations:        make(map[string]uint64),
		fileAuthLoadingEnabled: true,
	}
	w.publishAuthLoadStatus(idleAuthLoadStatus())
	w.dispatchCond = sync.NewCond(&w.dispatchMu)
	if store := sdkAuth.GetTokenStore(); store != nil {
		if persister, ok := store.(storePersister); ok {
			w.storePersister = persister
			log.Debug("persistence-capable token store detected; watcher will propagate persisted changes")
		}
		if provider, ok := store.(authDirProvider); ok {
			if fixed := strings.TrimSpace(provider.AuthDir()); fixed != "" {
				w.mirroredAuthDir = fixed
				log.Debugf("mirrored auth directory locked to %s", fixed)
			}
		}
	}
	return w, nil
}

// Start begins watching the configuration file and authentication directory
func (w *Watcher) Start(ctx context.Context) error {
	return w.start(ctx)
}

// Stop stops the file watcher
func (w *Watcher) Stop() error {
	w.stopped.Store(true)
	w.authLoadMu.Lock()
	if w.authLoadCancel != nil {
		w.authLoadCancel()
	}
	w.authLoadMu.Unlock()
	w.stopDispatch()
	w.stopConfigReloadTimer()
	w.stopServerUpdateTimer()
	return w.watcher.Close()
}

// SetConfig updates the current configuration
func (w *Watcher) SetConfig(cfg *config.Config) {
	cloned := cfg.CloneForRuntime()
	w.clientsMutex.Lock()
	defer w.clientsMutex.Unlock()
	w.config = cloned
	w.oldConfigYaml, _ = yaml.Marshal(cloned)
}

// SetPluginAuthParser updates the plugin auth parser used for file auth synthesis.
func (w *Watcher) SetPluginAuthParser(parser synthesizer.PluginAuthParser) {
	w.clientsMutex.Lock()
	defer w.clientsMutex.Unlock()
	w.pluginAuthParser = parser
}

// SetAuthUpdateQueue sets the queue used to emit auth updates.
func (w *Watcher) SetAuthUpdateQueue(queue chan<- AuthUpdateBatch) {
	w.setAuthUpdateQueue(queue)
}

func (w *Watcher) SetAuthLoadHooks(hooks AuthLoadHooks) {
	if w == nil {
		return
	}
	w.authLoadMu.Lock()
	w.authLoadHooks = hooks
	w.authLoadMu.Unlock()
}

func (w *Watcher) SetFileAuthLoadingEnabled(enabled bool) {
	if w == nil {
		return
	}
	w.clientsMutex.Lock()
	w.fileAuthLoadingEnabled = enabled
	if enabled {
		// Progressive opt-in arms the pre-activation event gate once.
		w.authEventGateArmed = true
	} else {
		// Disabled full scan also bypasses the progressive event gate.
		w.authEventGateArmed = false
	}
	w.clientsMutex.Unlock()
}

func (w *Watcher) fileAuthLoadingIsEnabled() bool {
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if w.authLoadStatus.Load() == nil {
		return true
	}
	return w.fileAuthLoadingEnabled
}

func (w *Watcher) authEventGateBlocks() bool {
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	return w.authEventGateArmed && !w.authEventGateOpen
}

func (w *Watcher) setLifecycleContext(ctx context.Context) {
	w.clientsMutex.Lock()
	w.lifecycleCtx = ctx
	w.clientsMutex.Unlock()
}

func (w *Watcher) authLoadContext() context.Context {
	w.clientsMutex.RLock()
	ctx := w.lifecycleCtx
	w.clientsMutex.RUnlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// activateAuthEventGate opens the progressive event gate and reports whether a
// pre-activation config change must be applied without starting a second scan.
func (w *Watcher) activateAuthEventGate() (applyPendingConfig bool) {
	w.clientsMutex.Lock()
	defer w.clientsMutex.Unlock()
	if !w.authEventGateArmed || w.authEventGateOpen {
		return false
	}
	w.authEventGateOpen = true
	applyPendingConfig = w.pendingConfigEvent
	w.pendingConfigEvent = false
	return applyPendingConfig
}

func (w *Watcher) markPendingConfigEvent() {
	w.clientsMutex.Lock()
	w.pendingConfigEvent = true
	w.clientsMutex.Unlock()
}

func (w *Watcher) advancePathGenerationLocked(path string) uint64 {
	if w.pathGenerations == nil {
		w.pathGenerations = make(map[string]uint64)
	}
	w.nextPathGeneration++
	w.pathGenerations[path] = w.nextPathGeneration
	return w.nextPathGeneration
}

func (w *Watcher) pathGenerationCurrentLocked(path string, generation uint64) bool {
	return w.pathGenerations[path] == generation
}

func (w *Watcher) MarkAuthPathChanged(path string) {
	if w == nil {
		return
	}
	normalized := w.normalizeAuthPath(path)
	if normalized == "" {
		return
	}
	w.clientsMutex.Lock()
	w.advancePathGenerationLocked(normalized)
	w.clientsMutex.Unlock()
}

// DispatchRuntimeAuthUpdate allows external runtime providers (e.g., websocket-driven auths)
// to push auth updates through the same queue used by file/config watchers.
// Returns true if the update was enqueued; false if no queue is configured.
func (w *Watcher) DispatchRuntimeAuthUpdate(update AuthUpdate) bool {
	return w.dispatchRuntimeAuthUpdate(update)
}

// DispatchPersistedAuthUpdate pushes already-persisted file auth updates through the watcher queue.
// Returns true if the update was enqueued; false if no queue is configured.
func (w *Watcher) DispatchPersistedAuthUpdate(update AuthUpdate) bool {
	return w.dispatchPersistedAuthUpdate(update)
}

// DispatchPersistedAuthUpdateWithRevision pushes already-persisted file auth updates through the watcher queue
// and returns the stamped monotonic revision.
func (w *Watcher) DispatchPersistedAuthUpdateWithRevision(update *AuthUpdate) (bool, uint64) {
	return w.dispatchPersistedAuthUpdateWithRevision(update)
}

// SnapshotCoreAuths converts current clients snapshot into core auth entries.
func (w *Watcher) SnapshotCoreAuths() []*coreauth.Auth {
	w.clientsMutex.RLock()
	var cfg *config.Config
	if w.config != nil {
		cfg = w.config.CloneForRuntime()
	}
	fileAuths := cloneFileAuths(w.fileAuthsByPath)
	runtimeAuths := cloneAuthMap(w.runtimeAuths)
	w.clientsMutex.RUnlock()
	ctx := &synthesizer.SynthesisContext{Config: cfg, Now: time.Now(), IDGenerator: synthesizer.NewStableIDGenerator()}
	auths, _ := synthesizer.NewConfigSynthesizer().Synthesize(ctx)
	for _, pathAuths := range fileAuths {
		for _, auth := range pathAuths {
			if auth != nil {
				auths = append(auths, auth.Clone())
			}
		}
	}
	for _, auth := range runtimeAuths {
		if auth != nil {
			auths = append(auths, auth.Clone())
		}
	}
	return auths
}

func cloneFileAuths(source map[string]map[string]*coreauth.Auth) map[string]map[string]*coreauth.Auth {
	cloned := make(map[string]map[string]*coreauth.Auth, len(source))
	for path, auths := range source {
		cloned[path] = cloneAuthMap(auths)
	}
	return cloned
}
