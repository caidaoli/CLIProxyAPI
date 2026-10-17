package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// CooldownStateRecord is a persisted runtime cooldown snapshot for one auth/model pair.
type CooldownStateRecord struct {
	Provider       string     `json:"provider,omitempty"`
	AuthID         string     `json:"auth_id"`
	AuthFile       string     `json:"-"`
	Model          string     `json:"model,omitempty"`
	Status         string     `json:"status,omitempty"`
	NextRetryAfter time.Time  `json:"next_retry_after"`
	Reason         string     `json:"reason,omitempty"`
	Quota          QuotaState `json:"quota,omitempty"`
	LastError      *Error     `json:"last_error,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at"`
	// IndependentState preserves a model's own cooldown beneath an
	// auth-level credential-quota layer so a restart cannot lose the
	// distinction and quota restoration keeps reinstate-exactly semantics.
	IndependentState *ModelState `json:"independent_state,omitempty"`
	// CredentialQuotaPropagated preserves wrap provenance for models that
	// carry no independent layer (e.g. the reporting model itself).
	CredentialQuotaPropagated bool `json:"credential_quota_propagated,omitempty"`
}

// CooldownStateSnapshot replaces persisted cooldown state for one auth.
// An empty Records slice removes persisted state for AuthID.
type CooldownStateSnapshot struct {
	AuthID  string
	Records []CooldownStateRecord
}

// CooldownStateStore persists runtime cooldown state independently from auth tokens.
type CooldownStateStore interface {
	Load(context.Context) ([]CooldownStateSnapshot, error)
	Apply(context.Context, []CooldownStateSnapshot) error
}

// CooldownStateStoreProvider exposes a backend-specific cooldown state store.
type CooldownStateStoreProvider interface {
	CooldownStateStore() CooldownStateStore
}

type cooldownStateFile struct {
	Version   int                   `json:"version"`
	AuthID    string                `json:"auth_id,omitempty"`
	Provider  string                `json:"provider,omitempty"`
	UpdatedAt time.Time             `json:"updated_at"`
	Records   []CooldownStateRecord `json:"records"`
}

// FileCooldownStateStore stores cooldown state as one .cds file per auth.
type FileCooldownStateStore struct {
	mu      sync.Mutex
	dir     string
	authDir string
	paths   map[string]string
}

// NewFileCooldownStateStore creates a file-backed cooldown state store rooted at dir.
func NewFileCooldownStateStore(dir string) *FileCooldownStateStore {
	return NewFileCooldownStateStoreWithAuthDir(dir, "")
}

// NewFileCooldownStateStoreWithAuthDir creates a store and derives per-auth .cds
// paths from auth files relative to authDir when possible.
func NewFileCooldownStateStoreWithAuthDir(dir, authDir string) *FileCooldownStateStore {
	return &FileCooldownStateStore{
		dir:     strings.TrimSpace(dir),
		authDir: strings.TrimSpace(authDir),
		paths:   make(map[string]string),
	}
}

// Load reads all cooldown state files. A missing directory is treated as empty state.
func (s *FileCooldownStateStore) Load(ctx context.Context) ([]CooldownStateSnapshot, error) {
	if s == nil || s.dir == "" {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errCtx := ctx.Err(); errCtx != nil {
		return nil, errCtx
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paths == nil {
		s.paths = make(map[string]string)
	}

	snapshots := make([]CooldownStateSnapshot, 0)
	errWalk := filepath.WalkDir(s.dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if entry == nil || entry.IsDir() {
			return nil
		}
		if !strings.EqualFold(filepath.Ext(entry.Name()), ".cds") {
			return nil
		}
		snapshot, errRead := readCooldownStateFile(ctx, path)
		if errRead != nil {
			return errRead
		}
		if snapshot.AuthID == "" {
			return nil
		}
		s.paths[snapshot.AuthID] = filepath.Clean(path)
		snapshots = append(snapshots, snapshot)
		return nil
	})
	if errWalk != nil {
		if errors.Is(errWalk, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read cooldown state directory: %w", errWalk)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].AuthID < snapshots[j].AuthID
	})
	return snapshots, nil
}

func readCooldownStateFile(ctx context.Context, path string) (CooldownStateSnapshot, error) {
	if errCtx := ctx.Err(); errCtx != nil {
		return CooldownStateSnapshot{}, errCtx
	}
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		if errors.Is(errRead, os.ErrNotExist) {
			return CooldownStateSnapshot{}, nil
		}
		return CooldownStateSnapshot{}, fmt.Errorf("read cooldown state %s: %w", path, errRead)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return CooldownStateSnapshot{}, nil
	}
	var envelope cooldownStateFile
	if errUnmarshal := json.Unmarshal(data, &envelope); errUnmarshal != nil {
		return CooldownStateSnapshot{}, fmt.Errorf("parse cooldown state %s: %w", path, errUnmarshal)
	}
	authID := strings.TrimSpace(envelope.AuthID)
	if authID == "" && len(envelope.Records) > 0 {
		authID = strings.TrimSpace(envelope.Records[0].AuthID)
	}
	return CooldownStateSnapshot{AuthID: authID, Records: envelope.Records}, nil
}

// Apply replaces cooldown state for the auth IDs present in snapshots.
func (s *FileCooldownStateStore) Apply(ctx context.Context, snapshots []CooldownStateSnapshot) error {
	if s == nil || s.dir == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errCtx := ctx.Err(); errCtx != nil {
		return errCtx
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paths == nil {
		s.paths = make(map[string]string)
	}

	type operation struct {
		authID  string
		path    string
		records []CooldownStateRecord
	}
	operations := make([]operation, 0, len(snapshots))
	seen := make(map[string]struct{}, len(snapshots))
	for _, snapshot := range snapshots {
		authID := strings.TrimSpace(snapshot.AuthID)
		if authID == "" {
			return fmt.Errorf("cooldown state: missing auth id")
		}
		if _, ok := seen[authID]; ok {
			return fmt.Errorf("cooldown state: duplicate auth id %s", authID)
		}
		seen[authID] = struct{}{}

		records := append([]CooldownStateRecord(nil), snapshot.Records...)
		if len(records) == 0 {
			operations = append(operations, operation{authID: authID})
			continue
		}
		for i := range records {
			recordAuthID := strings.TrimSpace(records[i].AuthID)
			if recordAuthID == "" {
				records[i].AuthID = authID
				continue
			}
			if recordAuthID != authID {
				return fmt.Errorf("cooldown state: record auth id %s does not match snapshot %s", recordAuthID, authID)
			}
		}
		path, errPath := s.statePath(records[0])
		if errPath != nil {
			return errPath
		}
		operations = append(operations, operation{authID: authID, path: filepath.Clean(path), records: records})
	}

	for _, op := range operations {
		if errCtx := ctx.Err(); errCtx != nil {
			return errCtx
		}
		oldPath := s.paths[op.authID]
		if len(op.records) == 0 {
			if oldPath != "" {
				if errRemove := os.Remove(oldPath); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
					return fmt.Errorf("remove cooldown state %s: %w", oldPath, errRemove)
				}
			}
			delete(s.paths, op.authID)
			continue
		}
		if errSave := writeCooldownStateGroup(ctx, op.path, op.records); errSave != nil {
			return errSave
		}
		if oldPath != "" && oldPath != op.path {
			if errRemove := os.Remove(oldPath); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
				return fmt.Errorf("remove old cooldown state %s: %w", oldPath, errRemove)
			}
		}
		s.paths[op.authID] = op.path
	}
	return nil
}

func writeCooldownStateGroup(ctx context.Context, path string, records []CooldownStateRecord) error {
	if errCtx := ctx.Err(); errCtx != nil {
		return errCtx
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Model < records[j].Model
	})
	envelope := cooldownStateFile{
		Version:   1,
		UpdatedAt: time.Now().UTC(),
		Records:   records,
	}
	if len(records) > 0 {
		envelope.AuthID = records[0].AuthID
		envelope.Provider = records[0].Provider
	}
	data, errMarshal := json.MarshalIndent(envelope, "", "  ")
	if errMarshal != nil {
		return fmt.Errorf("marshal cooldown state: %w", errMarshal)
	}
	data = append(data, '\n')

	dir := filepath.Dir(path)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		return fmt.Errorf("create cooldown state directory: %w", errMkdir)
	}

	tmpFile, errCreate := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if errCreate != nil {
		return fmt.Errorf("create cooldown state temp file: %w", errCreate)
	}
	tmp := tmpFile.Name()
	if _, errWrite := tmpFile.Write(data); errWrite != nil {
		if errClose := tmpFile.Close(); errClose != nil {
			_ = os.Remove(tmp)
			return fmt.Errorf("write cooldown state temp file: %w; close temp file: %v", errWrite, errClose)
		}
		_ = os.Remove(tmp)
		return fmt.Errorf("write cooldown state temp file: %w", errWrite)
	}
	if errClose := tmpFile.Close(); errClose != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close cooldown state temp file: %w", errClose)
	}
	if errRename := os.Rename(tmp, path); errRename != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace cooldown state file: %w", errRename)
	}
	return nil
}

func (s *FileCooldownStateStore) statePath(record CooldownStateRecord) (string, error) {
	rel := s.stateRelativePath(record)
	if rel == "" {
		return "", fmt.Errorf("cooldown state path: missing auth identity")
	}
	return filepath.Join(s.dir, rel), nil
}

func (s *FileCooldownStateStore) stateRelativePath(record CooldownStateRecord) string {
	authFile := strings.TrimSpace(record.AuthFile)
	if authFile != "" {
		if filepath.IsAbs(authFile) && strings.TrimSpace(s.authDir) != "" {
			if rel, errRel := filepath.Rel(s.authDir, authFile); errRel == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && rel != ".." {
				return cdsPathForRel(rel)
			}
		}
		if !filepath.IsAbs(authFile) {
			return cdsPathForRel(authFile)
		}
		return sanitizeCooldownFileName(filepath.Base(authFile))
	}
	return sanitizeCooldownFileName(strings.TrimSpace(record.AuthID))
}

func cdsPathForRel(rel string) string {
	clean := filepath.Clean(filepath.FromSlash(rel))
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return ""
	}
	dir := filepath.Dir(clean)
	base := sanitizeCooldownFileName(filepath.Base(clean))
	if base == "" {
		return ""
	}
	if dir == "." {
		return base
	}
	return filepath.Join(dir, base)
}

var cooldownFileNameUnsafe = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func sanitizeCooldownFileName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	ext := filepath.Ext(name)
	if ext != "" {
		name = strings.TrimSuffix(name, ext)
	}
	name = cooldownFileNameUnsafe.ReplaceAllString(name, "_")
	name = strings.Trim(name, "._-")
	if name == "" {
		return ""
	}
	return name + ".cds"
}

func cooldownAuthFile(auth *Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Attributes != nil {
		if path := strings.TrimSpace(auth.Attributes["path"]); path != "" {
			return path
		}
	}
	if fileName := strings.TrimSpace(auth.FileName); fileName != "" {
		return fileName
	}
	return ""
}
