package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/MoeForge/moego/json"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

var _ cliproxyauth.CooldownStateStore = (*PostgresCooldownStateStore)(nil)
var _ cliproxyauth.CooldownStateStoreProvider = (*PostgresStore)(nil)

// PostgresCooldownStateStore persists runtime cooldown state to PostgreSQL,
// independently from PostgresStore's auth token storage (which owns Save
// for auth records; a separate type avoids a method name collision).
type PostgresCooldownStateStore struct {
	s *PostgresStore
}

// NewPostgresCooldownStateStore wraps an existing PostgresStore to provide
// cooldown state persistence via the cooldown_store table.
// Returns a true interface nil when s is nil, avoiding the non-nil interface
// holding a nil pointer trap.
func NewPostgresCooldownStateStore(s *PostgresStore) cliproxyauth.CooldownStateStore {
	if s == nil {
		return nil
	}
	return &PostgresCooldownStateStore{s: s}
}

// cooldownStateEnvelope is the JSON content stored per-row, grouped by AuthID.
type cooldownStateEnvelope struct {
	Version   int                                `json:"version"`
	AuthID    string                             `json:"auth_id,omitempty"`
	Provider  string                             `json:"provider,omitempty"`
	UpdatedAt time.Time                          `json:"updated_at"`
	Records   []cliproxyauth.CooldownStateRecord `json:"records"`
}

// marshalCooldownEnvelope builds and marshals the JSON envelope for a group
// of records belonging to the same AuthID, sorted by Model for determinism.
func marshalCooldownEnvelope(authID, provider string, records []cliproxyauth.CooldownStateRecord) ([]byte, error) {
	sorted := append([]cliproxyauth.CooldownStateRecord(nil), records...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Model < sorted[j].Model
	})
	env := cooldownStateEnvelope{
		Version:   1,
		AuthID:    authID,
		Provider:  provider,
		UpdatedAt: time.Now().UTC(),
		Records:   sorted,
	}
	return json.Marshal(env)
}

func (c *PostgresCooldownStateStore) tableName() string {
	if c == nil || c.s == nil {
		return ""
	}
	name := c.s.cfg.CooldownTable
	if name == "" {
		name = defaultCooldownTable
	}
	return c.s.fullTableName(name)
}

// Load reads all cooldown rows while preserving their per-auth grouping.
func (c *PostgresCooldownStateStore) Load(ctx context.Context) ([]cliproxyauth.CooldownStateSnapshot, error) {
	if c == nil || c.s == nil || c.s.db == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`SELECT id, content FROM %s`, c.tableName())
	rows, err := c.s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("postgres cooldown: list: %w", err)
	}
	defer func() {
		if errClose := rows.Close(); errClose != nil {
			log.Errorf("postgres cooldown: close rows: %v", errClose)
		}
	}()

	out := make([]cliproxyauth.CooldownStateSnapshot, 0)
	for rows.Next() {
		var id string
		var content []byte
		if err = rows.Scan(&id, &content); err != nil {
			return nil, fmt.Errorf("postgres cooldown: scan: %w", err)
		}
		var env cooldownStateEnvelope
		if err = json.Unmarshal(content, &env); err != nil {
			return nil, fmt.Errorf("postgres cooldown: parse %s: %w", id, err)
		}
		records := append([]cliproxyauth.CooldownStateRecord(nil), env.Records...)
		for i := range records {
			records[i].AuthID = id
		}
		out = append(out, cliproxyauth.CooldownStateSnapshot{AuthID: id, Records: records})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres cooldown: iterate: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AuthID < out[j].AuthID
	})
	return out, nil
}

// Apply replaces cooldown state only for auth IDs present in snapshots.
func (c *PostgresCooldownStateStore) Apply(ctx context.Context, snapshots []cliproxyauth.CooldownStateSnapshot) (err error) {
	if c == nil || c.s == nil || c.s.db == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	table := c.tableName()
	type upsert struct {
		authID  string
		payload []byte
	}
	upserts := make([]upsert, 0, len(snapshots))
	deletes := make([]string, 0, len(snapshots))
	seen := make(map[string]struct{}, len(snapshots))
	for _, snapshot := range snapshots {
		authID := strings.TrimSpace(snapshot.AuthID)
		if authID == "" {
			return fmt.Errorf("postgres cooldown: missing auth id")
		}
		if _, ok := seen[authID]; ok {
			return fmt.Errorf("postgres cooldown: duplicate auth id %s", authID)
		}
		seen[authID] = struct{}{}
		if len(snapshot.Records) == 0 {
			deletes = append(deletes, authID)
			continue
		}

		records := append([]cliproxyauth.CooldownStateRecord(nil), snapshot.Records...)
		for i := range records {
			recordAuthID := strings.TrimSpace(records[i].AuthID)
			if recordAuthID == "" {
				records[i].AuthID = authID
				continue
			}
			if recordAuthID != authID {
				return fmt.Errorf("postgres cooldown: record auth id %s does not match snapshot %s", recordAuthID, authID)
			}
		}
		provider := strings.TrimSpace(records[0].Provider)
		payload, errMarshal := marshalCooldownEnvelope(authID, provider, records)
		if errMarshal != nil {
			return fmt.Errorf("postgres cooldown: marshal %s: %w", authID, errMarshal)
		}
		upserts = append(upserts, upsert{authID: authID, payload: payload})
	}
	if len(upserts) == 0 && len(deletes) == 0 {
		return nil
	}

	tx, err := c.s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("postgres cooldown: begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			if errRollback := tx.Rollback(); errRollback != nil && !errors.Is(errRollback, sql.ErrTxDone) {
				err = fmt.Errorf("%w; rollback transaction: %v", err, errRollback)
			}
		}
	}()

	upsertQuery := fmt.Sprintf(`
			INSERT INTO %s (id, content, created_at, updated_at)
			VALUES ($1, $2, NOW(), NOW())
			ON CONFLICT (id)
			DO UPDATE SET content = EXCLUDED.content, updated_at = NOW()
		`, table)
	for _, item := range upserts {
		if _, err = tx.ExecContext(ctx, upsertQuery, item.authID, json.RawMessage(item.payload)); err != nil {
			return fmt.Errorf("postgres cooldown: upsert %s: %w", item.authID, err)
		}
	}

	deleteQuery := fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, table)
	for _, authID := range deletes {
		if _, err = tx.ExecContext(ctx, deleteQuery, authID); err != nil {
			return fmt.Errorf("postgres cooldown: delete %s: %w", authID, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("postgres cooldown: commit: %w", err)
	}
	return nil
}
