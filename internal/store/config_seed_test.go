package store

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/kvstore"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

const seedTestPlaintextSecret = "seed-plaintext-secret"

// seedTestConfigsKeepingPlaintext hold a plaintext secret that cannot be removed before
// seeding: the file cannot be parsed, or a YAML anchor keeps the plaintext after the
// referencing key is hashed or dropped as a conflicting legacy field.
var seedTestConfigsKeepingPlaintext = map[string]string{
	"unparsable":      "remote-management:\n  secret-key: " + seedTestPlaintextSecret + "\nport: [8317\n",
	"anchored secret": "defaults: &management\n  secret-key: " + seedTestPlaintextSecret + "\nmanagement: *management\n",
	"anchored legacy secret beside v8 secret": "defaults: &legacy\n  secret-key: " + seedTestPlaintextSecret + "\nremote-management: *legacy\nmanagement:\n  secret-key: current-plaintext-secret\n",
}

var seedTestDriverID atomic.Uint64

func seedTestPlaintextConfig(section string) string {
	return "# operator note\nport: 8317\n" + section + ":\n  secret-key: " + seedTestPlaintextSecret + "\n"
}

// seedTestSettledConfigs hold no plaintext secret, so seeding must upload them unchanged.
func seedTestSettledConfigs(t *testing.T) map[string]string {
	t.Helper()
	hashed, errHash := bcrypt.GenerateFromPassword([]byte(seedTestPlaintextSecret), bcrypt.MinCost)
	if errHash != nil {
		t.Fatalf("bcrypt.GenerateFromPassword() error = %v", errHash)
	}
	return map[string]string{
		"hashed secret": "# operator note\nport: 8317\nremote-management:\n  secret-key: '" + string(hashed) + "'\n",
		"null secret":   "# operator note\nport: 8317\nmanagement:\n  secret-key: null\n",
	}
}

func writeSeedTestFile(t *testing.T, path, content string) {
	t.Helper()
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		t.Fatalf("create directory: %v", errMkdir)
	}
	if errWrite := os.WriteFile(path, []byte(content), 0o600); errWrite != nil {
		t.Fatalf("write %s: %v", path, errWrite)
	}
}

func readSeedTestFile(t *testing.T, path string) []byte {
	t.Helper()
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read %s: %v", path, errRead)
	}
	return data
}

// assertSeededWithHashedSecret checks that a seeded config carries the bcrypt hash of
// the plaintext secret under section and matches the local file, because later starts
// overwrite the local file with the remote copy.
func assertSeededWithHashedSecret(t *testing.T, seeded []byte, configPath, section string) {
	t.Helper()
	if bytes.Contains(seeded, []byte(seedTestPlaintextSecret)) {
		t.Fatalf("remote store seeded with the plaintext management secret:\n%s", seeded)
	}
	var doc map[string]any
	if errParse := yaml.Unmarshal(seeded, &doc); errParse != nil {
		t.Fatalf("parse seeded config: %v", errParse)
	}
	management, _ := doc[section].(map[string]any)
	secret, _ := management["secret-key"].(string)
	if errCompare := bcrypt.CompareHashAndPassword([]byte(secret), []byte(seedTestPlaintextSecret)); errCompare != nil {
		t.Fatalf("seeded %s.secret-key %q is not the bcrypt hash of the plaintext secret: %v", section, secret, errCompare)
	}
	if local := readSeedTestFile(t, configPath); !bytes.Equal(local, seeded) {
		t.Fatalf("seeded config differs from the local file:\nremote:\n%s\nlocal:\n%s", seeded, local)
	}
}

// newSeedTestObjectStore returns an object store whose bucket has no config object and
// records every config upload.
func newSeedTestObjectStore(t *testing.T, configPath string) (*ObjectTokenStore, *[][]byte) {
	t.Helper()
	// Every request answers 404, so the bucket holds no config.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	client, errClient := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{
		Creds:  credentials.NewStaticV4("access", "secret", ""),
		Region: "us-east-1",
	})
	if errClient != nil {
		t.Fatalf("minio.New() error = %v", errClient)
	}
	uploads := &[][]byte{}
	objectStore := &ObjectTokenStore{
		client:     client,
		cfg:        ObjectStoreConfig{Bucket: "bucket"},
		configPath: configPath,
		putObjectFn: func(_ context.Context, key string, data []byte, _ string) error {
			if key != objectStoreConfigKey {
				return fmt.Errorf("unexpected object upload %s", key)
			}
			*uploads = append(*uploads, append([]byte(nil), data...))
			return nil
		},
	}
	return objectStore, uploads
}

func TestObjectTokenStoreSeedHashesPlaintextManagementSecret(t *testing.T) {
	dir := t.TempDir()
	examplePath := filepath.Join(dir, "config.example.yaml")
	writeSeedTestFile(t, examplePath, seedTestPlaintextConfig("remote-management"))
	configPath := filepath.Join(dir, "objectstore", "config", "config.yaml")
	objectStore, uploads := newSeedTestObjectStore(t, configPath)

	if errSync := objectStore.syncConfigFromBucket(context.Background(), examplePath); errSync != nil {
		t.Fatalf("syncConfigFromBucket() error = %v", errSync)
	}
	if len(*uploads) != 1 {
		t.Fatalf("config uploads = %d, want 1", len(*uploads))
	}
	assertSeededWithHashedSecret(t, (*uploads)[0], configPath, "remote-management")
}

func TestObjectTokenStoreSeedUploadsSettledConfigUnchanged(t *testing.T) {
	for name, original := range seedTestSettledConfigs(t) {
		t.Run(name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			writeSeedTestFile(t, configPath, original)
			objectStore, uploads := newSeedTestObjectStore(t, configPath)

			if errSync := objectStore.syncConfigFromBucket(context.Background(), ""); errSync != nil {
				t.Fatalf("syncConfigFromBucket() error = %v", errSync)
			}
			if len(*uploads) != 1 || string((*uploads)[0]) != original {
				t.Fatalf("config uploads = %q, want exactly the original config %q", *uploads, original)
			}
			if local := readSeedTestFile(t, configPath); string(local) != original {
				t.Fatalf("local config rewritten to %q, want it untouched", local)
			}
		})
	}
}

// Bootstrap must fail rather than seed, and keep the local file as it was: a later
// start finds the same file and has to refuse it again.
func TestObjectTokenStoreSeedRefusesConfigThatKeepsPlaintextSecret(t *testing.T) {
	for name, content := range seedTestConfigsKeepingPlaintext {
		t.Run(name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			writeSeedTestFile(t, configPath, content)
			objectStore, uploads := newSeedTestObjectStore(t, configPath)

			for attempt := 1; attempt <= 2; attempt++ {
				if errSync := objectStore.syncConfigFromBucket(context.Background(), ""); errSync == nil {
					t.Fatalf("attempt %d: syncConfigFromBucket() succeeded, want bootstrap to fail", attempt)
				}
			}
			if len(*uploads) != 0 {
				t.Fatalf("uploaded a config that still holds the plaintext secret:\n%s", (*uploads)[0])
			}
			if local := readSeedTestFile(t, configPath); string(local) != content {
				t.Fatalf("local config rewritten to %q, want the original kept", local)
			}
		})
	}
}

// seedTestDatabase is a database/sql driver with an empty config table and
// configurable auth records; it records every config upsert.
type seedTestDatabase struct {
	mu          sync.Mutex
	upserts     []string
	authRecords [][2]string
}

type seedTestConn struct {
	database *seedTestDatabase
}

type seedTestTx struct{}

func (seedTestTx) Commit() error   { return nil }
func (seedTestTx) Rollback() error { return nil }

type seedTestRows struct {
	columns []string
	values  [][]driver.Value
	next    int
}

func (d *seedTestDatabase) Open(string) (driver.Conn, error) {
	return &seedTestConn{database: d}, nil
}

func (d *seedTestDatabase) recordedUpserts() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.upserts...)
}

func (*seedTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (*seedTestConn) Close() error {
	return nil
}

func (*seedTestConn) Begin() (driver.Tx, error) {
	return seedTestTx{}, nil
}

func (c *seedTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(strings.TrimSpace(query), "CREATE TABLE IF NOT EXISTS") || strings.HasPrefix(query, "SELECT pg_advisory_xact_lock(") {
		return driver.RowsAffected(0), nil
	}
	if !strings.Contains(query, "INSERT INTO") || len(args) != 2 {
		return nil, fmt.Errorf("unexpected statement %q", query)
	}
	content, ok := args[1].Value.(string)
	if !ok {
		return nil, errors.New("config content is not a string")
	}
	c.database.mu.Lock()
	c.database.upserts = append(c.database.upserts, content)
	c.database.mu.Unlock()
	return driver.RowsAffected(1), nil
}

func (c *seedTestConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.HasPrefix(query, "SELECT content FROM "):
		return &seedTestRows{columns: []string{"content"}}, nil
	case strings.HasPrefix(query, "SELECT id, content FROM "):
		c.database.mu.Lock()
		defer c.database.mu.Unlock()
		values := make([][]driver.Value, len(c.database.authRecords))
		for i, record := range c.database.authRecords {
			values[i] = []driver.Value{record[0], record[1]}
		}
		return &seedTestRows{columns: []string{"id", "content"}, values: values}, nil
	default:
		return nil, fmt.Errorf("unexpected query %q", query)
	}
}

func (r *seedTestRows) Columns() []string {
	return r.columns
}

func (*seedTestRows) Close() error {
	return nil
}

func (r *seedTestRows) Next(dest []driver.Value) error {
	if r.next == len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.next])
	r.next++
	return nil
}

func newSeedTestPostgresStore(t *testing.T, configPath string) (*PostgresStore, *seedTestDatabase) {
	t.Helper()
	database := &seedTestDatabase{}
	driverName := fmt.Sprintf("cliproxy_postgres_config_seed_test_%d", seedTestDriverID.Add(1))
	sql.Register(driverName, database)
	db, errOpen := sql.Open(driverName, "")
	if errOpen != nil {
		t.Fatalf("sql.Open() error = %v", errOpen)
	}
	t.Cleanup(func() {
		if errClose := db.Close(); errClose != nil {
			t.Errorf("db.Close() error = %v", errClose)
		}
	})
	postgresStore := &PostgresStore{
		db:         db,
		cfg:        PostgresStoreConfig{ConfigTable: defaultConfigTable},
		configPath: configPath,
	}
	return postgresStore, database
}

func TestPostgresStoreSeedHashesPlaintextManagementSecret(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "pgstore", "config", "config.yaml")
	writeSeedTestFile(t, configPath, seedTestPlaintextConfig("management"))
	postgresStore, database := newSeedTestPostgresStore(t, configPath)

	if errSync := postgresStore.syncConfigFromDatabase(context.Background(), ""); errSync != nil {
		t.Fatalf("syncConfigFromDatabase() error = %v", errSync)
	}
	upserts := database.recordedUpserts()
	if len(upserts) != 1 {
		t.Fatalf("config upserts = %d, want 1", len(upserts))
	}
	assertSeededWithHashedSecret(t, []byte(upserts[0]), configPath, "management")
}

func TestPostgresStoreSeedUploadsSettledConfigUnchanged(t *testing.T) {
	for name, original := range seedTestSettledConfigs(t) {
		t.Run(name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			writeSeedTestFile(t, configPath, original)
			postgresStore, database := newSeedTestPostgresStore(t, configPath)

			if errSync := postgresStore.syncConfigFromDatabase(context.Background(), ""); errSync != nil {
				t.Fatalf("syncConfigFromDatabase() error = %v", errSync)
			}
			if upserts := database.recordedUpserts(); len(upserts) != 1 || upserts[0] != original {
				t.Fatalf("config upserts = %q, want exactly the original config %q", upserts, original)
			}
			if local := readSeedTestFile(t, configPath); string(local) != original {
				t.Fatalf("local config rewritten to %q, want it untouched", local)
			}
		})
	}
}

// Bootstrap must fail rather than seed, and keep the local file as it was: a later
// start finds the same file and has to refuse it again.
func TestPostgresStoreSeedRefusesConfigThatKeepsPlaintextSecret(t *testing.T) {
	for name, content := range seedTestConfigsKeepingPlaintext {
		t.Run(name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			writeSeedTestFile(t, configPath, content)
			postgresStore, database := newSeedTestPostgresStore(t, configPath)

			for attempt := 1; attempt <= 2; attempt++ {
				if errSync := postgresStore.syncConfigFromDatabase(context.Background(), ""); errSync == nil {
					t.Fatalf("attempt %d: syncConfigFromDatabase() succeeded, want bootstrap to fail", attempt)
				}
			}
			if upserts := database.recordedUpserts(); len(upserts) != 0 {
				t.Fatalf("seeded a config that still holds the plaintext secret:\n%s", upserts[0])
			}
			if local := readSeedTestFile(t, configPath); string(local) != content {
				t.Fatalf("local config rewritten to %q, want the original kept", local)
			}
		})
	}
}

func TestPostgresStoreBootstrapPreservesKVAndReplacesAuthFiles(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config", "config.yaml")
	writeSeedTestFile(t, configPath, "port: 8317\n")
	postgresStore, database := newSeedTestPostgresStore(t, configPath)
	postgresStore.cfg.AuthTable = defaultAuthTable
	postgresStore.cfg.CooldownTable = defaultCooldownTable
	postgresStore.authDir = filepath.Join(dir, "auths")
	writeSeedTestFile(t, filepath.Join(postgresStore.AuthDir(), "active.json"), `{"type":"codex","access_token":"old"}`)
	preserved := map[string]string{
		"notes.txt":              "local notes",
		"nested/cache.bin":       "local state",
		"archive.json/notes.txt": "directory with a JSON suffix",
	}
	for name, content := range preserved {
		writeSeedTestFile(t, filepath.Join(postgresStore.AuthDir(), name), content)
	}

	kvPath := filepath.Join(postgresStore.AuthDir(), "kv.db")
	func() {
		kvStore, errOpen := kvstore.OpenBoltStore(kvPath)
		if errOpen != nil {
			t.Fatalf("OpenBoltStore() error = %v", errOpen)
		}
		defer func() {
			if errClose := kvStore.Close(); errClose != nil {
				t.Errorf("kvStore.Close() error = %v", errClose)
			}
		}()
		if written, errSet := kvStore.KVSet(context.Background(), "session-date-pin", []byte("2026-11-04"), kvstore.KVSetOptions{}); errSet != nil || !written {
			t.Fatalf("KVSet() = %v, %v; want true, nil", written, errSet)
		}
	}()

	for attempt := 1; attempt <= 2; attempt++ {
		for _, name := range []string{"stale.json", "nested/stale.JSON"} {
			writeSeedTestFile(t, filepath.Join(postgresStore.AuthDir(), name), `{"type":"codex"}`)
		}
		payload := fmt.Sprintf(`{"type":"codex","access_token":"fresh-%d"}`, attempt)
		database.mu.Lock()
		database.authRecords = [][2]string{{"active.json", payload}}
		if attempt == 1 {
			database.authRecords = append(database.authRecords, [2]string{"nested/active.JSON", payload})
		}
		database.mu.Unlock()

		if errBootstrap := postgresStore.Bootstrap(context.Background(), ""); errBootstrap != nil {
			t.Fatalf("attempt %d: Bootstrap() error = %v", attempt, errBootstrap)
		}
		if got := string(readSeedTestFile(t, filepath.Join(postgresStore.AuthDir(), "active.json"))); got != payload {
			t.Fatalf("attempt %d: mirrored auth = %q, want %q", attempt, got, payload)
		}
		for _, name := range []string{"stale.json", "nested/stale.JSON"} {
			if _, errStat := os.Stat(filepath.Join(postgresStore.AuthDir(), name)); !errors.Is(errStat, os.ErrNotExist) {
				t.Fatalf("attempt %d: stale auth %s still exists (err=%v)", attempt, name, errStat)
			}
		}
		nestedAuth := filepath.Join(postgresStore.AuthDir(), "nested", "active.JSON")
		if attempt == 1 {
			if got := string(readSeedTestFile(t, nestedAuth)); got != payload {
				t.Fatalf("mirrored nested auth = %q, want %q", got, payload)
			}
		} else if _, errStat := os.Stat(nestedAuth); !errors.Is(errStat, os.ErrNotExist) {
			t.Fatalf("deleted database auth still exists locally (err=%v)", errStat)
		}
		func() {
			kvStore, errOpen := kvstore.OpenBoltStore(kvPath)
			if errOpen != nil {
				t.Fatalf("attempt %d: reopen KV store: %v", attempt, errOpen)
			}
			defer func() {
				if errClose := kvStore.Close(); errClose != nil {
					t.Errorf("kvStore.Close() error = %v", errClose)
				}
			}()
			if got, found, errGet := kvStore.KVGet(context.Background(), "session-date-pin"); errGet != nil || !found || string(got) != "2026-11-04" {
				t.Fatalf("attempt %d: persisted KV = %q, %v, %v; want retained session date", attempt, got, found, errGet)
			}
		}()
		for name, content := range preserved {
			if got := string(readSeedTestFile(t, filepath.Join(postgresStore.AuthDir(), name))); got != content {
				t.Fatalf("attempt %d: local file %s = %q, want %q", attempt, name, got, content)
			}
		}
	}

	t.Run("directory symlink", func(t *testing.T) {
		outsideDir := t.TempDir()
		outsideAuth := filepath.Join(outsideDir, "active.json")
		const outsideContent = `{"type":"codex","access_token":"outside"}`
		writeSeedTestFile(t, outsideAuth, outsideContent)
		linkPath := filepath.Join(postgresStore.AuthDir(), "linked")
		if errSymlink := os.Symlink(outsideDir, linkPath); errSymlink != nil {
			if runtime.GOOS == "windows" {
				t.Skipf("symlinks unavailable: %v", errSymlink)
			}
			t.Fatalf("os.Symlink() error = %v", errSymlink)
		}
		const mirroredContent = `{"type":"codex","access_token":"mirrored"}`
		database.mu.Lock()
		database.authRecords = [][2]string{{"linked/active.json", mirroredContent}}
		database.mu.Unlock()

		if errBootstrap := postgresStore.Bootstrap(context.Background(), ""); errBootstrap != nil {
			t.Fatalf("Bootstrap() error = %v", errBootstrap)
		}
		if got := string(readSeedTestFile(t, outsideAuth)); got != outsideContent {
			t.Fatalf("external auth was overwritten: got %q, want %q", got, outsideContent)
		}
		if got := string(readSeedTestFile(t, filepath.Join(linkPath, "active.json"))); got != mirroredContent {
			t.Fatalf("mirrored auth = %q, want %q", got, mirroredContent)
		}
	})
}
