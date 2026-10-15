package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

type testConfigPersister struct {
	calls   int
	persist func() error
}

func (p *testConfigPersister) PersistConfig(context.Context) error {
	p.calls++
	if p.persist != nil {
		return p.persist()
	}
	return nil
}

func TestBootstrapGitBackedConfigPreparesSecretBeforePersisting(t *testing.T) {
	for _, layout := range []string{"management", "remote-management"} {
		t.Run(layout, func(t *testing.T) {
			dir := t.TempDir()
			examplePath := filepath.Join(dir, "config.example.yaml")
			configPath := filepath.Join(dir, "config.yaml")
			const secret = "bootstrap-plaintext-secret"
			if err := os.WriteFile(examplePath, []byte(layout+":\n  secret-key: "+secret+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			persister := &testConfigPersister{persist: func() error {
				data, err := os.ReadFile(configPath)
				if err != nil {
					return err
				}
				var doc map[string]map[string]string
				if err := yaml.Unmarshal(data, &doc); err != nil {
					return err
				}
				if strings.Contains(string(data), secret) {
					t.Fatal("plaintext secret reached config persistence")
				}
				return bcrypt.CompareHashAndPassword([]byte(doc[layout]["secret-key"]), []byte(secret))
			}}
			if err := bootstrapGitBackedConfig(context.Background(), examplePath, configPath, persister); err != nil {
				t.Fatal(err)
			}
			if persister.calls != 1 {
				t.Fatalf("PersistConfig calls = %d, want 1", persister.calls)
			}
		})
	}
}

type testRoundTripFunc func(*http.Request) (*http.Response, error)

func (f testRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestBootstrapGitBackedConfigUsesRemoteTemplateWhenLocalTemplateMissing(t *testing.T) {
	const fallbackURL = "https://raw.githubusercontent.com/caidaoli/CLIProxyAPI/refs/heads/main/config.example.yaml"
	const remoteConfig = "port: 8317\n"

	oldTransport := http.DefaultTransport
	t.Cleanup(func() {
		http.DefaultTransport = oldTransport
	})

	requests := 0
	http.DefaultTransport = testRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests++
		if req.URL.String() != fallbackURL {
			return nil, fmt.Errorf("unexpected fallback URL: %s", req.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(remoteConfig)),
			Request:    req,
		}, nil
	})

	tmpDir := t.TempDir()
	persister := &testConfigPersister{}
	configPath := filepath.Join(tmpDir, "gitstore", "config", "config.yaml")
	examplePath := filepath.Join(tmpDir, "config.example.yaml")

	if err := bootstrapGitBackedConfig(context.Background(), examplePath, configPath, persister); err != nil {
		t.Fatalf("bootstrapGitBackedConfig() error = %v", err)
	}
	if requests != 1 {
		t.Fatalf("fallback request count = %d, want 1", requests)
	}
	if persister.calls != 1 {
		t.Fatalf("PersistConfig calls = %d, want 1", persister.calls)
	}
	got, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read config: %v", err)
	}
	if string(got) != remoteConfig {
		t.Fatalf("config content = %q, want %q", string(got), remoteConfig)
	}
}
