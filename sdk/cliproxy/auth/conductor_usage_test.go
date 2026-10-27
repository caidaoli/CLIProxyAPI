package auth

import (
	"context"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

type usageModeExecutor struct {
	mode  bool
	known bool
}

func (e *usageModeExecutor) Identifier() string { return "usage-mode" }

func (e *usageModeExecutor) capture(ctx context.Context) {
	e.mode = coreusage.StreamFromContext(ctx)
	e.known = true
}

func (e *usageModeExecutor) Execute(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.capture(ctx)
	return cliproxyexecutor.Response{}, nil
}

func (e *usageModeExecutor) ExecuteStream(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.capture(ctx)
	chunks := make(chan cliproxyexecutor.StreamChunk)
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *usageModeExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }

func (e *usageModeExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *usageModeExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func TestContextWithRequestedModelAliasIncludesStream(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "stream_false", true: "stream_true"}[stream], func(t *testing.T) {
			ctx := contextWithRequestedModelAlias(context.Background(), cliproxyexecutor.Options{
				Stream: stream,
			}, "fallback-model")

			if got := coreusage.StreamFromContext(ctx); got != stream {
				t.Fatalf("stream = %v, want %v", got, stream)
			}
		})
	}
}

func TestContextWithRequestedModelAliasIncludesReasoningEffort(t *testing.T) {
	ctx := contextWithRequestedModelAlias(context.Background(), cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.RequestedModelMetadataKey:  "client-model",
			cliproxyexecutor.ReasoningEffortMetadataKey: "medium",
			cliproxyexecutor.ServiceTierMetadataKey:     "auto",
			cliproxyexecutor.GenerateMetadataKey:        false,
		},
	}, "fallback-model")

	if got := coreusage.RequestedModelAliasFromContext(ctx); got != "client-model" {
		t.Fatalf("requested model alias = %q, want %q", got, "client-model")
	}
	if got := coreusage.ReasoningEffortFromContext(ctx); got != "medium" {
		t.Fatalf("reasoning effort = %q, want %q", got, "medium")
	}
	gotServiceTier := coreusage.ServiceTierFromContext(ctx)
	if gotServiceTier != "auto" {
		t.Fatalf("service tier = %q, want %q", gotServiceTier, "auto")
	}
	if got := coreusage.GenerateFromContext(ctx); got {
		t.Fatalf("generate = %v, want false", got)
	}
}

func TestContextWithRequestedModelAliasDefaultsGenerateTrue(t *testing.T) {
	ctx := contextWithRequestedModelAlias(context.Background(), cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.RequestedModelMetadataKey: "client-model",
		},
	}, "fallback-model")

	if got := coreusage.GenerateFromContext(ctx); !got {
		t.Fatalf("generate = %v, want true", got)
	}
}

func TestContextWithRequestedModelAliasPreservesExistingGenerateFalse(t *testing.T) {
	ctx := coreusage.WithGenerate(context.Background(), false)
	ctx = contextWithRequestedModelAlias(ctx, cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.RequestedModelMetadataKey: "client-model",
		},
	}, "fallback-model")

	if got := coreusage.GenerateFromContext(ctx); got {
		t.Fatalf("generate = %v, want false", got)
	}
}

func TestManagerExecutionMarksUsageStreamMode(t *testing.T) {
	tests := []struct {
		name   string
		stream bool
	}{
		{name: "non-stream", stream: false},
		{name: "stream", stream: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const model = "test-model"
			manager := NewManager(nil, nil, nil)
			executor := &usageModeExecutor{}
			manager.RegisterExecutor(executor)
			auth := &Auth{ID: "usage-mode-auth-" + tt.name, Provider: executor.Identifier(), Status: StatusActive}
			if _, err := manager.Register(context.Background(), auth); err != nil {
				t.Fatalf("register auth: %v", err)
			}
			reg := registry.GetGlobalRegistry()
			reg.RegisterClient(auth.ID, executor.Identifier(), []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
			manager.RefreshSchedulerEntry(auth.ID)

			req := cliproxyexecutor.Request{Model: model}
			if tt.stream {
				if _, err := manager.ExecuteStream(context.Background(), []string{executor.Identifier()}, req, cliproxyexecutor.Options{Stream: true}); err != nil {
					t.Fatalf("ExecuteStream: %v", err)
				}
			} else if _, err := manager.Execute(context.Background(), []string{executor.Identifier()}, req, cliproxyexecutor.Options{}); err != nil {
				t.Fatalf("Execute: %v", err)
			}

			if !executor.known || executor.mode != tt.stream {
				t.Fatalf("stream mode = (%v, known=%v), want (%v, known=true)", executor.mode, executor.known, tt.stream)
			}
		})
	}
}

func TestContextWithRequestedModelAliasPropagatesAndClearsSelectionID(t *testing.T) {
	ctx := contextWithRequestedModelAlias(context.Background(), cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.SelectionIDMetadataKey: "sel-123",
		},
	}, "fallback-model")
	if got := coreusage.SelectionIDFromContext(ctx); got != "sel-123" {
		t.Fatalf("SelectionIDFromContext = %q, want sel-123", got)
	}

	// Explicit empty key in metadata clears an existing context SelectionID.
	clearedCtx := contextWithRequestedModelAlias(ctx, cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.SelectionIDMetadataKey: "",
		},
	}, "fallback-model")
	if got := coreusage.SelectionIDFromContext(clearedCtx); got != "" {
		t.Fatalf("SelectionIDFromContext after clear = %q, want empty", got)
	}
}
