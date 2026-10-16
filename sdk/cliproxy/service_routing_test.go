package cliproxy

import (
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

func TestNormalizedRoutingRuntimeState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy string
		want     string
	}{
		{name: "empty defaults to round-robin", strategy: "", want: coreauth.RoutingStrategyRoundRobin},
		{name: "round-robin alias", strategy: "rr", want: coreauth.RoutingStrategyRoundRobin},
		{name: "fill-first alias", strategy: "ff", want: coreauth.RoutingStrategyFillFirst},
		{name: "unknown falls back to round-robin", strategy: "bogus", want: coreauth.RoutingStrategyRoundRobin},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := normalizedRoutingRuntimeState(&config.Config{
				Routing: internalconfig.RoutingConfig{Strategy: tt.strategy},
			})
			if state.strategy != tt.want {
				t.Fatalf("strategy = %q, want %q", state.strategy, tt.want)
			}
		})
	}
}
