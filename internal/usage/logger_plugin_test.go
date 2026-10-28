package usage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

// withReusedGinContext attaches a gin.Context that already serves a different
// request, as happens when gin recycles it before async usage handling runs.
func withReusedGinContext(ctx context.Context) context.Context {
	gin.SetMode(gin.TestMode)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	return context.WithValue(ctx, "gin", ginCtx)
}

func TestRequestStatisticsRecordUsesRequestSnapshotNotGinContext(t *testing.T) {
	ctx := internallogging.WithEndpoint(context.Background(), "POST /v1/chat/completions")
	ctx = internallogging.WithResponseStatusHolder(ctx)
	internallogging.SetResponseStatus(ctx, http.StatusBadGateway)
	ctx = withReusedGinContext(ctx)

	stats := NewRequestStatistics()
	stats.Record(ctx, coreusage.Record{
		Model:       "gpt-5.6",
		RequestedAt: time.Date(2026, 10, 28, 8, 0, 0, 0, time.UTC),
	})

	snapshot := stats.Snapshot()
	if _, ok := snapshot.APIs["POST /v1/chat/completions"]; !ok {
		t.Fatalf("api keys = %v, want snapshot endpoint", snapshot.APIs)
	}
	if snapshot.FailureCount != 1 || snapshot.SuccessCount != 0 {
		t.Fatalf("success/failure = %d/%d, want 0/1 from snapshot status", snapshot.SuccessCount, snapshot.FailureCount)
	}
}

func TestRequestStatisticsRecordIncludesLatency(t *testing.T) {
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.4",
		RequestedAt: time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC),
		Latency:     1500 * time.Millisecond,
		Detail: coreusage.Detail{
			InputTokens:  10,
			OutputTokens: 20,
			TotalTokens:  30,
		},
	})

	snapshot := stats.Snapshot()
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 {
		t.Fatalf("details len = %d, want 1", len(details))
	}
	if details[0].LatencyMs != 1500 {
		t.Fatalf("latency_ms = %d, want 1500", details[0].LatencyMs)
	}
}

func TestRequestStatisticsRecordIncludesCacheWriteTokens(t *testing.T) {
	stats := NewRequestStatistics()
	stats.Record(context.Background(), coreusage.Record{
		APIKey:      "test-key",
		Model:       "gpt-5.6",
		RequestedAt: time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC),
		Detail: coreusage.Detail{
			InputTokens:         100,
			OutputTokens:        20,
			CacheReadTokens:     30,
			CacheCreationTokens: 40,
			TotalTokens:         120,
		},
	})

	detail := stats.Snapshot().APIs["test-key"].Models["gpt-5.6"].Details[0]
	if detail.Tokens.CacheWriteTokens != 40 {
		t.Fatalf("cache write tokens = %d, want 40", detail.Tokens.CacheWriteTokens)
	}
}

func TestRequestStatisticsMergeSnapshotDedupIgnoresLatency(t *testing.T) {
	stats := NewRequestStatistics()
	timestamp := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	first := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"test-key": {
				Models: map[string]ModelSnapshot{
					"gpt-5.4": {
						Details: []RequestDetail{{
							Timestamp: timestamp,
							LatencyMs: 0,
							Source:    "user@example.com",
							AuthIndex: "0",
							Tokens: TokenStats{
								InputTokens:  10,
								OutputTokens: 20,
								TotalTokens:  30,
							},
						}},
					},
				},
			},
		},
	}
	second := StatisticsSnapshot{
		APIs: map[string]APISnapshot{
			"test-key": {
				Models: map[string]ModelSnapshot{
					"gpt-5.4": {
						Details: []RequestDetail{{
							Timestamp: timestamp,
							LatencyMs: 2500,
							Source:    "user@example.com",
							AuthIndex: "0",
							Tokens: TokenStats{
								InputTokens:  10,
								OutputTokens: 20,
								TotalTokens:  30,
							},
						}},
					},
				},
			},
		},
	}

	result := stats.MergeSnapshot(first)
	if result.Added != 1 || result.Skipped != 0 {
		t.Fatalf("first merge = %+v, want added=1 skipped=0", result)
	}

	result = stats.MergeSnapshot(second)
	if result.Added != 0 || result.Skipped != 1 {
		t.Fatalf("second merge = %+v, want added=0 skipped=1", result)
	}

	snapshot := stats.Snapshot()
	details := snapshot.APIs["test-key"].Models["gpt-5.4"].Details
	if len(details) != 1 {
		t.Fatalf("details len = %d, want 1", len(details))
	}
}
