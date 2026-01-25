package management

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

type usageExportPayload struct {
	Version    int                      `json:"version"`
	ExportedAt time.Time                `json:"exported_at"`
	Usage      usage.StatisticsSnapshot `json:"usage"`
}

type usageImportPayload struct {
	Version int                      `json:"version"`
	Usage   usage.StatisticsSnapshot `json:"usage"`
}

// GetUsageStatistics returns combined statistics from database history and current session.
// If database plugin is not available, falls back to memory-only statistics.
func (h *Handler) GetUsageStatistics(c *gin.Context) {
	var snapshot usage.StatisticsSnapshot

	if dbPlugin := usage.GetDatabasePlugin(); dbPlugin != nil {
		// Combined query: database history + current session memory
		snapshot = dbPlugin.GetCombinedSnapshot()
	} else if h != nil && h.usageStats != nil {
		// Fallback to memory-only
		snapshot = h.usageStats.Snapshot()
	}

	c.JSON(http.StatusOK, gin.H{
		"usage":           snapshot,
		"failed_requests": snapshot.FailureCount,
	})
}

// ExportUsageStatistics returns a complete usage snapshot for backup/migration.
func (h *Handler) ExportUsageStatistics(c *gin.Context) {
	var snapshot usage.StatisticsSnapshot

	if dbPlugin := usage.GetDatabasePlugin(); dbPlugin != nil {
		snapshot = dbPlugin.GetCombinedSnapshot()
	} else if h != nil && h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}

	c.JSON(http.StatusOK, usageExportPayload{
		Version:    1,
		ExportedAt: time.Now().UTC(),
		Usage:      snapshot,
	})
}

// ImportUsageStatistics merges a previously exported usage snapshot into memory and database.
func (h *Handler) ImportUsageStatistics(c *gin.Context) {
	if h == nil || h.usageStats == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "usage statistics unavailable"})
		return
	}

	data, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	var payload usageImportPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if payload.Version != 0 && payload.Version != 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported version"})
		return
	}

	// Import to memory
	result := h.usageStats.MergeSnapshot(payload.Usage)

	// Import to database if available
	var dbAdded, dbSkipped int64
	if dbPlugin := usage.GetDatabasePlugin(); dbPlugin != nil {
		dbAdded, dbSkipped, _ = dbPlugin.ImportRecords(payload.Usage)
	}

	snapshot := h.usageStats.Snapshot()
	c.JSON(http.StatusOK, gin.H{
		"added":           result.Added,
		"skipped":         result.Skipped,
		"db_added":        dbAdded,
		"db_skipped":      dbSkipped,
		"total_requests":  snapshot.TotalRequests,
		"failed_requests": snapshot.FailureCount,
	})
}
