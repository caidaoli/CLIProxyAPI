package management

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/clienterror"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// isAuthUploadCredentialName reports whether a direct upload basename is an auth
// credential payload. .txt is accepted as an alias and rewritten to .json on save.
func isAuthUploadCredentialName(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	return strings.HasSuffix(lower, ".json") || strings.HasSuffix(lower, ".txt")
}

// normalizeAuthUploadCredentialName rewrites upload aliases to the on-disk name.
// Auth credentials are always stored as .json.
func normalizeAuthUploadCredentialName(name string) string {
	base := filepath.Base(strings.TrimSpace(name))
	if strings.HasSuffix(strings.ToLower(base), ".txt") {
		return base[:len(base)-4] + ".json"
	}
	return base
}

func queryRequestsAll(value string) bool {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "1", "true", "*":
		return true
	default:
		return false
	}
}

func authJSONFileNames(authDir string) ([]string, error) {
	entries, err := os.ReadDir(authDir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (h *Handler) downloadAllAuthFiles(c *gin.Context) {
	names, err := authJSONFileNames(h.cfg.AuthDir)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to read auth dir: %v", err)})
		return
	}

	var buf bytes.Buffer
	archive := zip.NewWriter(&buf)
	for _, name := range names {
		data, errRead := os.ReadFile(filepath.Join(h.cfg.AuthDir, name))
		if errRead != nil {
			_ = archive.Close()
			if os.IsNotExist(errRead) {
				c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("file not found during archive build: %s", name)})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to read file %s: %v", name, errRead)})
			return
		}
		writer, errCreate := archive.Create(name)
		if errCreate != nil {
			_ = archive.Close()
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to add file %s to archive: %v", name, errCreate)})
			return
		}
		if _, errWrite := writer.Write(data); errWrite != nil {
			_ = archive.Close()
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to write file %s to archive: %v", name, errWrite)})
			return
		}
	}
	if err = archive.Close(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to finalize archive: %v", err)})
		return
	}

	c.Header("Content-Disposition", `attachment; filename="auth-files.zip"`)
	c.Data(http.StatusOK, "application/zip", buf.Bytes())
}

// Download single auth file by name
func (h *Handler) DownloadAuthFile(c *gin.Context) {
	if queryRequestsAll(c.Query("all")) {
		h.downloadAllAuthFiles(c)
		return
	}
	name := strings.TrimSpace(c.Query("name"))
	if isUnsafeAuthFileName(name) {
		c.JSON(400, gin.H{"error": "invalid name"})
		return
	}
	if !strings.HasSuffix(strings.ToLower(name), ".json") {
		c.JSON(400, gin.H{"error": "name must end with .json"})
		return
	}
	full := filepath.Join(h.cfg.AuthDir, name)
	data, err := os.ReadFile(full)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(404, gin.H{"error": "file not found"})
		} else {
			c.JSON(500, gin.H{"error": fmt.Sprintf("failed to read file: %v", err)})
		}
		return
	}
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", name))
	c.Data(200, "application/json", data)
}

// Upload auth file: multipart (json/txt and/or archives), raw JSON with ?name=.json|.txt,
// or a raw archive body with ?name=.zip|.tar|.tar.gz|.tgz / matching Content-Type.
// .txt is accepted as a credential alias and rewritten to .json on disk.
// CLIProxyAPI auth bundles and Sub2API account data are expanded into native auth files.
// Supported archives: zip, tar, tar.gz/tgz.
func (h *Handler) UploadAuthFile(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	ctx := c.Request.Context()

	if c.ContentType() == "multipart/form-data" {
		h.uploadMultipartAuthFiles(c, ctx)
		return
	}

	name := strings.TrimSpace(c.Query("name"))
	contentType := strings.ToLower(strings.TrimSpace(c.ContentType()))
	format := detectAuthArchiveFormat(name, contentType)
	if format != authArchiveUnknown {
		data, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(clienterror.HTTPStatusFromErrorOr(err, http.StatusBadRequest), gin.H{"error": "failed to read body"})
			return
		}
		if len(data) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "empty archive body"})
			return
		}
		h.respondAuthBatchUpload(c, h.importAuthFilesFromArchiveBytes(ctx, format, data, maxAuthUploadFiles))
		return
	}

	if isUnsafeAuthFileName(name) {
		c.JSON(400, gin.H{"error": "invalid name"})
		return
	}
	if !isAuthUploadCredentialName(name) {
		c.JSON(400, gin.H{"error": "name must end with .json, .txt, .zip, .tar, .tar.gz, or .tgz"})
		return
	}
	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(clienterror.HTTPStatusFromErrorOr(err, http.StatusBadRequest), gin.H{"error": "failed to read body"})
		return
	}
	if result, handled := h.importCLIProxyAPIAuthBundle(ctx, data, maxAuthUploadFiles); handled {
		h.respondAuthBatchUpload(c, result)
		return
	}
	if result, handled := h.importSub2APIData(ctx, data, maxAuthUploadFiles); handled {
		h.respondAuthBatchUpload(c, result)
		return
	}
	if err = h.writeAuthFile(ctx, normalizeAuthUploadCredentialName(name), data); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, errInvalidAuthFile) {
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"status": "ok"})
}

// Delete auth files: single by name or all
//
// When all=true, the response is an NDJSON stream (application/x-ndjson):
//
//	{"type":"start","total":N}
//	{"type":"progress","index":1,"total":N,"name":"...","deleted":true}
//	{"type":"done","total":N,"deleted":M,"failed":K,"files":[...],"failed_items":[...]}
func (h *Handler) DeleteAuthFile(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	ctx := c.Request.Context()
	if queryRequestsAll(c.Query("all")) {
		query, _, errQuery := parseAuthFileListQuery(c)
		if errQuery != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errQuery.Error()})
			return
		}
		if _, hasSearch := c.GetQuery("search"); hasSearch {
			c.JSON(http.StatusBadRequest, gin.H{"error": "search is not supported when deleting all auth files"})
			return
		}
		if hasAuthFileDeleteFilters(query) {
			h.streamDeleteAuthFileCandidates(c, ctx, h.collectFilteredAuthFileCandidates(query))
			return
		}
		candidates, errCollect := h.collectUnfilteredAuthFileCandidates()
		if errCollect != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to read auth dir: %v", errCollect)})
			return
		}
		h.streamDeleteAuthFileCandidates(c, ctx, candidates)
		return
	}

	names, errNames := requestedAuthFileNamesForDelete(c)
	if errNames != nil {
		c.JSON(clienterror.HTTPStatusFromErrorOr(errNames, http.StatusBadRequest), gin.H{"error": errNames.Error()})
		return
	}
	if len(names) == 0 {
		c.JSON(400, gin.H{"error": "invalid name"})
		return
	}
	if len(names) == 1 {
		if _, status, errDelete := h.deleteAuthFileByName(ctx, names[0]); errDelete != nil {
			c.JSON(status, gin.H{"error": errDelete.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
		return
	}

	deletedFiles := make([]string, 0, len(names))
	failed := make([]gin.H, 0)
	for _, name := range names {
		deletedName, _, errDelete := h.deleteAuthFileByName(ctx, name)
		if errDelete != nil {
			failed = append(failed, gin.H{"name": name, "error": errDelete.Error()})
			continue
		}
		deletedFiles = append(deletedFiles, deletedName)
	}
	if len(failed) > 0 {
		c.JSON(http.StatusMultiStatus, gin.H{
			"status":  "partial",
			"deleted": len(deletedFiles),
			"files":   deletedFiles,
			"failed":  failed,
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "deleted": len(deletedFiles), "files": deletedFiles})
}

func hasAuthFileDeleteFilters(query authFileListQuery) bool {
	return query.Type != "" || query.ProblemOnly || query.DisabledOnly || query.EnabledOnly
}

type filteredAuthFileCandidate struct {
	authID string
	path   string
}

func (h *Handler) collectFilteredAuthFileCandidates(query authFileListQuery) []filteredAuthFileCandidate {
	candidates := make([]filteredAuthFileCandidate, 0)
	if h == nil || h.authManager == nil {
		return candidates
	}
	seenPaths := make(map[string]struct{})
	for _, auth := range h.authManager.List() {
		if auth == nil || isRuntimeOnlyAuth(auth) || !authFileListVisible(auth) || !authMatchesListStatusFilters(auth, query) {
			continue
		}
		candidate, okCandidate := h.filteredAuthFileCandidate(auth)
		if !okCandidate {
			continue
		}
		if _, seen := seenPaths[candidate.path]; seen {
			continue
		}
		seenPaths[candidate.path] = struct{}{}
		candidates = append(candidates, candidate)
	}
	return candidates
}

func (h *Handler) collectUnfilteredAuthFileCandidates() ([]filteredAuthFileCandidate, error) {
	if h == nil || h.cfg == nil {
		return nil, fmt.Errorf("auth config unavailable")
	}
	entries, err := os.ReadDir(h.cfg.AuthDir)
	if err != nil {
		return nil, err
	}
	candidates := make([]filteredAuthFileCandidate, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(strings.ToLower(name), ".json") {
			continue
		}
		full := filepath.Join(h.cfg.AuthDir, name)
		if !filepath.IsAbs(full) {
			if abs, errAbs := filepath.Abs(full); errAbs == nil {
				full = abs
			}
		}
		candidates = append(candidates, filteredAuthFileCandidate{path: full})
	}
	return candidates, nil
}

func writeAuthDeleteNDJSONEvent(c *gin.Context) func(any) {
	c.Writer.Header().Set("Content-Type", "application/x-ndjson")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("X-Accel-Buffering", "no")
	c.Status(http.StatusOK)
	return func(v any) {
		data, errMarshal := json.Marshal(v)
		if errMarshal != nil {
			return
		}
		data = append(data, '\n')
		_, _ = c.Writer.Write(data)
		if flusher, ok := c.Writer.(http.Flusher); ok {
			flusher.Flush()
		}
	}
}

func (h *Handler) streamDeleteAuthFileCandidates(c *gin.Context, ctx context.Context, candidates []filteredAuthFileCandidate) {
	writeEvent := writeAuthDeleteNDJSONEvent(c)
	total := len(candidates)
	writeEvent(gin.H{"type": "start", "total": total})

	deletedFiles := make([]string, 0, total)
	failed := make([]gin.H, 0)
	for index, candidate := range candidates {
		if ctx.Err() != nil {
			break
		}
		name := filepath.Base(candidate.path)
		event := gin.H{
			"type":  "progress",
			"index": index + 1,
			"total": total,
			"name":  name,
		}
		deletedName, _, errDelete := h.deleteFilteredAuthFile(ctx, candidate)
		if errDelete != nil {
			event["deleted"] = false
			event["error"] = errDelete.Error()
			failed = append(failed, gin.H{"name": name, "error": errDelete.Error()})
			writeEvent(event)
			continue
		}
		event["deleted"] = true
		deletedFiles = append(deletedFiles, deletedName)
		writeEvent(event)
	}

	writeEvent(gin.H{
		"type":         "done",
		"total":        total,
		"deleted":      len(deletedFiles),
		"failed":       len(failed),
		"files":        deletedFiles,
		"failed_items": failed,
	})
}

func (h *Handler) filteredAuthFileCandidate(auth *coreauth.Auth) (filteredAuthFileCandidate, bool) {
	if h == nil || h.cfg == nil || auth == nil {
		return filteredAuthFileCandidate{}, false
	}
	path, okPath := authFilePathWithinDir(h.cfg.AuthDir, authAttribute(auth, "path"))
	if !okPath {
		return filteredAuthFileCandidate{}, false
	}
	return filteredAuthFileCandidate{authID: auth.ID, path: path}, true
}

func authFilePathWithinDir(authDir, path string) (string, bool) {
	authDir = strings.TrimSpace(authDir)
	path = strings.TrimSpace(path)
	if authDir == "" || path == "" {
		return "", false
	}
	authDirAbs, errAuthDirAbs := filepath.Abs(authDir)
	if errAuthDirAbs != nil {
		return "", false
	}
	pathAbs, errPathAbs := filepath.Abs(path)
	if errPathAbs != nil {
		return "", false
	}
	authDirAbs = filepath.Clean(authDirAbs)
	pathAbs = filepath.Clean(pathAbs)
	rel, errRel := filepath.Rel(authDirAbs, pathAbs)
	if errRel != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", false
	}
	if isUnsafeAuthFileName(filepath.Base(pathAbs)) {
		return "", false
	}
	authDirReal, errAuthDirReal := filepath.EvalSymlinks(authDirAbs)
	if errAuthDirReal != nil {
		return "", false
	}
	parentReal, errParentReal := filepath.EvalSymlinks(filepath.Dir(pathAbs))
	if errParentReal != nil {
		return "", false
	}
	authDirReal = filepath.Clean(authDirReal)
	parentReal = filepath.Clean(parentReal)
	realRel, errRealRel := filepath.Rel(authDirReal, parentReal)
	if errRealRel != nil || realRel == ".." || strings.HasPrefix(realRel, ".."+string(os.PathSeparator)) || filepath.IsAbs(realRel) {
		return "", false
	}
	if info, errLstat := os.Lstat(pathAbs); errLstat != nil {
		if !os.IsNotExist(errLstat) {
			return "", false
		}
	} else if info.Mode()&os.ModeSymlink != 0 {
		return "", false
	}
	return filepath.Join(parentReal, filepath.Base(pathAbs)), true
}

func (h *Handler) deleteFilteredAuthFile(ctx context.Context, candidate filteredAuthFileCandidate) (string, int, error) {
	name := filepath.Base(candidate.path)
	if isUnsafeAuthFileName(name) {
		return "", http.StatusBadRequest, fmt.Errorf("invalid name")
	}
	release := h.beginSIWCFileMutation(ctx, candidate.path)
	defer release()
	if errUsage := h.deleteAuthUsage(ctx, h.authsForPath(candidate.path, candidate.authID)...); errUsage != nil {
		return name, http.StatusInternalServerError, fmt.Errorf("failed to delete usage statistics: %w", errUsage)
	}
	if errRemove := os.Remove(candidate.path); errRemove != nil {
		if os.IsNotExist(errRemove) {
			return name, http.StatusNotFound, errAuthFileNotFound
		}
		return name, http.StatusInternalServerError, fmt.Errorf("failed to remove file: %w", errRemove)
	}
	h.notifyAuthFileMutation(candidate.path)
	if errDeleteRecord := h.deleteTokenRecord(ctx, candidate.path); errDeleteRecord != nil {
		return name, http.StatusInternalServerError, errDeleteRecord
	}
	h.removeAuthsForPath(ctx, candidate.path, candidate.authID)
	return name, http.StatusOK, nil
}

type authUploadFailure struct {
	name string
	err  error
}

type authFileImportResult struct {
	uploaded int
	failed   []authUploadFailure
	// fatal is set when the uploaded container itself is unusable.
	fatal error
}

type authArchiveFormat int

const (
	authArchiveUnknown authArchiveFormat = iota
	authArchiveZip
	authArchiveTar
	authArchiveTarGz
)

func detectAuthArchiveFormat(filename, contentType string) authArchiveFormat {
	lowerName := strings.ToLower(strings.TrimSpace(filename))
	// Check compound suffixes first.
	switch {
	case strings.HasSuffix(lowerName, ".tar.gz"), strings.HasSuffix(lowerName, ".tgz"):
		return authArchiveTarGz
	case strings.HasSuffix(lowerName, ".tar"):
		return authArchiveTar
	case strings.HasSuffix(lowerName, ".zip"):
		return authArchiveZip
	}

	ct := strings.ToLower(strings.TrimSpace(contentType))
	switch {
	case strings.Contains(ct, "application/zip"),
		strings.Contains(ct, "application/x-zip-compressed"),
		strings.Contains(ct, "multipart/x-zip"):
		return authArchiveZip
	case strings.Contains(ct, "application/gzip"),
		strings.Contains(ct, "application/x-gzip"),
		strings.Contains(ct, "application/x-gtar"),
		strings.Contains(ct, "application/x-tar+gzip"):
		return authArchiveTarGz
	case strings.Contains(ct, "application/x-tar"),
		strings.Contains(ct, "application/tar"):
		return authArchiveTar
	default:
		return authArchiveUnknown
	}
}

func (h *Handler) uploadMultipartAuthFiles(c *gin.Context, ctx context.Context) {
	reader, errReader := c.Request.MultipartReader()
	if errReader != nil {
		c.JSON(clienterror.HTTPStatusFromErrorOr(errReader, http.StatusBadRequest), gin.H{"error": fmt.Sprintf("invalid multipart form: %v", errReader)})
		return
	}

	uploadedCount := 0
	failed := make([]authUploadFailure, 0)
	// Counts logical auth JSON files (archive entries expand into multiple).
	fileCount := 0
	// Counts multipart parts that carried a filename (including archive parts).
	partCount := 0
	for {
		part, errNext := reader.NextPart()
		if errors.Is(errNext, io.EOF) {
			break
		}
		if errNext != nil {
			c.JSON(clienterror.HTTPStatusFromErrorOr(errNext, http.StatusBadRequest), gin.H{"error": fmt.Sprintf("invalid multipart form: %v", errNext)})
			return
		}
		filename := strings.TrimSpace(part.FileName())
		if filename == "" {
			if errClose := part.Close(); errClose != nil {
				c.JSON(clienterror.HTTPStatusFromErrorOr(errClose, http.StatusBadRequest), gin.H{"error": fmt.Sprintf("invalid multipart form: %v", errClose)})
				return
			}
			continue
		}

		partCount++
		if partCount > maxAuthUploadFiles {
			_ = part.Close()
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": fmt.Sprintf("too many files: maximum is %d", maxAuthUploadFiles)})
			return
		}
		baseName := filepath.Base(filename)
		if format := detectAuthArchiveFormat(baseName, ""); format != authArchiveUnknown {
			data, errRead := io.ReadAll(part)
			if errClose := part.Close(); errClose != nil {
				log.WithError(errClose).Warn("failed to close uploaded auth archive part")
			}
			if errRead != nil {
				failed = append(failed, authUploadFailure{name: baseName, err: fmt.Errorf("failed to read uploaded archive: %w", errRead)})
				continue
			}
			result := h.importAuthFilesFromArchiveBytes(ctx, format, data, maxAuthUploadFiles-fileCount)
			if result.fatal != nil {
				if errors.Is(result.fatal, errAuthUploadFileLimit) {
					c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": result.fatal.Error()})
					return
				}
				failed = append(failed, authUploadFailure{name: baseName, err: result.fatal})
				continue
			}
			if fileCount+result.uploaded+len(result.failed) > maxAuthUploadFiles {
				c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": fmt.Sprintf("too many files: maximum is %d", maxAuthUploadFiles)})
				return
			}
			fileCount += result.uploaded + len(result.failed)
			uploadedCount += result.uploaded
			failed = append(failed, result.failed...)
			continue
		}

		if fileCount >= maxAuthUploadFiles {
			_ = part.Close()
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": fmt.Sprintf("too many files: maximum is %d", maxAuthUploadFiles)})
			return
		}

		result := h.importUploadedAuthFile(ctx, filename, part, maxAuthUploadFiles-fileCount)
		if errClose := part.Close(); errClose != nil {
			log.WithError(errClose).Warn("failed to close uploaded auth file part")
		}
		if result.fatal != nil {
			if errors.Is(result.fatal, errAuthUploadFileLimit) {
				c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": result.fatal.Error()})
				return
			}
			failed = append(failed, authUploadFailure{name: baseName, err: result.fatal})
			continue
		}
		logicalFiles := result.uploaded + len(result.failed)
		if fileCount+logicalFiles > maxAuthUploadFiles {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": fmt.Sprintf("too many files: maximum is %d", maxAuthUploadFiles)})
			return
		}
		fileCount += logicalFiles
		uploadedCount += result.uploaded
		failed = append(failed, result.failed...)
	}

	switch {
	case partCount == 0 || (fileCount == 0 && len(failed) == 0):
		c.JSON(http.StatusBadRequest, gin.H{"error": "no files uploaded"})
	case fileCount == 0 && len(failed) == 1:
		// Single archive/part that produced only a fatal/entry failure with zero successes.
		failure := failed[0]
		if errors.Is(failure.err, errAuthFileMustBeJSON) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "file must be .json, .txt, .zip, .tar, .tar.gz, or .tgz"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": failure.err.Error()})
	case fileCount == 1 && len(failed) == 1 && uploadedCount == 0:
		failure := failed[0]
		if errors.Is(failure.err, errAuthFileMustBeJSON) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "file must be .json, .txt, .zip, .tar, .tar.gz, or .tgz"})
			return
		}
		status := http.StatusInternalServerError
		if errors.Is(failure.err, errInvalidAuthFile) {
			status = http.StatusBadRequest
		}
		c.JSON(status, gin.H{"error": failure.err.Error()})
	case fileCount == 1 && uploadedCount == 1 && len(failed) == 0:
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	case len(failed) > 0:
		failedPayload := make([]gin.H, 0, len(failed))
		for _, failure := range failed {
			msg := failure.err.Error()
			if errors.Is(failure.err, errAuthFileMustBeJSON) {
				msg = "file must be .json or .txt"
			}
			failedPayload = append(failedPayload, gin.H{"name": failure.name, "error": msg})
		}
		c.JSON(http.StatusMultiStatus, gin.H{
			"status":   "partial",
			"uploaded": uploadedCount,
			"failed":   failedPayload,
		})
	default:
		c.JSON(http.StatusOK, gin.H{"status": "ok", "uploaded": uploadedCount})
	}
}

func (h *Handler) respondAuthBatchUpload(c *gin.Context, result authFileImportResult) {
	if result.fatal != nil {
		status := http.StatusBadRequest
		if errors.Is(result.fatal, errAuthUploadFileLimit) {
			status = http.StatusRequestEntityTooLarge
		}
		c.JSON(status, gin.H{"error": result.fatal.Error()})
		return
	}
	total := result.uploaded + len(result.failed)
	switch {
	case total == 0:
		c.JSON(http.StatusBadRequest, gin.H{"error": "archive contains no .json auth files"})
	case result.uploaded == 0 && len(result.failed) == 1:
		c.JSON(http.StatusBadRequest, gin.H{"error": result.failed[0].err.Error()})
	case len(result.failed) > 0:
		failedPayload := make([]gin.H, 0, len(result.failed))
		for _, failure := range result.failed {
			failedPayload = append(failedPayload, gin.H{"name": failure.name, "error": failure.err.Error()})
		}
		c.JSON(http.StatusMultiStatus, gin.H{
			"status":   "partial",
			"uploaded": result.uploaded,
			"failed":   failedPayload,
		})
	case result.uploaded == 1:
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	default:
		c.JSON(http.StatusOK, gin.H{"status": "ok", "uploaded": result.uploaded})
	}
}

func (h *Handler) importAuthFilesFromArchiveBytes(ctx context.Context, format authArchiveFormat, data []byte, maxFiles int) authFileImportResult {
	if len(data) == 0 {
		return authFileImportResult{fatal: fmt.Errorf("empty archive body")}
	}
	if maxFiles < 1 {
		return authFileImportResult{fatal: authUploadFileLimitError()}
	}
	switch format {
	case authArchiveZip:
		return h.importAuthFilesFromZipBytes(ctx, data, maxFiles)
	case authArchiveTar:
		return h.importAuthFilesFromTarReader(ctx, tar.NewReader(bytes.NewReader(data)), maxFiles)
	case authArchiveTarGz:
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return authFileImportResult{fatal: fmt.Errorf("invalid gzip archive: %w", err)}
		}
		defer gz.Close()
		return h.importAuthFilesFromTarReader(ctx, tar.NewReader(gz), maxFiles)
	default:
		return authFileImportResult{fatal: fmt.Errorf("unsupported archive format")}
	}
}

func (h *Handler) importAuthFilesFromZipBytes(ctx context.Context, data []byte, maxFiles int) authFileImportResult {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return authFileImportResult{fatal: fmt.Errorf("invalid zip archive: %w", err)}
	}
	return h.importAuthFilesFromZipReader(ctx, reader, maxFiles)
}

func (h *Handler) importAuthFilesFromZipReader(ctx context.Context, reader *zip.Reader, maxFiles int) authFileImportResult {
	result := authFileImportResult{failed: make([]authUploadFailure, 0)}
	if reader == nil {
		result.fatal = fmt.Errorf("invalid zip archive")
		return result
	}

	var uncompressedTotal int64
	seen := 0
	for _, entry := range reader.File {
		if entry == nil {
			continue
		}
		base, skip, skipErr := authArchiveEntryBaseName(entry.Name, entry.FileInfo().IsDir())
		if skip {
			if skipErr != nil {
				result.failed = append(result.failed, authUploadFailure{name: base, err: skipErr})
			}
			continue
		}

		seen++
		if seen > maxFiles {
			result.fatal = authUploadFileLimitError()
			return result
		}
		if entry.UncompressedSize64 > maxAuthArchiveUncompressedFile {
			result.failed = append(result.failed, authUploadFailure{
				name: base,
				err:  fmt.Errorf("file too large: maximum is %d bytes", maxAuthArchiveUncompressedFile),
			})
			continue
		}
		if uncompressedTotal+int64(entry.UncompressedSize64) > maxAuthArchiveUncompressedTotal {
			result.fatal = fmt.Errorf("archive uncompressed size exceeds limit of %d bytes", maxAuthArchiveUncompressedTotal)
			return result
		}

		rc, errOpen := entry.Open()
		if errOpen != nil {
			result.failed = append(result.failed, authUploadFailure{name: base, err: fmt.Errorf("failed to open archive entry: %w", errOpen)})
			continue
		}
		// Cap read to declared size + 1 to detect lying headers.
		payload, errRead := readAuthArchiveEntry(rc, int64(entry.UncompressedSize64))
		_ = rc.Close()
		if errRead != nil {
			result.failed = append(result.failed, authUploadFailure{name: base, err: errRead})
			continue
		}
		uncompressedTotal += int64(len(payload))

		logicalFiles := result.uploaded + len(result.failed)
		entryResult := h.importUploadedAuthFile(ctx, base, bytes.NewReader(payload), maxFiles-logicalFiles)
		if !mergeAuthFileImportResult(&result, base, entryResult) {
			return result
		}
	}

	if result.uploaded == 0 && len(result.failed) == 0 {
		result.fatal = fmt.Errorf("archive contains no .json auth files")
	}
	return result
}

func (h *Handler) importAuthFilesFromTarReader(ctx context.Context, reader *tar.Reader, maxFiles int) authFileImportResult {
	result := authFileImportResult{failed: make([]authUploadFailure, 0)}
	if reader == nil {
		result.fatal = fmt.Errorf("invalid tar archive")
		return result
	}

	var uncompressedTotal int64
	seen := 0
	for {
		header, errNext := reader.Next()
		if errors.Is(errNext, io.EOF) {
			break
		}
		if errNext != nil {
			result.fatal = fmt.Errorf("invalid tar archive: %w", errNext)
			return result
		}
		if header == nil {
			continue
		}
		isDir := header.Typeflag == tar.TypeDir
		base, skip, skipErr := authArchiveEntryBaseName(header.Name, isDir)
		if skip {
			if skipErr != nil {
				result.failed = append(result.failed, authUploadFailure{name: base, err: skipErr})
			}
			continue
		}
		// Only regular files / contiguous files are imported.
		switch header.Typeflag {
		case tar.TypeReg, tar.TypeRegA:
			// ok
		default:
			continue
		}

		seen++
		if seen > maxFiles {
			result.fatal = authUploadFileLimitError()
			return result
		}
		if header.Size > maxAuthArchiveUncompressedFile {
			result.failed = append(result.failed, authUploadFailure{
				name: base,
				err:  fmt.Errorf("file too large: maximum is %d bytes", maxAuthArchiveUncompressedFile),
			})
			// Drain remaining bytes for this entry so Next() stays aligned.
			_, _ = io.Copy(io.Discard, io.LimitReader(reader, header.Size))
			continue
		}
		if uncompressedTotal+header.Size > maxAuthArchiveUncompressedTotal {
			result.fatal = fmt.Errorf("archive uncompressed size exceeds limit of %d bytes", maxAuthArchiveUncompressedTotal)
			return result
		}

		payload, errRead := readAuthArchiveEntry(reader, header.Size)
		if errRead != nil {
			result.failed = append(result.failed, authUploadFailure{name: base, err: errRead})
			continue
		}
		uncompressedTotal += int64(len(payload))

		logicalFiles := result.uploaded + len(result.failed)
		entryResult := h.importUploadedAuthFile(ctx, base, bytes.NewReader(payload), maxFiles-logicalFiles)
		if !mergeAuthFileImportResult(&result, base, entryResult) {
			return result
		}
	}

	if result.uploaded == 0 && len(result.failed) == 0 {
		result.fatal = fmt.Errorf("archive contains no .json auth files")
	}
	return result
}

func mergeAuthFileImportResult(result *authFileImportResult, containerName string, imported authFileImportResult) bool {
	if imported.fatal != nil {
		if errors.Is(imported.fatal, errAuthUploadFileLimit) {
			result.fatal = imported.fatal
			return false
		}
		result.failed = append(result.failed, authUploadFailure{name: containerName, err: imported.fatal})
		return true
	}
	result.uploaded += imported.uploaded
	result.failed = append(result.failed, imported.failed...)
	return true
}

// authArchiveEntryBaseName normalizes an archive entry path into a safe basename.
// skip=true means the entry should be ignored (directories, junk, non-json).
// skipErr is set when the basename is present but unsafe (path traversal / invalid).
func authArchiveEntryBaseName(name string, isDir bool) (base string, skip bool, skipErr error) {
	name = strings.TrimSpace(name)
	if name == "" || isDir || strings.HasSuffix(name, "/") {
		return "", true, nil
	}
	base = filepath.Base(name)
	if base == "" || base == "." || base == ".." {
		return "", true, nil
	}
	if strings.HasPrefix(base, "._") || strings.EqualFold(base, ".DS_Store") {
		return base, true, nil
	}
	// Only accept the basename; reject absolute/unsafe names (zip/tar slip).
	if isUnsafeAuthFileName(base) {
		return base, true, fmt.Errorf("invalid name")
	}
	if !strings.HasSuffix(strings.ToLower(base), ".json") {
		// Non-auth payloads inside the archive are ignored, not fatal.
		return base, true, nil
	}
	return base, false, nil
}

func readAuthArchiveEntry(r io.Reader, declaredSize int64) ([]byte, error) {
	limit := int64(maxAuthArchiveUncompressedFile) + 1
	if declaredSize > 0 && declaredSize+1 < limit {
		limit = declaredSize + 1
	}
	payload, err := io.ReadAll(io.LimitReader(r, limit))
	if err != nil {
		return nil, fmt.Errorf("failed to read archive entry: %w", err)
	}
	if len(payload) > maxAuthArchiveUncompressedFile {
		return nil, fmt.Errorf("file too large: maximum is %d bytes", maxAuthArchiveUncompressedFile)
	}
	return payload, nil
}

func (h *Handler) importUploadedAuthFile(ctx context.Context, filename string, src io.Reader, maxFiles int) authFileImportResult {
	if maxFiles < 1 {
		return authFileImportResult{fatal: authUploadFileLimitError()}
	}
	name := filepath.Base(strings.TrimSpace(filename))
	if !isAuthUploadCredentialName(name) {
		return authFileImportResult{failed: []authUploadFailure{{name: name, err: errAuthFileMustBeJSON}}}
	}
	storedName := normalizeAuthUploadCredentialName(name)
	data, err := io.ReadAll(src)
	if err != nil {
		return authFileImportResult{failed: []authUploadFailure{{name: name, err: fmt.Errorf("failed to read uploaded file: %w", err)}}}
	}
	if result, handled := h.importCLIProxyAPIAuthBundle(ctx, data, maxFiles); handled {
		return result
	}
	if result, handled := h.importSub2APIData(ctx, data, maxFiles); handled {
		return result
	}
	if err := h.writeAuthFile(ctx, storedName, data); err != nil {
		return authFileImportResult{failed: []authUploadFailure{{name: name, err: err}}}
	}
	return authFileImportResult{uploaded: 1}
}

func (h *Handler) writeAuthFile(ctx context.Context, name string, data []byte) error {
	return h.writeAuthFileWithMode(ctx, name, data, false)
}

func (h *Handler) writeNewAuthFile(ctx context.Context, name string, data []byte) error {
	return h.writeAuthFileWithMode(ctx, name, data, true)
}

func (h *Handler) writeAuthFileWithMode(ctx context.Context, name string, data []byte, exclusive bool) error {
	dst := filepath.Join(h.cfg.AuthDir, filepath.Base(name))
	if !filepath.IsAbs(dst) {
		if abs, errAbs := filepath.Abs(dst); errAbs == nil {
			dst = abs
		}
	}
	data = fillMissingAuthEmailFromFileName(name, data)
	watcherManaged := h.authFileLoadingManagedByWatcher()
	var auth *coreauth.Auth
	if watcherManaged {
		metadata, errDecode := decodeAuthFileMetadata(data)
		if errDecode != nil {
			return errDecode
		}
		// The watcher registers this file; the stub only selects the Codex file lock.
		auth = &coreauth.Auth{ID: h.authIDForPath(dst), Metadata: metadata}
	} else {
		var errBuild error
		auth, errBuild = h.buildAuthFromFileData(dst, data)
		if errBuild != nil {
			return errBuild
		}
	}
	release := h.beginSIWCFileMutation(ctx, dst)
	defer release()
	if errWrite := h.writeAuthFileBytes(dst, data, auth, exclusive); errWrite != nil {
		return fmt.Errorf("failed to write file: %w", errWrite)
	}
	h.notifyAuthFileMutation(dst)
	if watcherManaged {
		return nil
	}
	if err := h.upsertAuthRecord(coreauth.WithAuthMaterialReplacement(ctx), auth); err != nil {
		return err
	}
	if h.postAuthPersistHook != nil {
		if errHook := h.postAuthPersistHook(ctx, auth); errHook != nil {
			return fmt.Errorf("post-auth persist hook failed: %w", errHook)
		}
	}
	return nil
}

func (h *Handler) writeAuthFileBytes(dst string, data []byte, auth *coreauth.Auth, exclusive bool) error {
	write := func() error {
		if exclusive {
			return writeNewFileBytes(dst, data)
		}
		return os.WriteFile(dst, data, 0o600)
	}
	if h.shouldLockCodexAuthFile(auth) {
		return codex.WithFileLock(dst, write)
	}
	return write()
}

func writeNewFileBytes(dst string, data []byte) error {
	file, errOpen := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errOpen != nil {
		return errOpen
	}
	if _, errWrite := file.Write(data); errWrite != nil {
		_ = file.Close()
		_ = os.Remove(dst)
		return errWrite
	}
	if errClose := file.Close(); errClose != nil {
		_ = os.Remove(dst)
		return errClose
	}
	return nil
}

func (h *Handler) shouldLockCodexAuthFile(auth *coreauth.Auth) bool {
	if coreauth.IsCodexAuthRecord(auth) {
		return true
	}
	if h != nil && h.authManager != nil && auth != nil && auth.ID != "" {
		if existing, ok := h.authManager.GetByID(auth.ID); ok && coreauth.IsCodexAuthRecord(existing) {
			return true
		}
	}
	return false
}

func (h *Handler) authFileLoadingManagedByWatcher() bool {
	if h == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.authLoadStatusProvider != nil
}

func fillMissingAuthEmailFromFileName(name string, data []byte) []byte {
	var metadata map[string]any
	if err := json.Unmarshal(data, &metadata); err != nil || metadata == nil {
		return data
	}
	if email, ok := metadata["email"].(string); ok && strings.TrimSpace(email) != "" {
		return data
	}
	email := authEmailFromFileName(name)
	if email == "" {
		return data
	}
	metadata["email"] = email
	normalized, err := json.Marshal(metadata)
	if err != nil {
		return data
	}
	return normalized
}

func authEmailFromFileName(name string) string {
	base := filepath.Base(strings.TrimSpace(name))
	base = strings.TrimSuffix(base, filepath.Ext(base))
	if base == "" {
		return ""
	}
	lowerBase := strings.ToLower(base)
	for _, prefix := range authFileEmailNamePrefixes {
		if strings.HasPrefix(lowerBase, prefix) {
			base = base[len(prefix):]
			break
		}
	}
	matches := authFileEmailNamePattern.FindStringSubmatch(base)
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

func requestedAuthFileNamesForDelete(c *gin.Context) ([]string, error) {
	if c == nil {
		return nil, nil
	}
	names := uniqueAuthFileNames(c.QueryArray("name"))
	if len(names) > 0 {
		return names, nil
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to read body")
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, nil
	}

	var objectBody struct {
		Name  string   `json:"name"`
		Names []string `json:"names"`
	}
	if body[0] == '[' {
		var arrayBody []string
		if err := json.Unmarshal(body, &arrayBody); err != nil {
			return nil, fmt.Errorf("invalid request body")
		}
		return uniqueAuthFileNames(arrayBody), nil
	}
	if err := json.Unmarshal(body, &objectBody); err != nil {
		return nil, fmt.Errorf("invalid request body")
	}

	out := make([]string, 0, len(objectBody.Names)+1)
	if strings.TrimSpace(objectBody.Name) != "" {
		out = append(out, objectBody.Name)
	}
	out = append(out, objectBody.Names...)
	return uniqueAuthFileNames(out), nil
}

func uniqueAuthFileNames(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(names))
	out := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out
}

func (h *Handler) deleteAuthFileByName(ctx context.Context, name string) (string, int, error) {
	name = strings.TrimSpace(name)
	if isUnsafeAuthFileName(name) {
		return "", http.StatusBadRequest, fmt.Errorf("invalid name")
	}

	targetPath := filepath.Join(h.cfg.AuthDir, filepath.Base(name))
	targetID := ""
	if targetAuth := h.findAuthForDelete(name); targetAuth != nil {
		if !isPluginVirtualSourceDelete(name, targetAuth) {
			return filepath.Base(name), http.StatusConflict, errPluginVirtualAuth
		}
		targetID = strings.TrimSpace(targetAuth.ID)
		if path := strings.TrimSpace(authAttribute(targetAuth, "path")); path != "" {
			targetPath = path
		}
	}
	if !filepath.IsAbs(targetPath) {
		if abs, errAbs := filepath.Abs(targetPath); errAbs == nil {
			targetPath = abs
		}
	}
	release := h.beginSIWCFileMutation(ctx, targetPath)
	defer release()
	if errUsage := h.deleteAuthUsage(ctx, h.authsForPath(targetPath, targetID)...); errUsage != nil {
		return filepath.Base(name), http.StatusInternalServerError, fmt.Errorf("failed to delete usage statistics: %w", errUsage)
	}
	if errRemove := os.Remove(targetPath); errRemove != nil {
		if os.IsNotExist(errRemove) {
			return filepath.Base(name), http.StatusNotFound, errAuthFileNotFound
		}
		return filepath.Base(name), http.StatusInternalServerError, fmt.Errorf("failed to remove file: %w", errRemove)
	}
	h.notifyAuthFileMutation(targetPath)
	if errDeleteRecord := h.deleteTokenRecord(ctx, targetPath); errDeleteRecord != nil {
		return filepath.Base(name), http.StatusInternalServerError, errDeleteRecord
	}
	h.removeAuthsForPath(ctx, targetPath, targetID)
	return filepath.Base(name), http.StatusOK, nil
}

func isPluginVirtualSourceDelete(name string, auth *coreauth.Auth) bool {
	if !coreauth.IsPluginVirtualAuth(auth) {
		return true
	}
	sourcePath := strings.TrimSpace(authAttribute(auth, coreauth.AttributeVirtualSource))
	if sourcePath == "" {
		sourcePath = strings.TrimSpace(authAttribute(auth, "path"))
	}
	if sourcePath == "" {
		return false
	}
	return strings.EqualFold(filepath.Base(strings.TrimSpace(name)), filepath.Base(sourcePath))
}

func (h *Handler) findAuthForDelete(name string) *coreauth.Auth {
	if h == nil || h.authManager == nil {
		return nil
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	if auth, ok := h.authManager.GetByID(name); ok {
		return auth
	}
	auths := h.authManager.List()
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if strings.TrimSpace(auth.FileName) == name {
			return auth
		}
		if filepath.Base(strings.TrimSpace(authAttribute(auth, "path"))) == name {
			return auth
		}
	}
	return nil
}

func (h *Handler) authIDForPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		if abs, errAbs := filepath.Abs(path); errAbs == nil {
			path = abs
		}
	}
	id := path
	if h != nil && h.cfg != nil {
		authDir := strings.TrimSpace(h.cfg.AuthDir)
		if resolvedAuthDir, errResolve := util.ResolveAuthDir(authDir); errResolve == nil && resolvedAuthDir != "" {
			authDir = resolvedAuthDir
		}
		if authDir != "" {
			authDir = filepath.Clean(authDir)
			if !filepath.IsAbs(authDir) {
				if abs, errAbs := filepath.Abs(authDir); errAbs == nil {
					authDir = abs
				}
			}
			if rel, errRel := filepath.Rel(authDir, path); errRel == nil && rel != "" {
				id = rel
			}
		}
	}
	// On Windows, normalize ID casing to avoid duplicate auth entries caused by case-insensitive paths.
	if runtime.GOOS == "windows" {
		id = strings.ToLower(id)
	}
	return id
}

func (h *Handler) registerAuthFromFile(ctx context.Context, path string, data []byte) error {
	if h.authManager == nil {
		return nil
	}
	auth, err := h.buildAuthFromFileData(path, data)
	if err != nil {
		return err
	}
	return h.upsertAuthRecord(ctx, auth)
}

func (h *Handler) buildAuthFromFileData(path string, data []byte) (*coreauth.Auth, error) {
	if path == "" {
		return nil, fmt.Errorf("auth path is empty")
	}
	if data == nil {
		var err error
		data, err = os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read auth file: %w", err)
		}
	}
	metadata, errDecode := decodeAuthFileMetadata(data)
	if errDecode != nil {
		return nil, errDecode
	}
	coreauth.NormalizeCredentialMetadata(metadata)
	// Loading tolerates invalid exclusion rules, so reject them here before anything is persisted.
	if _, errExcluded := synthesizer.ExtractExcludedModelsFromMetadata(metadata); errExcluded != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidAuthFile, errExcluded)
	}
	provider, _ := metadata["type"].(string)
	if provider == "" {
		provider = "unknown"
	}
	label := provider
	if email, ok := metadata["email"].(string); ok && email != "" {
		label = email
	}
	lastRefresh, hasLastRefresh := extractLastRefreshTimestamp(metadata)

	authID := h.authIDForPath(path)
	if authID == "" {
		authID = path
	}
	auth := (*coreauth.Auth)(nil)
	if h != nil && h.cfg != nil {
		sctx := &synthesizer.SynthesisContext{
			Config:      h.cfg,
			AuthDir:     h.cfg.AuthDir,
			Now:         time.Now(),
			IDGenerator: synthesizer.NewStableIDGenerator(),
		}
		generated, errSynthesize := synthesizer.SynthesizeAuthFile(sctx, path, data)
		if errSynthesize != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidAuthFile, errSynthesize)
		}
		if len(generated) > 0 && generated[0] != nil {
			auth = generated[0].Clone()
		}
	}
	if auth == nil {
		auth = &coreauth.Auth{
			ID:       authID,
			Provider: provider,
			Label:    label,
			Status:   coreauth.StatusActive,
			Attributes: map[string]string{
				"path":   path,
				"source": path,
			},
			Metadata:  metadata,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}
	auth.ID = authID
	auth.FileName = filepath.Base(path)
	if hasLastRefresh {
		auth.LastRefreshedAt = lastRefresh
	}
	if h != nil && h.authManager != nil {
		if existing, ok := h.authManager.GetByID(authID); ok {
			auth.CreatedAt = existing.CreatedAt
			if !coreauth.CredentialsChanged(existing, auth) {
				if !hasLastRefresh {
					auth.LastRefreshedAt = existing.LastRefreshedAt
				}
				auth.NextRefreshAfter = existing.NextRefreshAfter
			}
			auth.Runtime = existing.Runtime
		}
	}
	coreauth.ApplyCustomHeadersFromMetadata(auth)
	return auth, nil
}

func decodeAuthFileMetadata(data []byte) (map[string]any, error) {
	metadata := make(map[string]any)
	if errUnmarshal := json.Unmarshal(data, &metadata); errUnmarshal != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidAuthFile, errUnmarshal)
	}
	return metadata, nil
}

func (h *Handler) upsertAuthRecord(ctx context.Context, auth *coreauth.Auth) error {
	if h == nil || h.authManager == nil || auth == nil {
		return nil
	}
	if existing, ok := h.authManager.GetByID(auth.ID); ok {
		auth.CreatedAt = existing.CreatedAt
		_, err := h.authManager.Update(ctx, auth)
		return err
	}
	_, err := h.authManager.Register(ctx, auth)
	return err
}
