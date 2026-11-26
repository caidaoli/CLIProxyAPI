package common

import (
	"strings"

	"github.com/MoeForge/moego/json"
	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/signature"
	log "github.com/sirupsen/logrus"
)

// SanitizeAntigravityClaudeGeminiRequestSignaturesDoc normalizes signature replay in an Antigravity request.
func SanitizeAntigravityClaudeGeminiRequestSignaturesDoc(modelName string, doc *ojson.Doc) bool {
	return SanitizeAntigravityClaudeGeminiRequestSignaturesDocAt(modelName, doc, "request.")
}

// SanitizeAntigravityClaudeGeminiRequestSignaturesDocAt sanitizes replay under prefix.
func SanitizeAntigravityClaudeGeminiRequestSignaturesDocAt(modelName string, doc *ojson.Doc, prefix string) bool {
	if doc == nil {
		return false
	}
	contentsPath := prefix + "contents"
	signature.SanitizeGeminiSystemInstructionDoc(doc, contentsPath)
	contents := doc.Get(contentsPath)
	if !contents.IsArray() {
		return false
	}

	contentsArray := contents.Array()
	changed := false
	rewrittenContents := make([][]byte, 0, len(contentsArray))

	for contentIndex, content := range contentsArray {
		parts := content.Get("parts")
		if !parts.IsArray() {
			rewrittenContents = append(rewrittenContents, []byte(content.Raw))
			continue
		}

		isModelTurn := content.Get("role").String() == "model"
		partsArray := parts.Array()
		contentChanged := false
		rewrittenParts := make([][]byte, 0, len(partsArray))

		for partIndex, partResult := range partsArray {
			var part map[string]any
			decoder := json.NewDecoder(strings.NewReader(partResult.Raw))
			decoder.UseNumber()
			if err := decoder.Decode(&part); err != nil {
				rewrittenParts = append(rewrittenParts, []byte(partResult.Raw))
				continue
			}

			rawSignature, hasStringSignature := antigravityClaudeGeminiPartThoughtSignature(part)
			rawSigScan := scanAntigravityClaudeGeminiPartThoughtSignatureKeysInRaw(partResult.Raw)
			hasSignatureKey := hasStringSignature || antigravityClaudeGeminiPartHasThoughtSignatureKey(part) || rawSigScan.hasAny()

			if hasFunctionResponsePart(part) {
				if hasSignatureKey {
					changed = true
					contentChanged = true
					logAntigravityClaudeGeminiSignatureSanitize(modelName, "drop_signature", "functionResponse parts cannot replay Claude thinking signatures", contentIndex, partIndex, rawSignature)
					partDoc := stripAntigravityClaudeGeminiPartThoughtSignatureFields(part)
					rewrittenParts = append(rewrittenParts, partDoc.Bytes())
				} else {
					rewrittenParts = append(rewrittenParts, []byte(partResult.Raw))
				}
				continue
			}

			if !isModelTurn {
				if hasSignatureKey {
					changed = true
					contentChanged = true
					logAntigravityClaudeGeminiSignatureSanitize(modelName, "drop_signature", "non-model parts cannot replay Claude thinking signatures", contentIndex, partIndex, rawSignature)
					partDoc := stripAntigravityClaudeGeminiPartThoughtSignatureFields(part)
					rewrittenParts = append(rewrittenParts, partDoc.Bytes())
				} else {
					rewrittenParts = append(rewrittenParts, []byte(partResult.Raw))
				}
				continue
			}

			if part["thought"] == true {
				normalized, compatible := signature.CompatibleAntigravityClaudeThinkingSignature(rawSignature)
				if !compatible {
					changed = true
					contentChanged = true
					logAntigravityClaudeGeminiSignatureSanitize(modelName, "drop_thinking_block", "missing_or_incompatible_signature", contentIndex, partIndex, rawSignature)
					continue
				}
				text, _ := part["text"].(string)
				if strings.TrimSpace(text) == "" {
					changed = true
					contentChanged = true
					logAntigravityClaudeGeminiSignatureSanitize(modelName, "drop_thinking_block", "empty_thinking_text", contentIndex, partIndex, rawSignature)
					continue
				}
				canonicalVal, hasCanonicalString := stringAtPath(part, "thoughtSignature")
				hadNonCanonical := rawSigScan.hasNonCanonical ||
					rawSigScan.canonicalCount != 1 ||
					!hasCanonicalString ||
					canonicalVal != normalized
				if normalized != rawSignature || hadNonCanonical {
					changed = true
					contentChanged = true
					logAntigravityClaudeGeminiSignatureSanitize(modelName, "normalize_signature", "compatible_claude_signature", contentIndex, partIndex, rawSignature)
					partDoc := stripAntigravityClaudeGeminiPartThoughtSignatureFields(part)
					_ = partDoc.Set("thoughtSignature", normalized)
					rewrittenParts = append(rewrittenParts, partDoc.Bytes())
				} else {
					rewrittenParts = append(rewrittenParts, []byte(partResult.Raw))
				}
				continue
			}

			if hasSignatureKey {
				changed = true
				contentChanged = true
				logAntigravityClaudeGeminiSignatureSanitize(modelName, "drop_signature", "non-thinking parts should not carry Claude thinking signatures", contentIndex, partIndex, rawSignature)
				partDoc := stripAntigravityClaudeGeminiPartThoughtSignatureFields(part)
				rewrittenParts = append(rewrittenParts, partDoc.Bytes())
			} else {
				rewrittenParts = append(rewrittenParts, []byte(partResult.Raw))
			}
		}

		if len(rewrittenParts) == 0 {
			changed = true
			continue
		}
		if contentChanged || len(rewrittenParts) != len(partsArray) {
			contentDoc := ojson.NewDocString(content.Raw)
			_ = contentDoc.SetRawBytes("parts", JoinRawArray(rewrittenParts))
			rewrittenContents = append(rewrittenContents, contentDoc.Bytes())
		} else {
			rewrittenContents = append(rewrittenContents, []byte(content.Raw))
		}
	}

	if !changed {
		return false
	}
	return doc.SetRawBytes(contentsPath, JoinRawArray(rewrittenContents)) == nil
}

var antigravityClaudeGeminiThoughtSignaturePaths = [][]string{
	{"thoughtSignature"},
	{"thought_signature"},
	{"functionCall", "thoughtSignature"},
	{"functionCall", "thought_signature"},
	{"function_call", "thoughtSignature"},
	{"function_call", "thought_signature"},
	{"functionResponse", "thoughtSignature"},
	{"functionResponse", "thought_signature"},
	{"function_response", "thoughtSignature"},
	{"function_response", "thought_signature"},
	{"extra_content", "google", "thought_signature"},
}

type antigravityClaudeGeminiRawSignatureScan struct {
	canonicalCount  int
	hasNonCanonical bool
}

func (s antigravityClaudeGeminiRawSignatureScan) hasAny() bool {
	return s.canonicalCount > 0 || s.hasNonCanonical
}

type antigravityRawJSONFrame struct {
	isObject  bool
	expectKey bool
	key       string
}

func matchAntigravityClaudeGeminiSignatureStack(stack []antigravityRawJSONFrame) (bool, bool) {
	for _, frame := range stack {
		if !frame.isObject {
			return false, false
		}
	}
	for pathIdx, path := range antigravityClaudeGeminiThoughtSignaturePaths {
		if len(path) != len(stack) {
			continue
		}
		matched := true
		for i, key := range path {
			if stack[i].key != key {
				matched = false
				break
			}
		}
		if matched {
			return pathIdx == 0, pathIdx > 0
		}
	}
	return false, false
}

func scanAntigravityClaudeGeminiPartThoughtSignatureKeysInRaw(raw string) antigravityClaudeGeminiRawSignatureScan {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var stack []antigravityRawJSONFrame
	var scan antigravityClaudeGeminiRawSignatureScan

	for {
		t, errTok := dec.Token()
		if errTok != nil {
			break
		}

		switch v := t.(type) {
		case json.Delim:
			switch v {
			case '{':
				stack = append(stack, antigravityRawJSONFrame{isObject: true, expectKey: true})
			case '[':
				stack = append(stack, antigravityRawJSONFrame{isObject: false})
			case '}', ']':
				if len(stack) > 0 {
					stack = stack[:len(stack)-1]
				}
				if len(stack) > 0 && stack[len(stack)-1].isObject {
					stack[len(stack)-1].expectKey = true
					stack[len(stack)-1].key = ""
				}
			}
		case string:
			if len(stack) > 0 && stack[len(stack)-1].isObject {
				if stack[len(stack)-1].expectKey {
					stack[len(stack)-1].key = v
					stack[len(stack)-1].expectKey = false
					if isCanonical, isNonCanonical := matchAntigravityClaudeGeminiSignatureStack(stack); isCanonical {
						scan.canonicalCount++
					} else if isNonCanonical {
						scan.hasNonCanonical = true
					}
				} else {
					stack[len(stack)-1].expectKey = true
					stack[len(stack)-1].key = ""
				}
			}
		default:
			if len(stack) > 0 && stack[len(stack)-1].isObject {
				stack[len(stack)-1].expectKey = true
				stack[len(stack)-1].key = ""
			}
		}
	}
	return scan
}

func antigravityClaudeGeminiPartHasThoughtSignatureKey(part map[string]any) bool {
	for _, path := range antigravityClaudeGeminiThoughtSignaturePaths {
		if hasKeyAtPath(part, path...) {
			return true
		}
	}
	return false
}

func hasKeyAtPath(value map[string]any, path ...string) bool {
	var current any = value
	for _, key := range path {
		m, ok := current.(map[string]any)
		if !ok {
			return false
		}
		if _, exists := m[key]; !exists {
			return false
		}
		current = m[key]
	}
	return true
}

func antigravityClaudeGeminiPartThoughtSignature(part map[string]any) (string, bool) {
	for _, path := range antigravityClaudeGeminiThoughtSignaturePaths {
		if value, ok := stringAtPath(part, path...); ok {
			return value, true
		}
	}
	return "", false
}

func stripAntigravityClaudeGeminiPartThoughtSignatureFields(part map[string]any) *ojson.Doc {
	// Re-encode the decoded object so duplicate keys resolve to their last value.
	raw, _ := json.Marshal(part)
	doc := ojson.NewDoc(raw)
	for _, path := range antigravityClaudeGeminiThoughtSignaturePaths {
		if hasKeyAtPath(part, path...) {
			_ = doc.Delete(strings.Join(path, "."))
		}
	}
	if google := doc.Get("extra_content.google"); google.IsObject() && len(google.Map()) == 0 {
		_ = doc.Delete("extra_content.google")
	}
	if extraContent := doc.Get("extra_content"); extraContent.IsObject() && len(extraContent.Map()) == 0 {
		_ = doc.Delete("extra_content")
	}
	return doc
}

func hasFunctionResponsePart(part map[string]any) bool {
	if _, ok := part["functionResponse"]; ok {
		return true
	}
	_, ok := part["function_response"]
	return ok
}

func stringAtPath(value map[string]any, path ...string) (string, bool) {
	var current any = value
	for _, key := range path {
		m, ok := current.(map[string]any)
		if !ok {
			return "", false
		}
		current, ok = m[key]
		if !ok {
			return "", false
		}
	}
	s, ok := current.(string)
	return s, ok
}

func logAntigravityClaudeGeminiSignatureSanitize(modelName, action, reason string, contentIndex, partIndex int, rawSignature string) {
	fields := log.Fields{
		"component":         "signature_sanitizer",
		"translator":        "antigravity_gemini",
		"target_provider":   string(signature.SignatureProviderClaude),
		"action":            action,
		"reason":            reason,
		"model":             modelName,
		"content_index":     contentIndex,
		"part_index":        partIndex,
		"has_signature":     strings.TrimSpace(rawSignature) != "",
		"signature_length":  len(strings.TrimSpace(rawSignature)),
		"detected_provider": string(signature.DetectSignatureProviderForBlock(rawSignature, signature.SignatureBlockKindClaudeThinking)),
	}
	log.WithFields(fields).Debug("antigravity gemini translator: sanitized Claude target thoughtSignature before upstream")
}
