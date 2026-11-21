package config

import (
	"bytes"
	"errors"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

const maxConfigWarningContentRunes = 256

type configYAMLWarning struct {
	line   int
	detail string
}

// configTypeMismatchWarnings keeps a partially decoded config when all failures
// are value type mismatches. Structural errors still fail the load.
func configTypeMismatchWarnings(errDecode error) ([]configYAMLWarning, error) {
	if errDecode == nil {
		return nil, nil
	}

	var typeError *yaml.TypeError
	if !errors.As(errDecode, &typeError) {
		return nil, errDecode
	}

	warnings := make([]configYAMLWarning, 0, len(typeError.Errors))
	for _, issue := range typeError.Errors {
		warning, okMismatch := configTypeMismatchWarning(issue)
		if !okMismatch {
			return nil, errDecode
		}
		warnings = append(warnings, warning)
	}
	return warnings, nil
}

func logConfigYAMLWarnings(data []byte, warnings []configYAMLWarning) {
	for _, warning := range warnings {
		log.WithFields(log.Fields{
			"content": redactedConfigSourceLine(data, warning.line),
			"error":   warning.detail,
			"line":    warning.line,
		}).Warn("ignoring invalid config value")
	}
}

func configTypeMismatchWarning(issue string) (configYAMLWarning, bool) {
	line := 0
	detail := strings.TrimSpace(issue)
	if remainder, okPrefix := strings.CutPrefix(detail, "line "); okPrefix {
		if lineText, message, okLine := strings.Cut(remainder, ": "); okLine {
			if parsedLine, errLine := strconv.Atoi(lineText); errLine == nil {
				line = parsedLine
				detail = message
			}
		}
	}
	const prefix = "cannot unmarshal "
	if !strings.HasPrefix(detail, prefix) {
		return configYAMLWarning{}, false
	}
	sourceAndValue, target, okTarget := strings.Cut(strings.TrimPrefix(detail, prefix), " into ")
	if !okTarget {
		return configYAMLWarning{}, false
	}
	sourceType := sourceAndValue
	if valueAt := strings.IndexByte(sourceType, ' '); valueAt >= 0 {
		sourceType = sourceType[:valueAt]
	}
	return configYAMLWarning{
		line:   line,
		detail: prefix + sourceType + " into " + target,
	}, true
}

func redactedConfigSourceLine(data []byte, line int) string {
	lines := bytes.Split(data, []byte("\n"))
	if line < 1 || line > len(lines) {
		return "<unavailable>"
	}
	raw := strings.TrimSpace(strings.TrimSuffix(string(lines[line-1]), "\r"))
	if raw == "" {
		return "<empty>"
	}

	var document yaml.Node
	if errParse := yaml.Unmarshal([]byte(raw), &document); errParse != nil {
		return "<redacted>"
	}
	redactConfigLogNode(&document, false)
	rendered, errMarshal := yaml.Marshal(&document)
	if errMarshal != nil {
		return "<redacted>"
	}
	content := strings.Join(strings.Fields(string(rendered)), " ")
	if content == "" {
		return "<empty>"
	}
	runes := []rune(content)
	if len(runes) > maxConfigWarningContentRunes {
		content = string(runes[:maxConfigWarningContentRunes]) + "..."
	}
	return content
}

func redactConfigLogNode(node *yaml.Node, mappingKey bool) {
	if node == nil {
		return
	}
	node.Anchor = ""
	node.HeadComment = ""
	node.LineComment = ""
	node.FootComment = ""

	switch node.Kind {
	case yaml.DocumentNode:
		for _, child := range node.Content {
			redactConfigLogNode(child, false)
		}
	case yaml.MappingNode:
		for index := 0; index+1 < len(node.Content); index += 2 {
			redactConfigLogNode(node.Content[index], true)
			redactConfigLogNode(node.Content[index+1], false)
		}
	case yaml.SequenceNode:
		for _, child := range node.Content {
			redactConfigLogNode(child, false)
		}
	case yaml.AliasNode:
		node.Value = "<redacted>"
		node.Alias = nil
	case yaml.ScalarNode:
		if mappingKey || node.ShortTag() == "!!null" {
			return
		}
		node.Tag = "!!str"
		node.Value = "<redacted>"
		node.Style = 0
	}
}
