package codex

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/QuantumNous/new-api/relay/channel"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const webSearchHistoryTool = `{"type":"web_search","external_web_access":false}`

const ResponsesLiteHeader = "x-openai-internal-codex-responses-lite"
const responsesLiteMetadataPath = "client_metadata.ws_request_header_x_openai_internal_codex_responses_lite"

// ResponsesLiteEnabled follows the outbound header override precedence. The
// WebSocket metadata marker supplies the per-message mode when no channel
// override specifies it; body declarations must match the selected protocol.
func ResponsesLiteEnabled(c *gin.Context, info *relaycommon.RelayInfo, body []byte) (bool, error) {
	lite := c.GetHeader(ResponsesLiteHeader) == "true" || gjson.GetBytes(body, responsesLiteMetadataPath).Bool()
	headers, err := channel.ResolveHeaderOverride(info, c)
	if err != nil {
		return false, err
	}
	for name, value := range headers {
		if strings.EqualFold(name, ResponsesLiteHeader) {
			return value == "true", nil
		}
	}
	return lite, nil
}

// EnsureWebSearchHistoryTool declares the hosted tool needed to replay Codex
// search history. The subscription backend rejects that history without a
// declaration, including the tool-free requests used for context compaction.
// Existing history and tool choices stay intact; a request that offered no
// tools cannot execute the declaration added solely for history replay.
// The native /responses/compact endpoint has a different schema and callers
// must only apply this compatibility rule to /responses requests.
func EnsureWebSearchHistoryTool(body []byte, responsesLite bool) ([]byte, bool, error) {
	input := gjson.GetBytes(body, "input")
	if !input.Get(`#(type=="web_search_call")`).Exists() {
		return body, false, nil
	}
	changed := false
	var err error
	// A configured channel header also governs an existing per-message marker.
	if marker := gjson.GetBytes(body, responsesLiteMetadataPath); marker.Exists() && marker.Bool() != responsesLite {
		body, err = sjson.SetBytes(body, responsesLiteMetadataPath, fmt.Sprint(responsesLite))
		if err != nil {
			return nil, false, err
		}
		changed = true
	}
	tools := gjson.GetBytes(body, "tools")
	if containsWebSearchTool(tools) {
		return body, changed, nil
	}
	items := input.Array()
	additionalIndex := -1
	callerHasTools := len(tools.Array()) > 0
	for i, item := range items {
		if item.Get("type").String() != "additional_tools" {
			continue
		}
		itemTools := item.Get("tools")
		if containsWebSearchTool(itemTools) {
			return body, changed, nil
		}
		callerHasTools = callerHasTools || len(itemTools.Array()) > 0
		if additionalIndex < 0 {
			additionalIndex = i
		}
	}

	switch {
	case !responsesLite && tools.IsArray():
		body, err = sjson.SetRawBytes(body, "tools.-1", []byte(webSearchHistoryTool))
	case !responsesLite:
		body, err = sjson.SetRawBytes(body, "tools", []byte("["+webSearchHistoryTool+"]"))
	case additionalIndex >= 0:
		path := fmt.Sprintf("input.%d.tools", additionalIndex)
		if items[additionalIndex].Get("tools").IsArray() {
			body, err = sjson.SetRawBytes(body, path+".-1", []byte(webSearchHistoryTool))
		} else {
			body, err = sjson.SetRawBytes(body, path, []byte("["+webSearchHistoryTool+"]"))
		}
	default:
		// Responses Lite rejects top-level hosted tools. Preserve each raw
		// history item and keep the V2 compaction trigger as the final item.
		at := len(items)
		if items[at-1].Get("type").String() == "compaction_trigger" {
			at--
		}
		additional := []byte(`{"type":"additional_tools","role":"developer","tools":[` + webSearchHistoryTool + `]}`)
		rawItems := make([][]byte, 0, len(items)+1)
		for i, item := range items {
			if i == at {
				rawItems = append(rawItems, additional)
			}
			rawItems = append(rawItems, []byte(item.Raw))
		}
		if at == len(items) {
			rawItems = append(rawItems, additional)
		}
		rawInput := append([]byte{'['}, bytes.Join(rawItems, []byte{','})...)
		rawInput = append(rawInput, ']')
		body, err = sjson.SetRawBytes(body, "input", rawInput)
	}
	if err != nil {
		return nil, false, err
	}
	if !callerHasTools {
		choice := gjson.GetBytes(body, "tool_choice")
		if choice.Type == gjson.Null || (choice.Type == gjson.String && (choice.String() == "auto" || choice.String() == "none")) {
			body, err = sjson.SetBytes(body, "tool_choice", "none")
			if err != nil {
				return nil, false, err
			}
		}
	}
	return body, true, nil
}

func containsWebSearchTool(tools gjson.Result) bool {
	for _, tool := range tools.Array() {
		if strings.HasPrefix(tool.Get("type").String(), "web_search") {
			return true
		}
	}
	return false
}
