package relay

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// A non-streaming client such as OMP's image tool still has to reach the
// Codex backend through SSE, and the image_generation_call it produces must
// be counted for tool billing.
func TestCodexResponsesForcesStreamAndBillsImageGeneration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	oldTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() {
		constant.StreamingTimeout = oldTimeout
	})

	var upstreamBody []byte
	var upstreamEncoding string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		upstreamBody, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		upstreamEncoding = r.Header.Get("Content-Encoding")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"type":"response.output_item.done","output_index":0,"item":{"type":"image_generation_call","id":"img_1","status":"completed","result":"base64-a"}}`+"\n\n")
		_, _ = io.WriteString(w, `data: {"type":"response.completed","response":{"id":"resp_1","model":"gpt-5.5","status":"completed","output":[{"type":"image_generation_call","id":"img_1","status":"completed","result":"base64-a"}],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer server.Close()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("Content-Type", "application/json")
	common.SetContextKey(c, constant.ContextKeyOriginalModel, "gpt-5.5")
	common.SetContextKey(c, constant.ContextKeyChannelType, constant.ChannelTypeCodexImage)
	common.SetContextKey(c, constant.ContextKeyChannelBaseUrl, server.URL)
	common.SetContextKey(c, constant.ContextKeyChannelKey, `{"access_token":"token","account_id":"account"}`)

	request := &dto.OpenAIResponsesRequest{
		Model:      "gpt-5.5",
		Input:      []byte(`[{"role":"user","content":[{"type":"input_text","text":"draw a cat"}]}]`),
		Tools:      []byte(`[{"type":"image_generation","model":"gpt-image-2"}]`),
		ToolChoice: []byte(`{"type":"image_generation"}`),
		Store:      []byte(`false`),
	}
	info := relaycommon.GenRelayInfoResponses(c, request)
	info.DisablePing = true

	adaptor, body, closer, apiErr := PrepareResponsesRequest(c, info, request, true)
	require.Nil(t, apiErr)
	defer closer.Close()
	require.True(t, info.IsStream)

	response, err := adaptor.DoRequest(c, info, body)
	require.NoError(t, err)
	require.Equal(t, "zstd", upstreamEncoding)
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	upstreamBody, err = decoder.DecodeAll(upstreamBody, nil)
	require.NoError(t, err)
	httpResponse, ok := response.(*http.Response)
	require.True(t, ok)
	usage, apiErr := adaptor.DoResponse(c, httpResponse, info)
	require.Nil(t, apiErr)
	require.NotNil(t, usage)

	assert.True(t, gjson.GetBytes(upstreamBody, "stream").Bool())
	assert.Equal(t, "image_generation", gjson.GetBytes(upstreamBody, "tools.0.type").String())
	assert.Equal(t, "gpt-image-2", gjson.GetBytes(upstreamBody, "tools.0.model").String())
	require.Contains(t, info.ResponsesUsageInfo.BuiltInTools, dto.BuildInToolImageGeneration)
	assert.Equal(t, 1, info.ResponsesUsageInfo.BuiltInTools[dto.BuildInToolImageGeneration].CallCount)
	assert.Contains(t, w.Body.String(), `"type":"image_generation_call"`)
	assert.Contains(t, w.Body.String(), `"result":"base64-a"`)
}

// The regular Codex channel keeps the client's non-streaming request shape.
func TestRegularCodexResponsesKeepsNonStreamingRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var upstreamBody []byte
	var upstreamEncoding string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		upstreamBody, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		upstreamEncoding = r.Header.Get("Content-Encoding")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"resp_1","object":"response","model":"gpt-5.5","status":"completed","output":[],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}`)
	}))
	defer server.Close()

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("Content-Type", "application/json")
	common.SetContextKey(c, constant.ContextKeyOriginalModel, "gpt-5.5")
	common.SetContextKey(c, constant.ContextKeyChannelType, constant.ChannelTypeCodex)
	common.SetContextKey(c, constant.ContextKeyChannelBaseUrl, server.URL)
	common.SetContextKey(c, constant.ContextKeyChannelKey, `{"access_token":"token","account_id":"account"}`)

	request := &dto.OpenAIResponsesRequest{
		Model: "gpt-5.5",
		Input: []byte(`[{"role":"user","content":[{"type":"input_text","text":"hello"}]}]`),
	}
	info := relaycommon.GenRelayInfoResponses(c, request)

	adaptor, body, closer, apiErr := PrepareResponsesRequest(c, info, request, true)
	require.Nil(t, apiErr)
	defer closer.Close()
	require.False(t, info.IsStream)

	response, err := adaptor.DoRequest(c, info, body)
	require.NoError(t, err)
	require.Equal(t, "zstd", upstreamEncoding)
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	upstreamBody, err = decoder.DecodeAll(upstreamBody, nil)
	require.NoError(t, err)
	httpResponse, ok := response.(*http.Response)
	require.True(t, ok)
	_, apiErr = adaptor.DoResponse(c, httpResponse, info)
	require.Nil(t, apiErr)

	assert.False(t, gjson.GetBytes(upstreamBody, "stream").Bool())
	assert.Equal(t, "resp_1", gjson.GetBytes(w.Body.Bytes(), "id").String())
}

// Pass-through mode still has to compress: the client's original bytes are
// decompressed at the edge, so forwarding them unchanged would send the full
// JSON body to the Codex backend.
func TestCodexResponsesPassThroughCompressesBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	payload := `{"model":"gpt-5.5","input":[{"role":"user","content":"hello"}],"stream":true}`
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(payload))
	c.Request.Header.Set("Content-Type", "application/json")
	common.SetContextKey(c, constant.ContextKeyOriginalModel, "gpt-5.5")
	common.SetContextKey(c, constant.ContextKeyChannelType, constant.ChannelTypeCodex)
	common.SetContextKey(c, constant.ContextKeyChannelSetting, dto.ChannelSettings{PassThroughBodyEnabled: true})
	stream := true
	request := &dto.OpenAIResponsesRequest{Model: "gpt-5.5", Input: []byte(`[{"role":"user","content":"hello"}]`), Stream: &stream}
	info := relaycommon.GenRelayInfoResponses(c, request)

	_, body, closer, apiErr := PrepareResponsesRequest(c, info, request, true)
	require.Nil(t, apiErr)
	defer closer.Close()
	encoded, ok := body.(interface{ ContentEncoding() string })
	require.True(t, ok)
	assert.Equal(t, "zstd", encoded.ContentEncoding())

	compressed, err := io.ReadAll(body)
	require.NoError(t, err)
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(compressed, nil)
	require.NoError(t, err)
	assert.JSONEq(t, payload, string(decoded))
}
