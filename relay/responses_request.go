package relay

import (
	"fmt"
	"io"
	"net/http"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	relaychannel "github.com/QuantumNous/new-api/relay/channel"
	"github.com/QuantumNous/new-api/relay/channel/codex"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/model_setting"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// responsesBodyCompressionEnabled reports whether the outbound body should be
// zstd-compressed. Only the Codex backend accepts Content-Encoding: zstd, and
// only its /responses endpoint uses the compressed JSON body.
func responsesBodyCompressionEnabled(info *relaycommon.RelayInfo, compress bool) bool {
	return compress && info.RelayMode == relayconstant.RelayModeResponses && constant.IsCodexChannel(info.ChannelType)
}

func newResponsesRequestBody(info *relaycommon.RelayInfo, compress bool, jsonData []byte) (common.ReplayableBody, io.Closer, error) {
	if responsesBodyCompressionEnabled(info, compress) {
		return relaycommon.NewOutboundZstdJSONBody(jsonData)
	}
	return relaycommon.NewOutboundJSONBody(jsonData)
}

// PrepareResponsesRequest applies the same model, conversion and channel rules
// for HTTP and WebSocket requests. compressRequestBody enables zstd compression
// of the HTTP upstream body for Codex /responses; WebSocket callers pass false
// because they wrap the JSON in a text event. The caller closes closer after
// the attempt; passthrough bodies remain owned by the incoming request's
// BodyStorage. The returned adaptor retains route/conversion state for
// DoRequest/DoResponse.
func PrepareResponsesRequest(c *gin.Context, info *relaycommon.RelayInfo, req *dto.OpenAIResponsesRequest, compressRequestBody bool) (relaychannel.Adaptor, common.ReplayableBody, io.Closer, *types.NewAPIError) {
	info.InitChannelMeta(c)
	if info.RelayMode == relayconstant.RelayModeResponsesCompact &&
		!common.SupportsResponsesCompact(info.ChannelType, info.ApiType) {
		return nil, nil, nil, types.NewErrorWithStatusCode(
			fmt.Errorf("unsupported endpoint %q for api type %d", "/v1/responses/compact", info.ApiType),
			types.ErrorCodeInvalidRequest,
			http.StatusBadRequest,
			types.ErrOptionWithSkipRetry(),
		)
	}

	request, err := common.DeepCopy(req)
	if err != nil {
		return nil, nil, nil, types.NewError(fmt.Errorf("failed to copy responses request: %w", err), types.ErrorCodeInvalidRequest, types.ErrOptionWithSkipRetry())
	}
	if err := helper.ModelMappedHelper(c, info, request); err != nil {
		return nil, nil, nil, types.NewError(err, types.ErrorCodeChannelModelMappedError, types.ErrOptionWithSkipRetry())
	}
	if err := helper.ApplyReasoningModelSuffix(c, info, request); err != nil {
		return nil, nil, nil, newConvertRequestFailedError(c, info, err)
	}

	adaptor := GetAdaptor(info.ApiType)
	if adaptor == nil {
		return nil, nil, nil, types.NewError(fmt.Errorf("invalid api type: %d", info.ApiType), types.ErrorCodeInvalidApiType, types.ErrOptionWithSkipRetry())
	}
	adaptor.Init(info)
	codexResponses := constant.IsCodexChannel(info.ChannelType) &&
		info.RelayMode == relayconstant.RelayModeResponses
	forceStream := info.ChannelType == constant.ChannelTypeCodexImage && info.RelayMode == relayconstant.RelayModeResponses
	if forceStream {
		// The Codex image-tool channel always streams. The inbound client may
		// have asked for a non-streaming response, but the upstream contract is
		// SSE; the handler forwards those events unchanged.
		info.IsStream = true
	}
	if model_setting.GetGlobalSettings().PassThroughRequestEnabled || info.ChannelSetting.PassThroughBodyEnabled {
		storage, err := common.GetBodyStorage(c)
		if err != nil {
			return nil, nil, nil, types.NewError(err, types.ErrorCodeReadRequestBodyFailed, types.ErrOptionWithSkipRetry())
		}
		// Subscription history needs the same declaration in passthrough mode,
		// and the Codex backend rejects a body without stream=true. Requests
		// that need no edit stay on the original BodyStorage unless the
		// compressed upstream body requires materializing it.
		needsStreamPatch := forceStream && (request.Stream == nil || !*request.Stream)
		needsSearchPatch := codexResponses && gjson.GetBytes(request.Input, `#(type=="web_search_call")`).Exists()
		var jsonData []byte
		if needsStreamPatch || needsSearchPatch {
			jsonData, err = storage.Bytes()
			if err != nil {
				return nil, nil, nil, types.NewError(err, types.ErrorCodeReadRequestBodyFailed, types.ErrOptionWithSkipRetry())
			}
			changed := false
			if needsSearchPatch {
				lite, err := codex.ResponsesLiteEnabled(c, info, jsonData)
				if err != nil {
					return nil, nil, nil, types.NewError(err, types.ErrorCodeChannelHeaderOverrideInvalid, types.ErrOptionWithSkipRetry())
				}
				jsonData, changed, err = codex.EnsureWebSearchHistoryTool(jsonData, lite)
				if err != nil {
					return nil, nil, nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
				}
			}
			if needsStreamPatch {
				jsonData, err = sjson.SetBytes(jsonData, "stream", true)
				if err != nil {
					return nil, nil, nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
				}
				changed = true
			}
			if changed {
				body, closer, err := newResponsesRequestBody(info, compressRequestBody, jsonData)
				if err != nil {
					return nil, nil, nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
				}
				return adaptor, body, closer, nil
			}
		}
		if responsesBodyCompressionEnabled(info, compressRequestBody) {
			if jsonData == nil {
				jsonData, err = storage.Bytes()
				if err != nil {
					return nil, nil, nil, types.NewError(err, types.ErrorCodeReadRequestBodyFailed, types.ErrOptionWithSkipRetry())
				}
			}
			body, closer, err := newResponsesRequestBody(info, compressRequestBody, jsonData)
			if err != nil {
				return nil, nil, nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
			}
			return adaptor, body, closer, nil
		}
		body := common.NewReplayableBodyReader(storage)
		return adaptor, body, io.NopCloser(body), nil
	}

	convertedRequest, err := adaptor.ConvertOpenAIResponsesRequest(c, info, *request)
	if err != nil {
		return nil, nil, nil, newConvertRequestFailedError(c, info, err)
	}
	relaycommon.AppendRequestConversionFromRequest(info, convertedRequest)
	jsonData, err := common.Marshal(convertedRequest)
	if err != nil {
		return nil, nil, nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	jsonData, err = relaycommon.RemoveDisabledFields(jsonData, info.ChannelOtherSettings, info.ChannelSetting.PassThroughBodyEnabled)
	if err != nil {
		return nil, nil, nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	if len(info.ParamOverride) > 0 {
		jsonData, err = relaycommon.ApplyParamOverrideWithRelayInfo(jsonData, info)
		if err != nil {
			return nil, nil, nil, newAPIErrorFromParamOverride(err)
		}
	}
	if codexResponses {
		lite, err := codex.ResponsesLiteEnabled(c, info, jsonData)
		if err != nil {
			return nil, nil, nil, types.NewError(err, types.ErrorCodeChannelHeaderOverrideInvalid, types.ErrOptionWithSkipRetry())
		}
		jsonData, _, err = codex.EnsureWebSearchHistoryTool(jsonData, lite)
		if err != nil {
			return nil, nil, nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}
	}
	if forceStream {
		// Param overrides run after conversion; keep the upstream contract
		// last so a channel override cannot turn streaming back off.
		jsonData, err = sjson.SetBytes(jsonData, "stream", true)
		if err != nil {
			return nil, nil, nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}
	}

	if finalizer, ok := adaptor.(relaychannel.ResponsesRequestFinalizer); ok {
		jsonData, err = finalizer.FinalizeResponsesRequest(info, jsonData)
		if err != nil {
			return nil, nil, nil, newConvertRequestFailedError(c, info, err)
		}
	}

	logger.LogDebug(c, "requestBody: %s", jsonData)
	body, closer, err := newResponsesRequestBody(info, compressRequestBody, jsonData)
	if err != nil {
		return nil, nil, nil, types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	return adaptor, body, closer, nil
}
