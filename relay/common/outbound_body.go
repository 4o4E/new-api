package common

import (
	"io"

	"github.com/QuantumNous/new-api/common"
	"github.com/klauspost/compress/zstd"
)

// requestBodyZstdEncoder matches the Codex client's request compression level.
// EncodeAll is safe for concurrent use.
var requestBodyZstdEncoder = func() *zstd.Encoder {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(err)
	}
	return encoder
}()

// encodedReplayableBody exposes the Content-Encoding that the upstream request
// must declare for the bytes it carries.
type encodedReplayableBody struct {
	common.ReplayableBody
	encoding string
}

func (b encodedReplayableBody) ContentEncoding() string {
	return b.encoding
}

// NewOutboundJSONBody wraps the already-marshaled upstream request body into a
// BodyStorage. When disk cache is enabled and the payload exceeds the configured
// threshold, the data is written to a temp file and the original []byte can be
// GC'd, significantly reducing the heap residency while waiting for the
// upstream provider to respond (the dominant cost for large base64 payloads).
//
// In memory mode the underlying memoryStorage reuses the same backing array,
// so this is equivalent to bytes.NewReader(data) in terms of memory usage.
//
// The caller MUST invoke closer.Close() once the upstream call has finished
// (typically via defer) to release the disk file / memory accounting.
//
// The returned body exposes its size and replay capability without exposing
// io.Closer. Request construction uses that metadata to populate ContentLength
// and GetBody, while the caller retains ownership of the underlying storage
// through the separately returned closer.
func NewOutboundJSONBody(data []byte) (body common.ReplayableBody, closer io.Closer, err error) {
	storage, err := common.CreateBodyStorage(data)
	if err != nil {
		return nil, nil, err
	}
	return common.NewReplayableBodyReader(storage), storage, nil
}

// NewOutboundZstdJSONBody compresses the upstream request body with zstd and
// marks it for the Content-Encoding header. Compression happens before the
// body is stored, so retries and redirects replay the compressed bytes.
func NewOutboundZstdJSONBody(data []byte) (body common.ReplayableBody, closer io.Closer, err error) {
	compressed := requestBodyZstdEncoder.EncodeAll(data, make([]byte, 0, len(data)/4))
	storage, err := common.CreateBodyStorage(compressed)
	if err != nil {
		return nil, nil, err
	}
	return encodedReplayableBody{
		ReplayableBody: common.NewReplayableBodyReader(storage),
		encoding:       "zstd",
	}, storage, nil
}
