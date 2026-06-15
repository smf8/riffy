use std::borrow::Cow;

use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder, ZlibDecoder, ZstdDecoder};
use tokio::io::AsyncReadExt;

use crate::upstream::client::UpstreamResponse;

/// Returns the response body ready for JSON parsing, decompressing it when a
/// supported `content-encoding` is present. `None` when the encoding is
/// unsupported or the body fails to decompress.
pub async fn decode_body(response: &UpstreamResponse) -> Option<Cow<'_, [u8]>> {
    let Some(value) = response.headers.get(axum::http::header::CONTENT_ENCODING) else {
        return Some(Cow::Borrowed(response.body.as_ref()));
    };

    let Ok(encoding) = value.to_str() else {
        tracing::warn!("non-ascii content-encoding header, skipping body");
        return None;
    };
    let encoding = encoding.trim().to_ascii_lowercase();

    let compressed = response.body.as_ref();
    let result = match encoding.as_str() {
        "" | "identity" => return Some(Cow::Borrowed(compressed)),
        "gzip" | "x-gzip" => read_all(GzipDecoder::new(compressed)).await,
        "deflate" => read_all(ZlibDecoder::new(compressed)).await,
        "br" => read_all(BrotliDecoder::new(compressed)).await,
        "zstd" => read_all(ZstdDecoder::new(compressed)).await,
        other => {
            tracing::warn!(encoding = %other, "unsupported content-encoding, skipping body");
            return None;
        }
    };

    match result {
        Ok(bytes) => Some(Cow::Owned(bytes)),
        Err(e) => {
            tracing::warn!(error = %e, encoding = %encoding, "failed to decompress body, skipping");
            None
        }
    }
}

async fn read_all<R: tokio::io::AsyncRead + Unpin>(mut reader: R) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await?;
    Ok(out)
}
