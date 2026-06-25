use std::borrow::Cow;

use crate::upstream::body::decode_compressed_body;
use crate::upstream::client::UpstreamResponse;
use async_compression::tokio::bufread::{BrotliEncoder, GzipEncoder, ZlibEncoder, ZstdEncoder};
use axum::http::header::CONTENT_ENCODING;
use axum::http::HeaderMap;
use bytes::Bytes;
use tokio::io::AsyncReadExt;

const BODY: &[u8] = br#"{"name": "alice", "version": 1}"#;

fn response(encoding: Option<&str>, body: Vec<u8>) -> UpstreamResponse {
    let mut headers = HeaderMap::new();
    if let Some(enc) = encoding {
        headers.insert(CONTENT_ENCODING, enc.parse().unwrap());
    }
    UpstreamResponse {
        status: 200,
        headers,
        body: Bytes::from(body),
    }
}

async fn read_all<R: tokio::io::AsyncRead + Unpin>(mut reader: R) -> Vec<u8> {
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    out
}

#[tokio::test]
async fn absent_encoding_returns_borrowed_body() {
    let resp = response(None, BODY.to_vec());
    let decoded = decode_compressed_body(&resp).await.unwrap();
    assert!(matches!(decoded, Cow::Borrowed(_)));
    assert_eq!(&*decoded, BODY);
}

#[tokio::test]
async fn identity_encoding_returns_borrowed_body() {
    let resp = response(Some("identity"), BODY.to_vec());
    let decoded = decode_compressed_body(&resp).await.unwrap();
    assert!(matches!(decoded, Cow::Borrowed(_)));
    assert_eq!(&*decoded, BODY);
}

#[tokio::test]
async fn gzip_body_is_decompressed() {
    let compressed = read_all(GzipEncoder::new(BODY)).await;
    let resp = response(Some("gzip"), compressed);
    let decoded = decode_compressed_body(&resp).await.unwrap();
    assert_eq!(&*decoded, BODY);
}

#[tokio::test]
async fn x_gzip_alias_is_decompressed() {
    let compressed = read_all(GzipEncoder::new(BODY)).await;
    let resp = response(Some("x-gzip"), compressed);
    let decoded = decode_compressed_body(&resp).await.unwrap();
    assert_eq!(&*decoded, BODY);
}

#[tokio::test]
async fn deflate_body_is_decompressed_as_zlib() {
    let compressed = read_all(ZlibEncoder::new(BODY)).await;
    let resp = response(Some("deflate"), compressed);
    let decoded = decode_compressed_body(&resp).await.unwrap();
    assert_eq!(&*decoded, BODY);
}

#[tokio::test]
async fn brotli_body_is_decompressed() {
    let compressed = read_all(BrotliEncoder::new(BODY)).await;
    let resp = response(Some("br"), compressed);
    let decoded = decode_compressed_body(&resp).await.unwrap();
    assert_eq!(&*decoded, BODY);
}

#[tokio::test]
async fn zstd_body_is_decompressed() {
    let compressed = read_all(ZstdEncoder::new(BODY)).await;
    let resp = response(Some("zstd"), compressed);
    let decoded = decode_compressed_body(&resp).await.unwrap();
    assert_eq!(&*decoded, BODY);
}

#[tokio::test]
async fn encoding_is_case_insensitive() {
    let compressed = read_all(GzipEncoder::new(BODY)).await;
    let resp = response(Some("GZip"), compressed);
    let decoded = decode_compressed_body(&resp).await.unwrap();
    assert_eq!(&*decoded, BODY);
}

#[tokio::test]
async fn unsupported_encoding_is_skipped() {
    let resp = response(Some("compress"), BODY.to_vec());
    assert!(decode_compressed_body(&resp).await.is_none());
}

#[tokio::test]
async fn corrupt_gzip_body_is_skipped() {
    let resp = response(Some("gzip"), b"definitely not gzip".to_vec());
    assert!(decode_compressed_body(&resp).await.is_none());
}
