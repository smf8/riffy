use std::time::Duration;

use super::error::StoreError;
use super::{RawSample, SampleStore, SAMPLE_KEY_PREFIX};
use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamMaxlen, StreamRangeReply};
use redis::{from_redis_value, AsyncCommands, Value};

// Tracks endpoints that have a stream, so `list_endpoints` avoids a keyspace scan.
const ENDPOINTS_INDEX: &str = "riffy:samples:__endpoints__";

fn stream_key(endpoint: &str) -> String {
    format!("{SAMPLE_KEY_PREFIX}:{endpoint}")
}

pub struct RedisSampleStore {
    conn: ConnectionManager,
    sample_cap: usize,
    window_secs: u64,
}

impl RedisSampleStore {
    pub async fn connect(
        uri: &str,
        sample_cap: usize,
        window: Duration,
    ) -> Result<Self, StoreError> {
        let client = redis::Client::open(uri).map_err(StoreError::Redis)?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(StoreError::Redis)?;

        Ok(Self {
            conn,
            sample_cap: sample_cap.max(1),
            window_secs: window.as_secs().max(1),
        })
    }
}

#[async_trait::async_trait]
impl SampleStore for RedisSampleStore {
    async fn append_sample(&self, sample: &RawSample) -> Result<(), StoreError> {
        let mut fields: Vec<(&str, String)> = vec![
            ("timestamp", sample.timestamp.to_rfc3339()),
            ("baseline_status", sample.baseline_status.to_string()),
            ("baseline_body", sample.baseline_body.clone()),
            ("baseline_headers", sample.baseline_headers.clone()),
        ];
        if let Some(status) = sample.candidate_status {
            fields.push(("candidate_status", status.to_string()));
        }
        if let Some(body) = &sample.candidate_body {
            fields.push(("candidate_body", body.clone()));
        }
        if let Some(headers) = &sample.candidate_headers {
            fields.push(("candidate_headers", headers.clone()));
        }
        if let Some(status) = sample.control_status {
            fields.push(("control_status", status.to_string()));
        }
        if let Some(body) = &sample.control_body {
            fields.push(("control_body", body.clone()));
        }
        if let Some(headers) = &sample.control_headers {
            fields.push(("control_headers", headers.clone()));
        }
        if let Some(curl) = &sample.request_curl {
            fields.push(("request_curl", curl.clone()));
        }

        let key = stream_key(&sample.endpoint);
        // `~` trims whole macro-nodes (far cheaper than exact); EXPIRE ages out
        // endpoints whose samples have all left the window.
        let mut pipe = redis::pipe();
        pipe.xadd_maxlen(&key, StreamMaxlen::Approx(self.sample_cap), "*", &fields)
            .ignore();
        pipe.expire(&key, self.window_secs as i64).ignore();
        pipe.sadd(ENDPOINTS_INDEX, &sample.endpoint).ignore();

        let mut conn = self.conn.clone();
        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(StoreError::Redis)?;
        Ok(())
    }

    async fn fetch_samples(&self, endpoint: &str) -> Result<Vec<RawSample>, StoreError> {
        let mut conn = self.conn.clone();
        let reply: StreamRangeReply = conn
            .xrevrange_count(stream_key(endpoint), "+", "-", self.sample_cap)
            .await
            .map_err(StoreError::Redis)?;

        let now = Utc::now();
        let mut out = Vec::with_capacity(reply.ids.len());
        for entry in &reply.ids {
            let sample = sample_from_entry(entry, endpoint)?;
            if now.signed_duration_since(sample.timestamp).num_seconds() <= self.window_secs as i64
            {
                out.push(sample);
            }
        }
        Ok(out)
    }

    async fn get_sample(&self, endpoint: &str, id: &str) -> Result<Option<RawSample>, StoreError> {
        let mut conn = self.conn.clone();
        // XRANGE key id id returns the single entry with that exact stream id.
        let reply: StreamRangeReply = conn
            .xrange(stream_key(endpoint), id, id)
            .await
            .map_err(StoreError::Redis)?;
        match reply.ids.first() {
            Some(entry) => Ok(Some(sample_from_entry(entry, endpoint)?)),
            None => Ok(None),
        }
    }

    async fn list_endpoints(&self) -> Result<Vec<String>, StoreError> {
        let mut conn = self.conn.clone();
        conn.smembers(ENDPOINTS_INDEX)
            .await
            .map_err(StoreError::Redis)
    }

    async fn delete_endpoint(&self, endpoint: &str) -> Result<(), StoreError> {
        let mut conn = self.conn.clone();
        let mut pipe = redis::pipe();
        pipe.del(stream_key(endpoint)).ignore();
        pipe.srem(ENDPOINTS_INDEX, endpoint).ignore();
        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(StoreError::Redis)?;
        Ok(())
    }
}

fn sample_from_entry(entry: &StreamId, endpoint: &str) -> Result<RawSample, StoreError> {
    let timestamp = match stream_field(&entry.map, "timestamp")? {
        Some(ts) => DateTime::parse_from_rfc3339(&ts)
            .map_err(|e| {
                StoreError::Corrupt(format!("sample {} invalid timestamp: {e}", entry.id))
            })?
            .with_timezone(&Utc),
        None => {
            return Err(StoreError::Corrupt(format!(
                "sample {} missing timestamp",
                entry.id
            )))
        }
    };

    let baseline_status =
        parse_status(&entry.map, "baseline_status", &entry.id)?.ok_or_else(|| {
            StoreError::Corrupt(format!("sample {} missing baseline_status", entry.id))
        })?;
    let baseline_body = stream_field(&entry.map, "baseline_body")?
        .ok_or_else(|| StoreError::Corrupt(format!("sample {} missing baseline_body", entry.id)))?;
    // Pre-headers samples lack baseline_headers; default to {} so they stay readable.
    let baseline_headers =
        stream_field(&entry.map, "baseline_headers")?.unwrap_or_else(|| "{}".to_owned());

    Ok(RawSample {
        id: entry.id.clone(),
        endpoint: endpoint.to_owned(),
        timestamp,
        baseline_status,
        baseline_body,
        baseline_headers,
        candidate_status: parse_status(&entry.map, "candidate_status", &entry.id)?,
        candidate_body: stream_field(&entry.map, "candidate_body")?,
        candidate_headers: stream_field(&entry.map, "candidate_headers")?,
        control_status: parse_status(&entry.map, "control_status", &entry.id)?,
        control_body: stream_field(&entry.map, "control_body")?,
        control_headers: stream_field(&entry.map, "control_headers")?,
        request_curl: stream_field(&entry.map, "request_curl")?,
    })
}

fn parse_status(
    map: &std::collections::HashMap<String, Value>,
    name: &str,
    id: &str,
) -> Result<Option<u16>, StoreError> {
    match stream_field(map, name)? {
        Some(s) => Ok(Some(s.parse::<u16>().map_err(|e| {
            StoreError::Corrupt(format!("sample {id} invalid {name}: {e}"))
        })?)),
        None => Ok(None),
    }
}

fn stream_field(
    map: &std::collections::HashMap<String, Value>,
    name: &str,
) -> Result<Option<String>, StoreError> {
    match map.get(name) {
        Some(value) => Ok(Some(from_redis_value(value).map_err(StoreError::Redis)?)),
        None => Ok(None),
    }
}
