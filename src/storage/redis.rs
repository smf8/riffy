use std::collections::{HashMap, HashSet};

use super::error::StoreError;
use super::{DiffEntry, DiffSample, DiffStore, EndpointAggregation, FieldAggregation, SamplePage};
use crate::compare::flatten::FieldDiff;
use chrono::{DateTime, Utc};
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamRangeReply};
use redis::{from_redis_value, AsyncCommands, Value};

/// Redis-backed `DiffStore`: per-request diffs go to a stream (`XADD`),
/// aggregation snapshots to one hash per endpoint (`HSET`, pipelined).
pub struct RedisDiffStore {
    conn: ConnectionManager,
    stream_key: String,
    aggregation_key_prefix: String,
}

impl RedisDiffStore {
    /// Connect with an auto-reconnecting multiplexed connection.
    pub async fn connect(
        uri: &str,
        stream_key: String,
        aggregation_key_prefix: String,
    ) -> Result<Self, StoreError> {
        let client = redis::Client::open(uri).map_err(StoreError::Redis)?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(StoreError::Redis)?;

        Ok(Self {
            conn,
            stream_key,
            aggregation_key_prefix,
        })
    }
}

#[async_trait::async_trait]
impl DiffStore for RedisDiffStore {
    async fn append_diff(&self, entry: &DiffEntry) -> Result<(), StoreError> {
        let raw_json = serde_json::to_string(&entry.raw_fields).map_err(StoreError::Serialize)?;
        let noise_json =
            serde_json::to_string(&entry.noise_fields).map_err(StoreError::Serialize)?;

        let mut fields: Vec<(&str, String)> = vec![
            ("endpoint", entry.endpoint.clone()),
            ("timestamp", entry.timestamp.to_rfc3339()),
            ("raw_fields", raw_json),
            ("noise_fields", noise_json),
            ("baseline_status", entry.baseline_status.to_string()),
        ];
        if let Some(status) = entry.candidate_status {
            fields.push(("candidate_status", status.to_string()));
        }
        if let Some(status) = entry.control_status {
            fields.push(("control_status", status.to_string()));
        }

        // ConnectionManager is a cheap clonable handle to one multiplexed connection.
        let mut conn = self.conn.clone();
        let _id: String = conn
            .xadd(&self.stream_key, "*", &fields)
            .await
            .map_err(StoreError::Redis)?;

        Ok(())
    }

    async fn add_aggregation(&self, deltas: &[EndpointAggregation]) -> Result<(), StoreError> {
        if deltas.is_empty() {
            return Ok(());
        }

        // One atomic (MULTI/EXEC), pipelined round-trip for all endpoints. Each
        // count is an HINCRBY so concurrent instances sum into the same hash
        // instead of overwriting; per-field counts live as flat `raw:{path}` /
        // `noise:{path}` hash entries so HINCRBY can target them directly.
        // Atomic so a reader never observes a half-applied flush.
        let mut pipe = redis::pipe();
        pipe.atomic();
        for delta in deltas {
            let key = format!("{}:{}", self.aggregation_key_prefix, delta.endpoint);
            if delta.total > 0 {
                pipe.hincr(&key, "total", delta.total).ignore();
            }
            for (path, field) in &delta.fields {
                if field.raw_count > 0 {
                    pipe.hincr(&key, format!("raw:{path}"), field.raw_count)
                        .ignore();
                }
                if field.noise_count > 0 {
                    pipe.hincr(&key, format!("noise:{path}"), field.noise_count)
                        .ignore();
                }
            }
            pipe.hset(&key, "last_updated", delta.last_updated.to_rfc3339())
                .ignore();
        }

        let mut conn = self.conn.clone();
        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(StoreError::Redis)?;

        Ok(())
    }

    async fn get_aggregation(
        &self,
        endpoint: &str,
    ) -> Result<Option<EndpointAggregation>, StoreError> {
        let key = format!("{}:{}", self.aggregation_key_prefix, endpoint);
        let mut conn = self.conn.clone();
        let map: HashMap<String, String> = conn.hgetall(&key).await.map_err(StoreError::Redis)?;
        if map.is_empty() {
            return Ok(None);
        }
        Ok(Some(parse_aggregation(endpoint.to_owned(), &map)?))
    }

    async fn list_aggregations(&self) -> Result<Vec<EndpointAggregation>, StoreError> {
        let mut conn = self.conn.clone();
        let prefix = format!("{}:", self.aggregation_key_prefix);
        let pattern = format!("{prefix}*");

        // Cursor-based SCAN instead of KEYS so a large keyspace never blocks
        // the server. SCAN may repeat keys, so dedup before fetching.
        let mut cursor: u64 = 0;
        let mut keys: HashSet<String> = HashSet::new();
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(256)
                .query_async(&mut conn)
                .await
                .map_err(StoreError::Redis)?;
            keys.extend(batch);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // One pipelined round-trip for every endpoint hash.
        let keys: Vec<String> = keys.into_iter().collect();
        let mut pipe = redis::pipe();
        for key in &keys {
            pipe.hgetall(key);
        }
        let maps: Vec<HashMap<String, String>> = pipe
            .query_async(&mut conn)
            .await
            .map_err(StoreError::Redis)?;

        let mut out = Vec::with_capacity(keys.len());
        for (key, map) in keys.iter().zip(maps) {
            if map.is_empty() {
                continue;
            }
            let endpoint = key.strip_prefix(&prefix).unwrap_or(key).to_owned();
            out.push(parse_aggregation(endpoint, &map)?);
        }
        Ok(out)
    }

    async fn reset_aggregation(&self, endpoint: &str) -> Result<(), StoreError> {
        let key = format!("{}:{}", self.aggregation_key_prefix, endpoint);
        let mut conn = self.conn.clone();
        let _: () = conn.del(&key).await.map_err(StoreError::Redis)?;
        Ok(())
    }

    async fn recent_samples(
        &self,
        endpoint: &str,
        path: &str,
        limit: usize,
        offset: usize,
    ) -> Result<SamplePage, StoreError> {
        // Scan the stream newest-first in pages, collecting one sample past the
        // requested window so `has_more` is known. The stream is shared across
        // endpoints, so non-matching entries are filtered out here.
        const PAGE: usize = 256;
        let want = offset.saturating_add(limit).saturating_add(1);
        let mut conn = self.conn.clone();
        let mut matches: Vec<DiffSample> = Vec::new();
        let mut end = "+".to_string();

        loop {
            let reply: StreamRangeReply = conn
                .xrevrange_count(self.stream_key.as_str(), end.as_str(), "-", PAGE)
                .await
                .map_err(StoreError::Redis)?;
            if reply.ids.is_empty() {
                break;
            }
            let batch_len = reply.ids.len();
            let oldest_id = reply.ids.last().map(|entry| entry.id.clone());

            for entry in &reply.ids {
                if let Some(sample) = sample_from_entry(entry, endpoint, path)? {
                    matches.push(sample);
                    if matches.len() >= want {
                        break;
                    }
                }
            }

            if matches.len() >= want || batch_len < PAGE {
                break;
            }
            match oldest_id {
                // Exclusive upper bound: continue strictly older than this id.
                Some(id) => end = format!("({id}"),
                None => break,
            }
        }

        let has_more = matches.len() > offset.saturating_add(limit);
        let items = matches.into_iter().skip(offset).take(limit).collect();
        Ok(SamplePage {
            items,
            limit,
            offset,
            has_more,
        })
    }
}

/// Reconstruct an `EndpointAggregation` from a `riffy:agg:{endpoint}` hash.
/// `total` and `last_updated` are reserved keys; every other key is a
/// `raw:{path}` / `noise:{path}` per-field counter that is regrouped by path.
fn parse_aggregation(
    endpoint: String,
    map: &HashMap<String, String>,
) -> Result<EndpointAggregation, StoreError> {
    let mut total = 0u64;
    let mut last_updated: Option<DateTime<Utc>> = None;
    let mut fields: HashMap<String, FieldAggregation> = HashMap::new();

    for (key, value) in map {
        match key.as_str() {
            "total" => {
                total = value.parse::<u64>().map_err(|e| {
                    StoreError::Corrupt(format!("aggregation '{endpoint}' invalid total: {e}"))
                })?;
            }
            "last_updated" => {
                last_updated = Some(
                    DateTime::parse_from_rfc3339(value)
                        .map_err(|e| {
                            StoreError::Corrupt(format!(
                                "aggregation '{endpoint}' invalid last_updated: {e}"
                            ))
                        })?
                        .with_timezone(&Utc),
                );
            }
            other => {
                // Split only on the first ':' so paths may themselves contain ':'.
                let (kind, path) = match other.split_once(':') {
                    Some(parts) => parts,
                    None => continue, // unknown/stale field — ignore
                };
                let count = value.parse::<u64>().map_err(|e| {
                    StoreError::Corrupt(format!(
                        "aggregation '{endpoint}' field '{other}' invalid count: {e}"
                    ))
                })?;
                let field = fields.entry(path.to_owned()).or_default();
                match kind {
                    "raw" => field.raw_count = count,
                    "noise" => field.noise_count = count,
                    _ => continue, // unknown prefix — ignore
                }
            }
        }
    }

    let last_updated = last_updated.ok_or_else(|| {
        StoreError::Corrupt(format!("aggregation '{endpoint}' missing 'last_updated'"))
    })?;

    Ok(EndpointAggregation {
        endpoint,
        total,
        fields,
        last_updated,
    })
}

/// Build a `DiffSample` from one stream entry if it belongs to `endpoint` and
/// carries a diff at `path`; otherwise `None`.
fn sample_from_entry(
    entry: &StreamId,
    endpoint: &str,
    path: &str,
) -> Result<Option<DiffSample>, StoreError> {
    if stream_field(&entry.map, "endpoint")?.as_deref() != Some(endpoint) {
        return Ok(None);
    }

    let raw = diff_at_path(stream_field(&entry.map, "raw_fields")?.as_deref(), path)?;
    let noise = diff_at_path(stream_field(&entry.map, "noise_fields")?.as_deref(), path)?;
    if raw.is_none() && noise.is_none() {
        return Ok(None);
    }

    let timestamp = match stream_field(&entry.map, "timestamp")? {
        Some(ts) => DateTime::parse_from_rfc3339(&ts)
            .map_err(|e| {
                StoreError::Corrupt(format!("stream entry {} invalid timestamp: {e}", entry.id))
            })?
            .with_timezone(&Utc),
        None => return Ok(None),
    };

    Ok(Some(DiffSample {
        timestamp,
        raw,
        noise,
    }))
}

/// Read one stream entry field as a UTF-8 string (entries are always written
/// as bulk strings by `append_diff`).
fn stream_field(map: &HashMap<String, Value>, name: &str) -> Result<Option<String>, StoreError> {
    match map.get(name) {
        Some(value) => Ok(Some(from_redis_value(value).map_err(StoreError::Redis)?)),
        None => Ok(None),
    }
}

/// Parse a stored `{path: FieldDiff}` JSON map and pluck the diff at `path`.
fn diff_at_path(json: Option<&str>, path: &str) -> Result<Option<FieldDiff>, StoreError> {
    match json {
        Some(json) => {
            let map: HashMap<String, FieldDiff> =
                serde_json::from_str(json).map_err(StoreError::Deserialize)?;
            Ok(map.get(path).cloned())
        }
        None => Ok(None),
    }
}
