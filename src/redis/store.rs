use super::error::StoreError;
use super::{DiffEntry, DiffStore, EndpointAggregation};
use redis::aio::ConnectionManager;
use redis::AsyncCommands;

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
            ("primary_status", entry.primary_status.to_string()),
        ];
        if let Some(status) = entry.candidate_status {
            fields.push(("candidate_status", status.to_string()));
        }
        if let Some(status) = entry.secondary_status {
            fields.push(("secondary_status", status.to_string()));
        }

        // ConnectionManager is a cheap clonable handle to one multiplexed connection.
        let mut conn = self.conn.clone();
        let _id: String = conn
            .xadd(&self.stream_key, "*", &fields)
            .await
            .map_err(StoreError::Redis)?;

        Ok(())
    }

    async fn write_aggregation(
        &self,
        aggregations: &[EndpointAggregation],
    ) -> Result<(), StoreError> {
        if aggregations.is_empty() {
            return Ok(());
        }

        // One pipelined round-trip for all endpoints.
        let mut pipe = redis::pipe();
        for aggregation in aggregations {
            let fields_json =
                serde_json::to_string(&aggregation.fields).map_err(StoreError::Serialize)?;
            let key = format!("{}:{}", self.aggregation_key_prefix, aggregation.endpoint);

            pipe.hset_multiple(
                &key,
                &[
                    ("total", aggregation.total.to_string()),
                    ("fields", fields_json),
                    ("last_updated", aggregation.last_updated.to_rfc3339()),
                ],
            )
            .ignore();
        }

        let mut conn = self.conn.clone();
        let _: () = pipe
            .query_async(&mut conn)
            .await
            .map_err(StoreError::Redis)?;

        Ok(())
    }
}
