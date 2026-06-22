use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;

pub struct SentinelSentinelsCommand;

#[async_trait]
impl SubCommand for SentinelSentinelsCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // SENTINEL SENTINELS <master-name>
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("SENTINEL SENTINELS").into());
        }

        let name = match &items[2] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return Err(ProtocolError::InvalidArgument("master name").into()),
        };

        // If the master name does not match, return an empty array directly
        if server.app.config.sentinel_master_name != name {
            return Ok(Value::Array(None));
        }
        let nodes = server.app.cluster.nodes();
        let mut result = Vec::new();
        for (node_id, node) in nodes {
            let mut info = Vec::new();
            info.push(Value::BulkString(Some(Bytes::from_static(b"name"))));
            // Here returns node_id as the name of the sentinel
            info.push(Value::BulkString(Some(node_id.to_string().into())));

            info.push(Value::BulkString(Some(Bytes::from_static(b"ip"))));
            info.push(Value::BulkString(Some(
                node.endpoint.addr().to_string().into(),
            )));

            info.push(Value::BulkString(Some(Bytes::from_static(b"port"))));
            info.push(Value::BulkString(Some(
                node.endpoint.redis_port().to_string().into(),
            )));

            info.push(Value::BulkString(Some(Bytes::from_static(b"runid"))));
            info.push(Value::BulkString(Some(node_id.to_string().into())));

            info.push(Value::BulkString(Some(Bytes::from_static(b"flags"))));
            if server.app.cluster.is_survive(node_id).await {
                info.push(Value::BulkString(Some(Bytes::from_static(b"sentinel"))));
            } else {
                info.push(Value::BulkString(Some(Bytes::from_static(
                    b"sentinel,o_down,disconnected",
                ))));
            }

            result.push(Value::Array(Some(info)));
        }

        Ok(Value::Array(Some(result)))
    }
}
