use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use bytes::Bytes;

pub struct SentinelSlavesCommand;
#[async_trait]
impl SubCommand for SentinelSlavesCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if items.len() != 3 {
            return Err(ProtocolError::WrongArgCount("SENTINEL SLAVES").into());
        }

        let name = match &items[2] {
            Value::BulkString(Some(data)) => String::from_utf8_lossy(data).to_string(),
            Value::SimpleString(s) => s.clone(),
            _ => return Err(ProtocolError::InvalidArgument("master name").into()),
        };
        if server.app.config.sentinel_master_name != name {
            return Ok(Value::Array(None));
        }
        let slaves = server.app.cluster.last_slave();
        let current_node_id = server.app.cluster.node_id();
        let mut result = Vec::new();
        let leader_info = server.app.cluster.leader_addr().await;
        let last_leader = server.app.cluster.last_leader();

        for slave in slaves {
            let mut slave_info = Vec::new();
            slave_info.push(Value::BulkString(Some(Bytes::from_static(b"name"))));
            slave_info.push(Value::BulkString(Some(slave.endpoint.redis_addr().into())));
            slave_info.push(Value::BulkString(Some(Bytes::from_static(b"ip"))));
            slave_info.push(Value::BulkString(Some(
                slave.endpoint.addr().to_string().into(),
            )));
            slave_info.push(Value::BulkString(Some(Bytes::from_static(b"port"))));
            slave_info.push(Value::BulkString(Some(
                slave.endpoint.redis_port().to_string().into(),
            )));
            slave_info.push(Value::BulkString(Some(Bytes::from_static(b"runid"))));
            slave_info.push(Value::BulkString(Some(current_node_id.to_string().into())));
            slave_info.push(Value::BulkString(Some(Bytes::from_static(b"flags"))));
            if server.app.cluster.is_survive(slave.node_id).await {
                slave_info.push(Value::BulkString(Some(Bytes::from_static(b"slave"))));
            } else {
                slave_info.push(Value::BulkString(Some(Bytes::from_static(
                    b"slave,o_down,disconnected",
                ))));
            }

            slave_info.push(Value::BulkString(Some(Bytes::from_static(
                b"master-link-status",
            ))));
            if leader_info.is_some() {
                slave_info.push(Value::BulkString(Some(Bytes::from_static(b"ok"))));
            } else {
                slave_info.push(Value::BulkString(Some(Bytes::from_static(b"err"))));
            }

            slave_info.push(Value::BulkString(Some(Bytes::from_static(b"master-host"))));
            slave_info.push(Value::BulkString(Some(
                last_leader.addr().to_string().into(),
            )));
            slave_info.push(Value::BulkString(Some(Bytes::from_static(b"master-port"))));
            slave_info.push(Value::BulkString(Some(
                last_leader.port().to_string().into(),
            )));

            slave_info.push(Value::BulkString(Some(Bytes::from_static(
                b"slave-repl-offset",
            ))));
            slave_info.push(Value::BulkString(Some(Bytes::from_static(b"0"))));

            slave_info.push(Value::BulkString(Some(Bytes::from_static(
                b"slave-priority",
            ))));
            slave_info.push(Value::BulkString(Some(Bytes::from_static(b"100"))));

            result.push(Value::Array(Some(slave_info)));
        }

        Ok(Value::Array(Some(result)))
    }
}
