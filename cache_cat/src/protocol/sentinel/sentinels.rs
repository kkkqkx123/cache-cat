use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::Client;
use crate::protocol::sentinel::sentinel::SubCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

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

        // master name 不匹配直接返回空数组
        if server.app.config.sentinel_master_name != name {
            return Ok(Value::Array(None));
        }

        Ok(Value::Array(None))
    }
}
