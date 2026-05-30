use crate::error::CacheCatError;
use crate::protocol::command::{Client, SubCommand};
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;

pub struct ClientInfoCommand;

#[async_trait]
impl SubCommand for ClientInfoCommand {
    async fn execute(
        &self,
        _client: &mut Client,
        _items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        Ok(Value::BulkString(Some(b"test".to_vec())))
    }
}
