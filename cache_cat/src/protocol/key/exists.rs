//! EXISTS command implementation
//!
//! EXISTS key [key ...]
//! Returns the number of keys that exist from those specified as arguments.

use crate::error::{CacheCatError, ProtocolError, StorageError};
use crate::protocol::command::Command;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use async_trait::async_trait;
use openraft::ReadPolicy::LeaseRead;

/// EXISTS command parameters
#[derive(Debug, Clone, PartialEq)]
pub struct ExistsParams {
    pub keys: Vec<Vec<u8>>,
}

impl ExistsParams {
    /// Parse EXISTS command parameters from RESP array items
    /// Format: EXISTS key [key ...]
    fn parse(items: &[Value]) -> Result<Self, ProtocolError> {
        // Need at least: EXISTS key (2 items)
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("exists"));
        }

        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(items.len() - 1);
        for item in items.iter().skip(1) {
            let key = match item {
                Value::BulkString(Some(data)) => data.clone(),
                Value::SimpleString(s) => s.as_bytes().to_vec(),
                _ => return Err(ProtocolError::WrongArgCount("del")),
            };
            keys.push(key);
        }

        Ok(ExistsParams { keys })
    }
}

/// EXISTS command executor
pub struct ExistsCommand;

#[async_trait]
impl Command for ExistsCommand {
    async fn execute(&self, items: &[Value], server: &RedisServer) -> Result<Value, CacheCatError> {
        let params = ExistsParams::parse(items)?;
        let raft = &server.app.raft;
        let linearizer = raft
            .get_read_linearizer(LeaseRead)
            .await
            .map_err(|e| StorageError::ReadFailed(e.to_string()))?;
        linearizer
            .await_ready(&raft)
            .await
            .map_err(|e| StorageError::WriteFailed(e.to_string()))?;
        let mut counter = 0;
        let _shard_lock = server.app.state_machine.data.kvs.write_lock.lock();
        let _exclusive_lock = server.app.state_machine.data.kvs.read_lock.lock();
        for key in &params.keys {
            if server.app.state_machine.data.kvs.cache.contains_key(key) {
                counter += 1;
            }
        }
        Ok(Value::Integer(counter))
    }
}
