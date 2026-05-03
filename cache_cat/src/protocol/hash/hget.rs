use crate::error::{CacheCatError, ProtocolError, StorageError};
use crate::protocol::command::Command;
use crate::raft::network::rpc::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use async_trait::async_trait;
use openraft::ReadPolicy::LeaseRead;

pub struct HGetCommand;

#[async_trait]
impl Command for HGetCommand {
    async fn execute(&self, items: &[Value], server: &RedisServer) -> Result<Value, CacheCatError> {
        // Parse HGET key field
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("hget").into());
        }

        let key: Vec<u8> = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key").into()),
        };

        let field = match &items[2] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("field").into()),
        };

        let raft = &server.app.raft;
        let linearizer = raft
            .get_read_linearizer(LeaseRead)
            .await
            .map_err(|e| StorageError::ReadFailed(e.to_string()))?;
        linearizer
            .await_ready(&raft)
            .await
            .map_err(|e| StorageError::WriteFailed(e.to_string()))?;
        let read_lock = server.app.state_machine.data.kvs.read_lock.lock().await;
        let my_value = server.app.state_machine.data.kvs.cache.get(&key);
        drop(read_lock);

        match my_value {
            None => Ok(Value::BulkString(None)),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let option = guard.get(&field);
                    match option {
                        None => Ok(Value::BulkString(None)),
                        Some(value) => Ok(Value::BulkString(Some(value.as_ref().clone()))),
                    }
                }
                _ => Err(CacheCatError::from(ProtocolError::WrongType)),
            },
        }
    }
}
