//! HVALS command implementation
//!
//! HVALS key
//! Returns all values in the hash stored at key.

use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::{HashValue, ValueObject};
use crate::raft::types::entry::read_operation::ReadOperation;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Parsed HVALS arguments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HValsParams {
    pub key: Vec<u8>,
}

impl Display for HValsParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HVALS {}",
            String::from_utf8_lossy(&self.key)
        )
    }
}

/// HVALS command handler
pub struct HValsCommand;

impl HValsCommand {
    /// Parse arguments from RESP items
    /// Format: HVALS key
    fn parse_args(items: &[Value]) -> Result<HValsParams, ProtocolError> {
        if items.len() < 2 {
            return Err(ProtocolError::WrongArgCount("hvals"));
        }

        let key = match &items[1] {
            Value::BulkString(Some(data)) => data.clone(),
            Value::SimpleString(s) => s.as_bytes().to_vec(),
            _ => return Err(ProtocolError::InvalidArgument("key")),
        };

        Ok(HValsParams { key })
    }
}

impl RaftCommand for HValsCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;
        Ok(Operation::Read(ReadOperation::HVals(params)))
    }
}

#[async_trait]
impl Command for HValsCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);
            return Ok(Value::SimpleString(String::from("QUEUED")));
        }

        let params = Self::parse_args(items)?;

        let value = server
            .app
            .read(params.key, client.db_number)
            .await?;

        match value {
            None => Ok(Value::Array(Some(vec![]))),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();

                    let mut result = Vec::with_capacity(guard.len());

                    for value in guard.values() {
                        let value_bytes = match value {
                            HashValue::Str(str) => str.as_ref().clone(),
                            HashValue::Int(int) => int.to_string().into_bytes(),
                        };

                        result.push(Value::BulkString(Some(value_bytes)));
                    }

                    Ok(Value::Array(Some(result)))
                }
                _ => Err(CacheCatError::from(ProtocolError::WrongType)),
            },
        }
    }
}