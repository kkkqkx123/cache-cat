use crate::error::{CacheCatError, ProtocolError};
use crate::mocha::{EntrySnapshot, MochaOperation};
use crate::protocol::command::{Client, Command};
use crate::protocol::raft_command::RaftCommand;
use crate::raft::network::redis_server::RedisServer;
use crate::raft::types::core::mocha::cas::ComputeCommand;
use crate::raft::types::core::mocha::mocha::MyValue;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::bae_operation::BaseOperation::SRem;
use crate::raft::types::entry::request::Operation;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;

struct SRemArgs {
    key: Bytes,
    members: Vec<Bytes>,
}

pub struct SRemCommand;

impl SRemCommand {
    fn parse_args(items: &[Value]) -> Result<SRemArgs, ProtocolError> {
        if items.len() < 3 {
            return Err(ProtocolError::WrongArgCount("srem"));
        }

        // Parse key
        let key = items[1]
            .string_bytes_clone()
            .ok_or(ProtocolError::InvalidArgument("key"))?;

        // Parse members
        let members = items
            .iter()
            .skip(2)
            .map_while(Value::string_bytes_clone)
            .collect::<Vec<_>>();

        if members.len() < items.len() - 2 {
            return Err(ProtocolError::InvalidArgument("member"));
        }

        Ok(SRemArgs { key, members })
    }
}

impl RaftCommand for SRemCommand {
    fn raft_request(&self, items: &[Value]) -> Result<Operation, ProtocolError> {
        let params = Self::parse_args(items)?;

        Ok(Operation::Base(SRem(SRemReq {
            key: params.key,
            members: params.members,
        })))
    }
}

#[async_trait]
impl Command for SRemCommand {
    async fn execute(
        &self,
        client: &mut Client,
        items: &[Value],
        server: &RedisServer,
    ) -> Result<Value, CacheCatError> {
        // Transaction mode
        if let Some(vec) = client.transaction_queue.as_mut() {
            vec.push(self.raft_request(items)?);

            return Ok(Value::SimpleString(String::from("QUEUED")));
        }

        // Build raft operation
        let operation = self.raft_request(items)?;

        // Execute write
        let value = server.app.write(operation, client.db_number).await?;

        Ok(value)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SRemReq {
    pub key: Bytes,
    pub members: Vec<Bytes>,
}

impl Display for SRemReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SRemReq {{ key: {}, fields: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.members
        )
    }
}

impl ComputeCommand for SRemReq {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn into_base_op(self) -> BaseOperation {
        BaseOperation::SRem(self.clone())
    }

    fn mutate(
        self,
        entry: EntrySnapshot<MyValue>,
        _write_clock: u64,
    ) -> (MochaOperation<MyValue>, Value) {
        match &entry.value.data {
            ValueObject::Set(set) => {
                let mut set = set.lock();
                let mut deleted_count = 0;
                for member in &self.members {
                    if set.remove(member) {
                        deleted_count += 1;
                    }
                }
                let is_empty = set.is_empty();
                drop(set);

                if deleted_count == 0 {
                    return (MochaOperation::Abort, Value::Integer(0));
                }
                if is_empty {
                    return (MochaOperation::Remove, Value::Integer(deleted_count));
                }
                (
                    MochaOperation::Insert {
                        value: entry.value.clone(),
                        expire: entry.get_expire_policy(),
                    },
                    Value::Integer(deleted_count),
                )
            }
            _ => (
                MochaOperation::Abort,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            ),
        }
    }

    fn init(self) -> (MochaOperation<MyValue>, Value) {
        (MochaOperation::Abort, Value::Integer(0))
    }
}
