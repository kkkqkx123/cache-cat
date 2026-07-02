use crate::error::{CacheCatError, ProtocolError};
use crate::protocol::hash::hdel::HDelReq;
use crate::protocol::hash::hget::HGetParams;
use crate::protocol::hash::hgetall::HGetAllParams;
use crate::protocol::hash::hincrby::HIncrReq;
use crate::protocol::hash::hset::HSetReq;
use crate::protocol::hash::hvals::HValsParams;
use crate::raft::types::core::mocha::mocha::{MyCache, MyValue, Update};
use crate::raft::types::core::mocha::read_command::ReadCommand;
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use bytes::Bytes;

impl ReadCommand for HValsParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::Array(Some(vec![])),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();

                    let result = guard
                        .values()
                        .map(|v| Value::BulkString(Some(v.to_bytes())))
                        .collect::<Vec<_>>();

                    Value::Array(Some(result))
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}

impl ReadCommand for HGetAllParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::Map(Vec::new()),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let result = guard
                        .iter()
                        .map(|(field, value)| {
                            (
                                Value::BulkString(Some(field.clone())),
                                Value::BulkString(Some(value.to_bytes())),
                            )
                        })
                        .collect::<Vec<_>>();

                    Value::Map(result)
                }
                _ => CacheCatError::from(ProtocolError::WrongType).into(),
            },
        }
    }
}

impl ReadCommand for HGetParams {
    fn key(&self) -> &Bytes {
        &self.key
    }

    fn execute(&self, value: Option<MyValue>) -> Value {
        match value {
            None => Value::BulkString(None),
            Some(v) => match v.data {
                ValueObject::Hash(map) => {
                    let guard = map.lock();
                    let option = guard.get(&self.field);
                    match option {
                        None => Value::BulkString(None),
                        Some(value) => Value::BulkString(Some(value.to_bytes())),
                    }
                }
                _ => ProtocolError::WrongType.into(),
            },
        }
    }
}
impl MyCache {







    pub fn h_del(&self, param: HDelReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }

    pub fn h_set(&self, param: HSetReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
    pub fn h_incr(&self, param: HIncrReq, update: &mut Update) -> Value {
        self.execute_compute(param, update)
    }
}
