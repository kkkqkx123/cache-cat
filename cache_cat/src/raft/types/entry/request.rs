use crate::protocol::key::del::DelParams;
use crate::protocol::string::mset::MsetParams;
use crate::protocol::string::set::SetParams;
use crate::protocol::zset::zadd::ZAddParam;
use crate::raft::types::entry::bae_operation::BaseOperation;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A request to the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Base(BaseOperation),
    RedisSet(SetParams),
    RedisMset(MsetParams),
    RedisDel(DelParams),
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Base(op) => match op {
                BaseOperation::Set(req) => write!(f, "Set: {}", req),
                BaseOperation::LPush(req) => write!(f, "LPush: {}", req),
                BaseOperation::Del(req) => write!(f, "DEL: {}", req),
                BaseOperation::Incr(req) => write!(f, "Incr: {}", req),
                BaseOperation::Expire(req) => write!(f, "Expire: {}", req),
                BaseOperation::Append(req) => write!(f, "Append: {}", req),
                BaseOperation::HSet(req) => write!(f, "HSet: {}", req),
                BaseOperation::ZAdd(req) => write!(f, "ZAdd: {}", req),
            },
            Request::RedisSet(req) => write!(f, "RedisSet: {}", req),
            Request::RedisMset(req) => write!(f, "RedisMset: {}", req),
            Request::RedisDel(req) => write!(f, "RedisDel: {}", req),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicRequest {
    pub request: BaseOperation,
    pub version: u32,
}
