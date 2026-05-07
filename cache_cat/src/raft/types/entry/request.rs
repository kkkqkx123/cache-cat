use crate::protocol::key::del::DelParams;
use crate::protocol::key::rename::RenameParams;
use crate::protocol::string::mset::MsetParams;
use crate::protocol::string::set::SetParams;
use crate::raft::types::entry::bae_operation::BaseOperation;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A request to the KV store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    Base(u64, BaseOperation),
    Redis(u64, RedisOperation),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RedisOperation {
    RedisSet(SetParams),
    RedisMset(MsetParams),
    RedisDel(DelParams),
    RedisRename(RenameParams),
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Request::Base(.., op) => match op {
                BaseOperation::Set(req) => write!(f, "Set: {}", req),
                BaseOperation::LPush(req) => write!(f, "LPush: {}", req),
                BaseOperation::Del(req) => write!(f, "DEL: {}", req),
                BaseOperation::Incr(req) => write!(f, "Incr: {}", req),
                BaseOperation::Expire(req) => write!(f, "Expire: {}", req),
                BaseOperation::Append(req) => write!(f, "Append: {}", req),
                BaseOperation::HSet(req) => write!(f, "HSet: {}", req),
                BaseOperation::ZAdd(req) => write!(f, "ZAdd: {}", req),
                BaseOperation::SAdd(req) => write!(f, "SAdd: {}", req),
                BaseOperation::HIncr(req) => write!(f, "HIncr: {}", req),
                BaseOperation::Persist(req) => write!(f, "Persist: {}", req),
                BaseOperation::Insert(insert) => write!(f, "Insert: {}", insert),
            },
            Request::Redis(.., redis) => match redis {
                RedisOperation::RedisSet(req) => write!(f, "RedisSet: {}", req),
                RedisOperation::RedisMset(req) => write!(f, "RedisMset: {}", req),
                RedisOperation::RedisDel(req) => write!(f, "RedisDel: {}", req),
                RedisOperation::RedisRename(req) => write!(f, "RedisRename: {}", req),
            },
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtomicRequest {
    pub request: BaseOperation,
    pub version: u32,
    pub write_clock: u64,
}
