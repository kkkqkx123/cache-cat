use crate::protocol::key::expire::ExpireCondition;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BaseOperation {
    Set(SetReq),
    Del(DelReq),
    LPush(LPushReq),
    Incr(IncrReq),
    Expire(ExpireReq),
}
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ExpireReq {
    pub key: Arc<Vec<u8>>,
    pub expires_at: u64,
    pub condition: Option<ExpireCondition>,
}
impl fmt::Display for ExpireReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ExpireReq {{ key: {}, seconds: {}, condition: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.expires_at,
            self.condition
        )
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct IncrReq {
    pub key: Arc<Vec<u8>>,
    pub value: i64,
}
impl fmt::Display for IncrReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "IncrReq {{ key: {} }}",
            String::from_utf8_lossy(&self.key)
        )
    }
}
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct SetReq {
    pub key: Arc<Vec<u8>>,
    pub value: Arc<Vec<u8>>,
    pub ex_time: u64,
}
impl fmt::Display for SetReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SetReq {{ key: {}, value: {}, ex_time: {} }}",
            String::from_utf8_lossy(&self.key),
            String::from_utf8_lossy(&self.value),
            self.ex_time
        )
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct LPushReq {
    pub key: Arc<Vec<u8>>,
    pub elements: Vec<Arc<Vec<u8>>>,
}
impl fmt::Display for LPushReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LPushReq {{ key: {}, elements: {:?} }}",
            String::from_utf8_lossy(&self.key),
            self.elements
        )
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct DelReq {
    pub key: Arc<Vec<u8>>,
}
impl fmt::Display for DelReq {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DelReq {{ key: {} }}", String::from_utf8_lossy(&self.key))
    }
}
