use crate::raft::types::core::moka::moka::{MyCache, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::SAddReq;

impl MyCache {
    pub fn s_add(&self, sadd: SAddReq, update: &mut UpdateType<'_>) -> Value {
        todo!()
    }
}
