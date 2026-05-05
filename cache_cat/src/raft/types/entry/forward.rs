use crate::raft::types::entry::membership::JoinRequest;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ForwardRequestBody {
    Join(JoinRequest),
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ForwardRequest {
    pub forward_to_leader: u64,
    pub body: ForwardRequestBody,
}
