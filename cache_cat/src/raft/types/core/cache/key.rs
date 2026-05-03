use crate::protocol::key::expire::ExpireCondition;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::entry::bae_operation::{BaseOperation, DelReq, ExpireReq};
use crate::raft::types::entry::request::AtomicRequest;
use std::sync::Arc;

impl MyCache {
    pub fn expire(&self, expire_req: ExpireReq, update: &mut UpdateType<'_>) -> Value {
        let mut v = match self.cache.get(&expire_req.key) {
            Some(v) => v,
            None => return Value::Integer(0),
        };
        let should_update = match expire_req.condition {
            None => true,
            Some(ref condition) => match condition {
                ExpireCondition::Nx => v.expires_at == 0,
                ExpireCondition::Xx => v.expires_at != 0,
                ExpireCondition::Gt => v.expires_at != 0 && v.expires_at <= expire_req.expires_at,
                ExpireCondition::Lt => v.expires_at != 0 && v.expires_at >= expire_req.expires_at,
            },
        };
        if !should_update {
            return Value::Integer(0);
        }

        v.expires_at = expire_req.expires_at;
        match update {
            UpdateType::None => {
                self.cache.insert(expire_req.key, v);
            }
            UpdateType::Snapshot(queue) => {
                let key = expire_req.key.clone();
                v.version = v.version + 1;
                queue.push(AtomicRequest {
                    version: v.version,
                    request: BaseOperation::Expire(expire_req),
                });

                self.cache.insert(key, v);
            }
            UpdateType::CAS(cas_version) => {
                if *cas_version - 1 == v.version {
                    v.version += 1;
                    self.cache.insert(expire_req.key, v);
                }
            }
        }
        Value::Integer(1)
    }

    pub fn del(&self, del_req: DelReq, update: &mut UpdateType<'_>) -> bool {
        //是否删除了元素
        match update {
            UpdateType::None => {
                let existed = self.cache.remove(&del_req.key);
                existed.is_some()
            }

            UpdateType::Snapshot(queue) => {
                // 计算 version
                let version = if let Some(entry) = self.cache.get(&del_req.key) {
                    entry.version + 1
                } else {
                    1
                };
                queue.push(AtomicRequest {
                    version,
                    request: BaseOperation::Del(del_req.clone()),
                });

                let existed = self.cache.remove(&del_req.key);
                existed.is_some()
            }
            UpdateType::CAS(cas_version) => {
                if let Some(entry) = self.cache.get(&del_req.key) {
                    if entry.version == *cas_version - 1 {
                        self.cache.remove(&del_req.key);
                        return true;
                    }
                }
                false
            }
        }
    }
}
