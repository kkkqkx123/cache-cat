use crate::raft::types::core::moka::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::SortedSet;
use crate::raft::types::core::value_object::ValueObject::ZSet;
use crate::raft::types::entry::bae_operation::ZAddReq;
use parking_lot::Mutex;
use std::sync::Arc;

impl MyCache {
    pub async fn z_add(&self, zadd: ZAddReq, update: &mut UpdateType<'_>) -> Value {
        let my_value = self.cache.get(&zadd.key).await;
        match my_value {
            None => {
                let mut set = SortedSet::new();
                let changed_count = set.zadd(zadd.clone());
                self.cache
                    .insert(
                        zadd.key,
                        MyValue {
                            version: 1,
                            data: ZSet(Arc::new(Mutex::new(set))),
                            expires_at: 0,
                        },
                    )
                    .await;
                Value::Integer(changed_count)
            }
            Some(data) => match data.data {
                ZSet(zset) => {
                    let changed_count = zset.lock().zadd(zadd.clone());
                    Value::Integer(changed_count)
                }
                _ => Value::Error("zadd: key is not a zset".to_string()),
            },
        }
    }
}
