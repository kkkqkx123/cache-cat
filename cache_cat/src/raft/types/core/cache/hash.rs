use crate::raft::types::core::moka::cas::ComputeCommand;
use crate::raft::types::core::moka::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{BaseOperation, HSetReq};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

impl ComputeCommand for HSetReq {
    fn key(&self) -> Arc<Vec<u8>> {
        self.key.clone()
    }

    fn into_base_op(&self) -> BaseOperation {
        BaseOperation::HSet(self.clone())
    }

    fn mutate(self, data: &mut MyValue) -> (bool, Value) {
        if let ValueObject::Hash(map_arc) = &data.data {
            let mut count = 0;
            let mut map = map_arc.lock();
            for (k, v) in &self.elements {
                if map.insert(k.clone(), v.clone()).is_none() {
                    count += 1;
                }
            }
            // 返回 true 表示数据已变动，需要更新缓存
            (true, Value::Integer(count))
        } else {
            (
                false,
                Value::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
                ),
            )
        }
    }

    fn init(self) -> (ValueObject, Value) {
        let mut map = HashMap::new();
        let len = self.elements.len();
        for (k, v) in self.elements {
            map.insert(k, v);
        }
        (
            ValueObject::Hash(Arc::new(Mutex::new(map))),
            Value::Integer(len as i64),
        )
    }
}

impl MyCache {
    pub fn h_set(&self, hset: HSetReq, update: &mut UpdateType<'_>) -> Value {
        self.execute_compute(hset, update)
    }
}
