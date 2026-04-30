use crate::raft::types::core::cache::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::{ExpireReq, HSetReq};
use moka::ops::compute::{CompResult, Op};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

impl MyCache {
    pub async fn h_set(&self, hset: HSetReq, update: &mut UpdateType<'_>) -> Value {
        //     let result = self
        //         .cache
        //         .entry(hset.key)
        //         .and_compute_with(|maybe_entry| async move {
        //             match maybe_entry {
        //                 Some(entry) => {
        //                     let mut value = entry.into_value();
        //                     match &mut value.data {
        //                         ValueObject::Hash(data) => {
        //                             for element in hset.elements {
        //                                 data.insert(element.0, element.1);
        //                             }
        //                             Op::Put(value)
        //                         }
        //                         _ => Op::Nop,
        //                     }
        //                 }
        //                 None => {
        //                     let mut map = HashMap::new();
        //                     for element in hset.elements {
        //                         map.insert(element.0, element.1);
        //                     }
        //                     Op::Put(MyValue {
        //                         data: ValueObject::Hash(map),
        //                         expires_at: 0,
        //                         version: 1,
        //                     })
        //                 }
        //             }
        //         })
        //         .await;
        //     match result {
        //         CompResult::Inserted(entry)
        //         | CompResult::ReplacedWith(entry)
        //         | CompResult::Unchanged(entry) => match entry.into_value().data {
        //             ValueObject::Hash(map) => {
        //
        //             }
        //             _ => Value::Error("Key exists but is not a Hash".to_string()),
        //         },
        //         CompResult::StillNone(_) => {
        //             // 理论不会发生（因为我们 Put 了）
        //             Value::Error("Unexpected: key not found".to_string())
        //         }
        //         CompResult::Removed(_) => Value::Error("Unexpected: value removed".to_string()),
        //     }
        // }
        todo!()
    }
}
