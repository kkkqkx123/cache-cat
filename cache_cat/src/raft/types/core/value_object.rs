use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use crate::raft::types::entry::bae_operation::ZAddReq;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SortedSet {
    tree: BTreeMap<OrderedFloat<f64>, Vec<Arc<Vec<u8>>>>,
    hash: HashMap<Arc<Vec<u8>>, f64>,
}

impl SortedSet {
    pub fn new() -> Self {
        SortedSet {
            tree: BTreeMap::new(),
            hash: HashMap::new(),
        }
    }
    pub fn zadd(&mut self, req: ZAddReq) -> i64 {
        let gt = req.gt;
        let lt = req.lt;
        let nx = req.nx;
        let xx = req.xx;
        let ch = req.ch;

        let mut added_count = 0;
        let mut changed_count = 0;

        for (member, score) in req.members {
            let score = OrderedFloat(score);
            let existing_score = self.hash.get(&member);

            // 检查 NX/XX 标志
            if nx && existing_score.is_some() {
                // NX: 只添加新元素，已存在则跳过
                continue;
            }
            if xx && existing_score.is_none() {
                // XX: 只更新已存在元素，不存在则跳过
                continue;
            }

            // 检查 GT/LT 标志
            if let Some(&existing) = existing_score {
                if gt && score <= OrderedFloat::from(existing) {
                    // GT: 仅当新分数大于当前分数时才更新
                    continue;
                }
                if lt && score >= OrderedFloat::from(existing) {
                    // LT: 仅当新分数小于当前分数时才更新
                    continue;
                }
            }

            // 执行添加或更新
            if let Some(old_score) = self.hash.insert(Arc::clone(&member), score.0) {
                // 元素已存在，更新分数
                let old_score_ordered = OrderedFloat(old_score);

                // 从旧分数的集合中移除该成员
                if let Some(members) = self.tree.get_mut(&old_score_ordered) {
                    members.retain(|m| m != &member);
                    if members.is_empty() {
                        self.tree.remove(&old_score_ordered);
                    }
                }

                // 添加到新分数
                self.tree.entry(score).or_insert_with(Vec::new).push(Arc::clone(&member));

                changed_count += 1;
            } else {
                // 新元素
                self.tree.entry(score).or_insert_with(Vec::new).push(Arc::clone(&member));

                added_count += 1;
                changed_count += 1;
            }
        }

        // 根据 ch 标志决定返回值
        if ch {
            changed_count
        } else {
            added_count
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ValueObject {
    Int(i64),
    String(Arc<Vec<u8>>),
    #[serde(with = "mutex_vecdeque_serde")]
    List(Arc<Mutex<VecDeque<Arc<Vec<u8>>>>>),
    #[serde(with = "mutex_hashmap_serde")]
    Hash(Arc<Mutex<HashMap<Arc<Vec<u8>>, Arc<Vec<u8>>>>>),
    #[serde(with = "mutex_zset_serde")]
    ZSet(Arc<Mutex<SortedSet>>),
    Set(Arc<HashSet<Arc<Vec<u8>>>>),
}

// 通用序列化宏
macro_rules! impl_mutex_serde {
    ($mod_name:ident, $inner_type:ty) => {
        mod $mod_name {
            use super::*;
            use serde::de::Deserializer;
            use serde::{Deserialize, Serialize};

            pub fn serialize<S>(
                data: &Arc<Mutex<$inner_type>>,
                serializer: S,
            ) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let guard = data.lock();
                guard.serialize(serializer)
            }

            pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<Mutex<$inner_type>>, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = <$inner_type>::deserialize(deserializer)?;
                Ok(Arc::new(Mutex::new(value)))
            }
        }
    };
}

impl_mutex_serde!(mutex_vecdeque_serde, VecDeque<Arc<Vec<u8>>>);
impl_mutex_serde!(mutex_hashmap_serde, HashMap<Arc<Vec<u8>>, Arc<Vec<u8>>>);
impl_mutex_serde!(mutex_zset_serde, SortedSet);
