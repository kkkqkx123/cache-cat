use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, LinkedList, VecDeque};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ValueObject {
    Int(i64),

    String(Arc<Vec<u8>>),
    List(Arc<VecDeque<Arc<Vec<u8>>>>),

    ZSet(BTreeMap<Vec<u8>, Vec<u8>>),
    Set(Arc<HashSet<Arc<Vec<u8>>>>),
    Hash(Arc<HashMap<Arc<Vec<u8>>, Arc<Vec<u8>>>>),
}
