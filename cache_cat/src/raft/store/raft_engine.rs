use crate::error::{Error, Result};
use crate::raft::types::raft_types::TypeConfig;
use openraft::Entry;
use raft_engine::{Config, Engine, MessageExt, ReadableSize};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

pub fn create_raft_engine<P: AsRef<Path>>(path: P) -> Result<Arc<Engine>> {
    //如果找不到路径就创建
    if !path.as_ref().exists() {
        std::fs::create_dir_all(path.as_ref())?;
    }
    let path = path.as_ref().to_string_lossy().into_owned();
    let config = Config {
        dir: path.clone(),
        purge_threshold: ReadableSize::gb(2),
        batch_compression_threshold: ReadableSize::kb(0),
        ..Default::default()
    };
    match Engine::open(config) {
        Ok(raft_engine) => {
             Ok(Arc::new(raft_engine))
        }
        Err(err) => {
             Err(Error::config(format!("directory does not exist: {},{}", err, path)))
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MessageExtTyped;
impl MessageExt for MessageExtTyped {
    type Entry = Entry<TypeConfig>;

    fn index(e: &Self::Entry) -> u64 {
        e.log_id.index
    }
}
