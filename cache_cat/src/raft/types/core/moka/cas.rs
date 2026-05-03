use crate::raft::types::core::moka::moka::{MyCache, MyValue, UpdateType};
use crate::raft::types::core::response_value::Value;
use crate::raft::types::core::value_object::ValueObject;
use crate::raft::types::entry::bae_operation::BaseOperation;
use crate::raft::types::entry::request::AtomicRequest;
use moka::ops::compute::{CompResult, Op};
use std::sync::Arc;

pub trait ComputeCommand: Send + 'static {
    /// 获取缓存的 Key
    fn key(&self) -> Arc<Vec<u8>>;

    /// 将当前请求转换为基础操作 (用于 Snapshot 队列)
    fn into_base_op(&self) -> BaseOperation;

    /// 修改现有的值。如果类型匹配并成功修改，返回 true；如果不匹配返回 false (即 Op::Nop)
    /// 注意：这里获取 `self` 的所有权，避免不必要的 Clone
    fn mutate(self, data: &mut ValueObject) -> bool;

    /// 当缓存项不存在时，初始化新的值
    fn init(self) -> ValueObject;

    /// 从最终确定的数据中提取返回值 (不依赖 self)
    fn extract(data: &ValueObject) -> Value;
}

pub trait WriteCommand: Send + 'static {
    /// 获取缓存 Key
    fn key(&self) -> Arc<Vec<u8>>;

    /// 转换为 Raft 基础操作
    fn into_base_op(self) -> BaseOperation;

    /// 修改现有数据
    /// 返回: (是否更新缓存, 返回给客户端的 Value)
    fn mutate(&self, data: &mut ValueObject) -> (bool, Value);

    /// 初始化数据（当 Key 不存在时）
    /// 返回: (初始化的对象, 返回给客户端的 Value)
    fn init(&self) -> (ValueObject, Value);
}

impl MyCache {
    pub async fn execute_write<C>(&self, cmd: C, update: &mut UpdateType<'_>) -> Value
    where
        C: WriteCommand + Clone,
    {
        let key = cmd.key();
        // 1. 使用 Arc + Mutex 来在异步闭包内外共享返回值
        let return_value = Arc::new(parking_lot::Mutex::new(Value::Integer(0)));

        let result = match update {
            UpdateType::None => {
                let res_clone = Arc::clone(&return_value);
                self.cache
                    .entry(key)
                    .and_compute_with(|maybe_entry| {
                        let cmd = cmd.clone();
                        let res_clone = Arc::clone(&res_clone); // 再次 clone 以进入 async block
                        async move {
                            match maybe_entry {
                                Some(entry) => {
                                    let mut value = entry.into_value();
                                    let (changed, res) = cmd.mutate(&mut value.data);
                                    *res_clone.lock() = res; // 修改共享的值
                                    if changed {
                                        value.version += 1;
                                        Op::Put(value)
                                    } else {
                                        Op::Nop
                                    }
                                }
                                None => {
                                    let (new_obj, res) = cmd.init();
                                    *res_clone.lock() = res;
                                    Op::Put(MyValue {
                                        data: new_obj,
                                        expires_at: 0,
                                        version: 1,
                                    })
                                }
                            }
                        }
                    })
                    .await
            }

            UpdateType::Snapshot(queue) => {
                let res_clone = Arc::clone(&return_value);
                self.cache
                    .entry(key)
                    .and_compute_with(|maybe_entry| {
                        let cmd = cmd.clone();
                        let res_clone = Arc::clone(&res_clone);
                        async move {
                            let mut next_version = 1;
                            let op = match maybe_entry {
                                Some(entry) => {
                                    let mut value = entry.into_value();
                                    let (changed, res) = cmd.mutate(&mut value.data);
                                    *res_clone.lock() = res;
                                    value.version += 1;
                                    next_version = value.version;
                                    if changed { Op::Put(value) } else { Op::Nop }
                                }
                                None => {
                                    let (new_obj, res) = cmd.init();
                                    *res_clone.lock() = res;
                                    Op::Put(MyValue {
                                        data: new_obj,
                                        expires_at: 0,
                                        version: 1,
                                    })
                                }
                            };

                            queue.push(AtomicRequest {
                                request: cmd.into_base_op(),
                                version: next_version,
                            });
                            op
                        }
                    })
                    .await
            }

            UpdateType::CAS(cas_version) => {
                let expected_version = *cas_version - 1;
                let res_clone = Arc::clone(&return_value);
                self.cache
                    .entry(key)
                    .and_compute_with(|maybe_entry| {
                        let cmd = cmd.clone();
                        let res_clone = Arc::clone(&res_clone);
                        async move {
                            match maybe_entry {
                                Some(entry) => {
                                    let mut value = entry.into_value();
                                    if value.version != expected_version {
                                        *res_clone.lock() = Value::Integer(0);
                                        return Op::Nop;
                                    }
                                    let (changed, res) = cmd.mutate(&mut value.data);
                                    *res_clone.lock() = res;
                                    if changed {
                                        value.version += 1;
                                        Op::Put(value)
                                    } else {
                                        Op::Nop
                                    }
                                }
                                None => {
                                    let (new_obj, res) = cmd.init();
                                    *res_clone.lock() = res;
                                    Op::Put(MyValue {
                                        data: new_obj,
                                        expires_at: 0,
                                        version: 1,
                                    })
                                }
                            }
                        }
                    })
                    .await
            }
        };

        // 2. 检查 Moka 的执行结果
        match result {
            CompResult::StillNone(_) => Value::Error("Key not found".into()),
            _ => {
                // 3. 从 Mutex 中取出最终的返回值
                let final_res = return_value.lock().clone();
                final_res
            }
        }
    }

    pub async fn execute_compute<C>(&self, cmd: C, update: &mut UpdateType<'_>) -> Value
    where
        C: ComputeCommand,
    {
        let key = cmd.key();

        let result = match update {
            UpdateType::None => {
                self.cache
                    .entry(key)
                    .and_compute_with(|maybe_entry| async move {
                        match maybe_entry {
                            Some(entry) => {
                                let mut value = entry.into_value();
                                if cmd.mutate(&mut value.data) {
                                    Op::Put(value)
                                } else {
                                    Op::Nop
                                }
                            }
                            None => Op::Put(MyValue {
                                data: cmd.init(),
                                expires_at: 0,
                                version: 1,
                            }),
                        }
                    })
                    .await
            }
            UpdateType::Snapshot(queue) => {
                self.cache
                    .entry(key)
                    .and_compute_with(|maybe_entry| async move {
                        match maybe_entry {
                            Some(entry) => {
                                let mut value = entry.into_value();
                                value.version += 1;
                                queue.push(AtomicRequest {
                                    request: cmd.into_base_op(),
                                    version: value.version,
                                });

                                if cmd.mutate(&mut value.data) {
                                    Op::Put(value)
                                } else {
                                    Op::Nop
                                }
                            }
                            None => {
                                queue.push(AtomicRequest {
                                    request: cmd.into_base_op(),
                                    version: 1,
                                });
                                Op::Put(MyValue {
                                    data: cmd.init(),
                                    expires_at: 0,
                                    version: 1,
                                })
                            }
                        }
                    })
                    .await
            }
            UpdateType::CAS(cas_version) => {
                let expected_version = *cas_version - 1;
                self.cache
                    .entry(key)
                    .and_compute_with(|maybe_entry| async move {
                        match maybe_entry {
                            Some(entry) => {
                                let mut value = entry.into_value();
                                if value.version != expected_version {
                                    return Op::Nop;
                                }
                                value.version += 1;

                                if cmd.mutate(&mut value.data) {
                                    Op::Put(value)
                                } else {
                                    Op::Nop
                                }
                            }
                            None => Op::Put(MyValue {
                                data: cmd.init(),
                                expires_at: 0,
                                version: 1,
                            }),
                        }
                    })
                    .await
            }
        };

        // 统一处理结果并提取返回值
        match result {
            CompResult::Inserted(entry)
            | CompResult::ReplacedWith(entry)
            | CompResult::Unchanged(entry) => C::extract(&entry.into_value().data),
            CompResult::StillNone(_) => Value::Error("Unexpected: key not found".to_string()),
            CompResult::Removed(_) => Value::Error("Unexpected: value removed".to_string()),
        }
    }
}
