use crate::raft::types::core::response_value::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, watch};

// 客户端状态
struct ClientState {
    sender: watch::Sender<Option<Value>>,
    // 记录该客户端订阅了哪些频道，用于退订所有频道时清理
    subscribed_channels: HashSet<Vec<u8>>,
    subscribed_patterns: HashSet<Vec<u8>>,
}

pub struct PubSub {
    /// 精确频道订阅：频道 -> 订阅的客户端ID集合
    subs: Arc<RwLock<HashMap<Vec<u8>, HashSet<u64>>>>,
    /// 模式订阅：模式 -> 订阅的客户端ID集合
    patterns: Arc<RwLock<HashMap<Vec<u8>, HashSet<u64>>>>,
    /// 客户端状态管理：client_id -> ClientState
    clients: Arc<RwLock<HashMap<u64, ClientState>>>,
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            subs: Arc::new(RwLock::new(HashMap::new())),
            patterns: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 为客户端获取或创建状态，并返回新的 Receiver
    async fn get_or_create_client(&self, client_id: u64) -> watch::Receiver<Option<Value>> {
        let mut clients = self.clients.write().await;
        let state = clients.entry(client_id).or_insert_with(|| {
            let (tx, _) = watch::channel(None);
            ClientState {
                sender: tx,
                subscribed_channels: HashSet::new(),
                subscribed_patterns: HashSet::new(),
            }
        });
        // watch::Sender 可以通过 subscribe() 方法创建新的 Receiver
        // 所有通过 subscribe() 创建的 Receiver 都会收到后续的消息
        state.sender.subscribe()
    }

    /// 订阅多个精确频道
    pub async fn subscribe(
        &self,
        channels: Vec<Vec<u8>>,
        client_id: u64,
    ) -> (Value, watch::Receiver<Option<Value>>) {
        let rx = self.get_or_create_client(client_id).await;

        let mut responses = Vec::new();
        for channel in &channels {
            let resp = self.subscribe_single(channel.clone(), client_id).await;
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
        }

        // 记录该客户端订阅了这些频道
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for channel in &channels {
                    state.subscribed_channels.insert(channel.clone());
                }
            }
        }

        let aggregated_resp = Value::Array(Some(responses));
        (aggregated_resp, rx)
    }

    /// 订阅单个精确频道
    async fn subscribe_single(&self, channel: Vec<u8>, client_id: u64) -> Value {
        let mut subs = self.subs.write().await;
        subs.entry(channel.clone()).or_default().insert(client_id);

        let count = subs.get(&channel).map(|s| s.len()).unwrap_or(0) as i64;
        Value::Array(Some(vec![
            Value::SimpleString("subscribe".to_string()),
            Value::BulkString(Some(channel)),
            Value::Integer(count),
        ]))
    }

    /// 退订多个精确频道
    pub async fn unsubscribe(&self, channels: Vec<Vec<u8>>, client_id: u64) -> Value {
        let mut responses = Vec::new();
        for channel in &channels {
            let resp = self.unsubscribe_single(channel.clone(), client_id).await;
            if let Value::Array(Some(mut elements)) = resp {
                responses.append(&mut elements);
            }
        }

        // 从客户端状态中移除这些频道的记录
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for channel in &channels {
                    state.subscribed_channels.remove(channel);
                }
                // 如果没有订阅了，清理客户端
                if state.subscribed_channels.is_empty() && state.subscribed_patterns.is_empty() {
                    clients.remove(&client_id);
                }
            }
        }

        Value::Array(Some(responses))
    }

    /// 退订单个精确频道
    async fn unsubscribe_single(&self, channel: Vec<u8>, client_id: u64) -> Value {
        let mut subs = self.subs.write().await;
        match subs.get_mut(&channel) {
            Some(set) => {
                let count = set.len() as i64;
                let existed = set.remove(&client_id);
                if set.is_empty() {
                    subs.remove(&channel);
                }

                // 如果客户端实际订阅了这个频道，count 就是移除前的数量
                // 如果没订阅，count 是移除前的数量（包含这个客户端？不，should +1）
                Value::Array(Some(vec![
                    Value::SimpleString("unsubscribe".to_string()),
                    Value::BulkString(Some(channel)),
                    Value::Integer(if existed { count } else { count + 1 }),
                ]))
            }
            None => Value::Array(Some(vec![
                Value::SimpleString("unsubscribe".to_string()),
                Value::BulkString(Some(channel)),
                Value::Integer(0),
            ])),
        }
    }

    /// 退订客户端的所有精确频道
    pub async fn unsubscribe_all_channels(&self, client_id: u64) -> Value {
        let mut responses = Vec::new();

        // 获取该客户端订阅的所有频道
        let channels = {
            let clients = self.clients.read().await;
            clients
                .get(&client_id)
                .map(|state| {
                    state
                        .subscribed_channels
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        // 退订所有频道
        let mut subs = self.subs.write().await;
        for channel in &channels {
            if let Some(set) = subs.get_mut(channel) {
                let count = set.len() as i64;
                set.remove(&client_id);
                if set.is_empty() {
                    subs.remove(channel);
                }
                responses.push(Value::Array(Some(vec![
                    Value::SimpleString("unsubscribe".to_string()),
                    Value::BulkString(Some(channel.clone())),
                    Value::Integer(count),
                ])));
            }
        }
        drop(subs);

        // 清理客户端状态
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                state.subscribed_channels.clear();
                if state.subscribed_patterns.is_empty() {
                    clients.remove(&client_id);
                }
            }
        }

        Value::Array(Some(responses))
    }

    /// 订阅多个模式
    pub async fn psubscribe(
        &self,
        patterns: Vec<Vec<u8>>,
        client_id: u64,
    ) -> (Value, watch::Receiver<Option<Value>>) {
        let rx = self.get_or_create_client(client_id).await;

        let mut responses = Vec::new();
        for pattern in &patterns {
            let resp = self.psubscribe_single(pattern.clone(), client_id).await;
            responses.push(resp);
        }

        // 记录该客户端订阅了这些模式
        {
            let mut clients = self.clients.write().await;
            if let Some(state) = clients.get_mut(&client_id) {
                for pattern in &patterns {
                    state.subscribed_patterns.insert(pattern.clone());
                }
            }
        }

        let aggregated_resp = Value::Array(Some(responses));
        (aggregated_resp, rx)
    }

    /// 订阅单个模式
    async fn psubscribe_single(&self, pattern: Vec<u8>, client_id: u64) -> Value {
        let mut patterns = self.patterns.write().await;
        patterns
            .entry(pattern.clone())
            .or_default()
            .insert(client_id);

        let count = patterns.get(&pattern).map(|s| s.len()).unwrap_or(0) as i64;
        Value::Array(Some(vec![
            Value::SimpleString("psubscribe".to_string()),
            Value::BulkString(Some(pattern)),
            Value::Integer(count),
        ]))
    }

    /// 退订单个模式
    pub async fn punsubscribe(&self, pattern: &[u8], client_id: u64) -> Result<Value, Value> {
        let mut patterns = self.patterns.write().await;
        if let Some(set) = patterns.get_mut(pattern) {
            let count = set.len() as i64;
            if set.remove(&client_id) {
                if set.is_empty() {
                    patterns.remove(pattern);
                }

                // 从客户端状态中移除
                {
                    let mut clients = self.clients.write().await;
                    if let Some(state) = clients.get_mut(&client_id) {
                        state.subscribed_patterns.remove(pattern);
                        if state.subscribed_channels.is_empty()
                            && state.subscribed_patterns.is_empty()
                        {
                            clients.remove(&client_id);
                        }
                    }
                }

                let resp = Value::Array(Some(vec![
                    Value::SimpleString("punsubscribe".to_string()),
                    Value::BulkString(Some(pattern.to_vec())),
                    Value::Integer(count),
                ]));
                return Ok(resp);
            }
        }
        Err(Value::error("no such pattern subscription"))
    }

    /// 发布用户消息
    pub async fn publish_message(&self, channel: &[u8], message: Vec<u8>) -> Value {
        let pub_msg = Value::Array(Some(vec![
            Value::SimpleString("message".to_string()),
            Value::BulkString(Some(channel.to_vec())),
            Value::BulkString(Some(message)),
        ]));
        self.publish(channel, pub_msg).await
    }

    /// 发布消息
    pub async fn publish(&self, channel: &[u8], message: Value) -> Value {
        let mut delivered = 0i64;
        let mut targets = HashSet::new();

        // 精确频道订阅者
        let subs = self.subs.read().await;
        if let Some(set) = subs.get(channel) {
            targets.extend(set.iter().copied());
        }
        drop(subs);

        // 模式匹配订阅者
        let patterns = self.patterns.read().await;
        for (pattern, set) in patterns.iter() {
            if matches_pattern(channel, pattern) {
                targets.extend(set.iter().copied());
            }
        }
        drop(patterns);

        // 向目标客户端发送消息
        if !targets.is_empty() {
            let clients = self.clients.read().await;
            for client_id in targets {
                if let Some(state) = clients.get(&client_id) {
                    // watch::Sender 发送消息，所有 subscribe() 返回的 Receiver 都能收到
                    if state.sender.send(Some(message.clone())).is_ok() {
                        delivered += 1;
                    }
                }
            }
        }

        Value::Integer(delivered)
    }

    /// 完全移除客户端（连接断开时调用）
    pub async fn remove_client(&self, client_id: u64) {
        // 清理精确频道订阅
        let mut subs = self.subs.write().await;
        subs.retain(|_, set| {
            set.remove(&client_id);
            !set.is_empty()
        });
        drop(subs);

        // 清理模式订阅
        let mut patterns = self.patterns.write().await;
        patterns.retain(|_, set| {
            set.remove(&client_id);
            !set.is_empty()
        });
        drop(patterns);

        // 移除客户端状态，sender 被 drop 后，所有 Receiver 会收到 RecvError::Closed
        self.clients.write().await.remove(&client_id);
    }
}

/// 简单的 glob 风格模式匹配（支持 * 和 ?）
pub fn matches_pattern(channel: &[u8], pattern: &[u8]) -> bool {
    if pattern.is_empty() {
        return channel.is_empty();
    }

    match pattern[0] {
        b'*' => {
            for i in 0..=channel.len() {
                if matches_pattern(&channel[i..], &pattern[1..]) {
                    return true;
                }
            }
            false
        }
        b'?' => {
            if channel.is_empty() {
                false
            } else {
                matches_pattern(&channel[1..], &pattern[1..])
            }
        }
        c => {
            if channel.is_empty() || channel[0] != c {
                false
            } else {
                matches_pattern(&channel[1..], &pattern[1..])
            }
        }
    }
}
