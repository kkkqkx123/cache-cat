//! Redis-compatible Pub/Sub engine using papaya::HashMap and tokio::sync::watch.

use papaya::{HashMap, Operation};
use std::collections::HashSet;
use std::hash::RandomState;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubSubMessage {
    pub kind: MessageKind,
    pub channel: String,
    pub pattern: Option<String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    Message,
    PMessage,
    SMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeAck {
    Subscribe { channel: String, count: usize },
    PSubscribe { pattern: String, count: usize },
    SSubscribe { shard_channel: String, count: usize },
    Unsubscribe { channel: String, count: usize },
    PUnsubscribe { pattern: String, count: usize },
    SUnsubscribe { shard_channel: String, count: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubSubInfo {
    Channels(Vec<String>),
    NumSub(Vec<(String, usize)>),
    NumPat(usize),
    ShardChannels(Vec<String>),
    ShardNumSub(Vec<(String, usize)>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubSubSubcommand {
    Channels,
    NumSub,
    NumPat,
    ShardChannels,
    ShardNumSub,
}

type Payload = Option<PubSubMessage>;

struct SubscriptionSlot {
    sender: watch::Sender<Payload>,
    subscriber_count: AtomicUsize,
}

impl SubscriptionSlot {
    fn new() -> Self {
        let (tx, _) = watch::channel(None);
        Self {
            sender: tx,
            subscriber_count: AtomicUsize::new(0),
        }
    }

    fn subscribe(&self) -> watch::Receiver<Payload> {
        self.subscriber_count.fetch_add(1, Ordering::SeqCst);
        let mut rx = self.sender.subscribe();
        rx.borrow_and_update();
        rx
    }

    fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(Ordering::SeqCst)
    }
}

pub struct PubSub {
    channels: HashMap<String, Arc<SubscriptionSlot>>,
    patterns: HashMap<String, Arc<SubscriptionSlot>>,
    shard_channels: HashMap<String, Arc<SubscriptionSlot>>,
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSub {
    pub fn new() -> Self {
        Self {
            channels: HashMap::new(),
            patterns: HashMap::new(),
            shard_channels: HashMap::new(),
        }
    }

    pub fn connection(self: Arc<Self>) -> PubSubConnection {
        PubSubConnection::new(self)
    }

    pub fn subscribe(&self, channels: &[&str]) -> Vec<(String, watch::Receiver<Payload>)> {
        let pin = self.channels.pin();
        channels
            .iter()
            .map(|&ch| {
                let key = ch.to_string();
                let slot = get_or_insert_slot(&pin, key.clone());
                let rx = slot.subscribe();
                (key, rx)
            })
            .collect()
    }

    pub fn unsubscribe(&self, channels: &[&str]) -> usize {
        channels
            .iter()
            .map(|ch| self.unsubscribe_slot(&self.channels, ch))
            .sum()
    }

    fn unsubscribe_slot(&self, map: &HashMap<String, Arc<SubscriptionSlot>>, key: &str) -> usize {
        let key = key.to_string();
        let pin = map.pin();
        match pin.compute(key, |entry| match entry {
            Some((_, slot)) => {
                let prev = slot.subscriber_count.fetch_sub(1, Ordering::SeqCst);
                if prev == 1 {
                    Operation::Remove
                } else {
                    // 已在 slot 内减计数，map 条目不变
                    Operation::Abort(true)
                }
            }
            None => Operation::Abort(false),
        }) {
            papaya::Compute::Removed(..) | papaya::Compute::Aborted(true) => 1,
            _ => 0,
        }
    }

    pub fn psubscribe(&self, patterns: &[&str]) -> Vec<(String, watch::Receiver<Payload>)> {
        let pin = self.patterns.pin();
        patterns
            .iter()
            .map(|&pat| {
                let key = pat.to_string();
                let slot = get_or_insert_slot(&pin, key.clone());
                let rx = slot.subscribe();
                (key, rx)
            })
            .collect()
    }

    pub fn punsubscribe(&self, patterns: &[&str]) -> usize {
        patterns
            .iter()
            .map(|p| self.unsubscribe_slot(&self.patterns, p))
            .sum()
    }

    pub fn publish(&self, channel: &str, message: impl Into<Vec<u8>>) -> usize {
        let payload = message.into();
        self.publish_exact(channel, &payload) + self.publish_patterns(channel, &payload)
    }

    fn publish_exact(&self, channel: &str, payload: &[u8]) -> usize {
        let key = channel.to_string();
        let pin = self.channels.pin();
        let Some(slot) = pin.get(&key).cloned() else {
            return 0;
        };
        let count = slot.subscriber_count();
        if count == 0 {
            drop(pin);
            self.channels.pin().remove(&key);
            return 0;
        }
        let msg = PubSubMessage {
            kind: MessageKind::Message,
            channel: key,
            pattern: None,
            payload: payload.to_vec(),
        };
        let _ = slot.sender.send(Some(msg));
        count
    }

    fn publish_patterns(&self, channel: &str, payload: &[u8]) -> usize {
        let pin = self.patterns.pin();
        let matching: Vec<(String, Arc<SubscriptionSlot>)> = pin
            .iter()
            .filter(|(pat, _)| redis_glob_match(pat, channel))
            .map(|(pat, slot)| (pat.clone(), Arc::clone(slot)))
            .collect();
        drop(pin);

        let mut total = 0usize;
        for (pattern, slot) in matching {
            let n = slot.subscriber_count();
            if n == 0 {
                self.patterns.pin().remove(&pattern);
                continue;
            }
            let msg = PubSubMessage {
                kind: MessageKind::PMessage,
                channel: channel.to_string(),
                pattern: Some(pattern),
                payload: payload.to_vec(),
            };
            let _ = slot.sender.send(Some(msg));
            total += n;
        }
        total
    }

    pub fn ssubscribe(&self, shard_channels: &[&str]) -> Vec<(String, watch::Receiver<Payload>)> {
        let pin = self.shard_channels.pin();
        shard_channels
            .iter()
            .map(|&ch| {
                let key = ch.to_string();
                let slot = get_or_insert_slot(&pin, key.clone());
                let rx = slot.subscribe();
                (key, rx)
            })
            .collect()
    }

    pub fn sunsubscribe(&self, shard_channels: &[&str]) -> usize {
        shard_channels
            .iter()
            .map(|ch| self.unsubscribe_slot(&self.shard_channels, ch))
            .sum()
    }

    pub fn spublish(&self, shard_channel: &str, message: impl Into<Vec<u8>>) -> usize {
        let key = shard_channel.to_string();
        let pin = self.shard_channels.pin();
        let Some(slot) = pin.get(&key).cloned() else {
            return 0;
        };
        let count = slot.subscriber_count();
        if count == 0 {
            drop(pin);
            self.shard_channels.pin().remove(&key);
            return 0;
        }
        let msg = PubSubMessage {
            kind: MessageKind::SMessage,
            channel: key,
            pattern: None,
            payload: message.into(),
        };
        let _ = slot.sender.send(Some(msg));
        count
    }

    pub fn pubsub(&self, subcommand: PubSubSubcommand, args: &[&str]) -> PubSubInfo {
        match subcommand {
            PubSubSubcommand::Channels => {
                PubSubInfo::Channels(self.pubsub_channels(args.first().copied()))
            }
            PubSubSubcommand::NumSub => PubSubInfo::NumSub(self.pubsub_numsub(args)),
            PubSubSubcommand::NumPat => PubSubInfo::NumPat(self.pubsub_numpat()),
            PubSubSubcommand::ShardChannels => {
                PubSubInfo::ShardChannels(self.pubsub_shard_channels())
            }
            PubSubSubcommand::ShardNumSub => {
                PubSubInfo::ShardNumSub(self.pubsub_shard_numsub(args))
            }
        }
    }

    pub fn pubsub_channels(&self, pattern: Option<&str>) -> Vec<String> {
        let pin = self.channels.pin();
        pin.iter()
            .filter(|(_, slot)| slot.subscriber_count() > 0)
            .filter(|(ch, _)| match pattern {
                None => true,
                Some(pat) => redis_glob_match(pat, ch),
            })
            .map(|(ch, _)| ch.clone())
            .collect()
    }

    pub fn pubsub_numsub(&self, channels: &[&str]) -> Vec<(String, usize)> {
        let pin = self.channels.pin();
        channels
            .iter()
            .map(|&ch| {
                let count = pin
                    .get(&ch.to_string())
                    .map(|s| s.subscriber_count())
                    .unwrap_or(0);
                (ch.to_string(), count)
            })
            .collect()
    }

    pub fn pubsub_numpat(&self) -> usize {
        let pin = self.patterns.pin();
        pin.iter()
            .filter(|(_, slot)| slot.subscriber_count() > 0)
            .count()
    }

    pub fn pubsub_shard_channels(&self) -> Vec<String> {
        let pin = self.shard_channels.pin();
        pin.iter()
            .filter(|(_, slot)| slot.subscriber_count() > 0)
            .map(|(ch, _)| ch.clone())
            .collect()
    }

    pub fn pubsub_shard_numsub(&self, shard_channels: &[&str]) -> Vec<(String, usize)> {
        let pin = self.shard_channels.pin();
        shard_channels
            .iter()
            .map(|&ch| {
                let count = pin
                    .get(&ch.to_string())
                    .map(|s| s.subscriber_count())
                    .unwrap_or(0);
                (ch.to_string(), count)
            })
            .collect()
    }
}

pub struct PubSubConnection {
    hub: Arc<PubSub>,
    channels: HashSet<String>,
    patterns: HashSet<String>,
    shard_channels: HashSet<String>,
    channel_rxs: Vec<(String, watch::Receiver<Payload>)>,
    pattern_rxs: Vec<(String, watch::Receiver<Payload>)>,
    shard_rxs: Vec<(String, watch::Receiver<Payload>)>,
}

impl PubSubConnection {
    pub fn new(hub: Arc<PubSub>) -> Self {
        Self {
            hub,
            channels: HashSet::new(),
            patterns: HashSet::new(),
            shard_channels: HashSet::new(),
            channel_rxs: Vec::new(),
            pattern_rxs: Vec::new(),
            shard_rxs: Vec::new(),
        }
    }

    fn total_subscriptions(&self) -> usize {
        self.channels.len() + self.patterns.len() + self.shard_channels.len()
    }

    pub fn subscribe(&mut self, channels: &[&str]) -> (Vec<SubscribeAck>, Vec<watch::Receiver<Payload>>) {
        let mut acks = Vec::new();
        let mut new_rxs = Vec::new();
        for (name, rx) in self.hub.subscribe(channels) {
            if self.channels.insert(name.clone()) {
                acks.push(SubscribeAck::Subscribe {
                    channel: name.clone(),
                    count: self.total_subscriptions(),
                });
                self.channel_rxs.push((name, rx.clone()));
                new_rxs.push(rx);
            }
        }
        (acks, new_rxs)
    }

    pub fn unsubscribe(&mut self, channels: &[&str]) -> Vec<SubscribeAck> {
        let targets: Vec<String> = if channels.is_empty() {
            self.channels.iter().cloned().collect()
        } else {
            channels.iter().map(|s| (*s).to_string()).collect()
        };

        let mut acks = Vec::new();
        for ch in targets {
            if !self.channels.remove(&ch) {
                continue;
            }
            self.hub.unsubscribe(&[&ch]);
            self.channel_rxs.retain(|(name, _)| name != &ch);
            acks.push(SubscribeAck::Unsubscribe {
                channel: ch,
                count: self.total_subscriptions(),
            });
        }
        acks
    }

    pub fn psubscribe(&mut self, patterns: &[&str]) -> (Vec<SubscribeAck>, Vec<watch::Receiver<Payload>>) {
        let mut acks = Vec::new();
        let mut new_rxs = Vec::new();
        for (name, rx) in self.hub.psubscribe(patterns) {
            if self.patterns.insert(name.clone()) {
                acks.push(SubscribeAck::PSubscribe {
                    pattern: name.clone(),
                    count: self.total_subscriptions(),
                });
                self.pattern_rxs.push((name, rx.clone()));
                new_rxs.push(rx);
            }
        }
        (acks, new_rxs)
    }

    pub fn punsubscribe(&mut self, patterns: &[&str]) -> Vec<SubscribeAck> {
        let targets: Vec<String> = if patterns.is_empty() {
            self.patterns.iter().cloned().collect()
        } else {
            patterns.iter().map(|s| (*s).to_string()).collect()
        };

        let mut acks = Vec::new();
        for pat in targets {
            if !self.patterns.remove(&pat) {
                continue;
            }
            self.hub.punsubscribe(&[&pat]);
            self.pattern_rxs.retain(|(name, _)| name != &pat);
            acks.push(SubscribeAck::PUnsubscribe {
                pattern: pat,
                count: self.total_subscriptions(),
            });
        }
        acks
    }

    pub fn ssubscribe(
        &mut self,
        shard_channels: &[&str],
    ) -> (Vec<SubscribeAck>, Vec<watch::Receiver<Payload>>) {
        let mut acks = Vec::new();
        let mut new_rxs = Vec::new();
        for (name, rx) in self.hub.ssubscribe(shard_channels) {
            if self.shard_channels.insert(name.clone()) {
                acks.push(SubscribeAck::SSubscribe {
                    shard_channel: name.clone(),
                    count: self.total_subscriptions(),
                });
                self.shard_rxs.push((name, rx.clone()));
                new_rxs.push(rx);
            }
        }
        (acks, new_rxs)
    }

    pub fn sunsubscribe(&mut self, shard_channels: &[&str]) -> Vec<SubscribeAck> {
        let targets: Vec<String> = if shard_channels.is_empty() {
            self.shard_channels.iter().cloned().collect()
        } else {
            shard_channels.iter().map(|s| (*s).to_string()).collect()
        };

        let mut acks = Vec::new();
        for ch in targets {
            if !self.shard_channels.remove(&ch) {
                continue;
            }
            self.hub.sunsubscribe(&[&ch]);
            self.shard_rxs.retain(|(name, _)| name != &ch);
            acks.push(SubscribeAck::SUnsubscribe {
                shard_channel: ch,
                count: self.total_subscriptions(),
            });
        }
        acks
    }

    pub fn publish(&self, channel: &str, message: impl Into<Vec<u8>>) -> usize {
        self.hub.publish(channel, message)
    }

    pub fn spublish(&self, shard_channel: &str, message: impl Into<Vec<u8>>) -> usize {
        self.hub.spublish(shard_channel, message)
    }

    pub fn pubsub(&self, subcommand: PubSubSubcommand, args: &[&str]) -> PubSubInfo {
        self.hub.pubsub(subcommand, args)
    }

    pub fn receivers(&self) -> impl Iterator<Item = &watch::Receiver<Payload>> {
        self.channel_rxs
            .iter()
            .chain(self.pattern_rxs.iter())
            .chain(self.shard_rxs.iter())
            .map(|(_, rx)| rx)
    }
}

fn get_or_insert_slot<G: papaya::Guard>(
    pin: &papaya::HashMapRef<'_, String, Arc<SubscriptionSlot>, RandomState, G>,
    key: String,
) -> Arc<SubscriptionSlot> {
    Arc::clone(
        pin.update_or_insert_with(key, Arc::clone, || Arc::new(SubscriptionSlot::new())),
    )
}

pub fn redis_glob_match(pattern: &str, text: &str) -> bool {
    let p = pattern.as_bytes();
    let t = text.as_bytes();
    glob_match_impl(p, t, 0, 0)
}

fn glob_match_impl(p: &[u8], t: &[u8], mut pi: usize, mut ti: usize) -> bool {
    let mut star_pi = None;
    let mut star_ti = None;

    loop {
        if pi < p.len() {
            if p[pi] == b'\\' {
                if pi + 1 >= p.len() {
                    return false;
                }
                if ti >= t.len() || p[pi + 1] != t[ti] {
                    return false;
                }
                pi += 2;
                ti += 1;
                continue;
            }
            if p[pi] == b'*' {
                star_pi = Some(pi);
                star_ti = Some(ti);
                pi += 1;
                continue;
            }
            if p[pi] == b'?' {
                if ti >= t.len() {
                    return false;
                }
                pi += 1;
                ti += 1;
                continue;
            }
            if p[pi] == b'[' {
                if let Some((end, matched)) = match_bracket(p, pi, t, ti) {
                    if !matched {
                        return false;
                    }
                    pi = end + 1;
                    ti += 1;
                    continue;
                }
                return false;
            }
            if ti < t.len() && p[pi] == t[ti] {
                pi += 1;
                ti += 1;
                continue;
            }
        }
        if pi == p.len() && ti == t.len() {
            return true;
        }
        if let (Some(sp), Some(st)) = (star_pi, star_ti) {
            if st < t.len() {
                star_ti = Some(st + 1);
                pi = sp + 1;
                ti = star_ti.unwrap();
                continue;
            }
        }
        return false;
    }
}

fn match_bracket(p: &[u8], start: usize, t: &[u8], ti: usize) -> Option<(usize, bool)> {
    if ti >= t.len() {
        return None;
    }
    let mut i = start + 1;
    if i >= p.len() {
        return None;
    }

    let mut negate = false;
    if p[i] == b'^' {
        negate = true;
        i += 1;
    }

    let mut matched = false;
    while i < p.len() && p[i] != b']' {
        if p[i] == b'\\' {
            if i + 1 >= p.len() {
                return None;
            }
            if p[i + 1] == t[ti] {
                matched = true;
            }
            i += 2;
            continue;
        }
        if i + 2 < p.len() && p[i + 1] == b'-' && p[i + 2] != b']' {
            let lo = p[i];
            let hi = p[i + 2];
            if t[ti] >= lo && t[ti] <= hi {
                matched = true;
            }
            i += 3;
            continue;
        }
        if p[i] == t[ti] {
            matched = true;
        }
        i += 1;
    }
    if i >= p.len() || p[i] != b']' {
        return None;
    }
    Some((i, matched ^ negate))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_delivers_to_subscriber() {
        let ps = Arc::new(PubSub::new());
        let mut conn = ps.connection();
        let (_, mut rxs) = conn.subscribe(&["news"]);
        let mut rx = rxs.remove(0);

        assert_eq!(conn.publish("news", b"hello"), 1);
        rx.changed().await.unwrap();
        let msg = rx.borrow().clone().unwrap();
        assert_eq!(msg.kind, MessageKind::Message);
        assert_eq!(msg.channel, "news");
        assert_eq!(msg.payload, b"hello");
    }

    #[tokio::test]
    async fn pattern_subscriber_receives_pmessage() {
        let ps = PubSub::new();
        let mut rx = ps.psubscribe(&["news.*"])[0].1.clone();

        assert_eq!(ps.publish("news.sports", b"score"), 1);
        rx.changed().await.unwrap();
        let msg = rx.borrow().clone().unwrap();
        assert_eq!(msg.kind, MessageKind::PMessage);
        assert_eq!(msg.pattern.as_deref(), Some("news.*"));
    }

    #[tokio::test]
    async fn unsubscribe_all_channels() {
        let ps = Arc::new(PubSub::new());
        let mut conn = ps.connection();
        conn.subscribe(&["a", "b"]);
        let acks = conn.unsubscribe(&[]);
        assert_eq!(acks.len(), 2);
        assert!(conn.channels.is_empty());
    }

    #[test]
    fn glob_cases() {
        assert!(redis_glob_match("news.*", "news.sports"));
        assert!(!redis_glob_match("news.?", "news.sports"));
        assert!(redis_glob_match(r"foo\*bar", "foo*bar"));
        assert!(redis_glob_match("h[ae]llo", "hello"));
        assert!(redis_glob_match("h[ae]llo", "hallo"));
        assert!(!redis_glob_match("h[ae]llo", "hillo"));
    }
}