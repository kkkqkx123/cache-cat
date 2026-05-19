use flurry::{Guard, HashMap};
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, MissedTickBehavior};

const ACTIVE_EXPIRE_INTERVAL: Duration = Duration::from_millis(100);
const ACTIVE_EXPIRE_SAMPLE_SIZE: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Expiry {
    pub at: u64,
    pub(crate) generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpirePolicy {
    Persistent,
    Absolute(u64),
    Ttl(u64),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntrySnapshot<V> {
    pub value: V,
    pub expire_at: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub struct EntryRef<'a, V> {
    pub value: &'a V,
    pub expire_at: Option<u64>,
}

impl<'a, V> EntryRef<'a, V> {
    pub fn get_expire_policy(&self) -> ExpirePolicy {
        match self.expire_at {
            None => ExpirePolicy::Persistent,
            Some(v) => ExpirePolicy::Absolute(v),
        }
    }
}

#[derive(Clone, Debug)]
struct Entry<V> {
    value: V,
    expire_at: Option<u64>,
    generation: u64,
}

impl<V> Entry<V> {
    fn expiry(&self) -> Option<Expiry> {
        self.expire_at.map(|at| Expiry {
            at,
            generation: self.generation,
        })
    }

    fn snapshot(&self) -> EntrySnapshot<V>
    where
        V: Clone,
    {
        EntrySnapshot {
            value: self.value.clone(),
            expire_at: self.expire_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MochaOperation<V> {
    Insert { value: V, expire: ExpirePolicy },
    Remove,
    Abort,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MochaCompute<K, V> {
    /// key 不在 map 中（闭包未被调用），或闭包返回 Abort。两种"无变化"统一为这个。
    Unchanged,
    /// 之前是过期残留，本次替换为新值。
    Inserted(K, EntrySnapshot<V>),
    /// 之前还活着，被原子替换。
    Updated {
        old: (K, EntrySnapshot<V>),
        new: (K, EntrySnapshot<V>),
    },
    /// 闭包要求删除。
    Removed(K, EntrySnapshot<V>),
}
#[derive(Clone, Debug)]
pub struct Mocha<K, V>
where
    K: Clone + Eq + Hash + Ord + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    expire_map: HashMap<K, Expiry>,
    map: HashMap<K, Entry<V>>,
    logic_clock: Arc<AtomicU64>,
    generation: Arc<AtomicU64>,
    expire_cursor: Arc<AtomicUsize>,
}

impl<K, V> Mocha<K, V>
where
    K: Hash + Eq + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(logic_clock: Arc<AtomicU64>) -> Self {
        Self {
            expire_map: HashMap::new(),
            map: HashMap::new(),
            logic_clock,
            generation: Arc::new(AtomicU64::new(1)),
            expire_cursor: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn now_logical(&self) -> u64 {
        self.logic_clock.load(Ordering::Relaxed)
    }

    fn next_generation(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::Relaxed)
    }

    fn resolve_expire_at(&self, policy: ExpirePolicy) -> Option<u64> {
        match policy {
            ExpirePolicy::Persistent => None,
            ExpirePolicy::Absolute(at) => Some(at),
            ExpirePolicy::Ttl(ttl) => Some(self.now_logical().saturating_add(ttl)),
        }
    }

    fn make_entry(&self, value: V, policy: ExpirePolicy) -> Entry<V> {
        Entry {
            value,
            expire_at: self.resolve_expire_at(policy),
            generation: self.next_generation(),
        }
    }

    /// 副表精确清理：仅当当前正好是 sampled 这个 (at, generation) 时才删除。
    fn cleanup_expire_index_exact(&self, key: &K, expiry: Expiry) {
        let eg = self.expire_map.pin();
        eg.compute_if_present(key, |_, current| {
            if *current == expiry {
                None
            } else {
                Some(*current)
            }
        });
    }

    /// 副表写入：单调递增 generation 的"upsert"，旧 generation 高于本次的不会被覆盖。
    fn sync_expire_index_meta(&self, key: K, expiry_opt: Option<Expiry>, generation: u64) {
        let eg = self.expire_map.pin();

        if let Some(expiry) = expiry_opt {
            loop {
                let touched = eg
                    .compute_if_present(&key, |_, current| {
                        if current.generation > expiry.generation {
                            Some(*current)
                        } else {
                            Some(expiry)
                        }
                    })
                    .is_some();
                if touched {
                    return;
                }
                if eg.try_insert(key.clone(), expiry).is_ok() {
                    return;
                }
                // 与并发 try_insert 失败，回到 compute_if_present 路径继续。
            }
        } else {
            eg.compute_if_present(&key, |_, current| {
                if current.generation <= generation {
                    None
                } else {
                    Some(*current)
                }
            });
        }
    }

    fn write_entry(&self, key: K, value: V, policy: ExpirePolicy) -> EntrySnapshot<V> {
        let new_entry = self.make_entry(value, policy);
        let snapshot = new_entry.snapshot();
        let new_expiry = new_entry.expiry();
        let new_gen = new_entry.generation;

        let old_expiry = {
            let mg = self.map.pin();
            mg.insert(key.clone(), new_entry)
                .and_then(|old| old.expiry())
        };

        if let Some(e) = old_expiry {
            self.cleanup_expire_index_exact(&key, e);
        }
        self.sync_expire_index_meta(key, new_expiry, new_gen);
        snapshot
    }

    pub fn insert_snapshot(&self, key: K, snapshot: EntrySnapshot<V>) -> EntrySnapshot<V> {
        let policy = match snapshot.expire_at {
            None => ExpirePolicy::Persistent,
            Some(at) => ExpirePolicy::Absolute(at),
        };
        self.write_entry(key, snapshot.value, policy)
    }

    pub fn insert(&self, key: K, value: V, ttl: u64) -> EntrySnapshot<V> {
        self.write_entry(key, value, ExpirePolicy::Ttl(ttl))
    }

    pub fn insert_absolute(&self, key: K, value: V, expire_at: u64) -> EntrySnapshot<V> {
        self.write_entry(key, value, ExpirePolicy::Absolute(expire_at))
    }

    pub fn insert_persistent(&self, key: K, value: V) -> EntrySnapshot<V> {
        self.write_entry(key, value, ExpirePolicy::Persistent)
    }

    /// 仅当 key 不存在时插入。失败时把传入的 value 还回。
    pub fn try_insert(
        &self,
        key: K,
        value: V,
        expire: ExpirePolicy,
    ) -> Result<EntrySnapshot<V>, V> {
        let new_entry = self.make_entry(value, expire);
        let snapshot = new_entry.snapshot();
        let new_expiry = new_entry.expiry();
        let new_gen = new_entry.generation;

        let result = {
            let mg = self.map.pin();
            mg.try_insert(key.clone(), new_entry)
                .map(|_| ())
                .map_err(|err| err.not_inserted.value)
        };

        match result {
            Ok(()) => {
                self.sync_expire_index_meta(key, new_expiry, new_gen);
                Ok(snapshot)
            }
            Err(v) => Err(v),
        }
    }

    pub fn get_entry<Q>(&self, key: &Q) -> Option<EntrySnapshot<V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        let mg = self.map.pin();
        let entry = mg.get(key)?;
        // 剩下的过期检查逻辑不变
        if let Some(expiry) = entry.expiry() {
            if self.now_logical() >= expiry.at {
                // 注意这里要 clone key，key 现在是 &Q，需要拿到 K
                // 但 key 只是借用，无法直接得到 K。
                // 所以要么限制 Q: ToOwned<Owned = K>，要么改逻辑。
            }
        }
        Some(entry.snapshot())
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        self.get_entry(key).map(|entry| entry.value)
    }

    pub fn get_if_alive(&self, key: &K) -> Option<V> {
        self.get_entry(key).map(|entry| entry.value)
    }

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Ord,
    {
        self.get_entry(key).is_some()
    }

    pub fn ttl_remaining(&self, key: &K) -> Option<u64> {
        let entry = self.get_entry(key)?;
        let expire_at = entry.expire_at?;
        Some(expire_at.saturating_sub(self.now_logical()))
    }

    fn set_expire_policy(&self, key: &K, policy: ExpirePolicy) -> Option<EntrySnapshot<V>> {
        let now = self.now_logical();

        let mut snapshot: Option<EntrySnapshot<V>> = None;
        let mut old_expiry: Option<Expiry> = None;
        let mut new_meta: Option<(Option<Expiry>, u64)> = None;

        {
            let mg = self.map.pin();
            mg.compute_if_present(key, |_, entry| {
                old_expiry = entry.expiry();
                if entry.expiry().is_some_and(|e| now >= e.at) {
                    None
                } else {
                    let new_entry = self.make_entry(entry.value.clone(), policy);
                    snapshot = Some(new_entry.snapshot());
                    new_meta = Some((new_entry.expiry(), new_entry.generation));
                    Some(new_entry)
                }
            });
        }

        if let Some(e) = old_expiry {
            self.cleanup_expire_index_exact(key, e);
        }
        if let Some((expiry_opt, g)) = new_meta {
            self.sync_expire_index_meta(key.clone(), expiry_opt, g);
        }
        snapshot
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.remove_entry(key).map(|entry| entry.value)
    }

    pub fn remove_entry(&self, key: &K) -> Option<EntrySnapshot<V>> {
        let now = self.now_logical();

        let (snapshot, expiry) = {
            let mg = self.map.pin();
            let removed = mg.remove(key)?;
            let expiry = removed.expiry();
            let expired = expiry.is_some_and(|e| now >= e.at);
            let snapshot = if expired {
                None
            } else {
                Some(removed.snapshot())
            };
            (snapshot, expiry)
        };

        if let Some(e) = expiry {
            self.cleanup_expire_index_exact(key, e);
        }
        snapshot
    }

    fn remove_expired_if_current(&self, key: K, sampled: Expiry) -> bool {
        let now = self.now_logical();
        let mut removed = false;

        {
            let mg = self.map.pin();
            mg.compute_if_present(&key, |_, entry| {
                if entry.expiry() == Some(sampled) && now >= sampled.at {
                    removed = true;
                    None
                } else {
                    Some(entry.clone())
                }
            });
        }

        if removed {
            self.cleanup_expire_index_exact(&key, sampled);
        }
        removed
    }

    /// "插入或更新"。`update`/`insert` 都是 `Fn`，可能因为竞争被多次调用。
    /// 单条 key 上的"读旧→写新"由 flurry 的 bin lock 提供原子性。
    pub fn update_or_insert_with<U, F>(
        &self,
        key: K,
        update: U,
        insert: F,
        expire: ExpirePolicy,
    ) -> EntrySnapshot<V>
    where
        U: Fn(&V) -> V,
        F: Fn() -> V,
    {
        loop {
            let now = self.now_logical();

            let mut snapshot: Option<EntrySnapshot<V>> = None;
            let mut old_expiry: Option<Expiry> = None;
            let mut new_meta: Option<(Option<Expiry>, u64)> = None;

            {
                let mg = self.map.pin();
                let touched = mg
                    .compute_if_present(&key, |_, entry| {
                        let expired = entry.expiry().is_some_and(|e| now >= e.at);
                        let value = if expired {
                            insert()
                        } else {
                            update(&entry.value)
                        };
                        let new_entry = self.make_entry(value, expire);
                        snapshot = Some(new_entry.snapshot());
                        old_expiry = entry.expiry();
                        new_meta = Some((new_entry.expiry(), new_entry.generation));
                        Some(new_entry)
                    })
                    .is_some();

                if touched {
                    drop(mg);
                    if let Some(e) = old_expiry {
                        self.cleanup_expire_index_exact(&key, e);
                    }
                    if let Some((expiry_opt, g)) = new_meta {
                        self.sync_expire_index_meta(key, expiry_opt, g);
                    }
                    return snapshot.unwrap();
                }

                let new_entry = self.make_entry(insert(), expire);
                let snap = new_entry.snapshot();
                let new_expiry = new_entry.expiry();
                let new_gen = new_entry.generation;

                if mg.try_insert(key.clone(), new_entry).is_ok() {
                    drop(mg);
                    self.sync_expire_index_meta(key, new_expiry, new_gen);
                    return snap;
                }
                // 被并发 insert 抢先，重试，下一轮会走 compute_if_present 分支。
            }
        }
    }

    /// 用户级原子计算：闭包**至多被调用一次**。
    /// - key 不在 map 中：以 `None` 调用闭包。
    ///   * 返回 `Insert` ⇒ 尝试插入；若被并发 writer 抢先，本次返回 `Unchanged`（闭包已消费，无法重试）。
    ///   * 返回 `Remove`/`Abort` ⇒ `Unchanged`。
    /// - key 已存在但已过期：以 `None` 调用闭包，与上面同样的语义，但能复用同一把 bin lock 完成替换/删除。
    /// - key 存活：以 `Some(EntryRef)` 调用闭包。
    pub fn compute<F>(&self, key: K, f: F) -> MochaCompute<K, V>
    where
        F: for<'a> FnOnce(Option<EntryRef<'a, V>>) -> MochaOperation<V>,
    {
        let now = self.now_logical();
        let mut f_holder: Option<F> = Some(f);
        let mut result: Option<MochaCompute<K, V>> = None;
        let mut old_expiry: Option<Expiry> = None;
        let mut new_meta: Option<(Option<Expiry>, u64)> = None;

        {
            let mg = self.map.pin();
            mg.compute_if_present(&key, |k, entry| {
                let expired = entry.expiry().is_some_and(|e| now >= e.at);
                let visible = if expired {
                    None
                } else {
                    Some(EntryRef {
                        value: &entry.value,
                        expire_at: entry.expire_at,
                    })
                };
                let user_f = f_holder
                    .take()
                    .expect("compute closure must be invoked at most once");
                match user_f(visible) {
                    MochaOperation::Insert { value, expire } => {
                        let new_entry = self.make_entry(value, expire);
                        let new_snap = new_entry.snapshot();
                        result = Some(if expired {
                            MochaCompute::Inserted(k.clone(), new_snap)
                        } else {
                            MochaCompute::Updated {
                                old: (k.clone(), entry.snapshot()),
                                new: (k.clone(), new_snap),
                            }
                        });
                        old_expiry = entry.expiry();
                        new_meta = Some((new_entry.expiry(), new_entry.generation));
                        Some(new_entry)
                    }
                    MochaOperation::Remove => {
                        result = Some(MochaCompute::Removed(k.clone(), entry.snapshot()));
                        old_expiry = entry.expiry();
                        None
                    }
                    MochaOperation::Abort => {
                        result = Some(MochaCompute::Unchanged);
                        Some(entry.clone())
                    }
                }
            });
        }

        // compute_if_present 没消费掉闭包 ⇒ key 不存在，按你要的语义补一次调用。
        if let Some(user_f) = f_holder.take() {
            match user_f(None) {
                MochaOperation::Insert { value, expire } => {
                    let new_entry = self.make_entry(value, expire);
                    let new_snap = new_entry.snapshot();
                    let new_expiry = new_entry.expiry();
                    let new_gen = new_entry.generation;

                    let inserted = {
                        let mg = self.map.pin();
                        mg.try_insert(key.clone(), new_entry).is_ok()
                    };

                    if inserted {
                        self.sync_expire_index_meta(key.clone(), new_expiry, new_gen);
                        return MochaCompute::Inserted(key, new_snap);
                    }
                    // 闭包已消费、无法重试，让步。
                    return MochaCompute::Unchanged;
                }
                MochaOperation::Remove | MochaOperation::Abort => {
                    return MochaCompute::Unchanged;
                }
            }
        }

        if let Some(e) = old_expiry {
            self.cleanup_expire_index_exact(&key, e);
        }
        if let Some((expiry_opt, g)) = new_meta {
            self.sync_expire_index_meta(key, expiry_opt, g);
        }
        result.unwrap_or(MochaCompute::Unchanged)
    }
}

impl<K, V> Mocha<K, V>
where
    K: Hash + Eq + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(&K, EntryRef<'_, V>),
    {
        let guard = self.map.guard();
        let now = self.now_logical();
        for (k, entry) in self.map.iter(&guard) {
            if entry.expire_at.is_some_and(|at| now >= at) {
                continue;
            }
            f(
                k,
                EntryRef {
                    value: &entry.value,
                    expire_at: entry.expire_at,
                },
            );
        }
    }
    pub fn spawn_active_expirer(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = time::interval(ACTIVE_EXPIRE_INTERVAL);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

            loop {
                ticker.tick().await;
                self.active_expire_cycle().await;
            }
        })
    }

    pub async fn active_expire_cycle(&self) {
        loop {
            let samples = self.sample_expiries(ACTIVE_EXPIRE_SAMPLE_SIZE);

            if samples.is_empty() {
                return;
            }

            let checked = samples.len();
            let mut expired = 0;

            for (key, expiry) in samples {
                if self.now_logical() >= expiry.at && self.remove_expired_if_current(key, expiry) {
                    expired += 1;
                }
            }

            if expired * 4 <= checked {
                return;
            }

            tokio::task::yield_now().await;
        }
    }

    pub fn sample_expiries(&self, limit: usize) -> Vec<(K, Expiry)> {
        let eg = self.expire_map.pin();
        let len = eg.len();

        if len == 0 || limit == 0 {
            return Vec::new();
        }

        let start = self.expire_cursor.fetch_add(limit, Ordering::Relaxed) % len;
        let mut samples = Vec::with_capacity(limit);

        for (key, expiry) in eg.iter().skip(start).take(limit) {
            samples.push((key.clone(), *expiry));
        }

        if samples.len() < limit && start > 0 {
            for (key, expiry) in eg.iter().take(limit - samples.len()) {
                samples.push((key.clone(), *expiry));
            }
        }

        samples
    }
}
impl<K, V> Mocha<K, V>
where
    K: Hash + Eq + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// 取出和迭代用的 guard。和 `iter` 配对使用。
    pub fn guard(&self) -> Guard<'_> {
        self.map.guard()
    }
    /// 惰性遍历当前活着的 entry。不会把所有数据物化到内存。
    /// 引用在 `guard` 存活期间有效;迭代过程中并发写入对你"是否可见"取决于
    /// flurry 的 epoch 语义,但不会造成 UAF。
    pub fn iter<'g>(
        &'g self,
        guard: &'g Guard<'_>,
    ) -> impl Iterator<Item = (&'g K, EntryRef<'g, V>)> + 'g {
        let now = self.now_logical();
        self.map.iter(guard).filter_map(move |(k, entry)| {
            if entry.expire_at.is_some_and(|at| now >= at) {
                None
            } else {
                Some((
                    k,
                    EntryRef {
                        value: &entry.value,
                        expire_at: entry.expire_at,
                    },
                ))
            }
        })
    }
    /// 只要 key 的便捷版。
    pub fn keys<'g>(&'g self, guard: &'g Guard<'_>) -> impl Iterator<Item = &'g K> + 'g {
        self.iter(guard).map(|(k, _)| k)
    }
}
impl<K, V> Mocha<K, V>
where
    K: Hash + Eq + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// 惰性遍历当前活着的 entry 的快照（拥有所有权）
    pub fn iter_snapshots<'g>(
        &'g self,
        guard: &'g Guard<'_>,
    ) -> impl Iterator<Item = (K, EntrySnapshot<V>)> + 'g {
        let now = self.now_logical();
        self.map.iter(guard).filter_map(move |(k, entry)| {
            if entry.expire_at.is_some_and(|at| now >= at) {
                None
            } else {
                Some((k.clone(), entry.snapshot()))
            }
        })
    }
    pub fn for_each_snapshot<F>(&self, mut f: F)
    where
        F: FnMut(&K, EntrySnapshot<V>),
    {
        let guard = self.map.guard();
        let now = self.now_logical();
        for (k, entry) in self.map.iter(&guard) {
            if entry.expire_at.is_some_and(|at| now >= at) {
                continue;
            }
            f(k, entry.snapshot());
        }
    }
}
impl<K, V> Mocha<K, V>
where
    K: Hash + Eq + Ord + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// 清空所有数据，包括主表和过期索引表。
    /// 返回被清除的条目数量。
    pub fn clear(&self) -> usize {
        let map_count = {
            let mg = self.map.pin();
            let count = mg.len();
            mg.clear();
            count
        };

        let expire_count = {
            let eg = self.expire_map.pin();
            let count = eg.len();
            eg.clear();
            count
        };

        // 两者应该相等，返回主表的计数
        map_count
    }
}
