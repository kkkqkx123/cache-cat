#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use crate::mocha::{ExpirePolicy, Mocha, MochaCompute, MochaOperation};

    // ---------- helpers ----------
    fn advance_clock(clock: &Arc<AtomicU64>, delta: u64) {
        clock.fetch_add(delta, Ordering::Relaxed);
    }

    fn new_clock() -> Arc<AtomicU64> {
        Arc::new(AtomicU64::new(0))
    }

    // ---------- basic insert / get ----------
    #[test]
    fn test_insert_and_get() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert_persistent("key1".into(), "val1".into());
        assert_eq!(cache.get(&"key1".into()), Some("val1".into()));
        assert!(cache.contains_key_alive(&"key1".into()));
    }

    #[test]
    fn test_insert_with_ttl_not_expired() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert("key1".into(), "val1".into(), 10);
        advance_clock(&clock, 5);
        assert_eq!(cache.get(&"key1".into()), Some("val1".into()));
    }

    #[test]
    fn test_insert_with_ttl_expired_on_access() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert("key1".into(), "val1".into(), 10);
        advance_clock(&clock, 15);
        assert_eq!(cache.get(&"key1".into()), None);
        assert!(!cache.contains_key_alive(&"key1".into()));
    }

    #[test]
    fn test_insert_absolute_expiry() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert_absolute("key1".into(), "val1".into(), 100);
        advance_clock(&clock, 99);
        assert!(cache.get(&"key1".into()).is_some());
        advance_clock(&clock, 2);
        assert!(cache.get(&"key1".into()).is_none());
    }

    #[test]
    fn test_persistent_never_expires() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert_persistent("key1".into(), "val1".into());
        advance_clock(&clock, 100_000);
        assert_eq!(cache.get(&"key1".into()), Some("val1".into()));
    }

    // ---------- snapshot / expiry ----------
    #[test]
    fn test_entry_snapshot_expiry() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        let snap = cache.insert("k".into(), "v".into(), 50);
        assert!(snap.expire_at.is_some());
        assert!(snap.expiry().is_some());
        assert_eq!(snap.expiry().unwrap().at, 50);
        assert!(snap.generation > 0);
    }

    #[test]
    fn test_ttl_remaining() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert("k".into(), "v".into(), 100);
        advance_clock(&clock, 30);
        assert_eq!(cache.ttl_remaining(&"k".into()), Some(70));
        advance_clock(&clock, 70);
        println!("{:?}", cache.ttl_remaining(&"k".into()));
        assert_eq!(cache.ttl_remaining(&"k".into()), None);
        advance_clock(&clock, 1);
        assert_eq!(cache.ttl_remaining(&"k".into()), None);
    }

    #[test]
    fn test_expire_at_and_persist() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert_persistent("k".into(), "v".into());
        let expiry = cache.expire_at(&"k".into(), 200);
        assert!(expiry.is_some());
        assert_eq!(expiry.unwrap().at, 200);
        cache.persist(&"k".into());
        assert!(cache.get_entry(&"k".into()).unwrap().expire_at.is_none());
    }

    // ---------- remove ----------
    #[test]
    fn test_remove_existing() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert_persistent("k".into(), "v".into());
        assert_eq!(cache.remove(&"k".into()), Some("v".into()));
        assert!(cache.get(&"k".into()).is_none());
    }

    #[test]
    fn test_remove_expired() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert("k".into(), "v".into(), 10);
        advance_clock(&clock, 20);
        assert_eq!(cache.remove(&"k".into()), None);
    }

    #[test]
    fn test_remove_entry_snapshot() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert_persistent("k".into(), "v".into());
        let snap = cache.remove_entry(&"k".into()).unwrap();
        assert_eq!(snap.value, "v");
        assert!(snap.expire_at.is_none());
    }

    // ---------- update_or_insert_with ----------
    #[test]
    fn test_update_or_insert_with_inserts_when_absent() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        let snap = cache.update_or_insert_with(
            "k".into(),
            |v| format!("{}-updated", v),
            || "inserted".into(),
            ExpirePolicy::Persistent,
        );
        assert_eq!(snap.value, "inserted");
    }

    #[test]
    fn test_update_or_insert_with_updates_existing() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert_persistent("k".into(), "initial".into());
        let snap = cache.update_or_insert_with(
            "k".into(),
            |v| format!("{}-updated", v),
            || unreachable!(),
            ExpirePolicy::Persistent,
        );
        assert_eq!(snap.value, "initial-updated");
    }

    #[test]
    fn test_update_or_insert_with_skips_expired() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert("k".into(), "old".into(), 5);
        advance_clock(&clock, 10);
        let snap = cache.update_or_insert_with(
            "k".into(),
            |_| unreachable!(),
            || "fresh".into(),
            ExpirePolicy::Persistent,
        );
        assert_eq!(snap.value, "fresh");
    }

    // ---------- compute (fix: turbofish for T) ----------
    #[test]
    fn test_compute_insert() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());

        // Use `::<_, ()>` to specify T = ()
        let result = cache.compute::<_, ()>("k".into(), |opt| match opt {
            None => MochaOperation::Insert {
                value: "new".into(),
                expire: ExpirePolicy::Persistent,
            },
            Some(_) => MochaOperation::Abort(()),
        });

        match result {
            MochaCompute::Inserted(key, snap) => {
                assert_eq!(key, "k");
                assert_eq!(snap.value, "new");
            }
            _ => panic!("Expected Inserted"),
        }
    }

    #[test]
    fn test_compute_update() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert_persistent("k".into(), "old".into());

        let result = cache.compute::<_, ()>("k".into(), |opt| {
            let entry = opt.unwrap();
            MochaOperation::Insert {
                value: format!("{}-patched", entry.value),
                expire: ExpirePolicy::Persistent,
            }
        });

        match result {
            MochaCompute::Updated { old, new } => {
                assert_eq!(old.1.value, "old");
                assert_eq!(new.1.value, "old-patched");
            }
            _ => panic!("Expected Updated"),
        }
    }

    #[test]
    fn test_compute_remove() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert_persistent("k".into(), "v".into());

        let result = cache.compute::<_, ()>("k".into(), |_| MochaOperation::Remove);

        match result {
            MochaCompute::Removed(key, snap) => {
                assert_eq!(key, "k");
                assert_eq!(snap.value, "v");
            }
            _ => panic!("Expected Removed"),
        }
        assert!(cache.get(&"k".into()).is_none());
    }

    #[test]
    fn test_compute_abort() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        // Here T is inferred from the abort value (u32)
        let result = cache.compute("k".into(), |_| MochaOperation::Abort(42u32));
        match result {
            MochaCompute::Aborted(code) => assert_eq!(code, 42),
            _ => panic!("Expected Aborted"),
        }
    }

    // ---------- active expire (tokio runtime) ----------
    #[tokio::test]
    async fn test_active_expire_cycle_removes_expired_keys() {
        let clock = new_clock();
        let cache = Arc::new(Mocha::<String, String>::new(clock.clone()));

        cache.insert("k1".into(), "v1".into(), 10);
        cache.insert("k2".into(), "v2".into(), 10);
        cache.insert_persistent("k3".into(), "v3".into());

        advance_clock(&clock, 15);
        cache.active_expire_cycle().await;

        assert!(cache.get(&"k1".into()).is_none());
        assert!(cache.get(&"k2".into()).is_none());
        assert_eq!(cache.get(&"k3".into()), Some("v3".into()));
    }

    #[tokio::test]
    async fn test_active_expire_cycle_stops_when_no_expiries() {
        let clock = new_clock();
        let cache = Arc::new(Mocha::<String, String>::new(clock.clone()));
        cache.insert_persistent("p1".into(), "v".into());
        cache.active_expire_cycle().await; // just ensure no panic
    }

    #[tokio::test]
    async fn test_active_expire_cycle_respects_generation() {
        let clock = new_clock();
        let cache = Arc::new(Mocha::<String, String>::new(clock.clone()));

        cache.insert("k".into(), "v1".into(), 10);       // gen 1
        advance_clock(&clock, 5);
        cache.insert("k".into(), "v2".into(), 20);       // gen 2, expire at 25
        advance_clock(&clock, 15);                       // now = 20

        cache.active_expire_cycle().await;
        assert_eq!(cache.get(&"k".into()), Some("v2".into()));

        advance_clock(&clock, 10);                       // now = 30, expired
        cache.active_expire_cycle().await;
        assert!(cache.get(&"k".into()).is_none());
    }

    // ---------- expire index cleanup ----------
    #[test]
    fn test_expire_index_cleanup_on_update() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert("k".into(), "v1".into(), 10);
        cache.insert_persistent("k".into(), "v2".into());
        let snap = cache.get_entry(&"k".into()).unwrap();
        assert!(snap.expire_at.is_none());
    }

    #[test]
    fn test_expire_index_cleanup_on_remove() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert("k".into(), "v".into(), 10);
        cache.remove(&"k".into());
        cache.insert_persistent("k".into(), "new".into());
        assert_eq!(cache.get(&"k".into()), Some("new".into()));
    }

    // ---------- concurrency ----------
    #[tokio::test]
    async fn test_concurrent_inserts_and_reads() {
        let clock = new_clock();
        let cache = Arc::new(Mocha::<i32, i32>::new(clock.clone()));
        let mut handles = vec![];

        for i in 0..10 {
            let cache = cache.clone();
            let clock = clock.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..100 {
                    let key = i * 1000 + j;
                    cache.insert_persistent(key, key * 10);
                    if j % 10 == 0 {
                        advance_clock(&clock, 1);
                    }
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        for i in 0..10 {
            for j in 0..100 {
                let key = i * 1000 + j;
                assert_eq!(cache.get(&key), Some(key * 10));
            }
        }
    }

    #[tokio::test]
    async fn test_concurrent_insert_and_expire() {
        let clock = new_clock();
        let cache = Arc::new(Mocha::<String, String>::new(clock.clone()));

        let cache_clone = cache.clone();
        let clock_clone = clock.clone();
        let writer = tokio::spawn(async move {
            for i in 0..100 {
                let key = format!("k{}", i);
                cache_clone.insert(key.clone(), format!("v{}", i), 50 + i as i64);
                advance_clock(&clock_clone, 2);
            }
        });

        writer.await.unwrap();
        advance_clock(&clock, 200);

        for _ in 0..5 {
            cache.active_expire_cycle().await;
            tokio::task::yield_now().await;
        }

        for i in 0..100 {
            let key = format!("k{}", i);
            assert!(cache.get(&key).is_none(), "key {} should be expired", key);
        }
    }

    #[test]
    fn test_sample_expiries() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        for i in 0..50 {
            cache.insert(format!("k{}", i), format!("v{}", i), 100);
        }
        let samples = cache.sample_expiries(20);
        assert_eq!(samples.len(), 20);
        for (k, expiry) in &samples {
            assert!(k.starts_with('k'));
            assert_eq!(expiry.at, 100);
        }
    }

    #[test]
    fn test_sample_expiries_empty_when_no_expiring_keys() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        cache.insert_persistent("k".into(), "v".into());
        assert!(cache.sample_expiries(10).is_empty());
    }

    // ---------- edge ----------
    #[test]
    fn test_insert_same_key_multiple_times() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        let s1 = cache.insert("k".into(), "a".into(), 10);
        let s2 = cache.insert("k".into(), "b".into(), 20);
        assert_ne!(s1.generation, s2.generation);
        assert_eq!(cache.get(&"k".into()), Some("b".into()));
    }

    #[test]
    fn test_generation_monotonically_increases() {
        let clock = new_clock();
        let cache = Mocha::<String, String>::new(clock.clone());
        let mut last_gen = 0;
        for i in 0..10 {
            let snap = cache.insert_persistent(format!("k{}", i), i.to_string());
            assert!(snap.generation > last_gen);
            last_gen = snap.generation;
        }
    }
}