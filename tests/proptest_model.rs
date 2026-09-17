//! Property tests: the storage engine must behave identically to a BTreeMap
//! reference model under arbitrary operation sequences, including across reopens.

use proptest::prelude::*;
use std::collections::BTreeMap;
use storagekv::db::Db;

/// One operation we can apply to both the engine and the model.
#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Get(Vec<u8>),
    Reopen, // close and reopen the DB — exercises recovery mid-sequence
}

/// Keys are drawn from a SMALL alphabet on purpose: with few distinct keys,
/// random sequences frequently overwrite and delete the same key, which is
/// exactly where the interesting bugs (shadowing, tombstones, compaction) live.
fn key_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(prop::sample::select(vec![b'a', b'b', b'c', b'd', b'e']), 1..4)
}

fn value_strategy() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..12)
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (key_strategy(), value_strategy()).prop_map(|(k, v)| Op::Put(k, v)),
        key_strategy().prop_map(Op::Delete),
        key_strategy().prop_map(Op::Get),
        Just(Op::Reopen),
    ]
}

fn temp_dir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    // Unique per test process AND per invocation, so parallel/repeated runs don't collide.
    let uniq = format!(
        "storagekv_prop_{}_{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    p.push(uniq);
    p
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(300))]

    #[test]
    fn engine_matches_model(ops in prop::collection::vec(op_strategy(), 1..200)) {
        let dir = temp_dir();
        let mut db = Db::open(&dir).unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        // Force frequent flushes/compaction so the disk paths are heavily exercised.
        db.set_flush_threshold_for_test(64);

        for op in ops {
            match op {
                Op::Put(k, v) => {
                    db.put(&k, &v).unwrap();
                    model.insert(k, v);
                }
                Op::Delete(k) => {
                    db.delete(&k).unwrap();
                    model.remove(&k);
                }
                Op::Get(k) => {
                    let got = db.get(&k).unwrap();
                    let expected = model.get(&k).cloned();
                    prop_assert_eq!(got, expected, "mismatch on get({:?})", k);
                }
                Op::Reopen => {
                    drop(db);
                    db = Db::open(&dir).unwrap();
                    db.set_flush_threshold_for_test(64);
                }
            }
        }

        // Final full check: every key the model knows must match, and a few
        // never-written keys must be absent in both.
        for (k, v) in &model {
            let got = db.get(k).unwrap();
            prop_assert_eq!(got.as_ref(), Some(v));
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}