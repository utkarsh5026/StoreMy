//! Smoke tests for the worker-pool dispatch in [`storemy::database::Database`].
//!
//! These don't try to prove any concurrency invariant — they just confirm that
//! firing several statements through the pool at once does not deadlock and
//! that every reply makes it back to the caller.

use storemy::engine::StatementResult;

use crate::common::TestDb;

#[test]
fn parallel_inserts_into_separate_tables_all_complete() {
    let db = TestDb::with_workers(4);

    // One table per "client". Separate tables avoid lock contention so we're
    // really only testing the dispatcher, not heap-level concurrency.
    for i in 0..4 {
        db.run_ok(&format!("CREATE TABLE t{i} (id INT)"));
    }

    // Submit all four INSERTs without blocking, then collect the replies.
    let receivers: Vec<_> = (0..4)
        .map(|i| db.db.execute(format!("INSERT INTO t{i} VALUES ({i})")))
        .collect();

    for (i, rx) in receivers.into_iter().enumerate() {
        let result = rx.recv().expect("worker disconnected").unwrap();
        let StatementResult::Inserted { rows, table } = result else {
            panic!("expected Inserted, got {result:?}");
        };
        assert_eq!(rows, 1);
        assert_eq!(table, format!("t{i}"));
    }

    for i in 0..4 {
        assert_eq!(db.scan_all(&format!("t{i}")).len(), 1);
    }
}

#[test]
fn replies_arrive_even_when_only_one_worker_is_running() {
    // A single worker means statements are effectively serialized; we just
    // want to confirm the queue drains in order without anyone getting stuck.
    let db = TestDb::with_workers(1);
    db.run_ok("CREATE TABLE t (id INT)");

    let r1 = db.db.execute("INSERT INTO t VALUES (1)");
    let r2 = db.db.execute("INSERT INTO t VALUES (2)");
    let r3 = db.db.execute("INSERT INTO t VALUES (3)");

    for rx in [r1, r2, r3] {
        rx.recv().expect("worker disconnected").unwrap();
    }
    assert_eq!(db.scan_all("t").len(), 3);
}
