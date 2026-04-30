use std::collections::VecDeque;

use hashbrown::{HashMap, HashSet};
use parking_lot::Mutex;
use thiserror::Error;

use crate::{TransactionId, primitives::PageId};

const MAX_LOCK_RETRIES: usize = 100;
const LOCK_PAGE_BACKOFF_MULTIPLIER: [u32; 6] = [1, 2, 4, 8, 16, 32];

#[derive(Debug, Error)]
pub enum LockError {
    #[error("deadlock detected for transaction {0}")]
    Deadlock(TransactionId),

    #[error("timeout waiting for lock on page {file_id}:{page_no}")]
    Timeout { file_id: u64, page_no: u32 },

    #[error("lock not yet granted, retry")]
    NotYetGranted,
}

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum LockType {
    Shared,
    Exclusive,
}

impl LockType {
    pub(super) fn is_exclusive(self) -> bool {
        self == Self::Exclusive
    }

    pub(super) fn is_shared(self) -> bool {
        self == Self::Shared
    }
}

#[derive(Debug, Clone)]
pub(super) struct Lock {
    transaction_id: TransactionId,
    kind: LockType,
}

impl Lock {
    pub(super) fn new(transaction_id: TransactionId, kind: LockType) -> Self {
        Self {
            transaction_id,
            kind,
        }
    }
}

#[derive(Default, Debug, Clone)]
struct DependencyGraph {
    cache_valid: bool,
    was_cycle_last_time: bool,
    edges: HashMap<TransactionId, HashSet<TransactionId>>,
}

impl DependencyGraph {
    pub(super) fn add_transaction(&mut self, waiter: TransactionId, holder: TransactionId) {
        self.edges.entry(waiter).or_default().insert(holder);
        self.cache_valid = false;
    }

    pub(super) fn remove_transaction(&mut self, transaction_id: TransactionId) {
        self.edges.remove(&transaction_id);
        for holders in self.edges.values_mut() {
            holders.remove(&transaction_id);
        }
        self.cache_valid = false;
    }

    pub(super) fn has_cycle(&mut self) -> bool {
        if self.cache_valid {
            return self.was_cycle_last_time;
        }

        let mut in_degree: HashMap<TransactionId, usize> = HashMap::new();
        for (waiter, holders) in &self.edges {
            in_degree.entry(*waiter).or_insert(0);
            for holder in holders {
                *in_degree.entry(*holder).or_insert(0) += 1;
            }
        }

        let mut queue: VecDeque<TransactionId> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(&tid, _)| tid)
            .collect();

        let total = in_degree.len();
        let mut processed = 0;

        while let Some(node) = queue.pop_front() {
            processed += 1;
            if let Some(holders) = self.edges.get(&node) {
                for holder in holders {
                    let deg = in_degree.get_mut(holder).unwrap();
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(*holder);
                    }
                }
            }
        }

        let result = processed < total;
        self.was_cycle_last_time = result;
        self.cache_valid = true;
        result
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LockRequest {
    transaction_id: TransactionId,
    page_id: PageId,
    lock_type: LockType,
}

impl From<(TransactionId, PageId, LockType)> for LockRequest {
    fn from((transaction_id, page_id, lock_type): (TransactionId, PageId, LockType)) -> Self {
        Self {
            transaction_id,
            page_id,
            lock_type,
        }
    }
}

impl From<LockRequest> for (TransactionId, PageId, LockType) {
    fn from(req: LockRequest) -> Self {
        (req.transaction_id, req.page_id, req.lock_type)
    }
}

impl LockRequest {
    pub fn shared(transaction_id: TransactionId, page_id: PageId) -> Self {
        Self {
            transaction_id,
            page_id,
            lock_type: LockType::Shared,
        }
    }

    pub fn exclusive(transaction_id: TransactionId, page_id: PageId) -> Self {
        Self {
            transaction_id,
            page_id,
            lock_type: LockType::Exclusive,
        }
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn is_exclusive(&self) -> bool {
        self.lock_type.is_exclusive()
    }

    #[must_use]
    pub fn as_shared(&self) -> Self {
        Self {
            transaction_id: self.transaction_id,
            page_id: self.page_id,
            lock_type: LockType::Shared,
        }
    }

    #[must_use]
    pub fn as_exclusive(&self) -> Self {
        Self {
            transaction_id: self.transaction_id,
            page_id: self.page_id,
            lock_type: LockType::Exclusive,
        }
    }
}

// Owns the two maps that together form the lock table invariant:
// every entry in page_locks has a matching entry in transaction_locks and vice versa.
struct LockTable {
    page_locks: HashMap<PageId, Vec<Lock>>,
    transaction_locks: HashMap<TransactionId, HashMap<PageId, LockType>>,
}

#[allow(dead_code)] // unlock / inspect paths reserved for transaction integration
impl LockTable {
    fn new() -> Self {
        Self {
            page_locks: HashMap::new(),
            transaction_locks: HashMap::new(),
        }
    }

    fn has_sufficient_lock(&self, req: &LockRequest) -> bool {
        let Some(tx_pages) = self.transaction_locks.get(&req.transaction_id) else {
            return false;
        };
        let Some(curr) = tx_pages.get(&req.page_id) else {
            return false;
        };
        curr.is_exclusive() || (curr.is_shared() && req.lock_type.is_shared())
    }

    fn has_lock_type(&self, req: &LockRequest) -> bool {
        self.transaction_locks
            .get(&req.transaction_id)
            .and_then(|pages| pages.get(&req.page_id))
            .is_some_and(|lt| *lt == req.lock_type)
    }

    fn can_grant_immediately(&self, req: &LockRequest) -> bool {
        let Some(locks) = self.page_locks.get(&req.page_id) else {
            return true;
        };
        if locks.is_empty() {
            return true;
        }
        if req.is_exclusive() {
            return locks.iter().all(|l| l.transaction_id == req.transaction_id);
        }
        !locks
            .iter()
            .any(|l| l.transaction_id != req.transaction_id && l.kind.is_exclusive())
    }

    fn can_upgrade(&self, req: &LockRequest) -> bool {
        self.page_locks
            .get(&req.page_id)
            .is_none_or(|locks| locks.iter().all(|l| l.transaction_id == req.transaction_id))
    }

    fn add(&mut self, req: &LockRequest) {
        self.page_locks
            .entry(req.page_id)
            .or_default()
            .push(Lock::new(req.transaction_id, req.lock_type));

        self.transaction_locks
            .entry(req.transaction_id)
            .or_default()
            .insert(req.page_id, req.lock_type);
    }

    fn upgrade(&mut self, req: &LockRequest) {
        if let Some(locks) = self.page_locks.get_mut(&req.page_id)
            && let Some(lock) = locks
                .iter_mut()
                .find(|l| l.transaction_id == req.transaction_id)
        {
            lock.kind = LockType::Exclusive;
        }
        self.transaction_locks
            .entry(req.transaction_id)
            .or_default()
            .insert(req.page_id, LockType::Exclusive);
    }

    fn release(&mut self, transaction_id: TransactionId, page_id: PageId) {
        if let Some(locks) = self.page_locks.get_mut(&page_id) {
            locks.retain(|l| l.transaction_id != transaction_id);
            if locks.is_empty() {
                self.page_locks.remove(&page_id);
            }
        }
        if let Some(tx_pages) = self.transaction_locks.get_mut(&transaction_id) {
            tx_pages.remove(&page_id);
            if tx_pages.is_empty() {
                self.transaction_locks.remove(&transaction_id);
            }
        }
    }

    fn release_all(&mut self, transaction_id: TransactionId) -> Vec<PageId> {
        let Some(tx_pages) = self.transaction_locks.remove(&transaction_id) else {
            return vec![];
        };
        let affected: Vec<PageId> = tx_pages.into_keys().collect();
        for &page_id in &affected {
            if let Some(locks) = self.page_locks.get_mut(&page_id) {
                locks.retain(|l| l.transaction_id != transaction_id);
                if locks.is_empty() {
                    self.page_locks.remove(&page_id);
                }
            }
        }
        affected
    }

    fn is_locked(&self, page_id: PageId) -> bool {
        self.page_locks.get(&page_id).is_some_and(|l| !l.is_empty())
    }

    fn page_locks_for(&self, page_id: PageId) -> Option<&Vec<Lock>> {
        self.page_locks.get(&page_id)
    }
}

// Owns the two maps that together form the wait queue invariant:
// every request in page_queue has a matching entry in transaction_waiting and vice versa.
struct WaitQueue {
    page_queue: HashMap<PageId, VecDeque<LockRequest>>,
    transaction_waiting: HashMap<TransactionId, Vec<PageId>>,
}

impl WaitQueue {
    fn new() -> Self {
        Self {
            page_queue: HashMap::new(),
            transaction_waiting: HashMap::new(),
        }
    }

    fn enqueue(&mut self, req: &LockRequest) {
        let already_queued = self
            .page_queue
            .get(&req.page_id)
            .is_some_and(|q| q.iter().any(|r| r.transaction_id == req.transaction_id));

        if already_queued {
            return;
        }

        self.page_queue
            .entry(req.page_id)
            .or_default()
            .push_back(*req);

        self.transaction_waiting
            .entry(req.transaction_id)
            .or_default()
            .push(req.page_id);
    }

    fn dequeue(&mut self, req: &LockRequest) {
        if let Some(queue) = self.page_queue.get_mut(&req.page_id) {
            queue.retain(|r| r.transaction_id != req.transaction_id);
        }
        if let Some(pages) = self.transaction_waiting.get_mut(&req.transaction_id) {
            pages.retain(|&p| p != req.page_id);
        }
    }

    fn remove_all_for(&mut self, transaction_id: TransactionId) {
        let Some(pages) = self.transaction_waiting.remove(&transaction_id) else {
            return;
        };
        for page_id in pages {
            if let Some(queue) = self.page_queue.get_mut(&page_id) {
                queue.retain(|r| r.transaction_id != transaction_id);
            }
        }
    }

    fn pending_for(&self, page_id: PageId) -> Vec<LockRequest> {
        self.page_queue
            .get(&page_id)
            .map(|q| q.iter().copied().collect())
            .unwrap_or_default()
    }

    fn remove_granted(&mut self, page_id: PageId, granted: &[LockRequest]) {
        if let Some(queue) = self.page_queue.get_mut(&page_id) {
            for req in granted {
                queue.retain(|r| r.transaction_id != req.transaction_id);
            }
        }
    }
}

struct LockManagerState {
    dep_graph: DependencyGraph,
    lock_table: LockTable,
    wait_queue: WaitQueue,
}

#[allow(dead_code)] // release_lock / release_all_locks for explicit lock lifecycle APIs
impl LockManagerState {
    fn new() -> Self {
        Self {
            dep_graph: DependencyGraph::default(),
            lock_table: LockTable::new(),
            wait_queue: WaitQueue::new(),
        }
    }

    fn attempt_to_acquire_lock(&mut self, req: &LockRequest) -> Result<(), LockError> {
        if self.lock_table.has_sufficient_lock(req) {
            return Ok(());
        }

        if req.is_exclusive()
            && self.lock_table.has_lock_type(&req.as_shared())
            && self.lock_table.can_upgrade(req)
        {
            self.lock_table.upgrade(req);
            return Ok(());
        }

        if self.lock_table.can_grant_immediately(req) {
            self.grant_lock(req);
            return Ok(());
        }

        self.wait_queue.enqueue(req);
        self.update_dependencies(req);

        if self.dep_graph.has_cycle() {
            self.wait_queue.dequeue(req);
            self.dep_graph.remove_transaction(req.transaction_id);
            return Err(LockError::Deadlock(req.transaction_id));
        }

        Err(LockError::NotYetGranted)
    }

    fn grant_lock(&mut self, req: &LockRequest) {
        self.lock_table.add(req);
        self.dep_graph.remove_transaction(req.transaction_id);
    }

    fn update_dependencies(&mut self, req: &LockRequest) {
        let Some(locks) = self.lock_table.page_locks_for(req.page_id) else {
            return;
        };
        let deps: Vec<TransactionId> = locks
            .iter()
            .filter(|l| l.transaction_id != req.transaction_id)
            .filter(|l| req.is_exclusive() || l.kind.is_exclusive())
            .map(|l| l.transaction_id)
            .collect();

        for holder in deps {
            self.dep_graph.add_transaction(req.transaction_id, holder);
        }
    }

    fn release_lock(&mut self, transaction_id: TransactionId, page_id: PageId) {
        self.lock_table.release(transaction_id, page_id);
    }

    fn release_all_locks(&mut self, transaction_id: TransactionId) -> Vec<PageId> {
        self.lock_table.release_all(transaction_id)
    }

    fn process_wait_queue(&mut self, page_id: PageId) {
        let pending = self.wait_queue.pending_for(page_id);
        let mut granted = vec![];

        for req in pending {
            if self.lock_table.can_grant_immediately(&req) {
                self.grant_lock(&req);
                granted.push(req);
            }
        }

        self.wait_queue.remove_granted(page_id, &granted);
    }
}

pub(super) struct LockManager {
    state: Mutex<LockManagerState>,
}

#[allow(dead_code)] // page lock queries / single-page unlock for upper layers
impl LockManager {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(LockManagerState::new()),
        }
    }

    pub(super) fn is_page_locked(&self, page_id: PageId) -> bool {
        self.state.lock().lock_table.is_locked(page_id)
    }

    pub(super) fn lock_page(&self, req: LockRequest) -> Result<(), LockError> {
        let mut delay = std::time::Duration::from_millis(1);
        let max_delay = std::time::Duration::from_millis(50);

        for attempt in 0..MAX_LOCK_RETRIES {
            let result = self.state.lock().attempt_to_acquire_lock(&req);

            match result {
                Ok(()) => {
                    if attempt > 0 {
                        tracing::debug!(
                            txn_id = %req.transaction_id,
                            page_id = ?req.page_id,
                            exclusive = req.is_exclusive(),
                            attempts = attempt + 1,
                            "lock granted after wait",
                        );
                    }
                    return Ok(());
                }
                Err(LockError::Deadlock(tid)) => {
                    tracing::warn!(
                        victim = %tid,
                        page_id = ?req.page_id,
                        "deadlock detected, aborting victim",
                    );
                    return Err(LockError::Deadlock(tid));
                }
                Err(LockError::NotYetGranted) => {
                    if attempt == 0 {
                        tracing::debug!(
                            txn_id = %req.transaction_id,
                            page_id = ?req.page_id,
                            exclusive = req.is_exclusive(),
                            "lock contended, waiting",
                        );
                    }
                    let exp_factor = (attempt / 5).min(5);
                    delay = (delay * LOCK_PAGE_BACKOFF_MULTIPLIER[exp_factor]).min(max_delay);
                    std::thread::sleep(delay);
                }
                Err(e) => return Err(e),
            }
        }

        tracing::warn!(
            txn_id = %req.transaction_id,
            page_id = ?req.page_id,
            "lock timeout",
        );
        Err(LockError::Timeout {
            file_id: req.page_id.file_id.0,
            page_no: req.page_id.page_no.0,
        })
    }

    pub(super) fn unlock_page(&self, transaction_id: TransactionId, page_id: PageId) {
        let mut state = self.state.lock();
        state.lock_table.release(transaction_id, page_id);
        state.dep_graph.remove_transaction(transaction_id);
        state.process_wait_queue(page_id);
    }

    pub(super) fn unlock_all_pages(&self, transaction_id: TransactionId) {
        let mut state = self.state.lock();
        let affected_pages = state.lock_table.release_all(transaction_id);
        state.dep_graph.remove_transaction(transaction_id);
        state.wait_queue.remove_all_for(transaction_id);
        for page_id in affected_pages {
            state.process_wait_queue(page_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::{FileId, PageNumber};

    fn tid(id: u64) -> TransactionId {
        TransactionId(id)
    }

    fn page(file: u64, no: u32) -> PageId {
        PageId::new(FileId(file), PageNumber(no))
    }

    #[test]
    fn dep_graph_no_cycle_on_empty() {
        let mut g = DependencyGraph::default();
        assert!(!g.has_cycle());
    }

    #[test]
    fn dep_graph_no_cycle_on_chain() {
        let mut g = DependencyGraph::default();
        // T1 waits on T2, T2 waits on T3 — no cycle
        g.add_transaction(tid(1), tid(2));
        g.add_transaction(tid(2), tid(3));
        assert!(!g.has_cycle());
    }

    #[test]
    fn dep_graph_detects_direct_cycle() {
        let mut g = DependencyGraph::default();
        g.add_transaction(tid(1), tid(2));
        g.add_transaction(tid(2), tid(1));
        assert!(g.has_cycle());
    }

    #[test]
    fn dep_graph_detects_indirect_cycle() {
        let mut g = DependencyGraph::default();
        g.add_transaction(tid(1), tid(2));
        g.add_transaction(tid(2), tid(3));
        g.add_transaction(tid(3), tid(1));
        assert!(g.has_cycle());
    }

    #[test]
    fn dep_graph_cache_is_reused() {
        let mut g = DependencyGraph::default();
        g.add_transaction(tid(1), tid(2));
        assert!(!g.has_cycle());
        // Second call should hit cache without recomputing
        assert!(!g.has_cycle());
        assert!(g.cache_valid);
    }

    #[test]
    fn dep_graph_cache_invalidated_on_add() {
        let mut g = DependencyGraph::default();
        g.add_transaction(tid(1), tid(2));
        assert!(!g.has_cycle());
        assert!(g.cache_valid);
        g.add_transaction(tid(2), tid(1));
        assert!(!g.cache_valid);
        assert!(g.has_cycle());
    }

    #[test]
    fn dep_graph_remove_breaks_cycle() {
        let mut g = DependencyGraph::default();
        g.add_transaction(tid(1), tid(2));
        g.add_transaction(tid(2), tid(1));
        assert!(g.has_cycle());
        g.remove_transaction(tid(1));
        assert!(!g.has_cycle());
    }

    #[test]
    fn lock_request_shared_is_not_exclusive() {
        let req = LockRequest::shared(tid(1), page(1, 1));
        assert!(!req.is_exclusive());
    }

    #[test]
    fn lock_request_exclusive_is_exclusive() {
        let req = LockRequest::exclusive(tid(1), page(1, 1));
        assert!(req.is_exclusive());
    }

    #[test]
    fn lock_request_as_shared_downgrades() {
        let req = LockRequest::exclusive(tid(1), page(1, 1)).as_shared();
        assert!(!req.is_exclusive());
    }

    #[test]
    fn lock_request_as_exclusive_upgrades() {
        let req = LockRequest::shared(tid(1), page(1, 1)).as_exclusive();
        assert!(req.is_exclusive());
    }

    #[test]
    fn shared_lock_granted_on_free_page() {
        let lm = LockManager::new();
        let req = LockRequest::shared(tid(1), page(1, 1));
        assert!(lm.lock_page(req).is_ok());
        assert!(lm.is_page_locked(page(1, 1)));
    }

    #[test]
    fn exclusive_lock_granted_on_free_page() {
        let lm = LockManager::new();
        let req = LockRequest::exclusive(tid(1), page(1, 1));
        assert!(lm.lock_page(req).is_ok());
        assert!(lm.is_page_locked(page(1, 1)));
    }

    #[test]
    fn multiple_shared_locks_on_same_page() {
        let lm = LockManager::new();
        assert!(
            lm.lock_page(LockRequest::shared(tid(1), page(1, 1)))
                .is_ok()
        );
        assert!(
            lm.lock_page(LockRequest::shared(tid(2), page(1, 1)))
                .is_ok()
        );
    }

    #[test]
    fn same_transaction_shared_lock_is_idempotent() {
        let lm = LockManager::new();
        assert!(
            lm.lock_page(LockRequest::shared(tid(1), page(1, 1)))
                .is_ok()
        );
        assert!(
            lm.lock_page(LockRequest::shared(tid(1), page(1, 1)))
                .is_ok()
        );
    }

    #[test]
    fn exclusive_lock_blocked_by_other_shared_lock() {
        let lm = LockManager::new();
        lm.lock_page(LockRequest::shared(tid(1), page(1, 1)))
            .unwrap();
        let result = lm.lock_page(LockRequest::exclusive(tid(2), page(1, 1)));
        assert!(matches!(result, Err(LockError::Timeout { .. })));
    }

    #[test]
    fn shared_lock_blocked_by_other_exclusive_lock() {
        let lm = LockManager::new();
        lm.lock_page(LockRequest::exclusive(tid(1), page(1, 1)))
            .unwrap();
        let result = lm.lock_page(LockRequest::shared(tid(2), page(1, 1)));
        assert!(matches!(result, Err(LockError::Timeout { .. })));
    }

    #[test]
    fn exclusive_holder_can_request_shared_without_blocking() {
        let lm = LockManager::new();
        lm.lock_page(LockRequest::exclusive(tid(1), page(1, 1)))
            .unwrap();
        // T1 already holds exclusive — shared request should be immediately satisfied
        assert!(
            lm.lock_page(LockRequest::shared(tid(1), page(1, 1)))
                .is_ok()
        );
    }

    #[test]
    fn shared_lock_upgrades_to_exclusive_when_sole_holder() {
        let lm = LockManager::new();
        lm.lock_page(LockRequest::shared(tid(1), page(1, 1)))
            .unwrap();
        assert!(
            lm.lock_page(LockRequest::exclusive(tid(1), page(1, 1)))
                .is_ok()
        );
    }

    #[test]
    fn shared_lock_cannot_upgrade_when_other_transaction_holds_shared() {
        let lm = LockManager::new();
        lm.lock_page(LockRequest::shared(tid(1), page(1, 1)))
            .unwrap();
        lm.lock_page(LockRequest::shared(tid(2), page(1, 1)))
            .unwrap();
        // T1 cannot upgrade because T2 also holds a shared lock
        let result = lm.lock_page(LockRequest::exclusive(tid(1), page(1, 1)));
        assert!(matches!(result, Err(LockError::Timeout { .. })));
    }

    #[test]
    fn unlock_page_releases_lock() {
        let lm = LockManager::new();
        lm.lock_page(LockRequest::shared(tid(1), page(1, 1)))
            .unwrap();
        lm.unlock_page(tid(1), page(1, 1));
        assert!(!lm.is_page_locked(page(1, 1)));
    }

    #[test]
    fn unlock_all_pages_releases_all_locks_for_transaction() {
        let lm = LockManager::new();
        lm.lock_page(LockRequest::shared(tid(1), page(1, 1)))
            .unwrap();
        lm.lock_page(LockRequest::shared(tid(1), page(1, 2)))
            .unwrap();
        lm.unlock_all_pages(tid(1));
        assert!(!lm.is_page_locked(page(1, 1)));
        assert!(!lm.is_page_locked(page(1, 2)));
    }

    #[test]
    fn unlock_page_does_not_affect_other_transactions_locks() {
        let lm = LockManager::new();
        lm.lock_page(LockRequest::shared(tid(1), page(1, 1)))
            .unwrap();
        lm.lock_page(LockRequest::shared(tid(2), page(1, 1)))
            .unwrap();
        lm.unlock_page(tid(1), page(1, 1));
        assert!(lm.is_page_locked(page(1, 1)));
    }

    #[test]
    fn waiting_shared_lock_granted_after_exclusive_released() {
        use std::{sync::Arc, thread};

        let lm = Arc::new(LockManager::new());
        lm.lock_page(LockRequest::exclusive(tid(1), page(1, 1)))
            .unwrap();

        let lm2 = Arc::clone(&lm);
        let handle = thread::spawn(move || lm2.lock_page(LockRequest::shared(tid(2), page(1, 1))));

        // Give T2 time to enter the wait loop
        std::thread::sleep(std::time::Duration::from_millis(20));
        lm.unlock_page(tid(1), page(1, 1));

        assert!(handle.join().unwrap().is_ok());
    }

    #[test]
    fn deadlock_detected_between_two_transactions() {
        use std::{sync::Arc, thread};

        let lm = Arc::new(LockManager::new());

        // T1 holds page 1, T2 holds page 2
        lm.lock_page(LockRequest::exclusive(tid(1), page(1, 1)))
            .unwrap();
        lm.lock_page(LockRequest::exclusive(tid(2), page(1, 2)))
            .unwrap();

        let lm1 = Arc::clone(&lm);
        let lm2 = Arc::clone(&lm);

        // T1 waits for page 2, T2 waits for page 1 → deadlock
        let h1 = thread::spawn(move || lm1.lock_page(LockRequest::exclusive(tid(1), page(1, 2))));
        let h2 = thread::spawn(move || lm2.lock_page(LockRequest::exclusive(tid(2), page(1, 1))));

        let r1 = h1.join().unwrap();
        let r2 = h2.join().unwrap();

        // At least one transaction must be aborted with a deadlock error
        assert!(
            matches!(r1, Err(LockError::Deadlock(_))) || matches!(r2, Err(LockError::Deadlock(_))),
            "expected a deadlock error, got {r1:?} and {r2:?}"
        );
    }
}
