// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{
    AsyncGroup, DataConn, DataConnContainer, DataConnManager, ErrEntry, SendSyncNonNull,
    TxnFailureCause, TxnFailureReport, TxnFailureRollback,
};

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, mem};

/// An enum type representing the reasons for errors that can occur within `DataConn` operations.
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum DataConnError {
    /// Indicates a failure during the pre-commit process of one or more [`DataConn`] instances
    /// involved in a transaction.
    /// Contains a vector of data connection names and their corresponding errors.
    FailToPreCommitDataConn {
        /// The vector contains errors that occurred in each [`DataConn`] object.
        errors: Vec<ErrEntry>,
    },

    /// Indicates a failure during the commit process of one or more [`DataConn`] instances
    /// involved in a transaction.
    /// Contains a vector of data connection names and their corresponding errors.
    FailToCommitDataConn {
        /// The vector contains errors that occurred in each [`DataConn`] object.
        errors: Vec<ErrEntry>,
    },

    /// Indicates a failure during the post-commit process of one or more [`DataConn`] instances
    /// involved in a transaction.
    /// Contains a vector of data connection names and their corresponding errors.
    FailToPostCommitDataConn {
        /// The vector contains errors that occurred in each [`DataConn`] object.
        errors: Vec<ErrEntry>,
    },

    /// Indicates a failure to cast a retrieved [`DataConn`] to the expected type.
    FailToCastDataConn {
        /// The name of the data connection that failed to cast.
        name: Arc<str>,

        /// The type name to which the [`DataConn`] attempted to cast.
        target_type: &'static str,
    },
}

impl<C> DataConnContainer<C>
where
    C: DataConn + 'static,
{
    pub(crate) fn new(name: impl Into<Arc<str>>, data_conn: Box<C>) -> Self {
        Self {
            drop_fn: drop_data_conn::<C>,
            is_fn: is_data_conn::<C>,
            type_fn: type_of_data_conn::<C>,
            commit_fn: commit_data_conn::<C>,
            pre_commit_fn: pre_commit_data_conn::<C>,
            post_commit_fn: post_commit_data_conn::<C>,
            is_committed_fn: is_committed_data_conn::<C>,
            rollback_fn: rollback_data_conn::<C>,
            on_txn_failure_fn: on_txn_failure_data_conn::<C>,
            close_fn: close_data_conn::<C>,

            name: name.into(),
            data_conn,
        }
    }
}

fn drop_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        drop(Box::from_raw(typed_ptr));
    }
}

fn is_data_conn<C>(type_id: any::TypeId) -> bool
where
    C: DataConn + 'static,
{
    any::TypeId::of::<C>() == type_id
}

fn type_of_data_conn<C>() -> &'static str
where
    C: DataConn + 'static,
{
    any::type_name::<C>()
}

fn commit_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup) -> errs::Result<()>
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.commit(ag) }
}

fn pre_commit_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup) -> errs::Result<()>
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.pre_commit(ag) }
}

fn post_commit_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup) -> errs::Result<()>
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.post_commit(ag) }
}

fn is_committed_data_conn<C>(ptr: *const DataConnContainer) -> bool
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.is_committed() }
}

fn rollback_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup) -> errs::Result<()>
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.rollback(ag) }
}

fn on_txn_failure_data_conn<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
    reports: &[TxnFailureReport],
) where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        (*typed_ptr).data_conn.on_txn_failure(ag, reports);
    }
}

fn close_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        (*typed_ptr).data_conn.close();
    }
}

impl DataConnManager {
    pub(crate) fn new() -> Self {
        Self {
            vec: Vec::new(),
            index_map: HashMap::new(),
            committed: false,
        }
    }

    pub(crate) fn with_commit_order(names: &[&str]) -> Self {
        let mut index_map = HashMap::with_capacity(names.len());
        // Using rev because eariler ones take precedence when names overlap
        for (i, nm) in names.iter().rev().enumerate() {
            index_map.insert((*nm).into(), names.len() - 1 - i);
        }

        Self {
            vec: vec![None; names.len()],
            index_map,
            committed: false,
        }
    }

    pub(crate) fn add(&mut self, ssnnptr: SendSyncNonNull<DataConnContainer>) {
        let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
        if let Some(index) = self.index_map.get(&name) {
            // Because eariler ones take precedence when names overlap
            if self.vec[*index].is_none() {
                self.vec[*index] = Some(ssnnptr);
            }
        } else {
            let index = self.vec.len();
            self.vec.push(Some(ssnnptr));
            self.index_map.insert(name.clone(), index);
        }
    }

    pub(crate) fn find_by_name(
        &self,
        name: impl AsRef<str>,
    ) -> Option<SendSyncNonNull<DataConnContainer>> {
        if let Some(index) = self.index_map.get(name.as_ref()) {
            if *index < self.vec.len() {
                if let Some(ssnnptr) = &self.vec[*index] {
                    let ptr = ssnnptr.non_null_ptr.as_ptr();
                    let cont_name = unsafe { &(*ptr).name };
                    if cont_name.as_ref() == name.as_ref() {
                        return Some(*ssnnptr);
                    }
                }
            }
        }

        None
    }

    pub(crate) fn to_typed_ptr<C>(
        ssnnptr: &SendSyncNonNull<DataConnContainer>,
    ) -> errs::Result<*mut DataConnContainer<C>>
    where
        C: DataConn + 'static,
    {
        let ptr = ssnnptr.non_null_ptr.as_ptr();
        let name = unsafe { &(*ptr).name };
        let type_id = any::TypeId::of::<C>();
        let is_fn = unsafe { (*ptr).is_fn };

        if !is_fn(type_id) {
            return Err(errs::Err::new(DataConnError::FailToCastDataConn {
                name: name.clone(),
                target_type: any::type_name::<C>(),
            }));
        }

        let typed_ptr = ptr as *mut DataConnContainer<C>;
        Ok(typed_ptr)
    }

    pub(crate) fn new_failure_reports(&self) -> Vec<TxnFailureReport> {
        let mut reports = Vec::new();
        for ssnnptr in self.vec.iter().flatten() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let name = unsafe { (*ptr).name.clone() };
            let type_fn = unsafe { (*ptr).type_fn };
            let report = TxnFailureReport::new(name, type_fn());
            reports.push(report);
        }
        reports
    }

    pub(crate) fn commit(&mut self, reports: &mut [TxnFailureReport]) -> errs::Result<()> {
        let mut errors = Vec::new();

        let mut ag = AsyncGroup::new();
        for (i, ssnnptr) in self.vec.iter().flatten().enumerate() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let pre_commit_fn = unsafe { (*ptr).pre_commit_fn };
            let name = unsafe { &(*ptr).name };
            ag._index = i;
            ag._name = name.clone();
            if let Err(err) = pre_commit_fn(ptr, &mut ag) {
                errors.push(ErrEntry {
                    index: i,
                    name: name.clone(),
                    err,
                });
                break;
            }
        }
        ag.join_and_collect_errors(&mut errors);

        if !errors.is_empty() {
            for ee in errors.iter() {
                let report = &mut reports[ee.index];
                report.cause = TxnFailureCause::LogicFailure(ee.err.clone());
            }
            return Err(errs::Err::new(DataConnError::FailToPreCommitDataConn {
                errors,
            }));
        }

        let mut ag = AsyncGroup::new();
        for (i, ssnnptr) in self.vec.iter().flatten().enumerate() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let is_committed_fn = unsafe { (*ptr).is_committed_fn };
            if is_committed_fn(ptr) {
                // Set this alongside the committed state by PreCommit, during the rollback process
                //report.cause = TxnFailureCause::NoneByCommitted;
                continue;
            }
            let commit_fn = unsafe { (*ptr).commit_fn };
            let name = unsafe { &(*ptr).name };
            ag._index = i;
            ag._name = name.clone();
            if let Err(err) = commit_fn(ptr, &mut ag) {
                errors.push(ErrEntry {
                    index: i,
                    name: name.clone(),
                    err,
                });
                break;
            }
        }
        ag.join_and_collect_errors(&mut errors);

        if !errors.is_empty() {
            for ee in errors.iter() {
                let report = &mut reports[ee.index];
                report.cause = TxnFailureCause::CommitFailure(ee.err.clone());
            }
            return Err(errs::Err::new(DataConnError::FailToCommitDataConn {
                errors,
            }));
        }

        self.committed = true;

        let mut ag = AsyncGroup::new();
        for (i, ssnnptr) in self.vec.iter().flatten().enumerate() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let post_commit_fn = unsafe { (*ptr).post_commit_fn };
            let name = unsafe { &(*ptr).name };
            ag._index = i;
            ag._name = name.clone();
            if let Err(err) = post_commit_fn(ptr, &mut ag) {
                errors.push(ErrEntry {
                    index: i,
                    name: name.clone(),
                    err,
                });
                // don't break;
            }
        }
        ag.join_and_collect_errors(&mut errors);

        if !errors.is_empty() {
            for ee in errors.iter() {
                let report = &mut reports[ee.index];
                report.cause = TxnFailureCause::PostCommitFailure(ee.err.clone());
            }
            return Err(errs::Err::new(DataConnError::FailToPostCommitDataConn {
                errors,
            }));
        }

        Ok(())
    }

    pub(crate) fn rollback(&mut self, mut reports: Vec<TxnFailureReport>) {
        let mut errors = Vec::new();

        let mut ag = AsyncGroup::new();
        for (i, ssnnptr) in self.vec.iter().flatten().enumerate() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let report = &mut reports[i];
            let is_committed_fn = unsafe { (*ptr).is_committed_fn };
            if is_committed_fn(ptr) {
                if let TxnFailureCause::NoneByUncommitted = report.cause {
                    report.cause = TxnFailureCause::NoneByCommitted;
                }
                continue;
            }
            if self.committed {
                continue;
            }
            let rollback_fn = unsafe { (*ptr).rollback_fn };
            let name = unsafe { &(*ptr).name };
            ag._index = i;
            ag._name = name.clone();
            if let Err(err) = rollback_fn(ptr, &mut ag) {
                errors.push(ErrEntry {
                    index: i,
                    name: name.clone(),
                    err,
                });
            } else {
                report.rollback = TxnFailureRollback::NoneByRolledBack;
            }
        }
        ag.join_and_collect_errors(&mut errors);

        if !errors.is_empty() {
            for ee in errors.into_iter() {
                let report = &mut reports[ee.index];
                report.rollback = TxnFailureRollback::RollbackFailure(ee.err);
            }
        }

        let mut ag = AsyncGroup::new();
        for ssnnptr in self.vec.iter().flatten() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let on_txn_failure_fn = unsafe { (*ptr).on_txn_failure_fn };
            on_txn_failure_fn(ptr, &mut ag, &reports);
        }
        ag.join_and_ignore_errors();
    }

    pub(crate) fn close(&mut self) {
        self.index_map.clear();

        let vec: Vec<Option<SendSyncNonNull<DataConnContainer>>> = mem::take(&mut self.vec);
        for ssnnptr in vec.iter().flatten().rev() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let close_fn = unsafe { (*ptr).close_fn };
            let drop_fn = unsafe { (*ptr).drop_fn };
            close_fn(ptr);
            drop_fn(ptr);
        }
    }
}

impl Drop for DataConnManager {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests_of_data_conn {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };
    use std::{ptr, thread, time};

    const BASE_LINE: u32 = line!();

    #[derive(PartialEq, Copy, Clone)]
    enum Fail {
        Not,
        Commit,
        PreCommit,
        PostCommit,
        Rollback,
        PreCommitBecomeCommitted,
    }

    struct SyncDataConn {
        id: i8,
        committed: bool,
        fail: Fail,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl SyncDataConn {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::new {}", id));
            Self {
                id,
                committed: false,
                fail,
                logger,
            }
        }
    }
    impl Drop for SyncDataConn {
        fn drop(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::drop {}", self.id));
        }
    }
    impl DataConn for SyncDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail == Fail::Commit {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("SyncDataConn::commit {} failed", self.id));
                return Err(errs::Err::new("ZZZ".to_string()));
            }
            self.committed = true;
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::commit {}", self.id));
            Ok(())
        }
        fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail == Fail::PreCommit {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("SyncDataConn::pre_commit {} failed", self.id));
                return Err(errs::Err::new("zzz".to_string()));
            }
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::pre_commit {}", self.id));
            if self.fail == Fail::PreCommitBecomeCommitted {
                self.committed = true;
            }
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail == Fail::PostCommit {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("SyncDataConn::post_commit {} failed", self.id));
                return Err(errs::Err::new("!!!".to_string()));
            }
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::post_commit {}", self.id));
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail == Fail::Rollback {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("SyncDataConn::rollback {} failed", self.id));
                return Err(errs::Err::new("???".to_string()));
            }
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::rollback {}", self.id));
            Ok(())
        }
        fn on_txn_failure(&mut self, _ag: &mut AsyncGroup, reports: &[TxnFailureReport]) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn::on_txn_failure {}", self.id));
            logger.push(format!("TxnFailureReports={:?}", reports));
        }
        fn close(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::close {}", self.id));
        }
    }

    struct AsyncDataConn {
        id: i8,
        committed: Arc<AtomicBool>,
        fail: Fail,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl AsyncDataConn {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
            logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataConn::new {}", id));
            Self {
                id,
                committed: Arc::new(AtomicBool::new(false)),
                fail,
                logger,
            }
        }
    }
    impl Drop for AsyncDataConn {
        fn drop(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataConn::drop {}", self.id));
        }
    }
    impl DataConn for AsyncDataConn {
        fn commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
            let fail = self.fail;
            let logger = self.logger.clone();
            let id = self.id;
            let committed = self.committed.clone();
            ag.add(move || {
                thread::sleep(time::Duration::from_millis(100));
                if fail == Fail::Commit {
                    logger
                        .lock()
                        .unwrap()
                        .push(format!("AsyncDataConn::commit {} failed", id));
                    return Err(errs::Err::new("YYY".to_string()));
                }
                committed.store(true, Ordering::Release);
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::commit {}", id));
                Ok(())
            });
            Ok(())
        }
        fn pre_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
            let fail = self.fail;
            let logger = self.logger.clone();
            let id = self.id;
            let committed = self.committed.clone();
            ag.add(move || {
                thread::sleep(time::Duration::from_millis(100));
                if fail == Fail::PreCommit {
                    logger
                        .lock()
                        .unwrap()
                        .push(format!("AsyncDataConn::pre_commit {} failed", id));
                    return Err(errs::Err::new("yyy".to_string()));
                }
                if fail == Fail::PreCommitBecomeCommitted {
                    committed.store(true, Ordering::Release);
                }
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::pre_commit {}", id));
                Ok(())
            });
            Ok(())
        }
        fn post_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
            let logger = self.logger.clone();
            let id = self.id;
            let fail = self.fail;
            ag.add(move || {
                thread::sleep(time::Duration::from_millis(100));
                if fail == Fail::PostCommit {
                    logger
                        .lock()
                        .unwrap()
                        .push(format!("AsyncDataConn::post_commit {} failed", id));
                    return Err(errs::Err::new("!!!".to_string()));
                }
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::post_commit {}", id));
                Ok(())
            });
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed.load(Ordering::Acquire)
        }
        fn rollback(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
            let logger = self.logger.clone();
            let fail = self.fail;
            let id = self.id;
            ag.add(move || {
                thread::sleep(time::Duration::from_millis(100));
                if fail == Fail::Rollback {
                    logger
                        .lock()
                        .unwrap()
                        .push(format!("AsyncDataConn::rollback {} failed", id));
                    return Err(errs::Err::new("???".to_string()));
                }
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::rollback {}", id));
                Ok(())
            });
            Ok(())
        }
        fn on_txn_failure(&mut self, ag: &mut AsyncGroup, reports: &[TxnFailureReport]) {
            let reports_log = format!("TxnFailureReports={:?}", reports);
            let logger = self.logger.clone();
            let id = self.id;
            ag.add(move || {
                thread::sleep(time::Duration::from_millis(100));
                let mut logger = logger.lock().unwrap();
                logger.push(format!("AsyncDataConn::on_txn_failure {}", id));
                logger.push(reports_log);
                Ok(())
            });
        }
        fn close(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataConn::close {}", self.id));
        }
    }

    mod tests_of_data_conn_manager {
        use super::*;

        #[test]
        fn test_new() {
            let manager = DataConnManager::new();
            assert!(manager.vec.is_empty());
            assert!(manager.index_map.is_empty());
        }

        #[test]
        fn test_with_commit_order() {
            let manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);
            assert_eq!(manager.vec.len(), 3);
            assert!(manager.vec[0].is_none());
            assert!(manager.vec[1].is_none());
            assert!(manager.vec[2].is_none());
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
            assert_eq!(*manager.index_map.get("baz").unwrap(), 1);
        }

        #[test]
        fn test_new_and_add() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();
            assert!(manager.vec.is_empty());
            assert!(manager.index_map.is_empty());

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 1);
            assert_eq!(manager.index_map.len(), 1);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 0);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 2);
            assert_eq!(manager.index_map.len(), 2);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 0);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 1);
        }

        #[test]
        fn test_new_and_add_when_overlapping_name() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();
            assert!(manager.vec.is_empty());
            assert!(manager.index_map.is_empty());

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr0 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr0);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 1);
            assert_eq!(manager.index_map.len(), 1);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 0);
            assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr0);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
            let nnptr1 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr1);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 1);
            assert_eq!(manager.index_map.len(), 1);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 0);
            assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr0);
        }

        #[test]
        fn test_with_commit_order_and_add() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);
            assert_eq!(manager.vec.len(), 3);
            assert!(manager.vec[0].is_none());
            assert!(manager.vec[1].is_none());
            assert!(manager.vec[2].is_none());
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
            assert_eq!(*manager.index_map.get("baz").unwrap(), 1);

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 3);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 3);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 0);

            let conn = SyncDataConn::new(3, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("qux", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 4);
            assert_eq!(manager.index_map.len(), 4);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
            assert_eq!(*manager.index_map.get("qux").unwrap(), 3);
        }

        #[test]
        fn test_with_order_and_add_when_overlapping_name() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);
            assert_eq!(manager.vec.len(), 3);
            assert!(manager.vec[0].is_none());
            assert!(manager.vec[1].is_none());
            assert!(manager.vec[2].is_none());
            assert_eq!(manager.index_map.len(), 3);

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr0 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr0);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 3);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr0);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
            let nnptr1 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr1);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 3);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr0);
        }

        #[test]
        fn test_find_by_name_but_none() {
            let manager = DataConnManager::new();
            assert!(manager.find_by_name("foo").is_none());
            assert!(manager.find_by_name("bar").is_none());
        }

        #[test]
        fn test_find_by_name_and_found() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            if let Some(ssnnptr) = manager.find_by_name("foo") {
                let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
                assert_eq!(name.as_ref(), "foo");
            } else {
                panic!();
            }

            if let Some(ssnnptr) = manager.find_by_name("bar") {
                let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
                assert_eq!(name.as_ref(), "bar");
            } else {
                panic!();
            }
        }

        #[test]
        fn test_find_by_name_of_ordered_dataconn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::with_commit_order(&["baz", "qux", "foo"]);

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = SyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = SyncDataConn::new(3, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("baz", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            if let Some(ssnnptr) = manager.find_by_name("foo") {
                let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
                assert_eq!(name.as_ref(), "foo");
            } else {
                panic!();
            }

            if let Some(ssnnptr) = manager.find_by_name("bar") {
                let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
                assert_eq!(name.as_ref(), "bar");
            } else {
                panic!();
            }

            if let Some(ssnnptr) = manager.find_by_name("baz") {
                let name = unsafe { (*ssnnptr.non_null_ptr.as_ptr()).name.clone() };
                assert_eq!(name.as_ref(), "baz");
            } else {
                panic!();
            }

            assert!(manager.find_by_name("qux").is_none());
        }

        #[test]
        fn test_to_typed_ptr() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let ssnnptr = manager.find_by_name("foo").unwrap();
            if let Ok(typed_ssnnptr) = DataConnManager::to_typed_ptr::<SyncDataConn>(&ssnnptr) {
                assert_eq!(any::type_name_of_val(&typed_ssnnptr), "*mut sabi::DataConnContainer<sabi::data_conn::tests_of_data_conn::SyncDataConn>");
                assert_eq!(unsafe { (*typed_ssnnptr).name.clone() }, "foo".into());
            } else {
                panic!();
            }

            let ssnnptr = manager.find_by_name("bar").unwrap();
            if let Ok(typed_ssnnptr) = DataConnManager::to_typed_ptr::<AsyncDataConn>(&ssnnptr) {
                assert_eq!(any::type_name_of_val(&typed_ssnnptr), "*mut sabi::DataConnContainer<sabi::data_conn::tests_of_data_conn::AsyncDataConn>");
                assert_eq!(unsafe { (*typed_ssnnptr).name.clone() }, "bar".into());
            } else {
                panic!();
            }
        }

        #[test]
        fn test_to_typed_ptr_but_fail() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let ssnnptr = manager.find_by_name("foo").unwrap();
            if let Err(err) = DataConnManager::to_typed_ptr::<AsyncDataConn>(&ssnnptr) {
                match err.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(
                            *target_type,
                            "sabi::data_conn::tests_of_data_conn::AsyncDataConn"
                        );
                    }
                    _ => panic!(),
                }
            } else {
                panic!();
            }

            let ssnnptr = manager.find_by_name("bar").unwrap();
            if let Err(err) = DataConnManager::to_typed_ptr::<SyncDataConn>(&ssnnptr) {
                match err.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(name.as_ref(), "bar");
                        assert_eq!(
                            *target_type,
                            "sabi::data_conn::tests_of_data_conn::SyncDataConn"
                        );
                    }
                    _ => panic!(),
                }
            } else {
                panic!();
            }
        }

        #[test]
        fn test_new_failure_reports() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();

            let vec = manager.new_failure_reports();
            assert_eq!(vec.len(), 0);

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let vec = manager.new_failure_reports();
            assert_eq!(vec.len(), 2);

            let report = &vec[0];
            assert_eq!(report.data_conn_name, "foo".into());
            assert_eq!(
                report.data_conn_type,
                "sabi::data_conn::tests_of_data_conn::SyncDataConn"
            );

            let report = &vec[1];
            assert_eq!(report.data_conn_name, "bar".into());
            assert_eq!(
                report.data_conn_type,
                "sabi::data_conn::tests_of_data_conn::AsyncDataConn"
            );
        }

        #[test]
        fn test_commit_and_rollback_ok() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                assert!(manager.commit(&mut reports).is_ok());
                manager.rollback(reports);
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "AsyncDataConn::post_commit 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                    "AsyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[test]
        fn test_commit_with_order_and_rollback_ok() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(3, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("qux", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                assert!(manager.commit(&mut reports).is_ok());
                manager.rollback(reports);
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::new 3",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 3",
                    "AsyncDataConn::pre_commit 2", // because of async
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 3",
                    "AsyncDataConn::commit 2", // because of async
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::post_commit 3",
                    "AsyncDataConn::post_commit 2", // because of async
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"qux\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                    "SyncDataConn::on_txn_failure 3",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"qux\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                    "AsyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"qux\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                    "SyncDataConn::close 3",
                    "SyncDataConn::drop 3",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[test]
        fn test_commit_and_rollback_but_fail_first_sync_pre_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "foo".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "zzz");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }

                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 62),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 62),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 62),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 62),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[test]
        fn test_commit_and_rollback_but_fail_first_async_pre_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 2);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "foo".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "zzz");
                            assert_eq!(errors[1].index, 0);
                            assert_eq!(errors[1].name, "bar".into());
                            assert_eq!(errors[1].err.reason::<String>().unwrap(), "yyy");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }

                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 2",
                    "SyncDataConn::new 1",
                    "SyncDataConn::pre_commit 1 failed",
                    "AsyncDataConn::pre_commit 2 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"yyy\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 181, BASE_LINE + 62),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"yyy\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 181, BASE_LINE + 62),
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 2",
                    "SyncDataConn::new 1",
                    "SyncDataConn::pre_commit 1 failed",
                    "AsyncDataConn::pre_commit 2 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"yyy\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 181, BASE_LINE + 62),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"yyy\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 181, BASE_LINE + 62),
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[test]
        fn test_commit_and_rollback_but_fail_second_pre_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "yyy");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }

                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"yyy\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 181),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"yyy\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 181),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"yyy\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 181),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"yyy\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 181),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[test]
        fn test_commit_and_rollback_but_fail_first_sync_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "foo".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "ZZZ");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }

                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 47),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 47),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 47),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 47),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[test]
        fn test_commit_and_rollback_but_fail_first_async_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "foo".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "YYY");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }

                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 1",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 2",
                    "AsyncDataConn::pre_commit 1",
                    "SyncDataConn::commit 2",
                    "AsyncDataConn::commit 1 failed",
                    "AsyncDataConn::rollback 1",
                    "SyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]", BASE_LINE + 158),
                    "AsyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]", BASE_LINE + 158),
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "AsyncDataConn::close 1",
                    "AsyncDataConn::drop 1",
                ]
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 1",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 2",
                    "AsyncDataConn::pre_commit 1",
                    "SyncDataConn::commit 2",
                    "AsyncDataConn::commit 1 failed",
                    "AsyncDataConn::rollback 1",
                    "SyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]", BASE_LINE + 158),
                    "AsyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]", BASE_LINE + 158),
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "AsyncDataConn::close 1",
                    "AsyncDataConn::drop 1",
                ]
            );
        }

        #[test]
        fn test_commit_and_rollback_but_fail_second_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "YYY");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }

                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2 failed",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2 failed",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[test]
        fn test_commit_and_rollback_but_fail_first_sync_post_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 2);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "foo".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                            assert_eq!(errors[1].index, 1);
                            assert_eq!(errors[1].name, "bar".into());
                            assert_eq!(errors[1].err.reason::<String>().unwrap(), "!!!");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }

                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1 failed",
                    "AsyncDataConn::post_commit 2 failed",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 79, BASE_LINE + 205),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 79, BASE_LINE + 205),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1 failed",
                    "AsyncDataConn::post_commit 2 failed",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 79, BASE_LINE + 205),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 79, BASE_LINE + 205),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[test]
        fn test_commit_and_rollback_but_fail_first_async_post_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 2);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "foo".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                            assert_eq!(errors[1].index, 0);
                            assert_eq!(errors[1].name, "bar".into());
                            assert_eq!(errors[1].err.reason::<String>().unwrap(), "!!!");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }

                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 2",
                    "SyncDataConn::new 1",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1 failed",
                    "AsyncDataConn::post_commit 2 failed",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 205, BASE_LINE + 79),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 205, BASE_LINE + 79),
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 2",
                    "SyncDataConn::new 1",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1 failed",
                    "AsyncDataConn::post_commit 2 failed",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 205, BASE_LINE + 79),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 205, BASE_LINE + 79),
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[test]
        fn test_commit_and_rollback_but_fail_second_post_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }

                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "AsyncDataConn::post_commit 2 failed",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 205),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 205),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "AsyncDataConn::post_commit 2 failed",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 205),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", BASE_LINE + 205),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[test]
        fn test_only_rollback_and_first_is_sync() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let reports = manager.new_failure_reports();
                manager.rollback(reports);
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]"),
                    "AsyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]"),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[test]
        fn test_only_rollback_and_first_is_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let reports = manager.new_failure_reports();
                manager.rollback(reports);
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 1",
                    "SyncDataConn::new 2",
                    "SyncDataConn::rollback 2",
                    "AsyncDataConn::rollback 1",
                    "SyncDataConn::on_txn_failure 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]"),
                    "AsyncDataConn::on_txn_failure 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]"),
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "AsyncDataConn::close 1",
                    "AsyncDataConn::drop 1",
                ]
            );
        }

        #[test]
        fn test_only_rollback_and_second_rollback_failed() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Rollback);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let reports = manager.new_failure_reports();
                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::rollback 1",
                "AsyncDataConn::rollback 2 failed",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src/data_conn.rs, line = {} }}) }}]", BASE_LINE + 229),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src/data_conn.rs, line = {} }}) }}]", BASE_LINE + 229),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::rollback 1",
                "AsyncDataConn::rollback 2 failed",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src\\data_conn.rs, line = {} }}) }}]", BASE_LINE + 229),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src\\data_conn.rs, line = {} }}) }}]", BASE_LINE + 229),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }

        #[test]
        fn test_only_rollback_and_first_rollback_failed() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Rollback);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let reports = manager.new_failure_reports();
                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::rollback 1 failed",
                "AsyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src/data_conn.rs, line = {} }}) }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 96),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src/data_conn.rs, line = {} }}) }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 96),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::rollback 1 failed",
                "AsyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src\\data_conn.rs, line = {} }}) }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 96),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src\\data_conn.rs, line = {} }}) }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", BASE_LINE + 96),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }

        #[test]
        fn test_commit_and_rollback_and_first_commit_failed_and_second_rollback_failed() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Rollback);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "foo".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "ZZZ");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "AsyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1 failed",
                "SyncDataConn::rollback 1",
                "AsyncDataConn::rollback 2 failed",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src/data_conn.rs, line = {} }}) }}]", BASE_LINE + 47, BASE_LINE + 229),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src/data_conn.rs, line = {} }}) }}]", BASE_LINE + 47, BASE_LINE + 229),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "AsyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1 failed",
                "SyncDataConn::rollback 1",
                "AsyncDataConn::rollback 2 failed",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src\\data_conn.rs, line = {} }}) }}]", BASE_LINE + 47, BASE_LINE + 229),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src\\data_conn.rs, line = {} }}) }}]", BASE_LINE + 47, BASE_LINE + 229),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }

        #[test]
        fn test_commit_and_rollback_and_secod_commit_failed_and_first_rollback_failed() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Rollback);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "YYY");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "AsyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1",
                "AsyncDataConn::commit 2 failed",
                "AsyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "AsyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1",
                "AsyncDataConn::commit 2 failed",
                "AsyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }

        #[test]
        fn test_commit_and_rollback_and_pre_commit_become_committed_and_ok() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommitBecomeCommitted);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommitBecomeCommitted);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                assert!(manager.commit(&mut reports).is_ok());
                manager.rollback(reports);
            }

            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "AsyncDataConn::pre_commit 2",
                "SyncDataConn::post_commit 1",
                "AsyncDataConn::post_commit 2",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }

        #[test]
        fn test_commit_and_rollback_and_pre_commit_become_committed_but_failed() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommitBecomeCommitted);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                if let Err(e) = manager.commit(&mut reports) {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "YYY");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
                manager.rollback(reports);
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "AsyncDataConn::pre_commit 2",
                "AsyncDataConn::commit 2 failed",
                "AsyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit 1",
                "AsyncDataConn::pre_commit 2",
                "AsyncDataConn::commit 2 failed",
                "AsyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                "AsyncDataConn::on_txn_failure 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::data_conn::tests_of_data_conn::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\data_conn.rs, line = {} }}), rollback: NoneByRolledBack }}]", BASE_LINE + 158),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }
    }
}
