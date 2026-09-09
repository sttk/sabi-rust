// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::{AsyncGroup, DataConn, DataConnContainer, DataConnManager, ErrEntry, SendSyncNonNull};
use crate::{TxnFailureCause, TxnFailureReport, TxnFailureRollback};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{any, mem};

/// Represents errors that can occur during data connection operations.
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum DataConnError {
    /// An error indicating that one or more data connections failed during the pre-commit process.
    FailToPreCommitDataConn {
        /// A vector of errors, each containing the name of the data connection and the error itself.
        errors: Vec<ErrEntry>,
    },

    /// An error indicating that one or more data connections failed during the commit process.
    FailToCommitDataConn {
        /// A vector of errors, each containing the name of the data connection and the error itself.
        errors: Vec<ErrEntry>,
    },

    /// An error indicating that one or more data connections failed during the post-commit process.
    FailToPostCommitDataConn {
        /// A vector of errors, each containing the name of the data connection and the error itself.
        errors: Vec<ErrEntry>,
    },

    /// An error indicating that a data connection could not be cast to the target type.
    FailToCastDataConn {
        /// The name of the data connection that failed to cast.
        name: Arc<str>,

        /// The string representation of the target type to which the connection could not be cast.
        target_type: &'static str,
    },
}

// NOTE: Uses the thin-pointer approach (#[repr(C)] + function pointers).
//
// DataConn itself isn't generic, so rewriting this as Box<dyn DataConn> +
// downcasting via Any is technically possible and works correctly on its
// own.
//
// However, DataSrcContainer (thin-pointer approach) holds its raw pointer
// as SendSyncNonNull<DataSrcContainer>, i.e. with the default type
// arguments (S = NoopDataSrc, C = NoopDataConn). If DataConnContainer were
// dyn-DataConn-based, DataSrcContainer::create_data_conn_fn would have to
// return Box<C> to its caller, and that C would be determined by the
// static type of the function pointer read out through an unsafe pointer
// (i.e. the default type arguments) rather than by the actual C the
// caller (the user) requests via get_data_conn::<C>. That C ends up fixed
// at NoopDataConn regardless of what the caller actually asked for. This
// mismatch between the static C and the real runtime value, once passed
// into DataConnContainer::new, causes the value to be erased under the
// wrong type — a bug that was actually observed in practice (e.g. a
// MyDataConn ending up treated as a NoopDataConn).
//
// Since DataSrcContainer uses the thin-pointer approach, DataConnContainer
// keeps the same approach as well, in order to interface with it safely.

impl<C> DataConnContainer<C>
where
    C: DataConn + 'static,
{
    pub(crate) fn new(name: impl Into<Arc<str>>, data_conn: Box<C>) -> Self {
        Self {
            drop_fn: drop_data_conn::<C>,
            is_fn: is_data_conn::<C>,
            type_fn: type_of_data_conn::<C>,
            commit_fn: commit_data_conn_async::<C>,
            pre_commit_fn: pre_commit_data_conn_async::<C>,
            post_commit_fn: post_commit_data_conn_async::<C>,
            is_committed_fn: is_committed_data_conn::<C>,
            rollback_fn: rollback_data_conn_async::<C>,
            on_txn_failure_fn: on_txn_failure_data_conn_async::<C>,
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
    unsafe {
        drop(Box::from_raw(ptr as *mut DataConnContainer<C>));
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

fn commit_data_conn_async<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + '_>>
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    Box::pin(container.data_conn.commit_async(ag))
}

fn pre_commit_data_conn_async<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + '_>>
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    Box::pin(container.data_conn.pre_commit_async(ag))
}

fn post_commit_data_conn_async<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + '_>>
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    Box::pin(container.data_conn.post_commit_async(ag))
}

fn is_committed_data_conn<C>(ptr: *const DataConnContainer) -> bool
where
    C: DataConn + 'static,
{
    let container = unsafe { &*(ptr as *const DataConnContainer<C>) };
    container.data_conn.is_committed()
}

fn rollback_data_conn_async<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + '_>>
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    Box::pin(container.data_conn.rollback_async(ag))
}

fn on_txn_failure_data_conn_async<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
    reports: Arc<[TxnFailureReport]>,
) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    Box::pin(container.data_conn.on_txn_failure_async(ag, reports))
}

fn close_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    container.data_conn.close();
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
        // Using rev because earlier ones take precedence when names overlap
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
            // Because earlier ones take precedence when names overlap
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

    pub(crate) async fn commit_async(
        &mut self,
        reports: &mut [TxnFailureReport],
    ) -> errs::Result<()> {
        let mut errors = Vec::new();

        let mut ag = AsyncGroup::new();
        for (i, ssnnptr) in self.vec.iter().flatten().enumerate() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let pre_commit_fn = unsafe { (*ptr).pre_commit_fn };
            let name = unsafe { &(*ptr).name };
            ag._index = i;
            ag._name = name.clone();
            if let Err(err) = pre_commit_fn(ptr, &mut ag).await {
                errors.push(ErrEntry {
                    index: i,
                    name: name.clone(),
                    err,
                });
                break;
            }
        }
        ag.join_and_collect_errors_async(&mut errors).await;

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
            if let Err(err) = commit_fn(ptr, &mut ag).await {
                errors.push(ErrEntry {
                    index: i,
                    name: name.clone(),
                    err,
                });
                break;
            }
        }
        ag.join_and_collect_errors_async(&mut errors).await;

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
            if let Err(err) = post_commit_fn(ptr, &mut ag).await {
                errors.push(ErrEntry {
                    index: i,
                    name: name.clone(),
                    err,
                });
                // don't break;
            }
        }
        ag.join_and_collect_errors_async(&mut errors).await;

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

    pub(crate) async fn rollback_async(&mut self, mut reports: Vec<TxnFailureReport>) {
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
            if let Err(err) = rollback_fn(ptr, &mut ag).await {
                errors.push(ErrEntry {
                    index: i,
                    name: name.clone(),
                    err,
                });
            } else {
                report.rollback = TxnFailureRollback::NoneByRolledBack;
            }
        }
        ag.join_and_collect_errors_async(&mut errors).await;

        if !errors.is_empty() {
            for ee in errors.into_iter() {
                let report = &mut reports[ee.index];
                report.rollback = TxnFailureRollback::RollbackFailure(ee.err);
            }
        }

        let reports: Arc<[TxnFailureReport]> = Arc::from(reports);

        let mut ag = AsyncGroup::new();
        for ssnnptr in self.vec.iter().flatten() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let on_txn_failure_fn = unsafe { (*ptr).on_txn_failure_fn };
            on_txn_failure_fn(ptr, &mut ag, reports.clone()).await;
        }
        ag.join_and_ignore_errors_async().await;
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

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_data_conn {
    use super::*;
    use crate::tokio::_test_commons::*;
    use std::ptr;
    use std::sync::{Arc, Mutex};

    mod tests_of_data_conn_manager {
        use super::*;
        use std::panic::panic_any;

        #[tokio::test]
        async fn test_new() {
            let manager = DataConnManager::new();
            assert!(manager.vec.is_empty());
            assert!(manager.index_map.is_empty());
        }

        #[tokio::test]
        async fn test_with_commit_order() {
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

            let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 1);
            assert_eq!(manager.index_map.len(), 1);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 0);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
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

            let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr0 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr0);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 1);
            assert_eq!(manager.index_map.len(), 1);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 0);
            assert_eq!(manager.vec[0].clone().unwrap().non_null_ptr, nnptr0);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
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

            let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 3);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 3);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 0);

            let conn = SyncDataConn::new(3, logger.clone(), Fail::None);
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

            let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr0 = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr0);
            manager.add(ssnnptr);
            assert_eq!(manager.vec.len(), 3);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(manager.vec[2].clone().unwrap().non_null_ptr, nnptr0);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
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

            let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
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

            let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = SyncDataConn::new(2, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = SyncDataConn::new(3, logger.clone(), Fail::None);
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

            let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let nnptr = manager.find_by_name("foo").unwrap();
            if let Ok(typed_nnptr) = DataConnManager::to_typed_ptr::<SyncDataConn>(&nnptr) {
                assert_eq!(
                    any::type_name_of_val(&typed_nnptr),
                    "*mut sabi::tokio::DataConnContainer<sabi::tokio::_test_commons::SyncDataConn>"
                );
                assert_eq!(unsafe { (*typed_nnptr).name.clone() }, "foo".into());
            } else {
                panic!();
            }

            let nnptr = manager.find_by_name("bar").unwrap();
            if let Ok(typed_nnptr) = DataConnManager::to_typed_ptr::<AsyncDataConn>(&nnptr) {
                assert_eq!(any::type_name_of_val(&typed_nnptr), "*mut sabi::tokio::DataConnContainer<sabi::tokio::_test_commons::AsyncDataConn>");
                assert_eq!(unsafe { (*typed_nnptr).name.clone() }, "bar".into());
            } else {
                panic!();
            }
        }

        #[test]
        fn test_to_typed_ptr_but_fail() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();

            let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let ssnnptr = manager.find_by_name("foo").unwrap();
            if let Err(err) = DataConnManager::to_typed_ptr::<AsyncDataConn>(&ssnnptr) {
                match err.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(*target_type, "sabi::tokio::_test_commons::AsyncDataConn");
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
                        assert_eq!(*target_type, "sabi::tokio::_test_commons::SyncDataConn");
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

            let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            let ssnnptr = SendSyncNonNull::new(nnptr);
            manager.add(ssnnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
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
                "sabi::tokio::_test_commons::SyncDataConn"
            );

            let report = &vec[1];
            assert_eq!(report.data_conn_name, "bar".into());
            assert_eq!(
                report.data_conn_type,
                "sabi::tokio::_test_commons::AsyncDataConn"
            );
        }

        #[tokio::test]
        async fn test_commit_and_rollback_ok() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                assert!(manager.commit_async(&mut reports).await.is_ok());
                manager.rollback_async(reports).await;
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit_async 1",
                    "AsyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "AsyncDataConn::commit_async 2",
                    "SyncDataConn::post_commit_async 1",
                    "AsyncDataConn::post_commit_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_with_order_and_rollback_ok() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);

                let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(3, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("qux", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                assert!(manager.commit_async(&mut reports).await.is_ok());
                manager.rollback_async(reports).await;
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::new 3",
                    "SyncDataConn::pre_commit_async 1",
                    "SyncDataConn::pre_commit_async 3",
                    "AsyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "SyncDataConn::commit_async 3",
                    "AsyncDataConn::commit_async 2",
                    "SyncDataConn::post_commit_async 1",
                    "SyncDataConn::post_commit_async 3",
                    "AsyncDataConn::post_commit_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"qux\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
                    "SyncDataConn::on_txn_failure_async 3",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"qux\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"qux\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
                    "SyncDataConn::close 3",
                    "SyncDataConn::drop 3",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_and_rollback_but_fail_first_sync_pre_commit() {
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
                if let Err(e) = manager.commit_async(&mut reports).await {
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
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit_async 1 failed",
                    "SyncDataConn::rollback_async 1",
                    "AsyncDataConn::rollback_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 80),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 80),
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
                    "SyncDataConn::pre_commit_async 1 failed",
                    "SyncDataConn::rollback_async 1",
                    "AsyncDataConn::rollback_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 80),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 80),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_and_rollback_but_fail_first_async_pre_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommit);
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
                if let Err(e) = manager.commit_async(&mut reports).await {
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
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1 failed",
                "SyncDataConn::rollback_async 1",
                "AsyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 80),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 80),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1 failed",
                "SyncDataConn::rollback_async 1",
                "AsyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 80),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"zzz\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 80),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }

        #[tokio::test]
        async fn test_commit_and_rollback_but_fail_second_pre_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                if let Err(e) = manager.commit_async(&mut reports).await {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "foo".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "yyy");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
                manager.rollback_async(reports).await;
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 1",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit_async 2",
                    "AsyncDataConn::pre_commit_async 1 failed",
                    "SyncDataConn::rollback_async 2",
                    "AsyncDataConn::rollback_async 1",
                    "SyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"yyy\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 215),
                    "AsyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: LogicFailure(errs::Err {{ reason = alloc::string::String \"yyy\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 215),
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "AsyncDataConn::close 1",
                    "AsyncDataConn::drop 1",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_and_rollback_but_fail_first_sync_commit() {
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
                if let Err(e) = manager.commit_async(&mut reports).await {
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
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit_async 1",
                    "AsyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1 failed",
                    "SyncDataConn::rollback_async 1",
                    "AsyncDataConn::rollback_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 61),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 61),
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
                    "SyncDataConn::pre_commit_async 1",
                    "AsyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1 failed",
                    "SyncDataConn::rollback_async 1",
                    "AsyncDataConn::rollback_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 61),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 61),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_and_rollback_but_fail_first_async_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                if let Err(e) = manager.commit_async(&mut reports).await {
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
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 1",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit_async 2",
                    "AsyncDataConn::pre_commit_async 1",
                    "SyncDataConn::commit_async 2",
                    "AsyncDataConn::commit_async 1 failed",
                    "AsyncDataConn::rollback_async 1",
                    "SyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]", 192),
                    "AsyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]", 192),
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
                    "SyncDataConn::pre_commit_async 2",
                    "AsyncDataConn::pre_commit_async 1",
                    "SyncDataConn::commit_async 2",
                    "AsyncDataConn::commit_async 1 failed",
                    "AsyncDataConn::rollback_async 1",
                    "SyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]", 192),
                    "AsyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]", 192),
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "AsyncDataConn::close 1",
                    "AsyncDataConn::drop 1",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_and_rollback_but_fail_second_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
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
                if let Err(e) = manager.commit_async(&mut reports).await {
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
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit_async 1",
                    "AsyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "AsyncDataConn::commit_async 2 failed",
                    "AsyncDataConn::rollback_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
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
                    "SyncDataConn::pre_commit_async 1",
                    "AsyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "AsyncDataConn::commit_async 2 failed",
                    "AsyncDataConn::rollback_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_and_rollback_but_fail_first_sync_post_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                if let Err(e) = manager.commit_async(&mut reports).await {
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
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit_async 1",
                    "AsyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "AsyncDataConn::commit_async 2",
                    "SyncDataConn::post_commit_async 1 failed",
                    "AsyncDataConn::post_commit_async 2 failed",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 98, 237),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 98, 237),
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
                    "SyncDataConn::pre_commit_async 1",
                    "AsyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "AsyncDataConn::commit_async 2",
                    "SyncDataConn::post_commit_async 1 failed",
                    "AsyncDataConn::post_commit_async 2 failed",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 98, 237),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 98, 237),
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_and_rollback_but_fail_first_async_post_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                if let Err(e) = manager.commit_async(&mut reports).await {
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
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "AsyncDataConn::new 2",
                "SyncDataConn::new 1",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "AsyncDataConn::commit_async 2",
                "SyncDataConn::post_commit_async 1 failed",
                "AsyncDataConn::post_commit_async 2 failed",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 237, 98),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 237, 98),
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "AsyncDataConn::new 2",
                "SyncDataConn::new 1",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "AsyncDataConn::commit_async 2",
                "SyncDataConn::post_commit_async 1 failed",
                "AsyncDataConn::post_commit_async 2 failed",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 610 }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 98),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 610 }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 98),
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
            ]);
        }

        #[tokio::test]
        async fn test_commit_and_rollback_but_fail_second_post_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                if let Err(e) = manager.commit_async(&mut reports).await {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "foo".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "AsyncDataConn::new 2",
                "SyncDataConn::new 1",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "AsyncDataConn::commit_async 2",
                "SyncDataConn::post_commit_async 1 failed",
                "AsyncDataConn::post_commit_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 98),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 98),
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "AsyncDataConn::new 2",
                "SyncDataConn::new 1",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "AsyncDataConn::commit_async 2",
                "SyncDataConn::post_commit_async 1 failed",
                "AsyncDataConn::post_commit_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 98),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}]", 98),
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
            ]);
        }

        #[tokio::test]
        async fn test_commit_and_rollback_but_fail_second_post_commit_and_contains_no_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PostCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = NoCommitDataConn::new(3, logger.clone());
                let boxed = Box::new(DataConnContainer::new("baz", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit_async(&mut reports).await {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                        }
                        _ => panic_any(e),
                    }
                } else {
                    panic!();
                }
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(
                &logger.lock().unwrap()[0..24],
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "NoCommitDataConn::new 3",
                    "SyncDataConn::pre_commit_async 1",
                    "NoCommitDataConn::pre_commit_async 3",
                    "AsyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "NoCommitDataConn::commit_async 3",
                    "AsyncDataConn::commit_async 2",
                    "SyncDataConn::post_commit_async 1",
                    "NoCommitDataConn::post_commit_async 3",
                    "AsyncDataConn::post_commit_async 2 failed",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }}]", 237),
                    "NoCommitDataConn::on_txn_failure_async 3",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }}]", 237),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }}]", 237),
                    "NoCommitDataConn::close 3",
                    "NoCommitDataConn::drop 3",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
            #[cfg(windows)]
            assert_eq!(
                &logger.lock().unwrap()[0..24],
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "NoCommitDataConn::new 3",
                    "SyncDataConn::pre_commit_async 1",
                    "NoCommitDataConn::pre_commit_async 3",
                    "AsyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "NoCommitDataConn::commit_async 3",
                    "AsyncDataConn::commit_async 2",
                    "SyncDataConn::post_commit_async 1",
                    "NoCommitDataConn::post_commit_async 3",
                    "AsyncDataConn::post_commit_async 2 failed",
                    "SyncDataConn::on_txn_failure_async 1",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }}]", 237),
                    "NoCommitDataConn::on_txn_failure_async 3",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }}]", 237),
                    "AsyncDataConn::on_txn_failure_async 2",
                    &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: PostCommitFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::NoCommitDataConn\", cause: NoneByUncommitted, rollback: NoneByNotRolledBack }}]", 237),
                    "NoCommitDataConn::close 3",
                    "NoCommitDataConn::drop 3",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                ]
            );
        }

        #[tokio::test]
        async fn test_only_rollback_and_first_is_sync() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let reports = manager.new_failure_reports();
                manager.rollback_async(reports).await;
            }

            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::rollback_async 1",
                "AsyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]"),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]"),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }

        #[tokio::test]
        async fn test_only_rollback_and_first_is_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let reports = manager.new_failure_reports();
                manager.rollback_async(reports).await;
            }

            assert_eq!(*logger.lock().unwrap(), &[
                "AsyncDataConn::new 1",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback_async 2",
                "AsyncDataConn::rollback_async 1",
                "SyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]"),
                "AsyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]"),
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "AsyncDataConn::close 1",
                "AsyncDataConn::drop 1",
            ]);
        }

        #[tokio::test]
        async fn test_only_rollback_and_second_rollback_failed() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::Rollback);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let reports = manager.new_failure_reports();
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "AsyncDataConn::new 1",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback_async 2 failed",
                "AsyncDataConn::rollback_async 1",
                "SyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}) }}]", 120),
                "AsyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = {} }}) }}]", 120),
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "AsyncDataConn::close 1",
                "AsyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "AsyncDataConn::new 1",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback_async 2 failed",
                "AsyncDataConn::rollback_async 1",
                "SyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}) }}]", 120),
                "AsyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = {} }}) }}]", 120),
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "AsyncDataConn::close 1",
                "AsyncDataConn::drop 1",
            ]);
        }

        #[tokio::test]
        async fn test_only_rollback_and_first_rollback_failed() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::Rollback);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::None);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let reports = manager.new_failure_reports();
                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "AsyncDataConn::new 1",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback_async 2",
                "AsyncDataConn::rollback_async 1 failed",
                "SyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src/tokio/_test_commons.rs, line = {} }}) }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 263),
                "AsyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src/tokio/_test_commons.rs, line = {} }}) }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]", 263),
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "AsyncDataConn::close 1",
                "AsyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "AsyncDataConn::new 1",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback_async 2",
                "AsyncDataConn::rollback_async 1 failed",
                "SyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src\\tokio\\_test_commons.rs, line = 634 }}) }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]"),
                "AsyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src\\tokio\\_test_commons.rs, line = 634 }}) }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }}]"),
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "AsyncDataConn::close 1",
                "AsyncDataConn::drop 1",
            ]);
        }

        #[tokio::test]
        async fn test_commit_and_rollback_and_first_commit_failed_and_second_rollback_failed() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Rollback);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit_async(&mut reports).await {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "ZZZ");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }

                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1 failed",
                "SyncDataConn::rollback_async 1",
                "AsyncDataConn::rollback_async 2 failed",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src/tokio/_test_commons.rs, line = {} }}) }}]", 61, 263),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src/tokio/_test_commons.rs, line = {} }}) }}]", 61, 263),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1 failed",
                "SyncDataConn::rollback_async 1",
                "AsyncDataConn::rollback_async 2 failed",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src\\tokio\\_test_commons.rs, line = {} }}) }}]", 61, BASE_LINE + 240),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}, TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err {{ reason = alloc::string::String \"???\", file = src\\tokio\\_test_commons.rs, line = {} }}) }}]", 61, BASE_LINE + 240),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }

        #[tokio::test]
        async fn test_commit_and_rollback_and_secod_commit_failed_and_first_rollback_failed() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Rollback);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit_async(&mut reports).await {
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

                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "AsyncDataConn::commit_async 2 failed",
                "AsyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "AsyncDataConn::commit_async 2 failed",
                "AsyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }

        #[tokio::test]
        async fn test_commit_and_rollback_and_pre_commit_become_committed_and_ok() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommitBecomeCommitted);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommitBecomeCommitted);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();
                assert!(manager.commit_async(&mut reports).await.is_ok());
                manager.rollback_async(reports).await;
            }

            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "AsyncDataConn::commit_async 2",
                "SyncDataConn::post_commit_async 1",
                "AsyncDataConn::post_commit_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}]"),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }

        #[tokio::test]
        async fn test_commit_and_rollback_and_pre_commit_become_committed_but_failed() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommitBecomeCommitted);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                let ssnnptr = SendSyncNonNull::new(nnptr);
                manager.add(ssnnptr);

                let mut reports = manager.new_failure_reports();

                if let Err(e) = manager.commit_async(&mut reports).await {
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

                manager.rollback_async(reports).await;
            }

            #[cfg(unix)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "AsyncDataConn::commit_async 2 failed",
                "AsyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src/tokio/_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
            #[cfg(windows)]
            assert_eq!(*logger.lock().unwrap(), &[
                "SyncDataConn::new 1",
                "AsyncDataConn::new 2",
                "SyncDataConn::pre_commit_async 1",
                "AsyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "AsyncDataConn::commit_async 2 failed",
                "AsyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                "AsyncDataConn::on_txn_failure_async 2",
                &format!("TxnFailureReports=[TxnFailureReport {{ data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }}, TxnFailureReport {{ data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::AsyncDataConn\", cause: CommitFailure(errs::Err {{ reason = alloc::string::String \"YYY\", file = src\\tokio\\_test_commons.rs, line = {} }}), rollback: NoneByRolledBack }}]", 192),
                "AsyncDataConn::close 2",
                "AsyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
            ]);
        }
    }
}
