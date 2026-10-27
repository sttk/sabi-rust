// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{
    AsyncGroup, ErrEntry, SendSyncNonNull, TxnFailureCause, TxnFailureReport, TxnFailureRollback,
};
use crate::{DataConn, DataConnContainer, DataConnManager};

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, mem, ptr};

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum DataConnError {
    FailToPreCommitDataConn {
        errors: Vec<ErrEntry>,
    },
    FailToCommitDataConn {
        errors: Vec<ErrEntry>,
    },
    FailToPostCommitDataConn {
        errors: Vec<ErrEntry>,
    },
    FailToCastDataConn {
        name: Arc<str>,
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
            ordered_names: Box::new([]),
            committed: false,
        }
    }

    pub(crate) fn with_commit_order(names: &[&str]) -> Self {
        let names: Box<[Arc<str>]> = names.iter().map(|s| Arc::from(*s)).collect();

        let mut index_map = HashMap::with_capacity(names.len());
        // Using rev because eariler ones take precedence when names overlap
        for (i, nm) in names.iter().rev().enumerate() {
            index_map.insert(Arc::clone(nm), names.len() - 1 - i);
        }

        Self {
            vec: vec![None; names.len()],
            index_map,
            ordered_names: names,
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
            ag.index = i;
            ag.name = name.clone();
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
            ag.index = i;
            ag.name = name.clone();
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
            ag.index = i;
            ag.name = name.clone();
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
            ag.index = i;
            ag.name = name.clone();
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
        for (i, nm) in self.ordered_names.iter().enumerate() {
            self.index_map.insert(nm.clone(), i);
        }

        let mut vec: Vec<Option<SendSyncNonNull<DataConnContainer>>> =
            vec![None; self.ordered_names.len()];
        mem::swap(&mut self.vec, &mut vec);

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
mod _test_commit_and_rollback;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_data;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_new_and_add;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_utils;
// #[cfg_attr(coverage_nightly, coverage(off))]
// #[cfg(test)]
// mod _test_rollback;
// #[cfg_attr(coverage_nightly, coverage(off))]
// #[cfg(test)]
// mod _test_commit_order;
