// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod logic_data;
mod logic_run;
mod logic_txn;

use crate::{
    DataAcc, DataConn, DataConnContainer, DataConnManager, DataHub, DataRun, DataSrc,
    DataSrcManager, ErrEntry, LogicRun, LogicTxn, SendSyncNonNull, TxnFailureReport,
};

use crate::data_src::{copy_global_data_srcs_to_map, create_data_conn_from_global_data_src};

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, marker, ptr};

#[derive(Debug)]
pub enum DataHubError {
    FailToSetupLocalDataSrcs {
        errors: Vec<ErrEntry>,
    },

    NoDataSrcToCreateDataConn {
        name: Arc<str>,
        data_conn_type: &'static str,
    },

    FailToRunLogic {
        errors: Vec<ErrEntry>,
    },
}

impl DataRun {
    fn new() -> Self {
        let mut data_src_map = HashMap::new();
        copy_global_data_srcs_to_map(&mut data_src_map);

        Self {
            local_data_src_manager: DataSrcManager::new(true),
            data_src_map,
            data_conn_manager: DataConnManager::new(),
            fixed: false,
        }
    }

    fn with_commit_order(names: &[&str]) -> Self {
        let mut data_src_map = HashMap::new();
        copy_global_data_srcs_to_map(&mut data_src_map);

        Self {
            local_data_src_manager: DataSrcManager::new(true),
            data_src_map,
            data_conn_manager: DataConnManager::with_commit_order(names),
            fixed: false,
        }
    }

    #[inline]
    fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        if self.fixed {
            return;
        }
        self.local_data_src_manager.add(name, ds);
    }

    #[inline]
    fn disuses(&mut self, name: impl AsRef<str>) {
        if self.fixed {
            return;
        }
        self.data_src_map.remove(name.as_ref());
        self.local_data_src_manager.remove(name);
    }

    fn begin(&mut self) -> errs::Result<()> {
        self.fixed = true;

        let mut errors = Vec::new();

        self.local_data_src_manager.setup(&mut errors);
        if errors.is_empty() {
            self.local_data_src_manager
                .copy_ds_ready_to_map(&mut self.data_src_map);
            Ok(())
        } else {
            Err(errs::Err::new(DataHubError::FailToSetupLocalDataSrcs {
                errors,
            }))
        }
    }

    #[inline]
    pub(crate) fn new_failure_reports(&self) -> Vec<TxnFailureReport> {
        self.data_conn_manager.new_failure_reports()
    }

    #[inline]
    pub(crate) fn commit(&mut self, reports: &mut [TxnFailureReport]) -> errs::Result<()> {
        self.data_conn_manager.commit(reports)
    }

    #[inline]
    pub(crate) fn rollback(&mut self, reports: Vec<TxnFailureReport>) {
        self.data_conn_manager.rollback(reports)
    }

    #[inline]
    pub(crate) fn end(&mut self) {
        self.data_conn_manager.close();
        self.fixed = false;
    }

    pub fn get_data_conn<C>(&mut self, name: &str) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        if let Some(ssnnptr) = self.data_conn_manager.find_by_name(name) {
            let typed_ssnnptr = DataConnManager::to_typed_ptr::<C>(&ssnnptr)?;
            return Ok(unsafe { &mut (*typed_ssnnptr).data_conn });
        }

        if let Some((local, index)) = self.data_src_map.get(name) {
            let boxed = if *local {
                self.local_data_src_manager
                    .create_data_conn::<C>(*index, name)?
            } else {
                create_data_conn_from_global_data_src::<C>(*index, name)?
            };

            let ptr = Box::into_raw(boxed);
            if let Some(nnptr) = ptr::NonNull::new(ptr) {
                let ssnnptr = SendSyncNonNull::new(nnptr);
                self.data_conn_manager.add(ssnnptr);

                let typed_ptr = ptr.cast::<DataConnContainer<C>>();
                return Ok(unsafe { &mut (*typed_ptr).data_conn });
            } // else { /* impossible case. */ }
        }

        Err(errs::Err::new(DataHubError::NoDataSrcToCreateDataConn {
            name: name.into(),
            data_conn_type: any::type_name::<C>(),
        }))
    }
}

impl<T: 'static> DataAcc for DataHub<T> {
    type D = T;

    #[inline]
    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> errs::Result<&mut C> {
        self.run.get_data_conn(name)
    }

    #[inline]
    fn run<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        let mut r = self.run.begin();
        if r.is_ok() {
            r = logic_fn(self);
        }
        self.run.end();
        r
    }

    #[inline]
    fn start(&mut self) -> LogicRun<'_, T> {
        LogicRun::new(self, false)
    }
}

impl<T: 'static> DataHub<T> {
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        Self {
            run: DataRun::new(),
            _phantom: marker::PhantomData,
        }
    }

    #[inline]
    pub fn with_commit_order(names: &[&str]) -> Self {
        Self {
            run: DataRun::with_commit_order(names),
            _phantom: marker::PhantomData,
        }
    }

    fn _txn<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        let mut r = self.run.begin();
        if r.is_ok() {
            r = logic_fn(self);
        }

        let mut reports = self.run.new_failure_reports();

        if r.is_ok() {
            r = self.run.commit(&mut reports);
        }
        if r.is_err() {
            self.run.rollback(reports);
        }

        self.run.end();
        r
    }

    #[inline]
    fn _begin_txn(&mut self) -> LogicTxn<'_, T> {
        LogicTxn::new(self)
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_data_hub {
    use super::*;
    use crate::_test_commons::*;
    use crate::{DataConnError, DataSrcError};
    use std::sync::Mutex;

    #[test]
    fn test_new() {
        struct MyData;

        let hub = DataHub::<MyData>::new();
        assert!(hub.run.local_data_src_manager.vec_unready.is_empty());
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert!(hub.run.data_src_map.is_empty());
        assert!(hub.run.data_conn_manager.vec.is_empty());
        assert!(hub.run.data_conn_manager.index_map.is_empty());
        assert!(!hub.run.fixed);
    }

    #[test]
    fn test_uses_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct YourData;

        let mut hub = DataHub::<YourData>::new();
        hub.run
            .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        hub.run
            .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 2);
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert!(hub.run.data_src_map.is_empty());
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);

        assert!(hub.run.begin().is_ok());

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 2);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(hub.run.fixed);
    }

    #[test]
    fn test_uses_but_already_fixed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct MyData;

        let mut hub = DataHub::<MyData>::new();
        hub.run
            .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 1);
        assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 0);
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 0);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);

        assert!(hub.run.begin().is_ok());

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 1);
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 1);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(hub.run.fixed);

        hub.run
            .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 1);
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 1);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(hub.run.fixed);
    }

    #[test]
    fn test_disuses_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct Abc;

        let mut hub = DataHub::<Abc>::new();
        hub.run
            .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        hub.run
            .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 2);
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert!(hub.run.data_src_map.is_empty());
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);

        hub.run.disuses("foo");

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 1);
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert!(hub.run.data_src_map.is_empty());
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);

        hub.run.disuses("bar");

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert!(hub.run.data_src_map.is_empty());
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);
    }

    #[test]
    fn test_disuses_and_fix() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct Abc;

        let mut hub = DataHub::<Abc>::new();
        hub.run
            .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        hub.run
            .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 2);
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert!(hub.run.data_src_map.is_empty());
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);

        hub.run.disuses("foo");

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 1);
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert!(hub.run.data_src_map.is_empty());
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);

        hub.run.disuses("bar");

        assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert!(hub.run.data_src_map.is_empty());
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);

        hub.run
            .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        hub.run
            .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert!(hub.run.begin().is_ok());

        assert!(hub.run.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 2);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(hub.run.fixed);

        hub.run
            .uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        assert!(hub.run.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 2);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(hub.run.fixed);

        hub.run.disuses("bar");

        assert!(hub.run.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 2);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(hub.run.fixed);

        hub.run.end();

        assert!(hub.run.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 2);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);

        hub.run.disuses("bar");

        assert!(hub.run.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 1);
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 1);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);

        hub.run.disuses("foo");

        assert!(hub.run.local_data_src_manager.vec_unready.is_empty());
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 0);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);
    }

    #[test]
    fn test_begin_if_empty() {
        struct Abc;

        let mut hub = DataHub::<Abc>::new();
        assert!(hub.run.begin().is_ok());

        assert!(hub.run.local_data_src_manager.vec_unready.is_empty());
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 0);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(hub.run.fixed);

        hub.run.end();

        assert!(hub.run.local_data_src_manager.vec_unready.is_empty());
        assert!(hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(hub.run.local_data_src_manager.local);
        assert_eq!(hub.run.data_src_map.len(), 0);
        assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!hub.run.fixed);
    }

    #[test]
    fn test_begin_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;

            let mut hub = DataHub::<Abc>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.run
                .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(hub.run.local_data_src_manager.local, true);
            assert_eq!(hub.run.data_src_map.len(), 0);
            assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(hub.run.fixed, false);

            assert_eq!(hub.run.begin().is_ok(), true);

            assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(hub.run.local_data_src_manager.local, true);
            assert_eq!(hub.run.data_src_map.len(), 2);
            assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(hub.run.fixed, true);

            hub.run.end();

            assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(hub.run.local_data_src_manager.local, true);
            assert_eq!(hub.run.data_src_map.len(), 2);
            assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(hub.run.fixed, false);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_begin_but_failed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.run
                .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
            hub.run
                .uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            assert_eq!(hub.run.local_data_src_manager.vec_unready.len(), 3);
            assert_eq!(hub.run.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(hub.run.local_data_src_manager.local, true);
            assert_eq!(hub.run.data_src_map.len(), 0);
            assert_eq!(hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(hub.run.fixed, false);

            if let Err(err) = hub.run.begin() {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 1);
                        assert_eq!(errors[0].name, "bar".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "XXX");
                    }
                    _ => panic!(),
                }
            } else {
                panic!();
            }

            hub.run.end();
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2 failed",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_run_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.run
                .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            assert!(hub
                .run(move |_data| {
                    logger_clone
                        .lock()
                        .unwrap()
                        .push("execute logic".to_string());
                    Ok(())
                })
                .is_ok());
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_run_but_failed_to_begin() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.run
                .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));

            let logger_clone = logger.clone();
            if let Err(err) = hub.run(move |_data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic but fail".to_string());
                Ok(())
            }) {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 1);
                        assert_eq!(errors[0].name, "bar".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "XXX");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2 failed",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_run_but_failed_to_run_logic() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.run
                .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();
            if let Err(err) = hub.run(move |_data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic but fail".to_string());
                Err(errs::Err::new("logic error".to_string()))
            }) {
                match err.reason::<String>() {
                    Ok(s) => assert_eq!(s, "logic error"),
                    _ => panic!(),
                }
            } else {
                panic!();
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic but fail",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.run
                .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();

            let result = hub
                .start()
                .run_or_block(move |_data| {
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Ok(())
                })
                .run(move |_data| {
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_force(move |_data| {
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .end();

            assert!(result.is_ok());
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic-0",
                "execute logic-1",
                "execute logic-2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_but_failed_to_start() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.run
                .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();

            let result = hub
                .start()
                .run(move |_data| {
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Ok(())
                })
                .run_force(move |_data| {
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_or_block(move |_data| {
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 1);
                        assert_eq!(errors[0].name, "bar".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "XXX");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2 failed",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_and_failed_to_run_but_run_force_runs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.run
                .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();
            let logger_clone_3 = logger.clone();

            let result = hub
                .start()
                .run(move |_data| {
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
                .run(move |_data| {
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_or_block(move |_data| {
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .run_force(move |_data| {
                    logger_clone_3
                        .lock()
                        .unwrap()
                        .push("execute logic-3".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToRunLogic { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "LogicRun#run(logic-0)".into());
                        assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic-0",
                "execute logic-3",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_but_failed_to_run_or_block_then_skip_even_run_force() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.run
                .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();
            let logger_clone_3 = logger.clone();

            let result = hub
                .start()
                .run_or_block(move |_data| {
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
                .run(move |_data| {
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_force(move |_data| {
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .run_or_block(move |_data| {
                    logger_clone_3
                        .lock()
                        .unwrap()
                        .push("execute logic-3".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToRunLogic { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "LogicRun#run_or_block(logic-0)".into());
                        assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic-0",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_and_failed_to_run_force_then_skip_run_but_run_force_runs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            hub.run
                .uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();
            let logger_clone_3 = logger.clone();

            let result = hub
                .start()
                .run_force(move |_data| {
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
                .run(move |_data| {
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_force(move |_data| {
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .run_or_block(move |_data| {
                    logger_clone_3
                        .lock()
                        .unwrap()
                        .push("execute logic-3".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToRunLogic { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "LogicRun#run_force(logic-0)".into());
                        assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "execute logic-0",
                "execute logic-2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_get_data_conn_cached() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(e) = hub.run(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                Ok(())
            }) {
                panic!("{:?}", e);
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::setup 1",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_get_data_conn_and_no_data_src_to_create_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            let logger_clone = logger.clone();

            if let Err(e) = hub.run(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                Ok(())
            }) {
                match e.reason::<DataHubError>() {
                    Ok(DataHubError::NoDataSrcToCreateDataConn {
                        name,
                        data_conn_type,
                    }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(data_conn_type, &"sabi::_test_commons::SyncDataConn");
                    }
                    _ => panic!(),
                }
            }
        }

        assert_eq!(*logger.lock().unwrap(), &["execute logic",]);
    }

    #[test]
    fn test_get_data_conn_and_failed_to_creata_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run.uses(
                "foo",
                SyncDataSrc::new(1, logger.clone(), Fail::CreateDataConn),
            );

            let logger_clone = logger.clone();

            if let Err(e) = hub.run(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                Ok(())
            }) {
                match e.reason::<DataSrcError>() {
                    Ok(DataSrcError::FailToCreateDataConn {
                        name,
                        data_conn_type,
                    }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(data_conn_type, &"sabi::_test_commons::SyncDataConn");
                    }
                    _ => panic!(),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::setup 1",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_get_data_conn_and_failed_to_cast_data_conn() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;

            let mut hub = DataHub::<A>::new();

            hub.run
                .uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(e) = hub.run(move |data| {
                logger_clone
                    .lock()
                    .unwrap()
                    .push("execute logic".to_string());
                if let Err(e) = data.get_data_conn::<AsyncDataConn>("foo") {
                    match e.reason::<DataSrcError>() {
                        Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                            assert_eq!(name.as_ref(), "foo");
                            assert_eq!(target_type, &"sabi::_test_commons::AsyncDataConn");
                        }
                        _ => panic!("{e:?}"),
                    }
                } else {
                    panic!();
                }

                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;

                if let Err(e) = data.get_data_conn::<AsyncDataConn>("foo") {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                            assert_eq!(name.as_ref(), "foo");
                            assert_eq!(target_type, &"sabi::_test_commons::AsyncDataConn");
                            Err(e)
                        }
                        _ => panic!("{e:?}"),
                    }
                } else {
                    panic!();
                }
            }) {
                match e.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(target_type, &"sabi::_test_commons::AsyncDataConn");
                    }
                    _ => panic!(),
                }
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::setup 1",
                "execute logic",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn data_hub_implements_send_trait() {
        struct A;
        let mut data = DataHub::<A>::new();
        let handle = std::thread::spawn(move || {
            data.run(|_data| Ok(())).unwrap();
        });

        handle.join().unwrap();
    }
}
