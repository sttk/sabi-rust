// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataAcc, DataConn, DataHub, DataSrc, LogicRun, LogicRunner, LogicTxn};

use std::sync::Arc;

impl<T: 'static> LogicRunner<T> {
    #[inline]
    pub fn new(hub: DataHub<T>) -> Self {
        Self { hub }
    }

    #[inline]
    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        self.hub.run.uses(name, ds);
    }

    #[inline]
    pub fn disuses(&mut self, name: impl AsRef<str>) {
        self.hub.run.disuses(name);
    }

    #[inline]
    pub fn run<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        self.hub.run(logic_fn)
    }

    #[inline]
    pub fn start(&mut self) -> LogicRun<'_, T> {
        self.hub.start()
    }

    #[inline]
    pub fn txn<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        self.hub._txn(logic_fn)
    }

    #[inline]
    pub fn begin_txn(&mut self) -> LogicTxn<'_, T> {
        self.hub._begin_txn()
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_logic_data {
    use super::*;
    use crate::_test_commons::*;
    use crate::DataHubError;
    use std::sync::Mutex;

    #[test]
    fn test_new() {
        struct MyData;
        let hub = DataHub::<MyData>::new();
        let runner = LogicRunner::new(hub);

        assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.local);
        assert!(runner.hub.run.data_src_map.is_empty());
        assert!(runner.hub.run.data_conn_manager.vec.is_empty());
        assert!(runner.hub.run.data_conn_manager.index_map.is_empty());
        assert!(!runner.hub.run.fixed);
    }

    #[test]
    fn test_uses() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct MyData;
        let hub = DataHub::<MyData>::new();
        let mut runner = LogicRunner::new(hub);

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
        assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.local);
        assert!(runner.hub.run.data_src_map.is_empty());
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);

        assert!(runner.hub.run.begin().is_ok());

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 2);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.run.fixed);
    }

    #[test]
    fn test_uses_but_already_fixed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct MyData;
        let hub = DataHub::<MyData>::new();
        let mut runner = LogicRunner::new(hub);

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 1);
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);

        assert!(runner.hub.run.begin().is_ok());

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 1);
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 1);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.run.fixed);

        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 1);
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 1);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.run.fixed);
    }

    #[test]
    fn test_disuses_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct Abc;
        let hub = DataHub::<Abc>::new();
        let mut runner = LogicRunner::new(hub);

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
        assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.local);
        assert!(runner.hub.run.data_src_map.is_empty());
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);

        runner.disuses("foo");

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 1);
        assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.local);
        assert!(runner.hub.run.data_src_map.is_empty());
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);

        runner.disuses("bar");

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.local);
        assert!(runner.hub.run.data_src_map.is_empty());
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);
    }

    #[test]
    fn test_disuses_and_fix() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct Abc;
        let hub = DataHub::<Abc>::new();
        let mut runner = LogicRunner::new(hub);

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
        assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.local);
        assert!(runner.hub.run.data_src_map.is_empty());
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);

        runner.disuses("foo");

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 1);
        assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.local);
        assert!(runner.hub.run.data_src_map.is_empty());
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);

        runner.disuses("bar");

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.local);
        assert!(runner.hub.run.data_src_map.is_empty());
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert!(runner.hub.run.begin().is_ok());

        assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 2);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.run.fixed);

        runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 2);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.run.fixed);

        runner.disuses("bar");

        assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 2);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.run.fixed);

        runner.hub.run.end();

        assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 2);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);

        runner.disuses("bar");

        assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 1);
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 1);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);

        runner.disuses("foo");

        assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.run.fixed);
    }

    #[test]
    fn test_run_with_no_data_src() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());

            assert!(runner
                .run(|_data| {
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
                .is_ok());

            assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
            assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
            assert!(runner.hub.run.local_data_src_manager.local);
            assert_eq!(runner.hub.run.data_src_map.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
            assert!(!runner.hub.run.fixed);
        }

        assert_eq!(*logger.lock().unwrap(), &["Run the logic"]);
    }

    #[test]
    fn test_run_with_some_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.run.fixed, false);

            assert!(runner
                .run(|data| {
                    let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                    let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
                .is_ok());

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 2);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.run.fixed, false);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "Run the logic",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
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
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 3);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.run.fixed, false);

            if let Err(err) = runner.run(|_| {
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            }) {
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
    fn test_run_but_failed_to_run_logic() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            if let Err(err) = runner.run(|_| {
                logger
                    .lock()
                    .unwrap()
                    .push("Run the logic but failed".to_string());
                Err(errs::Err::new("logic error".to_string()))
            }) {
                match err.reason::<String>() {
                    Ok(s) => assert_eq!(s, "logic error"),
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
                "Run the logic but failed",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_start_with_no_data_src() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());

            let runner = runner.start();
            assert!(runner.end().is_ok());
        }

        assert_eq!(logger.lock().unwrap().len(), 0);
    }

    #[test]
    fn test_start_with_some_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let runner = runner.start();
            assert!(runner.end().is_ok());
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
    fn test_start_but_failed_to_begin() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            if let Err(err) = runner.start().end() {
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
    fn test_txn_with_no_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());

            assert!(runner
                .txn(|_data| {
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
                .is_ok());

            assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
            assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
            assert!(runner.hub.run.local_data_src_manager.local);
            assert_eq!(runner.hub.run.data_src_map.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
            assert!(!runner.hub.run.fixed);
        }

        assert_eq!(*logger.lock().unwrap(), &["Run the logic"]);
    }

    #[test]
    fn test_txn_with_some_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.run.fixed, false);

            assert!(runner
                .txn(|data| {
                    let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                    let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
                .is_ok());

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 2);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.run.fixed, false);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "Run the logic",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::commit 1",
                "SyncDataConn::commit 2",
                "SyncDataConn::post_commit 1",
                "SyncDataConn::post_commit 2",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_txn_but_failed_to_begin() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 3);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.run.fixed, false);

            if let Err(err) = runner.txn(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                let _conn3 = data.get_data_conn::<SyncDataConn>("baz")?;
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            }) {
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

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 3);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.run.fixed, false);
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

    //#[test]
    fn test_txn_but_failed_to_run_logic() {}

    //#[test]
    fn test_txn_but_failed_to_pre_commit() {}

    //#[test]
    fn test_txn_but_failed_to_commit() {}

    //#[test]
    fn test_txn_but_failed_to_post_commit() {}

    //#[test]
    fn test_txn_but_failed_to_rollback() {}

    //#[test]
    fn test_begin_txn_with_no_data_src() {}

    //#[test]
    fn test_begin_txn_with_some_data_src() {}

    //#[test]
    fn test_begin_txn_but_failed_to_begin() {}

    //#[test]
    fn test_txn_with_commit_order() {}

    //#[test]
    fn test_get_data_conn_by_creating_but_failed_to_cast() {}

    //#[test]
    fn test_get_data_conn_by_using_cache() {}

    //#[test]
    fn test_get_data_conn_by_using_cache_but_failed_to_cast() {}

    //#[test]
    fn test_get_data_conn_but_no_corresponding_data_src() {}
}
