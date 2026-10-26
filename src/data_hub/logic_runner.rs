// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataConn, DataHub, DataSrc, LogicRun, LogicRunner, LogicTxn};

use std::sync::Arc;

impl<T: 'static> LogicRunner<T> {
    /// Creates a new `LogicRunner` with the given [`DataHub`].
    ///
    /// # Parameters
    ///
    /// * `hub`: The `DataHub` instance to wrap.
    ///
    /// # Returns
    ///
    /// A new [`LogicRunner`].
    #[inline]
    pub fn new(hub: DataHub<T>) -> Self {
        Self { hub }
    }

    /// Registers a local data source for this runner.
    ///
    /// Local data sources registered here take precedence over global data sources when
    /// retrieving connections with the same name.
    ///
    /// # Type Parameters
    ///
    /// * `S`: The type of the data source implementing [`DataSrc<C>`].
    /// * `C`: The type of the data connection implementing [`DataConn`].
    ///
    /// # Parameters
    ///
    /// * `name`: The name identifier for the data source.
    /// * `ds`: The data source instance to register.
    #[inline]
    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        self.hub.uses(name, ds);
    }

    /// Deregisters a local or previously registered data source by name.
    ///
    /// # Parameters
    ///
    /// * `name`: The name of the data source to deregister.
    #[inline]
    pub fn disuses(&mut self, name: impl AsRef<str>) {
        self.hub.disuses(name);
    }

    /// Executes a single business logic closure non-transactionally.
    ///
    /// Sets up local data sources, invokes the closure with a mutable reference to [`DataHub`],
    /// and closes all participating data connections afterwards.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the closure, which must be
    ///   `FnMut(&mut DataHub<T>) -> errs::Result<()>`.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The business logic closure to execute.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: The result of the logic function's execution,
    ///   or an error if executing `logic_fn` fails.
    #[inline]
    pub fn run<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        self.hub.run(logic_fn)
    }

    /// Starts a builder chain for executing non-transactional business logic steps.
    ///
    /// # Returns
    ///
    /// A [`LogicRun`] builder to configure and execute sequential logic steps.
    #[inline]
    pub fn start(&mut self) -> LogicRun<'_, T> {
        LogicRun::new(&mut self.hub, false)
    }

    /// Executes a single business logic closure within a transaction.
    ///
    /// Sets up local data sources, runs the closure, and automatically commits all participating
    /// data connections if the closure succeeds. If the closure or commit phase fails, all
    /// connections are rolled back.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the closure, which must be
    ///   `FnMut(&mut DataHub<T>) -> errs::Result<()>`.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The transactional business logic closure to execute.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if the closure and the commit phase succeed,
    ///   or an `errs::Err` if any transaction phase fails.
    #[inline]
    pub fn txn<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        self.hub.txn(logic_fn)
    }

    #[inline]
    pub fn begin_txn(&mut self) -> LogicTxn<'_, T> {
        LogicTxn::new(&mut self.hub)
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_logic_runner {
    use super::*;
    use crate::_test_commons::*;
    use crate::{DataConnError, DataHubError, DataSrcError};
    use std::sync::Mutex;

    #[test]
    fn test_new() {
        struct MyData;
        let hub = DataHub::<MyData>::new();
        let runner = LogicRunner::new(hub);

        assert!(runner.hub.local_data_src_manager.vec_unready.is_empty());
        assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.local_data_src_manager.local);
        assert!(runner.hub.data_src_map.is_empty());
        assert!(runner.hub.data_conn_manager.vec.is_empty());
        assert!(runner.hub.data_conn_manager.index_map.is_empty());
        assert!(!runner.hub.fixed);
    }

    #[test]
    fn test_uses() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct MyData;
        let hub = DataHub::<MyData>::new();
        let mut runner = LogicRunner::new(hub);

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 2);
        assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.local_data_src_manager.local);
        assert!(runner.hub.data_src_map.is_empty());
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);

        assert!(runner.hub.begin().is_ok());

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.local_data_src_manager.local);
        assert_eq!(runner.hub.data_src_map.len(), 2);
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.fixed);
    }

    #[test]
    fn test_uses_but_already_fixed() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct MyData;
        let hub = DataHub::<MyData>::new();
        let mut runner = LogicRunner::new(hub);

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 1);
        assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
        assert!(runner.hub.local_data_src_manager.local);
        assert_eq!(runner.hub.data_src_map.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);

        assert!(runner.hub.begin().is_ok());

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 1);
        assert!(runner.hub.local_data_src_manager.local);
        assert_eq!(runner.hub.data_src_map.len(), 1);
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.fixed);

        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 1);
        assert!(runner.hub.local_data_src_manager.local);
        assert_eq!(runner.hub.data_src_map.len(), 1);
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.fixed);
    }

    #[test]
    fn test_disuses_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct Abc;
        let hub = DataHub::<Abc>::new();
        let mut runner = LogicRunner::new(hub);

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 2);
        assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.local_data_src_manager.local);
        assert!(runner.hub.data_src_map.is_empty());
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);

        runner.disuses("foo");

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 1);
        assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.local_data_src_manager.local);
        assert!(runner.hub.data_src_map.is_empty());
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);

        runner.disuses("bar");

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
        assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.local_data_src_manager.local);
        assert!(runner.hub.data_src_map.is_empty());
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);
    }

    #[test]
    fn test_disuses_and_fix() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        struct Abc;
        let hub = DataHub::<Abc>::new();
        let mut runner = LogicRunner::new(hub);

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 2);
        assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.local_data_src_manager.local);
        assert!(runner.hub.data_src_map.is_empty());
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);

        runner.disuses("foo");

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 1);
        assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.local_data_src_manager.local);
        assert!(runner.hub.data_src_map.is_empty());
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);

        runner.disuses("bar");

        assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
        assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.local_data_src_manager.local);
        assert!(runner.hub.data_src_map.is_empty());
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);

        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert!(runner.hub.begin().is_ok());

        assert!(runner.hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.local_data_src_manager.local);
        assert_eq!(runner.hub.data_src_map.len(), 2);
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.fixed);

        runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

        assert!(runner.hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.local_data_src_manager.local);
        assert_eq!(runner.hub.data_src_map.len(), 2);
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.fixed);

        runner.disuses("bar");

        assert!(runner.hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.local_data_src_manager.local);
        assert_eq!(runner.hub.data_src_map.len(), 2);
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.fixed);

        runner.hub.end();

        assert!(runner.hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.local_data_src_manager.local);
        assert_eq!(runner.hub.data_src_map.len(), 2);
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);

        runner.disuses("bar");

        assert!(runner.hub.local_data_src_manager.vec_unready.is_empty());
        assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 1);
        assert!(runner.hub.local_data_src_manager.local);
        assert_eq!(runner.hub.data_src_map.len(), 1);
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);

        runner.disuses("foo");

        assert!(runner.hub.local_data_src_manager.vec_unready.is_empty());
        assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
        assert!(runner.hub.local_data_src_manager.local);
        assert_eq!(runner.hub.data_src_map.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
        assert!(!runner.hub.fixed);
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

            assert!(runner.hub.local_data_src_manager.vec_unready.is_empty());
            assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
            assert!(runner.hub.local_data_src_manager.local);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert!(!runner.hub.fixed);
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

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);

            assert!(runner
                .run(|data| {
                    let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                    let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
                .is_ok());

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);
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

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 3);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);

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

            let r = runner.start();
            assert!(r.end().is_ok());
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

            let r = runner.start();
            assert!(r.end().is_ok());
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

            assert!(runner.hub.local_data_src_manager.vec_unready.is_empty());
            assert!(runner.hub.local_data_src_manager.vec_ready.is_empty());
            assert!(runner.hub.local_data_src_manager.local);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert!(!runner.hub.fixed);
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

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);

            assert!(runner
                .txn(|data| {
                    let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                    let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
                .is_ok());

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);
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

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 3);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);

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

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 3);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);
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
    fn test_txn_but_failed_to_run_logic() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            if let Err(err) = runner.txn(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                logger
                    .lock()
                    .unwrap()
                    .push("Run the logic but failed".to_string());
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
            logger.lock().unwrap()[..],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "Run the logic but failed",
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
    fn test_txn_but_failed_to_pre_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PreCommit));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            if let Err(err) = runner.txn(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                let _conn3 = data.get_data_conn::<SyncDataConn>("baz")?;
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            }) {
                match err.reason::<DataConnError>() {
                    Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 1);
                        assert_eq!(errors[0].name, "bar".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "zzz");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        #[cfg(unix)]
        assert_eq!(
            logger.lock().unwrap()[..],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2 failed",
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2",
                "SyncDataConn::rollback 3",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/_test_commons.rs, line = 88 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/_test_commons.rs, line = 88 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/_test_commons.rs, line = 88 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 3",
                "SyncDataConn::drop 3",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
        #[cfg(windows)]
        assert_eq!(
            logger.lock().unwrap()[..],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2 failed",
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2",
                "SyncDataConn::rollback 3",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\_test_commons.rs, line = 88 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\_test_commons.rs, line = 88 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\_test_commons.rs, line = 88 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 3",
                "SyncDataConn::drop 3",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_txn_but_failed_to_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Commit));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            if let Err(err) = runner.txn(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                let _conn3 = data.get_data_conn::<SyncDataConn>("baz")?;
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            }) {
                match err.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCommitDataConn { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 1);
                        assert_eq!(errors[0].name, "bar".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "ZZZ");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        #[cfg(unix)]
        assert_eq!(
            logger.lock().unwrap()[..],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::pre_commit 3",
                "SyncDataConn::commit 1",
                "SyncDataConn::commit 2 failed",
                "SyncDataConn::rollback 2",
                "SyncDataConn::rollback 3",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/_test_commons.rs, line = 66 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/_test_commons.rs, line = 66 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/_test_commons.rs, line = 66 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 3",
                "SyncDataConn::drop 3",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
        #[cfg(windows)]
        assert_eq!(
            logger.lock().unwrap()[..],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::pre_commit 3",
                "SyncDataConn::commit 1",
                "SyncDataConn::commit 2 failed",
                "SyncDataConn::rollback 2",
                "SyncDataConn::rollback 3",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\_test_commons.rs, line = 66 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\_test_commons.rs, line = 66 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\_test_commons.rs, line = 66 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 3",
                "SyncDataConn::drop 3",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_txn_but_failed_to_post_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PostCommit));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            if let Err(err) = runner.txn(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                let _conn3 = data.get_data_conn::<SyncDataConn>("baz")?;
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            }) {
                match err.reason::<DataConnError>() {
                    Ok(DataConnError::FailToPostCommitDataConn { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 1);
                        assert_eq!(errors[0].name, "bar".into());
                        assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        #[cfg(unix)]
        assert_eq!(
            logger.lock().unwrap()[0..40],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::pre_commit 3",
                "SyncDataConn::commit 1",
                "SyncDataConn::commit 2",
                "SyncDataConn::commit 3",
                "SyncDataConn::post_commit 1",
                "SyncDataConn::post_commit 2 failed",
                "SyncDataConn::post_commit 3",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/_test_commons.rs, line = 112 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/_test_commons.rs, line = 112 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/_test_commons.rs, line = 112 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                "SyncDataConn::close 3",
                "SyncDataConn::drop 3",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
        #[cfg(windows)]
        assert_eq!(
            logger.lock().unwrap()[0..40],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::pre_commit 3",
                "SyncDataConn::commit 1",
                "SyncDataConn::commit 2",
                "SyncDataConn::commit 3",
                "SyncDataConn::post_commit 1",
                "SyncDataConn::post_commit 2 failed",
                "SyncDataConn::post_commit 3",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\_test_commons.rs, line = 112 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\_test_commons.rs, line = 112 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\_test_commons.rs, line = 112 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                "SyncDataConn::close 3",
                "SyncDataConn::drop 3",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_txn_but_failed_to_rollback() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Rollback));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            if let Err(err) = runner.txn(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                let _conn3 = data.get_data_conn::<SyncDataConn>("baz")?;
                logger
                    .lock()
                    .unwrap()
                    .push("Run the logic but failed".to_string());
                Err(errs::Err::new("fail".to_string()))
            }) {
                match err.reason::<String>() {
                    Ok(s) => assert_eq!(s, "fail"),
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        #[cfg(unix)]
        assert_eq!(
            logger.lock().unwrap()[0..34],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn 3",
                "SyncDataConn::new 3",
                "Run the logic but failed",
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2 failed",
                "SyncDataConn::rollback 3",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/_test_commons.rs, line = 137 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/_test_commons.rs, line = 137 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/_test_commons.rs, line = 137 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 3",
                "SyncDataConn::drop 3",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
        #[cfg(windows)]
        assert_eq!(
            logger.lock().unwrap()[0..34],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn 3",
                "SyncDataConn::new 3",
                "Run the logic but failed",
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2 failed",
                "SyncDataConn::rollback 3",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\_test_commons.rs, line = 137 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\_test_commons.rs, line = 137 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\_test_commons.rs, line = 137 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 3",
                "SyncDataConn::drop 3",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_begin_txn_with_no_data_src() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());

            let txn = runner.begin_txn();
            assert!(txn.end_txn().is_ok());
        }

        assert_eq!(logger.lock().unwrap().len(), 0);
    }

    #[test]
    fn test_begin_txn_with_some_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let txn = runner.begin_txn();
            assert!(txn.end_txn().is_ok());
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
    fn test_begin_txn_but_failed_to_begin() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            if let Err(err) = runner.begin_txn().end_txn() {
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
    fn test_txn_with_commit_order() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::with_commit_order(&["bar", "foo"]));

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 2);
            assert_eq!(runner.hub.fixed, false);

            assert!(runner
                .txn(|data| {
                    let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                    let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
                .is_ok());

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 2);
            assert_eq!(runner.hub.fixed, false);
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
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::commit 2",
                "SyncDataConn::commit 1",
                "SyncDataConn::post_commit 2",
                "SyncDataConn::post_commit 1",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_get_data_conn_by_creating_but_failed_to_cast() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);

            if let Err(err) = runner.txn(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<AsyncDataConn>("bar")?;
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            }) {
                match err.reason::<DataSrcError>() {
                    Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(*name, "bar".into());
                        assert_eq!(*target_type, "sabi::_test_commons::AsyncDataConn");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);
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
                "SyncDataConn::rollback 1",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
    fn test_get_data_conn_by_using_cache() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::with_commit_order(&["bar", "foo"]));

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 2);
            assert_eq!(runner.hub.fixed, false);

            assert!(runner
                .txn(|data| {
                    let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                    let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;

                    let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                    let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger.lock().unwrap().push("Run the logic".to_string());
                    Ok(())
                })
                .is_ok());

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 2);
            assert_eq!(runner.hub.fixed, false);
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
                "SyncDataConn::pre_commit 2",
                "SyncDataConn::pre_commit 1",
                "SyncDataConn::commit 2",
                "SyncDataConn::commit 1",
                "SyncDataConn::post_commit 2",
                "SyncDataConn::post_commit 1",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_get_data_conn_by_using_cache_but_failed_to_cast() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);

            if let Err(err) = runner.txn(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;

                let _conn2 = data.get_data_conn::<AsyncDataConn>("bar")?;
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            }) {
                match err.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(*name, "bar".into());
                        assert_eq!(*target_type, "sabi::_test_commons::AsyncDataConn");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);
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
                "SyncDataConn::rollback 1",
                "SyncDataConn::rollback 2",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
    fn test_get_data_conn_but_no_corresponding_data_src() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);

            if let Err(err) = runner.txn(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<AsyncDataConn>("xxx")?;
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            }) {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::NoDataSrcToCreateDataConn {
                        name,
                        data_conn_type,
                    }) => {
                        assert_eq!(*name, "xxx".into());
                        assert_eq!(*data_conn_type, "sabi::_test_commons::AsyncDataConn");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }

            assert_eq!(runner.hub.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.local_data_src_manager.local, true);
            assert_eq!(runner.hub.data_src_map.len(), 2);
            assert_eq!(runner.hub.data_conn_manager.vec.len(), 0);
            assert_eq!(runner.hub.data_conn_manager.index_map.len(), 0);
            assert_eq!(runner.hub.fixed, false);
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
                "SyncDataConn::rollback 1",
                "SyncDataConn::on_txn_failure 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    mod test_data_acc_aggregation_to_data_hub {
        use super::*;
        use crate::DataAcc;
        use override_macro::{overridable, override_with};

        #[overridable]
        trait GettingDataAcc: DataAcc {
            fn get_text(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
                // getting a text by using _conn
                Ok("hello".to_string())
            }
        }

        #[overridable]
        trait SettingDataAcc: DataAcc {
            fn set_text(&mut self, _text: String) -> errs::Result<()> {
                let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
                // setting a text by using _conn
                Ok(())
            }
        }

        struct Hoge;
        impl GettingDataAcc for DataHub<Hoge> {}
        impl SettingDataAcc for DataHub<Hoge> {}

        #[overridable]
        trait HogeData {
            fn get_text(&mut self) -> errs::Result<String>;
            fn set_text(&mut self, text: String) -> errs::Result<()>;
        }

        #[override_with(GettingDataAcc, SettingDataAcc)]
        impl HogeData for DataHub<Hoge> {}

        fn hoge_logic(data: &mut impl HogeData) -> errs::Result<()> {
            let mut text = data.get_text()?;
            text = text.to_uppercase();
            data.set_text(text)?;
            Ok(())
        }

        #[test]
        fn test_run() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut runner = LogicRunner::new(DataHub::<Hoge>::new());
                runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
                if let Err(err) = runner.run(hoge_logic) {
                    panic!("{err:?}");
                }
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
        fn test_txn() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut runner = LogicRunner::new(DataHub::<Hoge>::new());
                runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
                if let Err(err) = runner.txn(hoge_logic) {
                    panic!("{err:?}");
                }
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
