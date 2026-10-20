// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{DataConn, DataHub, DataSrc, LogicRun, LogicRunner, LogicTxn};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

impl<T: 'static + Send> LogicRunner<T> {
    /// Creates a new asynchronous `LogicRunner` with the given [`DataHub`].
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
        S: DataSrc<C> + 'static,
        C: DataConn + 'static,
    {
        self.hub.run.uses(name, ds);
    }

    /// Deregisters a local or previously registered data source by name.
    ///
    /// # Parameters
    ///
    /// * `name`: The name of the data source to deregister.
    #[inline]
    pub fn disuses(&mut self, name: impl AsRef<str>) {
        self.hub.run.disuses(name);
    }

    /// Executes a single business logic closure non-transactionally and asynchronously.
    ///
    /// Sets up local data sources, invokes the closure with a mutable reference to [`DataHub`],
    /// and closes all participating data connections afterwards.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the closure returning a pinned future.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The business logic closure to execute asynchronously.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: The result of the logic function's execution,
    ///   or an error if executing `logic_fn` fails.
    #[inline]
    pub async fn run_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        self.hub.run.begin_async().await?;
        let r = logic_fn(&mut self.hub).await;
        self.hub.run.end();
        r
    }

    /// Starts a builder chain for executing non-transactional business logic steps
    /// asynchronously.
    ///
    /// # Returns
    ///
    /// A [`LogicRun`] builder to configure and execute sequential logic steps.
    #[inline]
    pub async fn start_async(&mut self) -> LogicRun<'_, T> {
        LogicRun::new_async(&mut self.hub, false).await
    }

    /// Executes a single business logic closure within an asynchronous transaction.
    ///
    /// Sets up local data sources, runs the closure, and automatically commits all participating
    /// data connections if the closure succeeds. If the closure or commit phase fails, all
    /// connections are rolled back.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the closure returning a pinned future.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: The transactional business logic closure to execute asynchronously.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if the closure and the commit phase succeed,
    ///   or an `errs::Err` if any transaction phase fails.
    #[inline]
    pub async fn txn_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        self.hub.run.begin_async().await?;
        let mut r = logic_fn(&mut self.hub).await;
        let mut reports = self.hub.run.new_failure_reports();
        if r.is_ok() {
            r = self.hub.run.commit_async(&mut reports).await;
        }
        if r.is_err() {
            self.hub.run.rollback_async(reports).await;
        }
        self.hub.run.end();
        r
    }

    /// Starts a builder chain for executing transactional business logic steps asynchronously.
    ///
    /// # Returns
    ///
    /// A [`LogicTxn`] builder to configure, execute, and commit or roll back sequential logic
    /// steps.
    #[inline]
    pub async fn begin_txn_async(&mut self) -> LogicTxn<'_, T> {
        LogicTxn::new_async(&mut self.hub).await
    }
}

/// Wraps an async closure or function returning `errs::Result<()>` into a boxed pinned future
/// suitable for [`LogicRunner`] and [`DataHub`](crate::tokio::DataHub) methods.
///
/// # Parameters
///
/// * `$f`: An async closure or function taking a `&mut DataHub<T>`.
#[macro_export]
#[doc(hidden)]
macro_rules! _logic {
    ($f:expr) => {
        |data| {
            let fut: std::pin::Pin<Box<dyn std::future::Future<Output = errs::Result<()>> + Send>> =
                Box::pin(async move { $f(data).await });
            fut
        }
    };
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_logic_runner {
    use super::*;
    use crate::tokio::_test_commons::*;
    use crate::tokio::{DataAcc, DataConnError, DataHubError, DataSrcError, LogicRunner};
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

    #[tokio::test]
    async fn test_uses() {
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

        assert!(runner.hub.run.begin_async().await.is_ok());

        assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
        assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
        assert!(runner.hub.run.local_data_src_manager.local);
        assert_eq!(runner.hub.run.data_src_map.len(), 2);
        assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
        assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
        assert!(runner.hub.run.fixed);
    }

    #[tokio::test]
    async fn test_uses_but_already_fixed() {
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

        assert!(runner.hub.run.begin_async().await.is_ok());

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

    #[tokio::test]
    async fn test_disuses_and_ok() {
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

    #[tokio::test]
    async fn test_disuses_and_fix() {
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

        assert!(runner.hub.run.begin_async().await.is_ok());

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

    #[tokio::test]
    async fn test_run_with_no_data_src() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());

            assert!(runner
                .run_async(|_data| {
                    let logger_clone = logger.clone();
                    Box::pin(async move {
                        logger_clone
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
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

    #[tokio::test]
    async fn test_run_with_some_data_srcs() {
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

            let logger_clone = logger.clone();
            assert!(runner
                .run_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
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

    #[tokio::test]
    async fn test_run_but_failed_to_begin() {
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

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .run_async(move |_| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
            {
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2 failed",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_run_but_failed_to_run_logic() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .run_async(|_| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic but failed".to_string());
                        Err(errs::Err::new("logic error".to_string()))
                    })
                })
                .await
            {
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "Run the logic but failed",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_start_with_no_data_src() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());

            let r = runner.start_async().await;
            assert!(r.end().is_ok());
        }

        assert_eq!(logger.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_start_with_some_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let runner = runner.start_async().await;
            assert!(runner.end().is_ok());
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_start_but_failed_to_begin() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            if let Err(err) = runner.start_async().await.end() {
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2 failed",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_with_no_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());

            let logger_clone = logger.clone();
            assert!(runner
                .txn_async(move |_data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
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

    #[tokio::test]
    async fn test_txn_with_some_data_srcs() {
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

            let logger_clone = logger.clone();
            assert!(runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "Run the logic",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::commit_async 2",
                "SyncDataConn::post_commit_async 1",
                "SyncDataConn::post_commit_async 2",
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

    #[tokio::test]
    async fn test_txn_but_failed_to_begin() {
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

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        let _conn3 = data.get_data_conn_async::<SyncDataConn>("baz").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
            {
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2 failed",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_but_failed_to_run_logic() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic but failed".to_string());
                        Err(errs::Err::new("logic error".to_string()))
                    })
                })
                .await
            {
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "Run the logic but failed",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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

    #[tokio::test]
    async fn test_txn_but_failed_to_pre_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PreCommit));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        let _conn3 = data.get_data_conn_async::<SyncDataConn>("baz").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
            {
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::setup_async 3",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn_async 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::pre_commit_async 2 failed",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::rollback_async 2",
                "SyncDataConn::rollback_async 3",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = 92 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = 92 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = 92 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::setup_async 3",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn_async 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::pre_commit_async 2 failed",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::rollback_async 2",
                "SyncDataConn::rollback_async 3",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\tokio\\_test_commons.rs, line = 92 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\tokio\\_test_commons.rs, line = 92 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\tokio\\_test_commons.rs, line = 92 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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

    #[tokio::test]
    async fn test_txn_but_failed_to_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Commit));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        let _conn3 = data.get_data_conn_async::<SyncDataConn>("baz").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
            {
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
            logger.lock().unwrap()[0..38],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::setup_async 3",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn_async 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::pre_commit_async 3",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::commit_async 2 failed",
                "SyncDataConn::rollback_async 2",
                "SyncDataConn::rollback_async 3",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
            logger.lock().unwrap()[0..38],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::setup_async 3",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn_async 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::pre_commit_async 3",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::commit_async 2 failed",
                "SyncDataConn::rollback_async 2",
                "SyncDataConn::rollback_async 3",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 67 }), rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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

    #[tokio::test]
    async fn test_txn_but_failed_to_post_commit() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PostCommit));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        let _conn3 = data.get_data_conn_async::<SyncDataConn>("baz").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
            {
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
            logger.lock().unwrap()[..],
            [
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::setup_async 3",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn_async 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::pre_commit_async 3",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::commit_async 2",
                "SyncDataConn::commit_async 3",
                "SyncDataConn::post_commit_async 1",
                "SyncDataConn::post_commit_async 2 failed",
                "SyncDataConn::post_commit_async 3",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 116 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 116 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure_async 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 116 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::setup_async 3",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn_async 3",
                "SyncDataConn::new 3",
                "Run the logic",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::pre_commit_async 3",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::commit_async 2",
                "SyncDataConn::commit_async 3",
                "SyncDataConn::post_commit_async 1",
                "SyncDataConn::post_commit_async 2 failed",
                "SyncDataConn::post_commit_async 3",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 116 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 116 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
                "SyncDataConn::on_txn_failure_async 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 116 }), rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }]",
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

    #[tokio::test]
    async fn test_txn_but_failed_to_rollback() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Rollback));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        let _conn3 = data.get_data_conn_async::<SyncDataConn>("baz").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic but failed".to_string());
                        Err(errs::Err::new("fail".to_string()))
                    })
                })
                .await
            {
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::setup_async 3",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn_async 3",
                "SyncDataConn::new 3",
                "Run the logic but failed",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::rollback_async 2 failed",
                "SyncDataConn::rollback_async 3",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 144 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 144 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 144 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::setup_async 3",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataSrc::create_data_conn_async 3",
                "SyncDataConn::new 3",
                "Run the logic but failed",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::rollback_async 2 failed",
                "SyncDataConn::rollback_async 3",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 144 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 144 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 3",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 144 }) }, TxnFailureReport { data_conn_name: \"baz\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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

    #[tokio::test]
    async fn test_begin_txn_with_no_data_src() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());

            let txn = runner.begin_txn_async().await;
            assert!(txn.end_txn_async().await.is_ok());
        }

        assert_eq!(logger.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_begin_txn_with_some_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let txn = runner.begin_txn_async().await;
            assert!(txn.end_txn_async().await.is_ok());
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_begin_txn_but_failed_to_begin() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));
            runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

            if let Err(err) = runner.begin_txn_async().await.end_txn_async().await {
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
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2 failed",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_txn_with_commit_order() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::with_commit_order(&["bar", "foo"]));

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 2);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 2);
            assert_eq!(runner.hub.run.fixed, false);

            let logger_clone = logger.clone();

            assert!(runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
                .is_ok());

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 2);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 2);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 2);
            assert_eq!(runner.hub.run.fixed, false);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "Run the logic",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::commit_async 2",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::post_commit_async 2",
                "SyncDataConn::post_commit_async 1",
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

    #[tokio::test]
    async fn test_get_data_conn_by_creating_but_failed_to_cast() {
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

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<AsyncDataConn>("bar").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
            {
                match err.reason::<DataSrcError>() {
                    Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(*name, "bar".into());
                        assert_eq!(*target_type, "sabi::tokio::_test_commons::AsyncDataConn");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }

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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[tokio::test]
    async fn test_get_data_conn_by_using_cache() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            struct Abc;
            let mut runner = LogicRunner::new(DataHub::<Abc>::with_commit_order(&["bar", "foo"]));

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 0);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 2);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 2);
            assert_eq!(runner.hub.run.fixed, false);

            let logger_clone = logger.clone();

            assert!(runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;

                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
                .is_ok());

            assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
            assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
            assert_eq!(runner.hub.run.local_data_src_manager.local, true);
            assert_eq!(runner.hub.run.data_src_map.len(), 2);
            assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 2);
            assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 2);
            assert_eq!(runner.hub.run.fixed, false);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "Run the logic",
                "SyncDataConn::pre_commit_async 2",
                "SyncDataConn::pre_commit_async 1",
                "SyncDataConn::commit_async 2",
                "SyncDataConn::commit_async 1",
                "SyncDataConn::post_commit_async 2",
                "SyncDataConn::post_commit_async 1",
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

    #[tokio::test]
    async fn test_get_data_conn_by_using_cache_but_failed_to_cast() {
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

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<SyncDataConn>("bar").await?;

                        let _conn2 = data.get_data_conn_async::<AsyncDataConn>("bar").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
            {
                match err.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(*name, "bar".into());
                        assert_eq!(*target_type, "sabi::tokio::_test_commons::AsyncDataConn");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }

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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataSrc::create_data_conn_async 2",
                "SyncDataConn::new 2",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::rollback_async 2",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::on_txn_failure_async 2",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
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

    #[tokio::test]
    async fn test_get_data_conn_but_no_corresponding_data_src() {
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

            let logger_clone = logger.clone();

            if let Err(err) = runner
                .txn_async(move |data| {
                    let logger_clone2 = logger_clone.clone();
                    Box::pin(async move {
                        let _conn1 = data.get_data_conn_async::<SyncDataConn>("foo").await?;
                        let _conn2 = data.get_data_conn_async::<AsyncDataConn>("xxx").await?;
                        logger_clone2
                            .lock()
                            .unwrap()
                            .push("Run the logic".to_string());
                        Ok(())
                    })
                })
                .await
            {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::NoDataSrcToCreateDataConn {
                        name,
                        data_conn_type,
                    }) => {
                        assert_eq!(*name, "xxx".into());
                        assert_eq!(*data_conn_type, "sabi::tokio::_test_commons::AsyncDataConn");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }

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
                "SyncDataSrc::setup_async 1",
                "SyncDataSrc::setup_async 2",
                "SyncDataSrc::create_data_conn_async 1",
                "SyncDataConn::new 1",
                "SyncDataConn::rollback_async 1",
                "SyncDataConn::on_txn_failure_async 1",
                "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    mod test_async_data_acc_aggregation_to_data_hub {
        use super::*;
        use crate::tokio::DataAcc;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_async_data_acc_aggregation_to_data_hub)]
        trait GettingDataAcc: DataAcc {
            async fn get_text_async(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn_async::<SyncDataConn>("foo").await?;
                // getting a text by using _conn
                Ok("hello".to_string())
            }
        }

        #[overridable(mod = test_async_data_acc_aggregation_to_data_hub)]
        trait SettingDataAcc: DataAcc {
            async fn set_text_async(&mut self, _text: String) -> errs::Result<()> {
                let _conn = self.get_data_conn_async::<SyncDataConn>("bar").await?;
                // setting a text by using _conn
                Ok(())
            }
        }

        struct Hoge;
        impl test_async_data_acc_aggregation_to_data_hub::GettingDataAcc for DataHub<Hoge> {}
        impl test_async_data_acc_aggregation_to_data_hub::SettingDataAcc for DataHub<Hoge> {}

        #[overridable(mod = test_async_data_acc_aggregation_to_data_hub)]
        trait HogeData {
            async fn get_text_async(&mut self) -> errs::Result<String>;
            async fn set_text_async(&mut self, text: String) -> errs::Result<()>;
        }

        #[override_with(
            test_async_data_acc_aggregation_to_data_hub::GettingDataAcc,
            test_async_data_acc_aggregation_to_data_hub::SettingDataAcc
        )]
        impl test_async_data_acc_aggregation_to_data_hub::HogeData for DataHub<Hoge> {}

        async fn hoge_logic_async(data: &mut impl HogeData) -> errs::Result<()> {
            let mut text = data.get_text_async().await?;
            text = text.to_uppercase();
            data.set_text_async(text).await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_run() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut runner = LogicRunner::new(DataHub::<Hoge>::new());
                runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
                if let Err(err) = runner.run_async(_logic!(hoge_logic_async)).await {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn_async 2",
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

        #[tokio::test]
        async fn test_txn() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut runner = LogicRunner::new(DataHub::<Hoge>::new());
                runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
                if let Err(err) = runner.txn_async(_logic!(hoge_logic_async)).await {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit_async 1",
                    "SyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "SyncDataConn::commit_async 2",
                    "SyncDataConn::post_commit_async 1",
                    "SyncDataConn::post_commit_async 2",
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

    struct A;

    trait Data {}
    impl Data for DataHub<A> {}

    async fn process_async(_data: &mut impl Data) -> errs::Result<()> {
        Ok(())
    }

    #[tokio::test]
    async fn data_hub_implements_send_trait() {
        let handle = tokio::spawn(async {
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.run_async(_logic!(process_async)).await.unwrap();
        });

        handle.await.unwrap();
    }

    #[tokio::test]
    async fn txn_async_in_spawn() {
        let handle = tokio::spawn(async {
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.txn_async(_logic!(process_async)).await.unwrap();
        });

        handle.await.unwrap();
    }
}
