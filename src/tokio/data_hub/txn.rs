// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{
    DataConn, DataHub, DataHubError, DataSrc, ErrEntry, Runner, RunnerErrAt, Txn, TxnDataHub,
};

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;

impl TxnDataHub {
    pub(crate) fn new(hub: DataHub) -> Self {
        Self { hub }
    }

    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C> + 'static,
        C: DataConn + 'static,
    {
        self.hub.uses(name, ds)
    }

    pub fn disuses(&mut self, name: impl AsRef<str>) {
        self.hub.disuses(name)
    }

    pub async fn run_async<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        for<'a> F:
            FnMut(&'a mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'a>>,
    {
        self.hub.run_async(logic_fn).await
    }

    pub async fn start_async(&mut self) -> Runner<'_> {
        self.hub.start_async().await
    }

    pub async fn txn_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'a> F:
            FnMut(&'a mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'a>>,
    {
        let mut r = self.hub.begin_async().await;
        if r.is_ok() {
            r = logic_fn(&mut self.hub).await;
        }

        let mut reports = self.hub.new_failure_reports();

        if r.is_ok() {
            r = self.hub.commit_async(&mut reports).await;
        }
        if r.is_err() {
            self.hub.rollback_async(reports).await;
        }

        self.hub.end();
        r
    }

    pub async fn begin_txn_async(&mut self) -> Txn<'_> {
        Txn::new_async(self).await
    }
}

impl<'a> Txn<'a> {
    pub(crate) async fn new_async(txn_hub: &'a mut TxnDataHub) -> Txn<'a> {
        if let Err(err) = txn_hub.hub.begin_async().await {
            Self {
                hub: &mut txn_hub.hub,
                err: RunnerErrAt::Begin { err },
                index: 0,
            }
        } else {
            Self {
                hub: &mut txn_hub.hub,
                err: RunnerErrAt::Run {
                    errors: Vec::with_capacity(0),
                },
                index: 0,
            }
        }
    }

    pub async fn run_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F:
            FnMut(&'b mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            RunnerErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("Txn#run_async(logic-{})", index).into(),
                            err,
                        });
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub async fn run_force_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F:
            FnMut(&'b mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            RunnerErrAt::Run { ref mut errors } => {
                if let Err(err) = logic_fn(self.hub).await {
                    errors.push(ErrEntry {
                        index,
                        name: format!("Txn#run_force_async(logic-{})", index).into(),
                        err,
                    });
                }
                self
            }
            _ => self,
        }
    }

    pub async fn run_or_block_async<F>(mut self, mut logic_fn: F) -> Self
    where
        for<'b> F:
            FnMut(&'b mut DataHub) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            RunnerErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("Txn#run_or_block_async(logic-{})", index).into(),
                            err,
                        });
                        self.err = RunnerErrAt::Block {
                            errors: mem::take(errors),
                        };
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub async fn end_txn_async(self) -> errs::Result<()> {
        match self.err {
            RunnerErrAt::Begin { err } => {
                self.hub.end();
                Err(err)
            }
            RunnerErrAt::Run { errors } => {
                let mut reports = self.hub.new_failure_reports();
                if errors.is_empty() {
                    let result = self.hub.commit_async(&mut reports).await;
                    if result.is_err() {
                        self.hub.rollback_async(reports).await;
                    }
                    self.hub.end();
                    result
                } else {
                    self.hub.rollback_async(reports).await;
                    self.hub.end();
                    Err(errs::Err::new(DataHubError::FailToRunLogics { errors }))
                }
            }
            RunnerErrAt::Block { errors } => {
                let reports = self.hub.new_failure_reports();
                self.hub.rollback_async(reports).await;
                self.hub.end();
                Err(errs::Err::new(DataHubError::FailToRunLogics { errors }))
            }
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_txn_data_hub {
    use super::*;
    use crate::tokio::_test_commons::*;
    use crate::tokio::{logic, DataAcc, DataConnError};
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_disuses() {
        let logger = Arc::new(Mutex::new(Vec::new()));
        let mut data = DataHub::new().for_txn();
        data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(data.hub.local_data_src_manager.vec_unready.len(), 2);
        data.disuses("foo");
        assert_eq!(data.hub.local_data_src_manager.vec_unready.len(), 1);
        data.disuses("bar");
        assert_eq!(data.hub.local_data_src_manager.vec_unready.len(), 0);
    }

    mod test_async_ok {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_async_ok)]
        trait HogeData {
            async fn process_async(&mut self) -> errs::Result<()>;
        }

        #[overridable(mod = test_async_ok)]
        trait FugaData {
            async fn get_value_async(&mut self) -> errs::Result<String>;
            async fn set_value_async(&mut self, v: &str) -> errs::Result<()>;
        }

        async fn hoge_logic_async(data: &mut (impl HogeData + 'static)) -> errs::Result<()> {
            data.process_async().await?;
            Ok(())
        }

        async fn fuga_logic_async(data: &mut (impl FugaData + 'static)) -> errs::Result<()> {
            let v = data.get_value_async().await?;
            let _ = data.set_value_async(&v).await;
            Ok(())
        }

        #[overridable(mod = test_async_ok)]
        trait FooDataAcc: DataAcc {
            async fn get_value_async(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn_async::<SyncDataConn>("foo").await?;
                Ok("hello".to_string())
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_async_ok)]
        trait BarDataAcc: DataAcc {
            async fn set_value_async(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn_async::<SyncDataConn>("bar").await?;
                assert_eq!(text, "hello");
                Ok(())
            }
        }
        impl BarDataAcc for DataHub {}

        #[overridable(mod = test_async_ok)]
        trait BazDataAcc: DataAcc {
            async fn process_async(&mut self) -> errs::Result<()> {
                self.run_async(logic!(fuga_logic_async)).await?;
                Ok(())
            }
        }
        impl BazDataAcc for DataHub {}

        #[override_with(
            test_async_ok::FooDataAcc,
            test_async_ok::BarDataAcc,
            test_async_ok::BazDataAcc
        )]
        impl test_async_ok::HogeData for DataHub {}

        #[override_with(
            test_async_ok::FooDataAcc,
            test_async_ok::BarDataAcc,
            test_async_ok::BazDataAcc
        )]
        impl test_async_ok::FugaData for DataHub {}

        #[tokio::test]
        async fn test_run_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.run_async(logic!(fuga_logic_async)).await {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_run_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.run_async(logic!(fuga_logic_async)).await {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_start() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data
                    .start_async()
                    .await
                    .run_async(logic!(fuga_logic_async))
                    .await
                    .end()
                {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_start_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data
                    .start_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .end()
                {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.txn_async(logic!(fuga_logic_async)).await {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.txn_async(logic!(hoge_logic_async)).await {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_begin_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(fuga_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_begin_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }
    }

    mod test_fail_to_run_async {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_fail_to_run_async)]
        trait HogeData {
            async fn process_async(&mut self) -> errs::Result<()>;
        }

        #[overridable(mod = test_fail_to_run_async)]
        trait FugaData {
            async fn get_value_async(&mut self) -> errs::Result<String>;
            async fn set_value_async(&mut self, v: &str) -> errs::Result<()>;
        }

        async fn hoge_logic_async(data: &mut impl HogeData) -> errs::Result<()> {
            data.process_async().await?;
            Ok(())
        }

        async fn fuga_logic_async(data: &mut impl FugaData) -> errs::Result<()> {
            let v = data.get_value_async().await?;
            let _ = data.set_value_async(&v).await;
            Err(errs::Err::new("fail"))
        }

        #[overridable(mod = test_fail_to_run_async)]
        trait FooDataAcc: DataAcc {
            async fn get_value_async(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn_async::<SyncDataConn>("foo").await?;
                Ok("hello".to_string())
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_fail_to_run_async)]
        trait BarDataAcc: DataAcc {
            async fn set_value_async(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn_async::<SyncDataConn>("bar").await?;
                assert_eq!(text, "hello");
                Ok(())
            }
        }
        impl BarDataAcc for DataHub {}

        #[overridable(mod = test_fail_to_run_async)]
        trait BazDataAcc: DataAcc {
            async fn process_async(&mut self) -> errs::Result<()> {
                self.run_async(logic!(fuga_logic_async)).await?;
                Ok(())
            }
        }
        impl BazDataAcc for DataHub {}

        #[override_with(
            test_fail_to_run_async::FooDataAcc,
            test_fail_to_run_async::BarDataAcc,
            test_fail_to_run_async::BazDataAcc
        )]
        impl test_fail_to_run_async::HogeData for DataHub {}

        #[override_with(
            test_fail_to_run_async::FooDataAcc,
            test_fail_to_run_async::BarDataAcc,
            test_fail_to_run_async::BazDataAcc
        )]
        impl test_fail_to_run_async::FugaData for DataHub {}

        #[tokio::test]
        async fn test_start() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .start_async()
                    .await
                    .run_async(logic!(fuga_logic_async))
                    .await
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Runner#run_async(logic-0)".into());
                            assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"fail");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_start_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .start_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Runner#run_async(logic-0)".into());
                            assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"fail");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_txn_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.txn_async(logic!(fuga_logic_async)).await {
                    assert_eq!(err.reason::<&str>().unwrap(), &"fail");
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.txn_async(logic!(hoge_logic_async)).await {
                    assert_eq!(err.reason::<&str>().unwrap(), &"fail");
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_begin_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(fuga_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run_async(logic-0)".into());
                            assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"fail");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }

        #[tokio::test]
        async fn test_begin_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run_async(logic-0)".into());
                            assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"fail");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }
    }

    mod test_runner_and_txn_async {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_runner_and_txn_async)]
        trait HogeData {
            async fn hoge_log_async(&mut self, s: &str) -> errs::Result<()>;
        }

        #[overridable(mod = test_runner_and_txn_async)]
        trait FugaData {
            async fn fuga_log_async(&mut self, s: &str) -> errs::Result<()>;
        }

        #[overridable(mod = test_runner_and_txn_async)]
        trait PiyoData {
            async fn piyo_log_async(&mut self, s: &str) -> errs::Result<()>;
        }

        #[overridable(mod = test_runner_and_txn_async)]
        trait HogeraData {
            async fn hogera_log_async(&mut self, s: &str) -> errs::Result<()>;
        }

        async fn hoge_logic_async(data: &mut impl HogeData) -> errs::Result<()> {
            data.hoge_log_async("Hoge").await
        }

        async fn fuga_logic_async(data: &mut impl FugaData) -> errs::Result<()> {
            data.fuga_log_async("Fuga").await
        }

        async fn piyo_logic_async(data: &mut impl PiyoData) -> errs::Result<()> {
            data.piyo_log_async("Piyo").await
        }

        async fn hogera_logic_async(data: &mut impl HogeraData) -> errs::Result<()> {
            data.hogera_log_async("Hogera").await
        }

        #[overridable(mod = test_runner_and_txn_async)]
        trait FooDataAcc: DataAcc {
            async fn hoge_log_async(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<SyncDataConn>("foo").await?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("foo/hoge/{}", s));
                Ok(())
            }
            async fn hogera_log_async(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<SyncDataConn>("foo").await?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("foo/hogera/{}", s));
                Err(errs::Err::new("hogera_logic_async failed"))
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_runner_and_txn_async)]
        trait BarDataAcc: DataAcc {
            async fn fuga_log_async(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<SyncDataConn>("bar").await?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("bar/fuga/{}", s));
                Ok(())
            }
            async fn piyo_log_async(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<SyncDataConn>("bar").await?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("bar/piyo/{}", s));
                Ok(())
            }
        }
        impl BarDataAcc for DataHub {}

        #[override_with(
            test_runner_and_txn_async::FooDataAcc,
            test_runner_and_txn_async::BarDataAcc
        )]
        impl test_runner_and_txn_async::HogeData for DataHub {}

        #[override_with(
            test_runner_and_txn_async::FooDataAcc,
            test_runner_and_txn_async::BarDataAcc
        )]
        impl test_runner_and_txn_async::FugaData for DataHub {}

        #[override_with(
            test_runner_and_txn_async::FooDataAcc,
            test_runner_and_txn_async::BarDataAcc
        )]
        impl test_runner_and_txn_async::PiyoData for DataHub {}

        #[override_with(
            test_runner_and_txn_async::FooDataAcc,
            test_runner_and_txn_async::BarDataAcc
        )]
        impl test_runner_and_txn_async::HogeraData for DataHub {}

        #[tokio::test]
        async fn runner_async_ok() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .start_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end()
                {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn runner_fail_to_start() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));

                if let Err(err) = data
                    .start_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), &"XXX");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2 failed",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn runner_fail_to_run() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .start_async()
                    .await
                    .run_async(logic!(hogera_logic_async))
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Runner#run_async(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic_async failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn runner_fail_to_run_force() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .start_async()
                    .await
                    .run_force_async(logic!(hogera_logic_async))
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Runner#run_force_async(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic_async failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn runner_fail_to_run_or_block() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .start_async()
                    .await
                    .run_or_block_async(logic!(hogera_logic_async))
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Runner#run_or_block_async(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic_async failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_async_ok() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
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
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_begin_txn_async() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), &"XXX");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2 failed",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_run_async() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(hogera_logic_async))
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run_async(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic_async failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
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
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_run_force_async() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_force_async(logic!(hogera_logic_async))
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run_force_async(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic_async failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
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
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_run_or_block_async() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_or_block_async(logic!(hogera_logic_async))
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run_or_block_async(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic_async failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataConn::rollback_async 1",
                    "SyncDataConn::on_txn_failure_async 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_begin_async() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));

                if let Err(err) = data.txn_async(logic!(hoge_logic_async)).await {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), &"XXX");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2 failed",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_commit_async() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Commit));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end_txn_async()
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
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit_async 1",
                    "SyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "SyncDataConn::commit_async 2 failed",
                    "SyncDataConn::rollback_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 61 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure_async 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/tokio/_test_commons.rs, line = 61 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit_async 1",
                    "SyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "SyncDataConn::commit_async 2 failed",
                    "SyncDataConn::rollback_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = 61 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure_async 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\tokio\\_test_commons.rs, line = 61 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_pre_commit_async() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PreCommit));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end_txn_async()
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
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit_async 1",
                    "SyncDataConn::pre_commit_async 2 failed",
                    "SyncDataConn::rollback_async 1",
                    "SyncDataConn::rollback_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = 80 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure_async 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/tokio/_test_commons.rs, line = 80 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit_async 1",
                    "SyncDataConn::pre_commit_async 2 failed",
                    "SyncDataConn::rollback_async 1",
                    "SyncDataConn::rollback_async 2",
                    "SyncDataConn::on_txn_failure_async 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\tokio\\_test_commons.rs, line = 80 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure_async 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\tokio\\_test_commons.rs, line = 80 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_post_commit_async() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PostCommit));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(fuga_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end_txn_async()
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
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit_async 1",
                    "SyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "SyncDataConn::commit_async 2",
                    "SyncDataConn::post_commit_async 1",
                    "SyncDataConn::post_commit_async 2 failed",
                    "SyncDataConn::on_txn_failure_async 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 98 }), rollback: NoneByNotRolledBack }]",
                    "SyncDataConn::on_txn_failure_async 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 98 }), rollback: NoneByNotRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit_async 1",
                    "SyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "SyncDataConn::commit_async 2",
                    "SyncDataConn::post_commit_async 1",
                    "SyncDataConn::post_commit_async 2 failed",
                    "SyncDataConn::on_txn_failure_async 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 98 }), rollback: NoneByNotRolledBack }]",
                    "SyncDataConn::on_txn_failure_async 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 98 }), rollback: NoneByNotRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_rollback_async() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::new().for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Rollback));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .run_force_async(logic!(hogera_logic_async))
                    .await
                    .run_or_block_async(logic!(piyo_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "Txn#run_force_async(logic-1)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic_async failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "foo/hogera/Hogera",
                    "SyncDataConn::rollback_async 1 failed",
                    "SyncDataConn::on_txn_failure_async 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/tokio/_test_commons.rs, line = 120 }) }]",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "foo/hogera/Hogera",
                    "SyncDataConn::rollback_async 1 failed",
                    "SyncDataConn::on_txn_failure_async 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::tokio::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\tokio\\_test_commons.rs, line = 120 }) }]",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
        }
    }
}
