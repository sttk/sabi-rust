// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::{DataConn, DataHub, DataSrc, Runner, TxnDataHub};
use crate::ErrEntry;

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug)]
pub enum TxnError {
    FailToRunLogics { errors: Vec<ErrEntry> },
}

impl TxnDataHub {
    pub fn new(hub: DataHub) -> Self {
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

enum TxnErrAt {
    Begin { err: errs::Err },
    Run { errors: Vec<ErrEntry> },
    Block { errors: Vec<ErrEntry> },
}

pub struct Txn<'a> {
    hub: &'a mut DataHub,
    err: TxnErrAt,
    index: usize,
}

impl<'a> Txn<'a> {
    pub async fn new_async(txn_hub: &'a mut TxnDataHub) -> Txn<'a> {
        if let Err(err) = txn_hub.hub.begin_async().await {
            Self {
                hub: &mut txn_hub.hub,
                err: TxnErrAt::Begin { err },
                index: 0,
            }
        } else {
            Self {
                hub: &mut txn_hub.hub,
                err: TxnErrAt::Run {
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
            TxnErrAt::Run { ref mut errors } => {
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
            TxnErrAt::Run { ref mut errors } => {
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
            TxnErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub).await {
                        errors.push(ErrEntry {
                            index,
                            name: format!("Txn#run_or_block_async(logic-{})", index).into(),
                            err,
                        });
                        self.err = TxnErrAt::Block {
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
            TxnErrAt::Begin { err } => {
                self.hub.end();
                Err(err)
            }
            TxnErrAt::Run { errors } => {
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
                    Err(errs::Err::new(TxnError::FailToRunLogics { errors }))
                }
            }
            TxnErrAt::Block { errors } => {
                let reports = self.hub.new_failure_reports();
                self.hub.rollback_async(reports).await;
                self.hub.end();
                Err(errs::Err::new(TxnError::FailToRunLogics { errors }))
            }
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_txn_data_hub {
    use super::super::{logic, AsyncGroup, DataAcc};
    use super::*;
    use crate::TxnFailureReport;
    use std::sync::{Arc, Mutex};

    #[derive(PartialEq, Clone, Copy)]
    enum Failure {
        None,
        Setup,
        PreCommit,
        Commit,
        PostCommit,
        Rollback,
    }

    struct FooDataConn {
        id: i8,
        text: String,
        committed: bool,
        failure: Failure,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl FooDataConn {
        fn new(id: i8, s: &str, f: Failure, logger: Arc<Mutex<Vec<String>>>) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("FooDataConn::new {}", id));
            }
            Self {
                id,
                text: s.to_string(),
                logger,
                committed: false,
                failure: f,
            }
        }
        async fn get_text_async(&self) -> String {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::get_text_async {}", self.id));
            self.text.clone()
        }
    }
    impl Drop for FooDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::drop {}", self.id));
        }
    }
    impl DataConn for FooDataConn {
        async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            if self.failure == Failure::Commit {
                logger.push(format!("FooDataConn::commit_async failed {}", self.id));
                return Err(errs::Err::new("commit_async error"));
            }
            self.committed = true;
            logger.push(format!("FooDataConn::commit_async {}", self.id));
            Ok(())
        }
        async fn pre_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            if self.failure == Failure::PreCommit {
                logger.push(format!("FooDataConn::pre_commit_async failed {}", self.id));
                return Err(errs::Err::new("pre_commit_async error"));
            }
            logger.push(format!("FooDataConn::pre_commit_async {}", self.id));
            Ok(())
        }
        async fn post_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            if self.failure == Failure::PostCommit {
                logger.push(format!("FooDataConn::post_commit_async failed {}", self.id));
                return Err(errs::Err::new("post_commit_async error"));
            }
            logger.push(format!("FooDataConn::post_commit_async {}", self.id));
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed
        }
        async fn rollback_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            if self.failure == Failure::Rollback {
                logger.push(format!("FooDataConn::rollback_async failed {}", self.id));
                return Err(errs::Err::new("rollback_async error"));
            }
            logger.push(format!("FooDataConn::rollback_async {}", self.id));
            Ok(())
        }
        async fn on_txn_failure_async(
            &mut self,
            _ag: &mut AsyncGroup,
            _reports: Arc<[TxnFailureReport]>,
        ) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::on_txn_failure_async {}", self.id));
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::close {}", self.id));
        }
    }

    struct FooDataSrc {
        id: i8,
        failure: Failure,
        text: String,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl FooDataSrc {
        fn new(id: i8, s: &str, f: Failure, logger: Arc<Mutex<Vec<String>>>) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("FooDataSrc::new {}", id));
            }
            Self {
                id,
                logger,
                failure: f,
                text: s.to_string(),
            }
        }
    }
    impl Drop for FooDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<FooDataConn> for FooDataSrc {
        async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::Setup {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("FooDataSrc::setup_async {} failed", self.id));
                }
                return Err(errs::Err::new("XXX".to_string()));
            }
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("FooDataSrc::setup_async {}", self.id));
            }
            Ok(())
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataSrc::close {}", self.id));
        }
        async fn create_data_conn_async(&mut self) -> errs::Result<Box<FooDataConn>> {
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("FooDataSrc::create_data_src_async {}", self.id));
            }
            let conn = FooDataConn::new(self.id, &self.text, self.failure, self.logger.clone());
            Ok(Box::new(conn))
        }
    }

    struct BarDataConn {
        id: i8,
        text: Option<String>,
        ds_text: Arc<Mutex<String>>,
        committed: bool,
        failure: Failure,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl BarDataConn {
        fn new(
            id: i8,
            ds_text: Arc<Mutex<String>>,
            f: Failure,
            logger: Arc<Mutex<Vec<String>>>,
        ) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("BarDataConn::new {}", id));
            }
            Self {
                id,
                text: None,
                ds_text,
                logger,
                committed: false,
                failure: f,
            }
        }
        async fn set_text_async(&mut self, s: &str) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::set_text_async {}", self.id));
            self.text = Some(s.to_string());
        }
    }
    impl Drop for BarDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::drop {}", self.id));
        }
    }
    impl DataConn for BarDataConn {
        async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::Commit {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("BarDataConn::commit failed {}", self.id));
                return Err(errs::Err::new("commit error"));
            }
            self.committed = true;
            match &self.text {
                Some(s) => {
                    *self.ds_text.lock().unwrap() = s.to_string();
                }
                None => {
                    *self.ds_text.lock().unwrap() = "".to_string();
                }
            }
            self.logger
                .lock()
                .unwrap()
                .push(format!("BarDataConn::commit_async {}", self.id));
            Ok(())
        }
        async fn pre_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::pre_commit_async {}", self.id));
            Ok(())
        }
        async fn post_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::post_commit_async {}", self.id));
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed
        }
        async fn rollback_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::rollback_async {}", self.id));
            Ok(())
        }
        async fn on_txn_failure_async(
            &mut self,
            _ag: &mut AsyncGroup,
            _reports: Arc<[TxnFailureReport]>,
        ) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::on_txn_failure_async {}", self.id));
        }
        fn close(&mut self) {
            let text = self.text.clone().unwrap_or("".to_string());
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn.text = {}", text));
            logger.push(format!("BarDataConn::close {}", self.id));
        }
    }

    struct BarDataSrc {
        id: i8,
        text: Arc<Mutex<String>>,
        failure: Failure,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl BarDataSrc {
        fn new(id: i8, f: Failure, logger: Arc<Mutex<Vec<String>>>) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("BarDataSrc::new {}", id));
            }
            Self {
                id,
                text: Arc::new(Mutex::new(String::new())),
                failure: f,
                logger,
            }
        }
    }
    impl Drop for BarDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<BarDataConn> for BarDataSrc {
        async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::Setup {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("BarDataSrc::setup_async {} failed", self.id));
                }
                return Err(errs::Err::new("xxx".to_string()));
            }
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc::setup_async {}", self.id));
            Ok(())
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            let text = self.text.lock().unwrap();
            logger.push(format!("BarDataSrc.text = {}", text));
            logger.push(format!("BarDataSrc::close {}", self.id));
        }
        async fn create_data_conn_async(&mut self) -> errs::Result<Box<BarDataConn>> {
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("BarDataSrc::create_data_src_async {}", self.id));
            }
            let conn = BarDataConn::new(
                self.id,
                self.text.clone(),
                self.failure,
                self.logger.clone(),
            );
            Ok(Box::new(conn))
        }
    }

    #[test]
    fn test_disuses() {
        let logger = Arc::new(Mutex::new(Vec::new()));
        let mut data = TxnDataHub::new(DataHub::new());
        data.uses(
            "foo",
            FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
        );
        data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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

        async fn hoge_logic_async(data: &mut impl HogeData) -> errs::Result<()> {
            data.process_async().await?;
            Ok(())
        }

        async fn fuga_logic_async(data: &mut impl FugaData) -> errs::Result<()> {
            let v = data.get_value_async().await?;
            let _ = data.set_value_async(&v).await;
            Ok(())
        }

        #[overridable(mod = test_async_ok)]
        trait FooDataAcc: DataAcc {
            async fn get_value_async(&mut self) -> errs::Result<String> {
                let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
                Ok(conn.get_text_async().await)
            }
        }
        impl test_async_ok::FooDataAcc for DataHub {}

        #[overridable(mod = test_async_ok)]
        trait BarDataAcc: DataAcc {
            async fn set_value_async(&mut self, text: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<BarDataConn>("bar").await?;
                conn.set_text_async(text).await;
                Ok(())
            }
        }
        impl test_async_ok::BarDataAcc for DataHub {}

        #[overridable(mod = test_async_ok)]
        trait BazDataAcc: DataAcc {
            async fn process_async(&mut self) -> errs::Result<()> {
                self.run_async(logic!(fuga_logic_async)).await?;
                Ok(())
            }
        }
        impl test_async_ok::BazDataAcc for DataHub {}

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
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.run_async(logic!(fuga_logic_async)).await {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                logger.lock().unwrap()[..],
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_run_async_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.run_async(logic!(fuga_logic_async)).await {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_start_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data
                    .start_async()
                    .await
                    .run_async(logic!(fuga_logic_async))
                    .await
                    .end()
                {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_start_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data
                    .start_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .end()
                {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_txn_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.txn_async(logic!(fuga_logic_async)).await {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                logger.lock().unwrap()[..],
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::pre_commit_async 1",
                    "BarDataConn::pre_commit_async 2",
                    "FooDataConn::commit_async 1",
                    "BarDataConn::commit_async 2",
                    "FooDataConn::post_commit_async 1",
                    "BarDataConn::post_commit_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = hello", // because committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_txn_async_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.txn_async(logic!(hoge_logic_async)).await {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::pre_commit_async 1",
                    "BarDataConn::pre_commit_async 2",
                    "FooDataConn::commit_async 1",
                    "BarDataConn::commit_async 2",
                    "FooDataConn::post_commit_async 1",
                    "BarDataConn::post_commit_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = hello", // because committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_begin_txn_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::pre_commit_async 1",
                    "BarDataConn::pre_commit_async 2",
                    "FooDataConn::commit_async 1",
                    "BarDataConn::commit_async 2",
                    "FooDataConn::post_commit_async 1",
                    "BarDataConn::post_commit_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = hello", // because committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_begin_txn_async_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::pre_commit_async 1",
                    "BarDataConn::pre_commit_async 2",
                    "FooDataConn::commit_async 1",
                    "BarDataConn::commit_async 2",
                    "FooDataConn::post_commit_async 1",
                    "BarDataConn::post_commit_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = hello", // because committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }
    }

    mod test_fail_to_run_async {
        use super::*;
        use crate::tokio::DataHubError;
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
                let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
                Ok(conn.get_text_async().await)
            }
        }
        impl test_fail_to_run_async::FooDataAcc for DataHub {}

        #[overridable(mod = test_fail_to_run_async)]
        trait BarDataAcc: DataAcc {
            async fn set_value_async(&mut self, text: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<BarDataConn>("bar").await?;
                conn.set_text_async(text).await;
                Ok(())
            }
        }
        impl test_fail_to_run_async::BarDataAcc for DataHub {}

        #[overridable(mod = test_fail_to_run_async)]
        trait BazDataAcc: DataAcc {
            async fn process_async(&mut self) -> errs::Result<()> {
                self.run_async(logic!(fuga_logic_async)).await?;
                Ok(())
            }
        }
        impl test_fail_to_run_async::BazDataAcc for DataHub {}

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
        async fn test_run_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.run_async(logic!(fuga_logic_async)).await {
                    match err.reason::<&str>() {
                        Ok(s) => assert_eq!(s, &"fail"),
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_run_async_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.run_async(logic!(hoge_logic_async)).await {
                    match err.reason::<&str>() {
                        Ok(s) => assert_eq!(s, &"fail"),
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_start_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_start_async_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_txn_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.txn_async(logic!(fuga_logic_async)).await {
                    assert_eq!(err.reason::<&str>().unwrap(), &"fail");
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::rollback_async 1",
                    "BarDataConn::rollback_async 2",
                    "FooDataConn::on_txn_failure_async 1",
                    "BarDataConn::on_txn_failure_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_txn_async_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.txn_async(logic!(hoge_logic_async)).await {
                    assert_eq!(err.reason::<&str>().unwrap(), &"fail");
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::rollback_async 1",
                    "BarDataConn::rollback_async 2",
                    "FooDataConn::on_txn_failure_async 1",
                    "BarDataConn::on_txn_failure_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_begin_txn_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(fuga_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    match err.reason::<TxnError>() {
                        Ok(TxnError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::rollback_async 1",
                    "BarDataConn::rollback_async 2",
                    "FooDataConn::on_txn_failure_async 1",
                    "BarDataConn::on_txn_failure_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn test_begin_txn_async_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data
                    .begin_txn_async()
                    .await
                    .run_async(logic!(hoge_logic_async))
                    .await
                    .end_txn_async()
                    .await
                {
                    match err.reason::<TxnError>() {
                        Ok(TxnError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::rollback_async 1",
                    "BarDataConn::rollback_async 2",
                    "FooDataConn::on_txn_failure_async 1",
                    "BarDataConn::on_txn_failure_async 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }
    }

    mod test_runner_and_txn_for_async {
        use super::*;
        use crate::tokio::DataHubError;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_runner_and_txn_for_async)]
        trait HogeData {
            async fn hoge_log_async(&mut self, s: &str) -> errs::Result<()>;
        }

        #[overridable(mod = test_runner_and_txn_for_async)]
        trait FugaData {
            async fn fuga_log_async(&mut self, s: &str) -> errs::Result<()>;
        }

        #[overridable(mod = test_runner_and_txn_for_async)]
        trait PiyoData {
            async fn piyo_log_async(&mut self, s: &str) -> errs::Result<()>;
        }

        #[overridable(mod = test_runner_and_txn_for_async)]
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

        #[overridable(mod = test_runner_and_txn_for_async)]
        trait FooDataAcc: DataAcc {
            async fn hoge_log_async(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("foo/hoge/{}", s));
                Ok(())
            }
            async fn hogera_log_async(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("foo/hogera/{}", s));
                Err(errs::Err::new("hogera_logic failed"))
            }
        }
        impl test_runner_and_txn_for_async::FooDataAcc for DataHub {}

        #[overridable(mod = test_runner_and_txn_for_async)]
        trait BarDataAcc: DataAcc {
            async fn fuga_log_async(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<BarDataConn>("bar").await?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("bar/fuga/{}", s));
                Ok(())
            }
            async fn piyo_log_async(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<BarDataConn>("bar").await?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("bar/piyo/{}", s));
                Ok(())
            }
        }
        impl test_runner_and_txn_for_async::BarDataAcc for DataHub {}

        #[override_with(
            test_runner_and_txn_for_async::FooDataAcc,
            test_runner_and_txn_for_async::BarDataAcc
        )]
        impl test_runner_and_txn_for_async::HogeData for DataHub {}

        #[override_with(
            test_runner_and_txn_for_async::FooDataAcc,
            test_runner_and_txn_for_async::BarDataAcc
        )]
        impl test_runner_and_txn_for_async::FugaData for DataHub {}

        #[override_with(
            test_runner_and_txn_for_async::FooDataAcc,
            test_runner_and_txn_for_async::BarDataAcc
        )]
        impl test_runner_and_txn_for_async::PiyoData for DataHub {}

        #[override_with(
            test_runner_and_txn_for_async::FooDataAcc,
            test_runner_and_txn_for_async::BarDataAcc
        )]
        impl test_runner_and_txn_for_async::HogeraData for DataHub {}

        #[tokio::test]
        async fn runner_ok() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "foo/hoge/Hoge",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn runner_fail_to_run() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                                &"hogera_logic failed"
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn runner_fail_to_run_force() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                                &"hogera_logic failed"
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn runner_fail_to_run_or_block() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                                &"hogera_logic failed"
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_ok() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "foo/hoge/Hoge",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "FooDataConn::pre_commit_async 1",
                    "BarDataConn::pre_commit_async 2",
                    "FooDataConn::commit_async 1",
                    "BarDataConn::commit_async 2",
                    "FooDataConn::post_commit_async 1",
                    "BarDataConn::post_commit_async 2",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_begin_txn() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::Setup, logger.clone()));

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
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), &"xxx");
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2 failed",
                    "FooDataSrc::close 1",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_run() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    match err.reason::<TxnError>() {
                        Ok(TxnError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run_async(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic failed"
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "FooDataConn::rollback_async 1",
                    "BarDataConn::rollback_async 2",
                    "FooDataConn::on_txn_failure_async 1",
                    "BarDataConn::on_txn_failure_async 2",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_run_force() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    match err.reason::<TxnError>() {
                        Ok(TxnError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run_force_async(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic failed"
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "FooDataConn::rollback_async 1",
                    "BarDataConn::rollback_async 2",
                    "FooDataConn::on_txn_failure_async 1",
                    "BarDataConn::on_txn_failure_async 2",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_run_or_block() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    match err.reason::<TxnError>() {
                        Ok(TxnError::FailToRunLogics { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run_or_block_async(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic failed"
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "FooDataConn::rollback_async 1",
                    "FooDataConn::on_txn_failure_async 1",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_begin() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::Setup, logger.clone()));

                if let Err(err) = data.txn_async(logic!(hoge_logic_async)).await {
                    match err.reason::<crate::tokio::DataHubError>() {
                        Ok(crate::tokio::DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), &"xxx");
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2 failed",
                    "FooDataSrc::close 1",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[tokio::test]
        async fn txn_fail_to_commit() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::Commit, logger.clone()));

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
                    match err.reason::<crate::tokio::DataConnError>() {
                        Ok(crate::tokio::DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"commit error");
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "foo/hoge/Hoge",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "FooDataConn::pre_commit_async 1",
                    "BarDataConn::pre_commit_async 2",
                    "FooDataConn::commit_async 1",
                    "BarDataConn::commit failed 2",
                    "BarDataConn::rollback_async 2",
                    "FooDataConn::on_txn_failure_async 1",
                    "BarDataConn::on_txn_failure_async 2",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }
    }
}
