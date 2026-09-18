use super::{AsyncGroup, DataConn, DataSrc};
use crate::TxnFailureReport;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::time;

#[derive(PartialEq, Copy, Clone)]
pub(crate) enum Fail {
    None,
    Commit,
    PreCommit,
    PostCommit,
    Rollback,
    PreCommitBecomeCommitted,
    CreateDataConn,
    Setup,
}

pub(crate) struct SyncDataConn {
    id: i8,
    committed: AtomicBool,
    fail: Fail,
    pub(crate) logger: Arc<std::sync::Mutex<Vec<String>>>,
}

impl SyncDataConn {
    pub(crate) fn new(id: i8, logger: Arc<std::sync::Mutex<Vec<String>>>, fail: Fail) -> Self {
        logger
            .lock()
            .unwrap()
            .push(format!("SyncDataConn::new {}", id));
        Self {
            id,
            committed: AtomicBool::new(false),
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
    async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let fail = self.fail;
        let id = self.id;
        let logger = self.logger.clone();
        let committed = &self.committed;
        if fail == Fail::Commit {
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::commit_async {} failed", id));
            return Err(errs::Err::new("ZZZ".to_string()));
        }
        committed.store(true, Ordering::Release);
        logger
            .lock()
            .unwrap()
            .push(format!("SyncDataConn::commit_async {}", id));
        Ok(())
    }

    async fn pre_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let fail = self.fail;
        let id = self.id;
        let logger = self.logger.clone();
        if fail == Fail::PreCommit {
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::pre_commit_async {} failed", id));
            return Err(errs::Err::new("zzz".to_string()));
        }
        logger
            .lock()
            .unwrap()
            .push(format!("SyncDataConn::pre_commit_async {}", id));
        Ok(())
    }

    async fn post_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let fail = self.fail;
        let id = self.id;
        let logger = self.logger.clone();
        if fail == Fail::PostCommit {
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::post_commit_async {} failed", id));
            return Err(errs::Err::new("!!!".to_string()));
        }
        logger
            .lock()
            .unwrap()
            .push(format!("SyncDataConn::post_commit_async {}", id));
        Ok(())
    }

    fn is_committed(&self) -> bool {
        self.committed.load(Ordering::Acquire)
    }

    async fn rollback_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let fail = self.fail;
        let id = self.id;
        let logger = self.logger.clone();
        if fail == Fail::Rollback {
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::rollback_async {} failed", id));
            return Err(errs::Err::new("!!!".to_string()));
        }
        logger
            .lock()
            .unwrap()
            .push(format!("SyncDataConn::rollback_async {}", id));
        Ok(())
    }

    async fn on_txn_failure_async(
        &mut self,
        _ag: &mut AsyncGroup,
        reports: Arc<[TxnFailureReport]>,
    ) {
        let logger = self.logger.clone();
        let mut logger = logger.lock().unwrap();
        logger.push(format!("SyncDataConn::on_txn_failure_async {}", self.id));
        logger.push(format!("TxnFailureReports={:?}", reports));
    }

    fn close(&mut self) {
        self.logger
            .lock()
            .unwrap()
            .push(format!("SyncDataConn::close {}", self.id));
    }
}

pub(crate) struct AsyncDataConn {
    id: i8,
    committed: Arc<AtomicBool>,
    fail: Fail,
    pub(crate) logger: Arc<std::sync::Mutex<Vec<String>>>,
}

impl AsyncDataConn {
    pub(crate) fn new(id: i8, logger: Arc<std::sync::Mutex<Vec<String>>>, fail: Fail) -> Self {
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
    async fn commit_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        let fail = self.fail;
        let id = self.id;
        let logger = self.logger.clone();
        let committed = self.committed.clone();
        ag.add(async move {
            time::sleep(time::Duration::from_millis(100)).await;
            if fail == Fail::Commit {
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::commit_async {} failed", id));
                return Err(errs::Err::new("YYY".to_string()));
            }
            committed.store(true, Ordering::Release);
            logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataConn::commit_async {}", id));
            Ok(())
        });
        Ok(())
    }

    async fn pre_commit_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        let fail = self.fail;
        let id = self.id;
        let logger = self.logger.clone();
        ag.add(async move {
            time::sleep(time::Duration::from_millis(100)).await;
            if fail == Fail::PreCommit {
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::pre_commit_async {} failed", id));
                return Err(errs::Err::new("yyy".to_string()));
            }
            logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataConn::pre_commit_async {}", id));
            Ok(())
        });
        Ok(())
    }

    async fn post_commit_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        let logger = self.logger.clone();
        let id = self.id;
        let fail = self.fail;
        ag.add(async move {
            time::sleep(time::Duration::from_millis(100)).await;
            if fail == Fail::PostCommit {
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::post_commit_async {} failed", id));
                return Err(errs::Err::new("!!!".to_string()));
            }
            logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataConn::post_commit_async {}", id));
            Ok(())
        });
        Ok(())
    }

    fn is_committed(&self) -> bool {
        self.committed.load(Ordering::Acquire)
    }

    async fn rollback_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        let logger = self.logger.clone();
        let id = self.id;
        let fail = self.fail;
        ag.add(async move {
            time::sleep(time::Duration::from_millis(100)).await;
            if fail == Fail::Rollback {
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::rollback_async {} failed", id));
                return Err(errs::Err::new("???".to_string()));
            }
            logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataConn::rollback_async {}", id));
            Ok(())
        });
        Ok(())
    }

    async fn on_txn_failure_async(
        &mut self,
        ag: &mut AsyncGroup,
        reports: Arc<[TxnFailureReport]>,
    ) {
        let reports_log = format!("TxnFailureReports={:?}", reports);
        let logger = self.logger.clone();
        let id = self.id;
        ag.add(async move {
            time::sleep(time::Duration::from_millis(100)).await;
            let mut logger = logger.lock().unwrap();
            logger.push(format!("AsyncDataConn::on_txn_failure_async {}", id));
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

pub(crate) struct NoCommitDataConn {
    id: i8,
    pub(crate) logger: Arc<std::sync::Mutex<Vec<String>>>,
}

impl NoCommitDataConn {
    pub(crate) fn new(id: i8, logger: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
        logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::new {}", id));
        Self { id, logger }
    }
}

impl Drop for NoCommitDataConn {
    fn drop(&mut self) {
        self.logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::drop {}", self.id));
    }
}

impl DataConn for NoCommitDataConn {
    async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        self.logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::commit_async {}", self.id));
        Ok(())
    }

    async fn pre_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        self.logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::pre_commit_async {}", self.id));
        Ok(())
    }

    async fn post_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        self.logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::post_commit_async {}", self.id));
        Ok(())
    }

    fn is_committed(&self) -> bool {
        false
    }

    async fn rollback_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        self.logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::rollback_async {}", self.id));
        Ok(())
    }

    async fn on_txn_failure_async(
        &mut self,
        _ag: &mut AsyncGroup,
        reports: Arc<[TxnFailureReport]>,
    ) {
        let logger = self.logger.clone();
        let mut logger = logger.lock().unwrap();
        logger.push(format!(
            "NoCommitDataConn::on_txn_failure_async {}",
            self.id
        ));
        logger.push(format!("TxnFailureReports={:?}", reports));
    }

    fn close(&mut self) {
        self.logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::close {}", self.id));
    }
}

pub(crate) struct SyncDataSrc {
    id: i8,
    logger: Arc<std::sync::Mutex<Vec<String>>>,
    fail: Fail,
}

impl SyncDataSrc {
    pub(crate) fn new(id: i8, logger: Arc<std::sync::Mutex<Vec<String>>>, fail: Fail) -> Self {
        let logger_clone = logger.clone();
        logger_clone
            .lock()
            .unwrap()
            .push(format!("SyncDataSrc::new {}", id));
        Self {
            id,
            logger: logger,
            fail,
        }
    }
}

impl Drop for SyncDataSrc {
    fn drop(&mut self) {
        let logger = self.logger.clone();
        logger
            .lock()
            .unwrap()
            .push(format!("SyncDataSrc::drop {}", self.id));
    }
}

impl DataSrc<SyncDataConn> for SyncDataSrc {
    async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        let logger = self.logger.clone();
        if self.fail == Fail::Setup {
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataSrc::setup_async {} failed", self.id));
            return Err(errs::Err::new("XXX".to_string()));
        }
        logger
            .lock()
            .unwrap()
            .push(format!("SyncDataSrc::setup_async {}", self.id));
        Ok(())
    }

    fn close(&mut self) {
        let logger = self.logger.clone();
        logger
            .lock()
            .unwrap()
            .push(format!("SyncDataSrc::close {}", self.id));
    }

    async fn create_data_conn_async(&mut self) -> errs::Result<Box<SyncDataConn>> {
        let logger = self.logger.clone();
        {
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataSrc::create_data_conn_async {}", self.id));
        }
        if self.fail == Fail::CreateDataConn {
            return Err(errs::Err::new("eeee".to_string()));
        }
        let conn = SyncDataConn::new(self.id, logger.clone(), self.fail);
        Ok(Box::new(conn))
    }
}

pub(crate) struct AsyncDataSrc {
    id: i8,
    fail: Fail,
    logger: Arc<std::sync::Mutex<Vec<String>>>,
    wait: u64,
}

impl AsyncDataSrc {
    pub(crate) fn new(id: i8, logger: Arc<std::sync::Mutex<Vec<String>>>, fail: Fail) -> Self {
        let logger_clone = logger.clone();
        logger_clone
            .lock()
            .unwrap()
            .push(format!("AsyncDataSrc::new {}", id));
        Self {
            id,
            fail,
            logger,
            wait: 0,
        }
    }
}

impl Drop for AsyncDataSrc {
    fn drop(&mut self) {
        let logger = self.logger.clone();
        logger
            .lock()
            .unwrap()
            .push(format!("AsyncDataSrc::drop {}", self.id));
    }
}

impl DataSrc<AsyncDataConn> for AsyncDataSrc {
    async fn setup_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        let logger = self.logger.clone();
        let fail = self.fail;
        let id = self.id;
        let wait = self.wait;
        ag.add(async move {
            tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
            let mut logger = logger.lock().unwrap();
            if fail == Fail::Setup {
                logger.push(format!("AsyncDataSrc::setup_async {} failed to setup", id));
                return Err(errs::Err::new("XXX".to_string()));
            }
            logger.push(format!("AsyncDataSrc::setup_async {}", id));
            Ok(())
        });
        Ok(())
    }

    fn close(&mut self) {
        let logger = self.logger.clone();
        logger
            .lock()
            .unwrap()
            .push(format!("AsyncDataSrc::close {}", self.id));
    }

    async fn create_data_conn_async(&mut self) -> errs::Result<Box<AsyncDataConn>> {
        let logger = self.logger.clone();
        {
            logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataSrc::create_data_conn_async {}", self.id));
        }
        if self.fail == Fail::CreateDataConn {
            return Err(errs::Err::new("EEEE".to_string()));
        }
        let conn = AsyncDataConn::new(self.id, logger.clone(), self.fail);
        Ok(Box::new(conn))
    }
}
