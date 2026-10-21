use crate::{AsyncGroup, DataConn, DataSrc, TxnFailureReport};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time;

#[derive(PartialEq, Copy, Clone)]
pub(crate) enum Fail {
    None,
    Setup,
    PreCommit,
    Commit,
    PostCommit,
    Rollback,
    PreCommitBecomeCommitted,
    CreateDataConn,
    SetupPanicked,
    PreCommitPanicked,
    CommitPanicked,
    PostCommitPanicked,
    RollbackPanicked,
    OnTxnFailurePanicked,
}

pub(crate) struct SyncDataConn {
    id: i8,
    committed: bool,
    fail: Fail,
    pub(crate) logger: Arc<Mutex<Vec<String>>>,
}

impl SyncDataConn {
    pub(crate) fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
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
        } else if self.fail == Fail::CommitPanicked {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::commit {} panicked", self.id));
            panic!("commit panicked");
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
        } else if self.fail == Fail::PreCommitPanicked {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::pre_commit {} panicked", self.id));
            panic!("pre_commit panicked");
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
        } else if self.fail == Fail::PostCommitPanicked {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::post_commit {} panicked", self.id));
            panic!("post_commit panicked");
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
        } else if self.fail == Fail::RollbackPanicked {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::rollback {} panicked", self.id));
            panic!("rollback panicked");
        }
        self.logger
            .lock()
            .unwrap()
            .push(format!("SyncDataConn::rollback {}", self.id));
        Ok(())
    }

    fn on_txn_failure(&mut self, _ag: &mut AsyncGroup, reports: &[TxnFailureReport]) {
        if self.fail == Fail::OnTxnFailurePanicked {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::on_txn_failure {} panicked", self.id));
            panic!("on_txn_failure panicked");
        }
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

pub(crate) struct AsyncDataConn {
    id: i8,
    committed: Arc<AtomicBool>,
    fail: Fail,
    pub(crate) logger: Arc<Mutex<Vec<String>>>,
}

impl AsyncDataConn {
    pub(crate) fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
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
            } else if fail == Fail::CommitPanicked {
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::commit {} panicked", id));
                panic!("commit panic");
            } else if fail == Fail::PreCommitPanicked {
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::pre_commit {} panicked", id));
                panic!("pre_commit panic");
            } else if fail == Fail::PreCommitBecomeCommitted {
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
            } else if fail == Fail::PostCommitPanicked {
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::post_commit {} panicked", id));
                panic!("post_commit panic");
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
            } else if fail == Fail::RollbackPanicked {
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::rollback {} panicked", id));
                panic!("rollback panic");
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
        let fail = self.fail;
        ag.add(move || {
            thread::sleep(time::Duration::from_millis(100));
            let mut logger = logger.lock().unwrap();
            if fail == Fail::OnTxnFailurePanicked {
                logger.push(format!("AsyncDataConn::on_txn_failure {} panicked", id));
                panic!("on_txn_failure panicked");
            }
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

pub(crate) struct NoCommitDataConn {
    id: i8,
    pub(crate) logger: Arc<Mutex<Vec<String>>>,
}

impl NoCommitDataConn {
    pub(crate) fn new(id: i8, logger: Arc<Mutex<Vec<String>>>) -> Self {
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
    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        self.logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::commit {}", self.id));
        Ok(())
    }

    fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        self.logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::pre_commit {}", self.id));
        Ok(())
    }

    fn post_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        self.logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::post_commit {}", self.id));
        Ok(())
    }

    fn is_committed(&self) -> bool {
        false
    }

    fn rollback(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        self.logger
            .lock()
            .unwrap()
            .push(format!("NoCommitDataConn::rollback {}", self.id));
        Ok(())
    }

    fn on_txn_failure(&mut self, _ag: &mut AsyncGroup, reports: &[TxnFailureReport]) {
        let mut logger = self.logger.lock().unwrap();
        logger.push(format!("NoCommitDataConn::on_txn_failure {}", self.id));
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
    logger: Arc<Mutex<Vec<String>>>,
    fail: Fail,
}

impl SyncDataSrc {
    pub(crate) fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
        logger
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
        self.logger
            .lock()
            .unwrap()
            .push(format!("SyncDataSrc::drop {}", self.id));
    }
}

impl DataSrc<SyncDataConn> for SyncDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        if self.fail == Fail::Setup {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataSrc::setup {} failed", self.id));
            return Err(errs::Err::new("XXX".to_string()));
        } else if self.fail == Fail::SetupPanicked {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataSrc::setup {} panicked", self.id));
            panic!("setup panic");
        }
        self.logger
            .lock()
            .unwrap()
            .push(format!("SyncDataSrc::setup {}", self.id));
        Ok(())
    }

    fn close(&mut self) {
        self.logger
            .lock()
            .unwrap()
            .push(format!("SyncDataSrc::close {}", self.id));
    }

    fn create_data_conn(&mut self) -> errs::Result<Box<SyncDataConn>> {
        {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataSrc::create_data_conn {}", self.id));
        }
        if self.fail == Fail::CreateDataConn {
            return Err(errs::Err::new("eeee".to_string()));
        }
        let conn = SyncDataConn::new(self.id, self.logger.clone(), self.fail);
        Ok(Box::new(conn))
    }
}

pub(crate) struct AsyncDataSrc {
    id: i8,
    fail: Fail,
    logger: Arc<Mutex<Vec<String>>>,
    wait: u64,
}

impl AsyncDataSrc {
    pub(crate) fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
        logger
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
        self.logger
            .lock()
            .unwrap()
            .push(format!("AsyncDataSrc::drop {}", self.id));
    }
}

impl DataSrc<AsyncDataConn> for AsyncDataSrc {
    fn setup(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        let logger = self.logger.clone();
        let fail = self.fail;
        let id = self.id;
        let wait = self.wait;
        ag.add(move || {
            std::thread::sleep(std::time::Duration::from_millis(wait));
            let mut logger = logger.lock().unwrap();
            if fail == Fail::Setup {
                logger.push(format!("AsyncDataSrc::setup {} failed", id));
                return Err(errs::Err::new("XXX".to_string()));
            } else if fail == Fail::SetupPanicked {
                logger.push(format!("AsyncDataSrc::setup {} panicked", id));
                panic!("setup panic");
            }
            logger.push(format!("AsyncDataSrc::setup {}", id));
            Ok(())
        });
        Ok(())
    }

    fn close(&mut self) {
        self.logger
            .lock()
            .unwrap()
            .push(format!("AsyncDataSrc::close {}", self.id));
    }

    fn create_data_conn(&mut self) -> errs::Result<Box<AsyncDataConn>> {
        {
            self.logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataSrc::create_data_conn {}", self.id));
        }
        let conn = AsyncDataConn::new(self.id, self.logger.clone(), self.fail);
        Ok(Box::new(conn))
    }
}
