use crate::tokio::data_conn::_test_data::*;
use crate::tokio::*;
use std::sync::Arc;

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
        } else if self.fail == Fail::SetupPanicked {
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataSrc::setup_async {} panicked", self.id));
            panic!("setup_async panic");
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
                logger.push(format!("AsyncDataSrc::setup_async {} failed", id));
                return Err(errs::Err::new("XXX".to_string()));
            } else if fail == Fail::SetupPanicked {
                logger.push(format!("AsyncDataSrc::setup_async {} panicked", id));
                panic!("setup_async panic");
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
