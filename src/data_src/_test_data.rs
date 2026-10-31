use crate::data_conn::_test_data::*;
use crate::*;
use std::sync::{Arc, Mutex};

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
