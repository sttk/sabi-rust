// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataAcc, DataConn, DataHub};

impl DataAcc for DataHub {
    fn get_data_conn<C: DataConn + 'static>(
        &mut self,
        name: impl AsRef<str>,
    ) -> errs::Result<&mut C> {
        DataHub::get_data_conn(self, name)
    }
}

#[cfg(test)]
mod tests_of_data_acc {
    use super::*;
    use crate::{AsyncGroup, DataSrc};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    struct FooDataConn {
        id: i8,
        text: String,
        committed: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl FooDataConn {
        fn new(id: i8, s: &str, logger: Arc<Mutex<Vec<String>>>) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("FooDataConn::new {}", id));
            }
            Self {
                id,
                text: s.to_string(),
                logger,
                committed: false,
            }
        }
        fn get_text(&self) -> String {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::get_text {}", self.id));
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
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            self.committed = true;
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::commit {}", self.id));
            Ok(())
        }
        fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::pre_commit {}", self.id));
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::post_commit {}", self.id));
        }
        fn should_force_back(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::rollback {}", self.id));
        }
        fn force_back(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::force_back {}", self.id));
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::close {}", self.id));
        }
    }

    struct FooDataSrc {
        id: i8,
        logger: Arc<Mutex<Vec<String>>>,
        fail: bool,
        text: String,
    }
    impl FooDataSrc {
        fn new(id: i8, s: &str, logger: Arc<Mutex<Vec<String>>>, fail: bool) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("FooDataSrc::new {}", id));
            }
            Self {
                id,
                logger,
                fail,
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
        fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("FooDataSrc::setup {} failed", self.id));
                }
                return Err(errs::Err::new("XXX".to_string()));
            }
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("FooDataSrc::setup {}", self.id));
            }
            Ok(())
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataSrc::close {}", self.id));
        }
        fn create_data_conn(&mut self) -> errs::Result<Box<FooDataConn>> {
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("FooDataSrc::create_data_src {}", self.id));
            }
            let conn = FooDataConn::new(self.id, &self.text, self.logger.clone());
            Ok(Box::new(conn))
        }
    }

    struct BarDataConn {
        id: i8,
        text: Option<String>,
        ds_text: Rc<RefCell<String>>,
        committed: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl BarDataConn {
        fn new(id: i8, ds_text: Rc<RefCell<String>>, logger: Arc<Mutex<Vec<String>>>) -> Self {
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
            }
        }
        fn set_text(&mut self, s: &str) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::set_text {}", self.id));
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
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            self.committed = true;
            match &self.text {
                Some(s) => {
                    *self.ds_text.borrow_mut() = s.to_string();
                }
                None => {
                    *self.ds_text.borrow_mut() = "".to_string();
                }
            }
            self.logger
                .lock()
                .unwrap()
                .push(format!("BarDataConn::commit {}", self.id));
            Ok(())
        }
        fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::pre_commit {}", self.id));
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::post_commit {}", self.id));
        }
        fn should_force_back(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::rollback {}", self.id));
        }
        fn force_back(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::force_back {}", self.id));
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn.text = {}", self.text.clone().unwrap()));
            logger.push(format!("BarDataConn::close {}", self.id));
        }
    }

    struct BarDataSrc {
        id: i8,
        text: Rc<RefCell<String>>,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl BarDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("BarDataSrc::new {}", id));
            }
            Self {
                id,
                text: Rc::new(RefCell::new(String::new())),
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
        fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc::setup {}", self.id));
            Ok(())
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc.text = {}", self.text.borrow()));
            logger.push(format!("BarDataSrc::close {}", self.id));
        }
        fn create_data_conn(&mut self) -> errs::Result<Box<BarDataConn>> {
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("BarDataSrc::create_data_src {}", self.id));
            }
            let conn = BarDataConn::new(self.id, self.text.clone(), self.logger.clone());
            Ok(Box::new(conn))
        }
    }

    mod test_run_method {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_run_method)]
        trait SampleData {
            fn get_value(&mut self) -> errs::Result<String>;
            fn set_value(&mut self, v: &str) -> errs::Result<()>;
        }

        fn sample_logic(data: &mut impl SampleData) -> errs::Result<()> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_run_method)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> errs::Result<String> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_run_method)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(test_run_method::FooDataAcc, test_run_method::BarDataAcc)]
        impl SampleData for DataHub {}

        #[test]
        fn test() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new();

                data.uses("foo", FooDataSrc::new(1, "hello", logger.clone(), false));
                data.uses("bar", BarDataSrc::new(2, logger.clone()));

                if let Err(_) = data.run(sample_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "FooDataConn::get_text 1",
                    "BarDataConn::set_text 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }
    }

    mod test_txn_method {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_txn_method)]
        trait SampleData {
            fn get_value(&mut self) -> errs::Result<String>;
            fn set_value(&mut self, v: &str) -> errs::Result<()>;
        }

        fn sample_logic(data: &mut impl SampleData) -> errs::Result<()> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_txn_method)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> errs::Result<String> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_txn_method)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(test_txn_method::FooDataAcc, test_txn_method::BarDataAcc)]
        impl test_txn_method::SampleData for DataHub {}

        #[test]
        fn test() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new();

                data.uses("foo", FooDataSrc::new(1, "hello", logger.clone(), false));
                data.uses("bar", BarDataSrc::new(2, logger.clone()));

                if let Err(_) = data.txn(sample_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "FooDataConn::get_text 1",
                    "BarDataConn::set_text 2",
                    "FooDataConn::pre_commit 1",
                    "BarDataConn::pre_commit 2",
                    "FooDataConn::commit 1",
                    "BarDataConn::commit 2",
                    "FooDataConn::post_commit 1",
                    "BarDataConn::post_commit 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "BarDataSrc.text = hello", // because committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }
    }
}
