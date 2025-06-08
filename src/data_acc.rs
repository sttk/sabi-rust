// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataConn, DataHub};
use errs::Err;

/// The trait that aggregates data access operations to external data services
/// into logical units, with methods providing default implementations.
///
/// The organization of these units is flexible; it can be per data service, per
/// functional area, or any other meaningful grouping. Implementations of this trait
/// use the `get_data_conn` method to obtain a `DataConn` object by the name specified
/// during data source registration (via the global `uses` function or `DataHub::uses` method).
/// This `DataConn` then facilitates data access operations to the associated data service.
///
/// Methods declared in `DataAcc` traits can be overridden by *Data* traits which will be
/// passed to logic functions as their arguments using the `override_macro` crate.
/// This design allows for the separation of data input/output logic into specific `DataAcc`
/// implementations, while `DataHub` aggregates all these methods.
/// Logic functions, however, only see the methods declared in the *Data* trait,
/// enabling a clear separation and aggregation of data input/output methods.
pub trait DataAcc {
    /// Retrieves a mutable reference to a `DataConn` object associated with the given name.
    ///
    /// This method is used in default method implementations of `DataAcc` to obtain connections to specific
    /// external data services to perform data access operations.
    ///
    /// # Parameters
    ///
    /// * `name`: The name of the data source, as registered with `uses` (global or `DataHub`'s method).
    ///
    /// # Returns
    ///
    /// * `Result<&mut C, Err>`: A mutable reference to the `DataConn` if found and castable
    ///   to the requested type `C`, or an `Err` if the data source is not found,
    ///   or the `DataConn` cannot be cast to the specified type.
    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> Result<&mut C, Err>;
}

impl DataAcc for DataHub {
    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> Result<&mut C, Err> {
        DataHub::get_data_conn(self, name)
    }
}

#[cfg(test)]
mod tests_of_data_acc {
    use super::*;
    use crate::data_hub::{clear_global_data_srcs_fixed, TEST_SEQ};
    use crate::{setup, uses, AsyncGroup, DataHubError, DataSrc};
    use override_macro::{overridable, override_with};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    struct FooDataSrc {
        id: i8,
        text: String,
        logger: Arc<Mutex<Vec<String>>>,
        will_fail: bool,
    }

    impl FooDataSrc {
        fn new(id: i8, s: &str, logger: Arc<Mutex<Vec<String>>>, will_fail: bool) -> Self {
            Self {
                id,
                text: s.to_string(),
                logger,
                will_fail,
            }
        }
    }

    impl Drop for FooDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataSrc {} dropped", self.id));
        }
    }

    impl DataSrc<FooDataConn> for FooDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            let mut logger = self.logger.lock().unwrap();
            if self.will_fail {
                logger.push(format!("FooDataSrc {} failed to setup", self.id));
                return Err(Err::new("XXX".to_string()));
            }
            logger.push(format!("FooDataSrc {} setupped", self.id));
            Ok(())
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataSrc {} closed", self.id));
        }

        fn create_data_conn(&mut self) -> Result<Box<FooDataConn>, Err> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataSrc {} created FooDataConn", self.id));
            let conn = FooDataConn::new(self.id, &self.text, self.logger.clone());
            Ok(Box::new(conn))
        }
    }

    struct FooDataConn {
        id: i8,
        text: String,
        committed: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl FooDataConn {
        fn new(id: i8, s: &str, logger: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                id,
                text: s.to_string(),
                logger,
                committed: false,
            }
        }
        fn get_text(&self) -> String {
            self.text.clone()
        }
    }

    impl Drop for FooDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn {} dropped", self.id));
        }
    }

    impl DataConn for FooDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            self.committed = true;
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn {} committed", self.id));
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn {} post committed", self.id));
        }
        fn should_force_back(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn {} rollbacked", self.id));
        }
        fn force_back(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn {} forced back", self.id));
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn {} closed", self.id));
        }
    }

    struct BarDataSrc {
        id: i8,
        text: Rc<RefCell<String>>,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl BarDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>) -> Self {
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
            logger.push(format!("BarDataSrc {} dropped", self.id));
        }
    }

    impl DataSrc<BarDataConn> for BarDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc {} setupped", self.id));
            Ok(())
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc.text = {}", self.text.borrow()));
            logger.push(format!("BarDataSrc {} closed", self.id));
        }

        fn create_data_conn(&mut self) -> Result<Box<BarDataConn>, Err> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc {} created BarDataConn", self.id));
            let conn = BarDataConn::new(self.id, self.text.clone(), self.logger.clone());
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
            Self {
                id,
                text: None,
                ds_text,
                logger,
                committed: false,
            }
        }
        fn set_text(&mut self, s: &str) {
            self.text = Some(s.to_string());
        }
    }

    impl Drop for BarDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn {} dropped", self.id));
        }
    }

    impl DataConn for BarDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
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
                .push(format!("BarDataConn {} committed", self.id));
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn {} post committed", self.id));
        }
        fn should_force_back(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn {} rollbacked", self.id));
        }
        fn force_back(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn {} forced back", self.id));
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn.text = {}", self.text.clone().unwrap()));
            logger.push(format!("BarDataConn {} closed", self.id));
        }
    }

    mod test_logic_argument {
        use super::*;

        #[overridable(mod = test_logic_argument)]
        trait SampleData {
            fn get_value(&mut self) -> Result<String, Err>;
            fn set_value(&mut self, v: &str) -> Result<(), Err>;
        }

        fn sample_logic(data: &mut impl SampleData) -> Result<(), Err> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_logic_argument)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> Result<String, Err> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_logic_argument)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> Result<(), Err> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(test_logic_argument::FooDataAcc, test_logic_argument::BarDataAcc)]
        impl test_logic_argument::SampleData for DataHub {}

        #[test]
        fn test() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                uses("foo", FooDataSrc::new(1, "hello", logger.clone(), false));
                uses("bar", BarDataSrc::new(2, logger.clone()));

                let result = setup();
                assert!(result.is_ok());

                let mut hub = DataHub::new();

                if let Err(_) = sample_logic(&mut hub) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc 1 setupped",
                    "BarDataSrc 2 setupped",
                    "FooDataSrc 1 created FooDataConn",
                    "BarDataSrc 2 created BarDataConn",
                    "BarDataConn.text = hello",
                    "BarDataConn 2 closed",
                    "BarDataConn 2 dropped",
                    "FooDataConn 1 closed",
                    "FooDataConn 1 dropped",
                    "BarDataSrc.text = ",
                    "BarDataSrc 2 closed",
                    "BarDataSrc 2 dropped",
                    "FooDataSrc 1 closed",
                    "FooDataSrc 1 dropped",
                ],
            );
        }
    }

    mod test_data_hub_run_using_global {
        use super::*;

        #[overridable(mod = test_data_hub_run_using_global)]
        trait SampleData {
            fn get_value(&mut self) -> Result<String, Err>;
            fn set_value(&mut self, v: &str) -> Result<(), Err>;
        }

        fn sample_logic(data: &mut impl SampleData) -> Result<(), Err> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_data_hub_run_using_global)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> Result<String, Err> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_data_hub_run_using_global)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> Result<(), Err> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(
            test_data_hub_run_using_global::FooDataAcc,
            test_data_hub_run_using_global::BarDataAcc
        )]
        impl test_data_hub_run_using_global::SampleData for DataHub {}

        #[test]
        fn test() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::new()));
            {
                uses("foo", FooDataSrc::new(1, "Hello", logger.clone(), false));
                uses("bar", BarDataSrc::new(2, logger.clone()));

                let result = setup();
                assert!(result.is_ok());

                let mut hub = DataHub::new();
                assert!(hub.run(sample_logic).is_ok());
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc 1 setupped",
                    "BarDataSrc 2 setupped",
                    "FooDataSrc 1 created FooDataConn",
                    "BarDataSrc 2 created BarDataConn",
                    "BarDataConn.text = Hello",
                    "BarDataConn 2 closed",
                    "BarDataConn 2 dropped",
                    "FooDataConn 1 closed",
                    "FooDataConn 1 dropped",
                    "BarDataSrc.text = ",
                    "BarDataSrc 2 closed",
                    "BarDataSrc 2 dropped",
                    "FooDataSrc 1 closed",
                    "FooDataSrc 1 dropped",
                ],
            );
        }
    }

    mod test_data_hub_run_using_local {
        use super::*;

        #[overridable(mod = test_data_hub_run_using_local)]
        trait SampleData {
            fn get_value(&mut self) -> Result<String, Err>;
            fn set_value(&mut self, v: &str) -> Result<(), Err>;
        }

        fn sample_logic(data: &mut impl SampleData) -> Result<(), Err> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_data_hub_run_using_local)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> Result<String, Err> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_data_hub_run_using_local)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> Result<(), Err> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(
            test_data_hub_run_using_local::FooDataAcc,
            test_data_hub_run_using_local::BarDataAcc
        )]
        impl test_data_hub_run_using_local::SampleData for DataHub {}

        #[test]
        fn test() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::new()));
            {
                if let Ok(_auto_shutdown) = setup() {
                    let mut hub = DataHub::new();
                    hub.uses("foo", FooDataSrc::new(1, "Hello", logger.clone(), false));
                    hub.uses("bar", BarDataSrc::new(2, logger.clone()));

                    assert!(hub.run(sample_logic).is_ok());
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc 1 setupped",
                    "BarDataSrc 2 setupped",
                    "FooDataSrc 1 created FooDataConn",
                    "BarDataSrc 2 created BarDataConn",
                    "BarDataConn.text = Hello",
                    "BarDataConn 2 closed",
                    "BarDataConn 2 dropped",
                    "FooDataConn 1 closed",
                    "FooDataConn 1 dropped",
                    "BarDataSrc.text = ",
                    "BarDataSrc 2 closed",
                    "BarDataSrc 2 dropped",
                    "FooDataSrc 1 closed",
                    "FooDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_not_run_logic_if_fail_to_setup_local_data_src() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::new()));
            {
                let result = setup();
                assert!(result.is_ok());

                let mut hub = DataHub::new();
                hub.uses("foo", FooDataSrc::new(1, "Hello", logger.clone(), true));
                hub.uses("bar", BarDataSrc::new(2, logger.clone()));

                if let Err(err) = hub.run(sample_logic) {
                    match err.reason::<DataHubError>() {
                        Ok(r) => match r {
                            DataHubError::FailToSetupLocalDataSrcs { errors } => {
                                assert_eq!(errors.len(), 1);
                                if let Some(err) = errors.get("foo") {
                                    match err.reason::<String>() {
                                        Ok(s) => assert_eq!(s, "XXX"),
                                        Err(_) => panic!(),
                                    }
                                } else {
                                    panic!();
                                }
                            }
                            _ => panic!(),
                        },
                        Err(_) => panic!(),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc 1 failed to setup",
                    "BarDataSrc 2 dropped",
                    "FooDataSrc 1 dropped",
                ],
            );
        }
    }

    mod test_data_hub_run_using_global_and_local {
        use super::*;

        #[overridable(mod = test_data_hub_run_using_global_and_local)]
        trait SampleData {
            fn get_value(&mut self) -> Result<String, Err>;
            fn set_value(&mut self, v: &str) -> Result<(), Err>;
        }

        fn sample_logic(data: &mut impl SampleData) -> Result<(), Err> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_data_hub_run_using_global_and_local)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> Result<String, Err> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_data_hub_run_using_global_and_local)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> Result<(), Err> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(
            test_data_hub_run_using_global_and_local::FooDataAcc,
            test_data_hub_run_using_global_and_local::BarDataAcc
        )]
        impl test_data_hub_run_using_global_and_local::SampleData for DataHub {}
        #[test]
        fn test() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::new()));
            {
                uses("bar", BarDataSrc::new(1, logger.clone()));
                let result = setup();
                assert!(result.is_ok());

                let mut hub = DataHub::new();
                hub.uses("foo", FooDataSrc::new(2, "Hello", logger.clone(), false));

                assert!(hub.run(sample_logic).is_ok());
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "BarDataSrc 1 setupped",
                    "FooDataSrc 2 setupped",
                    "FooDataSrc 2 created FooDataConn",
                    "BarDataSrc 1 created BarDataConn",
                    "BarDataConn.text = Hello",
                    "BarDataConn 1 closed",
                    "BarDataConn 1 dropped",
                    "FooDataConn 2 closed",
                    "FooDataConn 2 dropped",
                    "FooDataSrc 2 closed",
                    "FooDataSrc 2 dropped",
                    "BarDataSrc.text = ",
                    "BarDataSrc 1 closed",
                    "BarDataSrc 1 dropped",
                ],
            );
        }
    }

    mod test_data_hub_txn_using_global {
        use super::*;

        #[overridable(mod = test_data_hub_txn_using_global)]
        trait SampleData {
            fn get_value(&mut self) -> Result<String, Err>;
            fn set_value(&mut self, v: &str) -> Result<(), Err>;
        }

        fn sample_logic(data: &mut impl SampleData) -> Result<(), Err> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_data_hub_txn_using_global)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> Result<String, Err> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_data_hub_txn_using_global)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> Result<(), Err> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(
            test_data_hub_txn_using_global::FooDataAcc,
            test_data_hub_txn_using_global::BarDataAcc
        )]
        impl test_data_hub_txn_using_global::SampleData for DataHub {}

        #[test]
        fn test() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::new()));
            {
                uses("foo", FooDataSrc::new(1, "Hello", logger.clone(), false));
                uses("bar", BarDataSrc::new(2, logger.clone()));

                let result = setup();
                assert!(result.is_ok());

                let mut hub = DataHub::new();
                assert!(hub.txn(sample_logic).is_ok());
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc 1 setupped",
                    "BarDataSrc 2 setupped",
                    "FooDataSrc 1 created FooDataConn",
                    "BarDataSrc 2 created BarDataConn",
                    "FooDataConn 1 committed",
                    "BarDataConn 2 committed",
                    "FooDataConn 1 post committed",
                    "BarDataConn 2 post committed",
                    "BarDataConn.text = Hello",
                    "BarDataConn 2 closed",
                    "BarDataConn 2 dropped",
                    "FooDataConn 1 closed",
                    "FooDataConn 1 dropped",
                    "BarDataSrc.text = Hello",
                    "BarDataSrc 2 closed",
                    "BarDataSrc 2 dropped",
                    "FooDataSrc 1 closed",
                    "FooDataSrc 1 dropped",
                ],
            );
        }
    }

    mod test_data_hub_txn_using_local {
        use super::*;

        #[overridable(mod = test_data_hub_txn_using_local)]
        trait SampleData {
            fn get_value(&mut self) -> Result<String, Err>;
            fn set_value(&mut self, v: &str) -> Result<(), Err>;
        }

        fn sample_logic(data: &mut impl SampleData) -> Result<(), Err> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_data_hub_txn_using_local)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> Result<String, Err> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_data_hub_txn_using_local)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> Result<(), Err> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(
            test_data_hub_txn_using_local::FooDataAcc,
            test_data_hub_txn_using_local::BarDataAcc
        )]
        impl test_data_hub_txn_using_local::SampleData for DataHub {}

        #[test]
        fn test() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::new()));
            {
                let result = setup();
                assert!(result.is_ok());

                let mut hub = DataHub::new();
                hub.uses("foo", FooDataSrc::new(1, "Hello", logger.clone(), false));
                hub.uses("bar", BarDataSrc::new(2, logger.clone()));

                assert!(hub.txn(sample_logic).is_ok());
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc 1 setupped",
                    "BarDataSrc 2 setupped",
                    "FooDataSrc 1 created FooDataConn",
                    "BarDataSrc 2 created BarDataConn",
                    "FooDataConn 1 committed",
                    "BarDataConn 2 committed",
                    "FooDataConn 1 post committed",
                    "BarDataConn 2 post committed",
                    "BarDataConn.text = Hello",
                    "BarDataConn 2 closed",
                    "BarDataConn 2 dropped",
                    "FooDataConn 1 closed",
                    "FooDataConn 1 dropped",
                    "BarDataSrc.text = Hello",
                    "BarDataSrc 2 closed",
                    "BarDataSrc 2 dropped",
                    "FooDataSrc 1 closed",
                    "FooDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_not_run_logic_in_txn_if_fail_to_setup_local_data_src() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::new()));
            {
                let result = setup();
                assert!(result.is_ok());

                let mut hub = DataHub::new();
                hub.uses("foo", FooDataSrc::new(1, "Hello", logger.clone(), true));
                hub.uses("bar", BarDataSrc::new(2, logger.clone()));

                if let Err(err) = hub.txn(sample_logic) {
                    match err.reason::<DataHubError>() {
                        Ok(r) => match r {
                            DataHubError::FailToSetupLocalDataSrcs { errors } => {
                                assert_eq!(errors.len(), 1);
                                if let Some(err) = errors.get("foo") {
                                    match err.reason::<String>() {
                                        Ok(s) => assert_eq!(s, "XXX"),
                                        Err(_) => panic!(),
                                    }
                                } else {
                                    panic!();
                                }
                            }
                            _ => panic!(),
                        },
                        Err(_) => panic!(),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc 1 failed to setup",
                    "BarDataSrc 2 dropped",
                    "FooDataSrc 1 dropped",
                ],
            );
        }

        fn failing_logic(_data: &mut impl SampleData) -> Result<(), Err> {
            Err(Err::new("ZZZ".to_string()))
        }

        #[test]
        fn test_fail_to_run_logic_in_txn_and_rollback() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::new()));
            {
                let result = setup();
                assert!(result.is_ok());

                let mut hub = DataHub::new();
                hub.uses("foo", FooDataSrc::new(1, "Hello", logger.clone(), false));
                hub.uses("bar", BarDataSrc::new(2, logger.clone()));

                if let Err(err) = hub.txn(failing_logic) {
                    match err.reason::<String>() {
                        Ok(s) => assert_eq!(s, "ZZZ"),
                        Err(_) => panic!(),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc 1 setupped",
                    "BarDataSrc 2 setupped",
                    "BarDataSrc.text = ",
                    "BarDataSrc 2 closed",
                    "BarDataSrc 2 dropped",
                    "FooDataSrc 1 closed",
                    "FooDataSrc 1 dropped",
                ],
            );
        }
    }

    mod test_data_hub_txn_using_global_and_local {
        use super::*;

        #[overridable(mod = test_data_hub_txn_using_global_and_local)]
        trait SampleData {
            fn get_value(&mut self) -> Result<String, Err>;
            fn set_value(&mut self, v: &str) -> Result<(), Err>;
        }

        fn sample_logic(data: &mut impl SampleData) -> Result<(), Err> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_data_hub_txn_using_global_and_local)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> Result<String, Err> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_data_hub_txn_using_global_and_local)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> Result<(), Err> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(
            test_data_hub_txn_using_global_and_local::FooDataAcc,
            test_data_hub_txn_using_global_and_local::BarDataAcc
        )]
        impl test_data_hub_txn_using_global_and_local::SampleData for DataHub {}

        #[test]
        fn test() {
            let _unused = TEST_SEQ.lock().unwrap();
            clear_global_data_srcs_fixed();

            let logger = Arc::new(Mutex::new(Vec::new()));
            {
                uses("bar", BarDataSrc::new(1, logger.clone()));

                let result = setup();
                assert!(result.is_ok());

                let mut hub = DataHub::new();
                hub.uses("foo", FooDataSrc::new(2, "Hello", logger.clone(), false));

                assert!(hub.txn(sample_logic).is_ok());
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "BarDataSrc 1 setupped",
                    "FooDataSrc 2 setupped",
                    "FooDataSrc 2 created FooDataConn",
                    "BarDataSrc 1 created BarDataConn",
                    "FooDataConn 2 committed",
                    "BarDataConn 1 committed",
                    "FooDataConn 2 post committed",
                    "BarDataConn 1 post committed",
                    "BarDataConn.text = Hello",
                    "BarDataConn 1 closed",
                    "BarDataConn 1 dropped",
                    "FooDataConn 2 closed",
                    "FooDataConn 2 dropped",
                    "FooDataSrc 2 closed",
                    "FooDataSrc 2 dropped",
                    "BarDataSrc.text = Hello",
                    "BarDataSrc 1 closed",
                    "BarDataSrc 1 dropped",
                ],
            );
        }
    }
}
