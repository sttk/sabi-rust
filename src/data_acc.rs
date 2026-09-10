// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataAcc, DataConn, DataHub};

impl DataAcc for DataHub {
    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> errs::Result<&mut C> {
        DataHub::get_data_conn(self, name)
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_data_acc {
    use super::*;
    use crate::_test_commons::*;
    use std::sync::{Arc, Mutex};

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
                let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
                Ok("hello".to_string())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_run_method)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
                assert_eq!(text, "hello");
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

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.run(sample_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
                Ok("hello".to_string())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_txn_method)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
                assert_eq!(text, "hello");
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

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.txn(sample_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
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
                ],
            );
        }
    }
}
