// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataAcc, DataConn, DataHub, Runner};

impl DataAcc for DataHub {
    fn get_data_conn<C: DataConn + 'static>(&mut self, name: &str) -> errs::Result<&mut C> {
        DataHub::get_data_conn(self, name)
    }

    fn run<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        logic_fn(self)
    }

    fn start(&mut self) -> Runner<'_> {
        Runner::new(self, true)
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_data_acc {
    use super::*;
    use crate::_test_commons::*;
    use std::sync::{Arc, Mutex};

    mod test_get_data_conn {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_get_data_conn)]
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

        #[overridable(mod = test_get_data_conn)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
                Ok("hello".to_string())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_get_data_conn)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
                assert_eq!(text, "hello");
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(test_get_data_conn::FooDataAcc, test_get_data_conn::BarDataAcc)]
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

    mod test_run {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_run)]
        trait HogeData {
            fn process(&mut self) -> errs::Result<()>;
        }

        #[overridable(mod = test_run)]
        trait FugaData {
            fn get_value(&mut self) -> errs::Result<String>;
            fn set_value(&mut self, v: &str) -> errs::Result<()>;
        }

        fn hoge_logic(data: &mut impl HogeData) -> errs::Result<()> {
            data.process()?;
            Ok(())
        }

        fn fuga_logic(data: &mut impl FugaData) -> errs::Result<()> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_run)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
                Ok("hello".to_string())
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_run)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
                assert_eq!(text, "hello");
                Ok(())
            }
        }
        impl BarDataAcc for DataHub {}

        #[overridable(mod = test_run)]
        trait BazDataAcc: DataAcc {
            fn process(&mut self) -> errs::Result<()> {
                self.run(fuga_logic)?;
                Ok(())
            }
        }
        impl BazDataAcc for DataHub {}

        #[override_with(test_run::FooDataAcc, test_run::BarDataAcc, test_run::BazDataAcc)]
        impl test_run::HogeData for DataHub {}

        #[override_with(test_run::FooDataAcc, test_run::BarDataAcc, test_run::BazDataAcc)]
        impl test_run::FugaData for DataHub {}

        #[test]
        fn test() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.run(hoge_logic) {
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

    mod test_start {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_start)]
        trait HogeData {
            fn process(&mut self) -> errs::Result<()>;
        }

        #[overridable(mod = test_start)]
        trait FugaData {
            fn get_value(&mut self) -> errs::Result<String>;
            fn set_value(&mut self, v: &str) -> errs::Result<()>;
        }

        fn hoge_logic(data: &mut impl HogeData) -> errs::Result<()> {
            data.process()?;
            Ok(())
        }

        fn fuga_logic(data: &mut impl FugaData) -> errs::Result<()> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_start)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
                Ok("hello".to_string())
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_start)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
                assert_eq!(text, "hello");
                Ok(())
            }
        }
        impl BarDataAcc for DataHub {}

        #[overridable(mod = test_start)]
        trait BazDataAcc: DataAcc {
            fn process(&mut self) -> errs::Result<()> {
                self.start().run(fuga_logic).end()?;
                Ok(())
            }
        }
        impl BazDataAcc for DataHub {}

        #[override_with(test_start::FooDataAcc, test_start::BarDataAcc, test_start::BazDataAcc)]
        impl test_start::HogeData for DataHub {}

        #[override_with(test_start::FooDataAcc, test_start::BarDataAcc, test_start::BazDataAcc)]
        impl test_start::FugaData for DataHub {}

        #[test]
        fn test() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.start().run(hoge_logic).end() {
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
}
