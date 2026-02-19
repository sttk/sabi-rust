#[cfg(test)]
mod txn_tests {
    use sabi::{AsyncGroup, DataConn, DataSrc};
    use std::sync::Arc;

    mod data_src {
        use super::*;

        pub struct FooDataSrc {
            pub text: Arc<String>,
        }

        impl FooDataSrc {
            pub fn new(text: Arc<String>) -> Self {
                Self { text }
            }
        }

        impl DataSrc<FooDataConn> for FooDataSrc {
            fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                Ok(())
            }
            fn close(&mut self) {}
            fn create_data_conn(&mut self) -> errs::Result<Box<FooDataConn>> {
                Ok(Box::new(FooDataConn::new(self.text.clone())))
            }
        }

        pub struct FooDataConn {
            text: Arc<String>,
            temp: String,
        }

        impl FooDataConn {
            fn new(text: Arc<String>) -> Self {
                Self {
                    text: text.clone(),
                    temp: text.to_string(),
                }
            }

            pub fn get_text(&self) -> String {
                self.temp.clone()
            }

            pub fn set_text(&mut self, s: String) {
                self.temp = s;
            }
        }

        impl DataConn for FooDataConn {
            fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                *Arc::make_mut(&mut self.text) = self.temp.clone();
                println!("commit text: {}", self.text.clone());
                Ok(())
            }
            fn rollback(&mut self, _ag: &mut AsyncGroup) {
                self.temp = self.text.to_string();
            }
            fn close(&mut self) {}
        }

        pub struct BarDataSrc {
            pub num: Arc<u32>,
        }

        impl BarDataSrc {
            pub fn new(num: Arc<u32>) -> Self {
                Self { num }
            }
        }

        impl DataSrc<BarDataConn> for BarDataSrc {
            fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                Ok(())
            }
            fn close(&mut self) {}
            fn create_data_conn(&mut self) -> errs::Result<Box<BarDataConn>> {
                Ok(Box::new(BarDataConn::new(self.num.clone())))
            }
        }

        pub struct BarDataConn {
            num: Arc<u32>,
            tmp: u32,
        }

        impl BarDataConn {
            fn new(num: Arc<u32>) -> Self {
                Self {
                    num: num.clone(),
                    tmp: *num,
                }
            }

            pub fn get_num(&self) -> u32 {
                self.tmp
            }

            pub fn set_num(&mut self, n: u32) {
                self.tmp = n;
            }
        }

        impl DataConn for BarDataConn {
            fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                *Arc::make_mut(&mut self.num) = self.tmp.clone();
                println!("commit num: {}", self.num.clone());
                Ok(())
            }
            fn rollback(&mut self, _ag: &mut AsyncGroup) {
                self.tmp = *self.num;
            }
            fn close(&mut self) {}
        }
    }

    mod logic_layer {
        use override_macro::overridable;

        #[overridable]
        pub trait MyData {
            fn get_text(&mut self) -> errs::Result<String>;
            fn set_text(&mut self, text: String) -> errs::Result<()>;
            fn get_num(&mut self) -> errs::Result<u32>;
            fn set_num(&mut self, num: u32) -> errs::Result<()>;
        }

        pub fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
            let mut text = data.get_text()?;
            text = text.to_uppercase();
            data.set_text(text)?;
            let mut num = data.get_num()?;
            num += 100;
            data.set_num(num)?;
            Ok(())
        }
    }

    mod data_access_layer {
        use override_macro::overridable;
        use sabi::DataAcc;

        use super::data_src::{BarDataConn, FooDataConn};

        #[overridable]
        pub trait GettingDataAcc: DataAcc {
            fn get_text(&mut self) -> errs::Result<String> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }

            fn get_num(&mut self) -> errs::Result<u32> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                Ok(conn.get_num())
            }
        }

        #[overridable]
        pub trait SettingDataAcc: DataAcc {
            fn set_text(&mut self, text: String) -> errs::Result<()> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                println!("set text: {}", text.clone());
                conn.set_text(text);
                Ok(())
            }

            fn set_num(&mut self, num: u32) -> errs::Result<()> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                println!("set num: {}", num.clone());
                conn.set_num(num);
                Ok(())
            }
        }
    }

    mod hub {
        use override_macro::override_with;
        use sabi::DataHub;

        use super::data_access_layer::{GettingDataAcc, SettingDataAcc};
        use super::logic_layer::MyData;

        impl GettingDataAcc for DataHub {}
        impl SettingDataAcc for DataHub {}

        #[override_with(GettingDataAcc, SettingDataAcc)]
        impl MyData for DataHub {}
    }

    mod app {
        use sabi::DataHub;
        use std::sync::Arc;

        use super::data_src::{BarDataSrc, FooDataSrc};
        use super::logic_layer::my_logic;

        #[test]
        fn test() {
            let text = Arc::new("hello".to_string());
            let num = Arc::new(23);

            let foo_ds = FooDataSrc::new(text.clone());
            let bar_ds = BarDataSrc::new(num.clone());

            assert!(sabi::uses("foo", foo_ds).is_ok());

            let _auto_shutdown = sabi::setup().unwrap();

            let mut data = DataHub::new();
            data.uses("bar", bar_ds);

            if let Err(e) = data.txn(my_logic) {
                panic!("{e:?}");
            }

            assert_eq!(*text, "hello");
            assert_eq!(*num, 23);
        }
    }
}
