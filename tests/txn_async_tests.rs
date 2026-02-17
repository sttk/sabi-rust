#[cfg(feature = "tokio")]
#[cfg(test)]
mod txn_async_tests {
    use sabi::tokio::{AsyncGroup, DataConn, DataSrc};
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
            async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                Ok(())
            }
            fn close(&mut self) {}
            async fn create_data_conn_async(&mut self) -> errs::Result<Box<FooDataConn>> {
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

            pub async fn get_text_async(&self) -> String {
                self.temp.clone()
            }

            pub async fn set_text_async(&mut self, s: String) {
                self.temp = s;
            }
        }

        impl DataConn for FooDataConn {
            async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                *Arc::make_mut(&mut self.text) = self.temp.clone();
                println!("commit text: {}", self.text.clone());
                Ok(())
            }
            async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {
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
            async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                Ok(())
            }
            fn close(&mut self) {}
            async fn create_data_conn_async(&mut self) -> errs::Result<Box<BarDataConn>> {
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

            pub async fn get_num_async(&self) -> u32 {
                self.tmp
            }

            pub async fn set_num_async(&mut self, n: u32) {
                self.tmp = n;
            }
        }

        impl DataConn for BarDataConn {
            async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                *Arc::make_mut(&mut self.num) = self.tmp.clone();
                println!("commit num: {}", self.num.clone());
                Ok(())
            }
            async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {
                self.tmp = *self.num;
            }
            fn close(&mut self) {}
        }
    }

    mod logic_layer {
        use override_macro::overridable;

        #[overridable]
        pub trait MyData {
            async fn get_text_async(&mut self) -> errs::Result<String>;
            async fn set_text_async(&mut self, text: String) -> errs::Result<()>;
            async fn get_num_async(&mut self) -> errs::Result<u32>;
            async fn set_num_async(&mut self, num: u32) -> errs::Result<()>;
        }

        pub async fn my_logic_async(data: &mut impl MyData) -> errs::Result<()> {
            let mut text = data.get_text_async().await?;
            text = text.to_uppercase();
            data.set_text_async(text).await?;
            let mut num = data.get_num_async().await?;
            num += 100;
            data.set_num_async(num).await?;
            Ok(())
        }
    }

    mod data_access_layer {
        use override_macro::overridable;
        use sabi::tokio::DataAcc;

        use super::data_src::{BarDataConn, FooDataConn};

        #[overridable]
        pub trait GettingDataAcc: DataAcc {
            async fn get_text_async(&mut self) -> errs::Result<String> {
                let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
                Ok(conn.get_text_async().await)
            }

            async fn get_num_async(&mut self) -> errs::Result<u32> {
                let conn = self.get_data_conn_async::<BarDataConn>("bar").await?;
                Ok(conn.get_num_async().await)
            }
        }

        #[overridable]
        pub trait SettingDataAcc: DataAcc {
            async fn set_text_async(&mut self, text: String) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
                println!("set text: {}", text.clone());
                conn.set_text_async(text).await;
                Ok(())
            }

            async fn set_num_async(&mut self, num: u32) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<BarDataConn>("bar").await?;
                println!("set num: {}", num.clone());
                conn.set_num_async(num).await;
                Ok(())
            }
        }
    }

    mod data_hub {
        use override_macro::override_with;
        use sabi::tokio::DataHub;

        use super::data_access_layer::{GettingDataAcc, SettingDataAcc};
        use super::logic_layer::MyData;

        impl GettingDataAcc for DataHub {}
        impl SettingDataAcc for DataHub {}

        #[override_with(GettingDataAcc, SettingDataAcc)]
        impl MyData for DataHub {}
    }

    mod app {
        use sabi::tokio::DataHub;
        use std::sync::Arc;

        use super::data_src::{BarDataSrc, FooDataSrc};
        use super::logic_layer::my_logic_async;

        #[tokio::test]
        async fn test() {
            let text = Arc::new("hello".to_string());
            let num = Arc::new(23);

            let foo_ds = FooDataSrc::new(text.clone());
            let bar_ds = BarDataSrc::new(num.clone());

            sabi::tokio::uses_async("foo", foo_ds).await;

            let _auto_shutdown = sabi::tokio::setup_async().await.unwrap();

            let mut data = DataHub::new();
            data.uses("bar", bar_ds);

            if let Err(e) = data.txn_async(sabi::tokio::logic!(my_logic_async)).await {
                panic!("{e:?}");
            }

            assert_eq!(*text, "hello");
            assert_eq!(*num, 23);
        }
    }
}
