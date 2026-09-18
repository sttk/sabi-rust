#[cfg(feature = "tokio")]
#[cfg(test)]
mod txn_async_tests {
    use sabi::tokio::{AsyncGroup, DataConn, DataSrc};
    use std::collections::HashMap;
    use std::sync::{Arc, LazyLock, RwLock};

    static STORE: LazyLock<Arc<RwLock<HashMap<&'static str, String>>>> =
        LazyLock::new(|| Arc::new(RwLock::new(HashMap::new())));

    mod data_src {
        use super::*;

        pub struct FooDataSrc {
            key: &'static str,
        }

        impl FooDataSrc {
            pub fn new(key: &'static str) -> Self {
                Self { key }
            }
        }

        impl DataSrc<FooDataConn> for FooDataSrc {
            async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                Ok(())
            }
            fn close(&mut self) {}
            async fn create_data_conn_async(&mut self) -> errs::Result<Box<FooDataConn>> {
                Ok(Box::new(FooDataConn::new(self.key)))
            }
        }

        pub struct FooDataConn {
            key: &'static str,
            value: String,
            committed: bool,
        }

        impl FooDataConn {
            fn new(key: &'static str) -> Self {
                Self {
                    key,
                    value: "".to_string(),
                    committed: false,
                }
            }

            pub async fn get_text_async(&self) -> Option<String> {
                STORE.read().unwrap().get(self.key).cloned()
            }

            pub async fn set_text_async<S: AsRef<str>>(&mut self, value: S) {
                self.value = value.as_ref().to_string();
            }
        }

        impl DataConn for FooDataConn {
            async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                STORE.write().unwrap().insert(self.key, self.value.clone());
                self.committed = true;
                Ok(())
            }
            fn is_committed(&self) -> bool {
                self.committed
            }
            async fn rollback_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                Ok(())
            }
            fn close(&mut self) {}
        }
    }

    mod logic_layer {
        use override_macro::overridable;

        #[overridable]
        pub trait HogeData {
            async fn get_text_async(&mut self) -> errs::Result<String>;
            async fn set_text_async(&mut self, text: String) -> errs::Result<()>;
        }

        pub async fn hoge_logic_async(data: &mut impl HogeData) -> errs::Result<()> {
            let mut text = data.get_text_async().await?;
            text = text.to_uppercase();
            data.set_text_async(text).await?;
            Ok(())
        }
    }

    mod data_access_layer {
        use override_macro::overridable;
        use sabi::tokio::DataAcc;

        use super::data_src::FooDataConn;

        #[overridable]
        pub trait GettingDataAcc: DataAcc {
            async fn get_text_async(&mut self) -> errs::Result<String> {
                let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
                Ok(conn.get_text_async().await.unwrap())
            }
        }

        #[overridable]
        pub trait SettingDataAcc: DataAcc {
            async fn set_text_async(&mut self, text: String) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
                conn.set_text_async(text).await;
                Ok(())
            }
        }
    }

    mod data_hub {
        use override_macro::override_with;
        use sabi::tokio::DataHub;

        use super::data_access_layer::{GettingDataAcc, SettingDataAcc};
        use super::logic_layer::HogeData;

        impl GettingDataAcc for DataHub {}
        impl SettingDataAcc for DataHub {}

        #[override_with(GettingDataAcc, SettingDataAcc)]
        impl HogeData for DataHub {}
    }

    mod app {
        use super::data_src::FooDataSrc;
        use super::logic_layer::hoge_logic_async;
        use super::*;
        use sabi::tokio::{logic, DataHub};

        #[tokio::test]
        async fn test_async() {
            {
                STORE.write().unwrap().insert("key", "hello".to_string());
            }

            assert!(sabi::tokio::uses("foo", FooDataSrc::new("key")).is_ok());
            let _auto_shutdown = sabi::tokio::setup_async().await.unwrap();

            test_data_hub_run_async().await;

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "hello");
            }

            {
                STORE.write().unwrap().insert("key", "world".to_string());
            }

            test_runner_async().await;

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "world");
            }

            {
                STORE.write().unwrap().insert("key", "hello".to_string());
            }

            test_data_hub_txn_async().await;

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "HELLO");
            }

            {
                STORE.write().unwrap().insert("key", "world".to_string());
            }

            test_txn_async().await;

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "WORLD");
            }
        }

        async fn test_data_hub_run_async() {
            let mut hub = DataHub::for_txn();
            if let Err(err) = hub.run_async(logic!(hoge_logic_async)).await {
                panic!("{err:?}");
            }
        }

        async fn test_runner_async() {
            let mut hub = DataHub::for_txn();
            if let Err(err) = hub
                .start_async()
                .await
                .run_async(logic!(hoge_logic_async))
                .await
                .end()
            {
                panic!("{err:?}");
            }
        }

        async fn test_data_hub_txn_async() {
            let mut hub = DataHub::for_txn();
            if let Err(err) = hub.txn_async(logic!(hoge_logic_async)).await {
                panic!("{err:?}");
            }
        }

        async fn test_txn_async() {
            let mut hub = DataHub::for_txn();
            if let Err(err) = hub
                .begin_txn_async()
                .await
                .run_async(logic!(hoge_logic_async))
                .await
                .end_txn_async()
                .await
            {
                panic!("{err:?}");
            }
        }
    }
}
