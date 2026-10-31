#[cfg(feature = "tokio")]
#[cfg(test)]
mod run_async_tests {
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
            committed: bool,
        }

        impl FooDataConn {
            fn new(key: &'static str) -> Self {
                Self {
                    key,
                    committed: false,
                }
            }

            pub async fn get_text_async(&self) -> Option<String> {
                STORE.read().unwrap().get(self.key).cloned()
            }

            pub async fn set_text_async<S: AsRef<str>>(&self, value: S) {
                STORE
                    .write()
                    .unwrap()
                    .insert(self.key, value.as_ref().to_string());
            }
        }

        impl DataConn for FooDataConn {
            async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
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
        pub trait MyData {
            async fn get_text_async(&mut self) -> errs::Result<String>;
            async fn set_text_async(&mut self, text: String) -> errs::Result<()>;
        }

        pub async fn my_logic_async(data: &mut impl MyData) -> errs::Result<()> {
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

    struct HogeScope;
    struct FugaScope;

    mod data_hub {
        use super::*;
        use override_macro::override_with;
        use sabi::tokio::DataHub;

        use super::data_access_layer::{GettingDataAcc, SettingDataAcc};
        use super::logic_layer::MyData;

        impl GettingDataAcc for DataHub<HogeScope> {}
        impl SettingDataAcc for DataHub<HogeScope> {}

        #[override_with(GettingDataAcc, SettingDataAcc)]
        impl MyData for DataHub<HogeScope> {}

        impl GettingDataAcc for DataHub<FugaScope> {}
        impl SettingDataAcc for DataHub<FugaScope> {}

        #[override_with(GettingDataAcc, SettingDataAcc)]
        impl MyData for DataHub<FugaScope> {}
    }

    mod app {
        use super::data_src::FooDataSrc;
        use super::logic_layer::my_logic_async;
        use super::*;
        use sabi::tokio::{logic, DataHub, LogicRunner};

        sabi::tokio::uses!("foo", FooDataSrc::new("key"));

        #[tokio::test]
        async fn test_async() {
            let _autho_shutdown = sabi::tokio::setup_async().await.unwrap();

            test_async_with_hoge_scope().await;
            test_async_with_fuga_scope().await;
        }

        async fn test_async_with_hoge_scope() {
            {
                STORE.write().unwrap().insert("key", "hello".to_string());
            }

            test_data_hub_run_async_with_hoge_scope().await;

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "HELLO");
            }

            {
                STORE.write().unwrap().insert("key", "world".to_string());
            }

            test_runner_async_with_hoge_scope().await;

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "WORLD");
            }
        }

        async fn test_async_with_fuga_scope() {
            {
                STORE.write().unwrap().insert("key", "hello".to_string());
            }

            test_data_hub_run_async_with_fuga_scope().await;

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "HELLO");
            }

            {
                STORE.write().unwrap().insert("key", "world".to_string());
            }

            test_runner_async_with_fuga_scope().await;

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "WORLD");
            }
        }

        async fn test_data_hub_run_async_with_hoge_scope() {
            let mut runner = LogicRunner::new(DataHub::<HogeScope>::new());
            if let Err(err) = runner.run_async(logic!(my_logic_async)).await {
                panic!("{err:?}");
            }
        }

        async fn test_runner_async_with_hoge_scope() {
            let mut runner = LogicRunner::new(DataHub::<HogeScope>::new());
            if let Err(err) = runner
                .start_async()
                .await
                .run_async(logic!(my_logic_async))
                .await
                .end()
            {
                panic!("{err:?}");
            }
        }

        async fn test_data_hub_run_async_with_fuga_scope() {
            let mut runner = LogicRunner::new(DataHub::<FugaScope>::new());
            if let Err(err) = runner.run_async(logic!(my_logic_async)).await {
                panic!("{err:?}");
            }
        }

        async fn test_runner_async_with_fuga_scope() {
            let mut runner = LogicRunner::new(DataHub::<FugaScope>::new());
            if let Err(err) = runner
                .start_async()
                .await
                .run_async(logic!(my_logic_async))
                .await
                .end()
            {
                panic!("{err:?}");
            }
        }
    }
}
