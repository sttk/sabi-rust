#[cfg(test)]
mod txn_tests {
    use sabi::{AsyncGroup, DataConn, DataSrc};
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
            fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                Ok(())
            }
            fn close(&mut self) {}
            fn create_data_conn(&mut self) -> errs::Result<Box<FooDataConn>> {
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

            pub fn get_text(&self) -> Option<String> {
                STORE.read().unwrap().get(self.key).cloned()
            }

            pub fn set_text<S: AsRef<str>>(&mut self, value: S) {
                self.value = value.as_ref().to_string();
            }
        }

        impl DataConn for FooDataConn {
            fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                STORE.write().unwrap().insert(self.key, self.value.clone());
                self.committed = true;
                Ok(())
            }
            fn is_committed(&self) -> bool {
                self.committed
            }
            fn rollback(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
                Ok(())
            }
            fn close(&mut self) {}
        }
    }

    mod logic_layer {
        use override_macro::overridable;

        #[overridable]
        pub trait HogeData {
            fn get_text(&mut self) -> errs::Result<String>;
            fn set_text(&mut self, text: String) -> errs::Result<()>;
        }

        pub fn hoge_logic(data: &mut impl HogeData) -> errs::Result<()> {
            let mut text = data.get_text()?;
            text = text.to_uppercase();
            data.set_text(text)?;
            Ok(())
        }
    }

    mod data_access_layer {
        use override_macro::overridable;
        use sabi::DataAcc;

        use super::data_src::FooDataConn;

        #[overridable]
        pub trait GettingDataAcc: DataAcc {
            fn get_text(&mut self) -> errs::Result<String> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text().unwrap())
            }
        }

        #[overridable]
        pub trait SettingDataAcc: DataAcc {
            fn set_text(&mut self, text: String) -> errs::Result<()> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                conn.set_text(text);
                Ok(())
            }
        }
    }

    mod data_hub {
        use override_macro::override_with;
        use sabi::DataHub;

        use super::data_access_layer::{GettingDataAcc, SettingDataAcc};
        use super::logic_layer::HogeData;

        impl GettingDataAcc for DataHub {}
        impl SettingDataAcc for DataHub {}

        #[override_with(GettingDataAcc, SettingDataAcc)]
        impl HogeData for DataHub {}
    }

    mod app {
        use super::data_src::FooDataSrc;
        use super::logic_layer::hoge_logic;
        use super::*;
        use sabi::DataHub;

        #[test]
        fn test() {
            {
                STORE.write().unwrap().insert("key", "hello".to_string());
            }

            assert!(sabi::uses("foo", FooDataSrc::new("key")).is_ok());
            let _auto_shutdown = sabi::setup().unwrap();

            test_data_hub_run();

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "hello");
            }

            {
                STORE.write().unwrap().insert("key", "world".to_string());
            }

            test_runner();

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "world");
            }

            {
                STORE.write().unwrap().insert("key", "hello".to_string());
            }

            test_data_hub_txn();

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "HELLO");
            }

            {
                STORE.write().unwrap().insert("key", "world".to_string());
            }

            test_txn();

            {
                let text = STORE.read().unwrap().get("key").unwrap().to_string();
                assert_eq!(text, "WORLD");
            }
        }

        fn test_data_hub_run() {
            let mut hub = DataHub::new().for_txn();
            if let Err(err) = hub.run(hoge_logic) {
                panic!("{err:?}");
            }
        }

        fn test_runner() {
            let mut hub = DataHub::new().for_txn();
            if let Err(err) = hub.start().run(hoge_logic).end() {
                panic!("{err:?}");
            }
        }

        fn test_data_hub_txn() {
            let mut hub = DataHub::new().for_txn();
            if let Err(err) = hub.txn(hoge_logic) {
                panic!("{err:?}");
            }
        }

        fn test_txn() {
            let mut hub = DataHub::new().for_txn();
            if let Err(err) = hub.begin_txn().run(hoge_logic).end_txn() {
                panic!("{err:?}");
            }
        }
    }
}
