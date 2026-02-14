// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::{DataAcc, DataConn, DataHub};

impl DataAcc for DataHub {
    /// Retrieves a data connection of a specific type from the `DataHub`.
    ///
    /// This asynchronous method attempts to get a data connection identified by `name`.
    /// The connection type `C` must implement the `DataConn` trait and have a `'static` lifetime.
    ///
    /// # Arguments
    ///
    /// * `name` - An identifier for the data connection to retrieve.
    ///
    /// # Type Parameters
    ///
    /// * `C` - The expected type of the data connection, which must implement `DataConn`.
    ///
    /// # Returns
    ///
    /// A `Result` which is `Ok` containing a mutable reference to the data connection
    /// if found and castable to type `C`, or an `Err` if the connection is not found
    /// or cannot be cast.
    async fn get_data_conn_async<C>(&mut self, name: impl AsRef<str>) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        DataHub::get_data_conn_async(self, name).await
    }
}

#[cfg(test)]
mod tests_of_data_acc {
    use super::super::{AsyncGroup, DataSrc};
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
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
        async fn get_text_async(&self) -> String {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::get_text_async {}", self.id));
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
        async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            self.committed = true;
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::commit_async {}", self.id));
            Ok(())
        }
        async fn pre_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::pre_commit_async {}", self.id));
            Ok(())
        }
        async fn post_commit_async(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::post_commit_async {}", self.id));
        }
        fn should_force_back(&self) -> bool {
            self.committed
        }
        async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::rollback_async {}", self.id));
        }
        async fn force_back_async(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::force_back_async {}", self.id));
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
        async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("FooDataSrc::setup_async {} failed", self.id));
                }
                return Err(errs::Err::new("XXXX".to_string()));
            }
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("FooDataSrc::setup_async {}", self.id));
            }
            Ok(())
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataSrc::close {}", self.id));
        }
        async fn create_data_conn_async(&mut self) -> errs::Result<Box<FooDataConn>> {
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("FooDataSrc::create_data_src_async {}", self.id));
            }
            let conn = FooDataConn::new(self.id, &self.text, self.logger.clone());
            Ok(Box::new(conn))
        }
    }

    struct BarDataConn {
        id: i8,
        text: Option<String>,
        ds_text: Arc<tokio::sync::Mutex<String>>,
        committed: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl BarDataConn {
        fn new(
            id: i8,
            ds_text: Arc<tokio::sync::Mutex<String>>,
            logger: Arc<Mutex<Vec<String>>>,
        ) -> Self {
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
        async fn set_text_async(&mut self, s: &str) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::set_text_async {}", self.id));
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
        async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            self.committed = true;
            match &self.text {
                Some(s) => {
                    let mut guard = self.ds_text.lock().await;
                    *guard = s.to_string();
                }
                None => {
                    let mut guard = self.ds_text.lock().await;
                    *guard = "".to_string();
                }
            }
            self.logger
                .lock()
                .unwrap()
                .push(format!("BarDataConn::commit_async {}", self.id));
            Ok(())
        }
        async fn pre_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::pre_commit_async {}", self.id));
            Ok(())
        }
        async fn post_commit_async(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::post_commit_async {}", self.id));
        }
        fn should_force_back(&self) -> bool {
            self.committed
        }
        async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::rollback_async {}", self.id));
        }
        async fn force_back_async(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::force_back_async {}", self.id));
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn.text = {}", self.text.clone().unwrap()));
            logger.push(format!("BarDataConn::close {}", self.id));
        }
    }

    struct BarDataSrc {
        id: i8,
        text: Arc<tokio::sync::Mutex<String>>,
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
                text: Arc::new(tokio::sync::Mutex::new(String::new())),
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
        async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc::setup_async {}", self.id));
            Ok(())
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            if let Ok(guard) = self.text.try_lock() {
                logger.push(format!("BarDataSrc.text = {}", *guard));
            }
            logger.push(format!("BarDataSrc::close {}", self.id));
        }
        async fn create_data_conn_async(&mut self) -> errs::Result<Box<BarDataConn>> {
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("BarDataSrc::create_data_src_async {}", self.id));
            }
            let conn = BarDataConn::new(self.id, self.text.clone(), self.logger.clone());
            Ok(Box::new(conn))
        }
    }

    mod test_run_async_method {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_run_async_method)]
        trait SampleAsyncData {
            async fn get_value_async(&mut self) -> errs::Result<String>;
            async fn set_value_async(&mut self, v: &str) -> errs::Result<()>;
        }

        fn sample_logic_async(
            data: &mut impl SampleAsyncData,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + '_>> {
            Box::pin(async move {
                let v = data.get_value_async().await?;
                let _ = data.set_value_async(&v).await;
                let v = data.get_value_async().await?;
                let _ = data.set_value_async(&v).await;
                Ok(())
            })
        }

        #[overridable(mod = test_run_async_method)]
        trait FooDataAcc: DataAcc {
            async fn get_value_async(&mut self) -> errs::Result<String> {
                let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
                Ok(conn.get_text_async().await)
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_run_async_method)]
        trait BarDataAcc: DataAcc {
            async fn set_value_async(&mut self, text: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<BarDataConn>("bar").await?;
                conn.set_text_async(text).await;
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(test_run_async_method::FooDataAcc, test_run_async_method::BarDataAcc)]
        impl SampleAsyncData for DataHub {}

        #[tokio::test]
        async fn test() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new();

                data.uses("foo", FooDataSrc::new(1, "hello", logger.clone(), false));
                data.uses("bar", BarDataSrc::new(2, logger.clone()));

                if let Err(_) = data.run_async(sample_logic_async).await {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::get_text_async 1",
                    "BarDataConn::set_text_async 2",
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

    mod test_txn_async_method {
        use super::*;
        use crate::logic;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_txn_async_method)]
        trait SampleAsyncData {
            async fn get_value_async(&mut self) -> errs::Result<String>;
            async fn set_value_async(&mut self, v: &str) -> errs::Result<()>;
        }

        fn sample_logic(
            data: &mut impl SampleAsyncData,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + '_>> {
            Box::pin(async {
                let v = data.get_value_async().await?;
                let _ = data.set_value_async(&v).await;
                let v = data.get_value_async().await?;
                let _ = data.set_value_async(&v).await;
                Ok(())
            })
        }

        async fn sample_logic_async(data: &mut impl SampleAsyncData) -> errs::Result<()> {
            let v = data.get_value_async().await?;
            let _ = data.set_value_async(&v).await;
            let v = data.get_value_async().await?;
            let _ = data.set_value_async(&v).await;
            Ok(())
        }

        #[overridable(mod = test_txn_async_method)]
        trait FooDataAcc: DataAcc {
            async fn get_value_async(&mut self) -> errs::Result<String> {
                let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
                Ok(conn.get_text_async().await)
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_txn_async_method)]
        trait BarDataAcc: DataAcc {
            async fn set_value_async(&mut self, text: &str) -> errs::Result<()> {
                let conn = self.get_data_conn_async::<BarDataConn>("bar").await?;
                conn.set_text_async(text).await;
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(test_txn_async_method::FooDataAcc, test_txn_async_method::BarDataAcc)]
        impl test_txn_async_method::SampleAsyncData for DataHub {}

        #[tokio::test]
        async fn test_logic() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new();

                data.uses("foo", FooDataSrc::new(1, "hello", logger.clone(), false));
                data.uses("bar", BarDataSrc::new(2, logger.clone()));

                if let Err(_) = data.txn_async(sample_logic).await {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::get_text_async 1",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::pre_commit_async 1",
                    "BarDataConn::pre_commit_async 2",
                    "FooDataConn::commit_async 1",
                    "BarDataConn::commit_async 2",
                    "FooDataConn::post_commit_async 1",
                    "BarDataConn::post_commit_async 2",
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

        #[tokio::test]
        async fn test_logic_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new();

                data.uses("foo", FooDataSrc::new(1, "hello", logger.clone(), false));
                data.uses("bar", BarDataSrc::new(2, logger.clone()));

                if let Err(_) = data
                    .txn_async(|data| Box::pin(sample_logic_async(data)))
                    .await
                {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::get_text_async 1",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::pre_commit_async 1",
                    "BarDataConn::pre_commit_async 2",
                    "FooDataConn::commit_async 1",
                    "BarDataConn::commit_async 2",
                    "FooDataConn::post_commit_async 1",
                    "BarDataConn::post_commit_async 2",
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

        #[tokio::test]
        async fn test_logic_macro() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new();

                data.uses("foo", FooDataSrc::new(1, "hello", logger.clone(), false));
                data.uses("bar", BarDataSrc::new(2, logger.clone()));

                if let Err(_) = data.txn_async(logic!(sample_logic_async)).await {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup_async 1",
                    "BarDataSrc::setup_async 2",
                    "FooDataSrc::create_data_src_async 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text_async 1",
                    "BarDataSrc::create_data_src_async 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::get_text_async 1",
                    "BarDataConn::set_text_async 2",
                    "FooDataConn::pre_commit_async 1",
                    "BarDataConn::pre_commit_async 2",
                    "FooDataConn::commit_async 1",
                    "BarDataConn::commit_async 2",
                    "FooDataConn::post_commit_async 1",
                    "BarDataConn::post_commit_async 2",
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
