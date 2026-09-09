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
    /// # Parameters
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
    async fn get_data_conn_async<C>(&mut self, name: &str) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        DataHub::get_data_conn_async(self, name).await
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_data_acc {
    use super::*;
    use crate::tokio::_test_commons::*;
    use crate::tokio::logic;
    use std::sync::{Arc, Mutex};

    mod test_run_async_method {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_run_async_method)]
        trait SampleAsyncData {
            async fn get_value_async(&mut self) -> errs::Result<String>;
            async fn set_value_async(&mut self, v: &str) -> errs::Result<()>;
        }

        async fn sample_logic_async(data: &mut (impl SampleAsyncData + Send)) -> errs::Result<()> {
            let v = data.get_value_async().await?;
            let _ = data.set_value_async(&v).await;
            let v = data.get_value_async().await?;
            let _ = data.set_value_async(&v).await;
            Ok(())
        }

        #[overridable(mod = test_run_async_method)]
        trait FooDataAcc: DataAcc {
            async fn get_value_async(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn_async::<SyncDataConn>("foo").await?;
                Ok("hello".to_string())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_run_async_method)]
        trait BarDataAcc: DataAcc {
            async fn set_value_async(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn_async::<SyncDataConn>("bar").await?;
                assert_eq!(text, "hello");
                Ok(())
            }
        }

        impl BarDataAcc for DataHub {}

        #[override_with(test_run_async_method::FooDataAcc, test_run_async_method::BarDataAcc)]
        impl SampleAsyncData for DataHub {}

        #[tokio::test]
        async fn test_logic() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::new();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.run_async(logic!(sample_logic_async)).await {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn_async 2",
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

    mod test_txn_async_method {
        use super::*;
        use crate::tokio::logic;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_txn_async_method)]
        trait SampleAsyncData {
            async fn get_value_async(&mut self) -> errs::Result<String>;
            async fn set_value_async(&mut self, v: &str) -> errs::Result<()>;
        }

        async fn sample_logic_async(data: &mut (impl SampleAsyncData + Send)) -> errs::Result<()> {
            let v = data.get_value_async().await?;
            let _ = data.set_value_async(&v).await;
            let v = data.get_value_async().await?;
            let _ = data.set_value_async(&v).await;
            Ok(())
        }

        #[overridable(mod = test_txn_async_method)]
        trait FooDataAcc: DataAcc {
            async fn get_value_async(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn_async::<SyncDataConn>("foo").await?;
                Ok("hello".to_string())
            }
        }

        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_txn_async_method)]
        trait BarDataAcc: DataAcc {
            async fn set_value_async(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn_async::<SyncDataConn>("bar").await?;
                assert_eq!(text, "hello");
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

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.txn_async(logic!(sample_logic_async)).await {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup_async 1",
                    "SyncDataSrc::setup_async 2",
                    "SyncDataSrc::create_data_conn_async 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn_async 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit_async 1",
                    "SyncDataConn::pre_commit_async 2",
                    "SyncDataConn::commit_async 1",
                    "SyncDataConn::commit_async 2",
                    "SyncDataConn::post_commit_async 1",
                    "SyncDataConn::post_commit_async 2",
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
