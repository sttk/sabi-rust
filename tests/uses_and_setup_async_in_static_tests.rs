#[cfg(feature = "tokio")]
#[cfg(test)]
mod uses_and_setup_async_in_static_tests {
    use sabi::tokio::{AsyncGroup, DataConn, DataSrc};
    use std::sync::{LazyLock, Mutex};

    static LOGGER: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

    struct MyDataConn {}
    impl MyDataConn {
        fn new() -> Self {
            Self {}
        }
    }
    impl DataConn for MyDataConn {
        async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            Ok(())
        }
        async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {}
        fn close(&mut self) {}
    }

    struct MyDataSrc {
        id: i8,
        fail: bool,
    }
    impl MyDataSrc {
        fn new(id: i8, fail: bool) -> Self {
            Self { id, fail }
        }
    }
    impl Drop for MyDataSrc {
        fn drop(&mut self) {
            LOGGER
                .lock()
                .unwrap()
                .push(format!("MyDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<MyDataConn> for MyDataSrc {
        async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail {
                LOGGER
                    .lock()
                    .unwrap()
                    .push(format!("MyDataSrc::setup {} failed", self.id));
                return Err(errs::Err::new("setup error".to_string()));
            }
            LOGGER
                .lock()
                .unwrap()
                .push(format!("MyDataSrc::setup {}", self.id));
            Ok(())
        }
        fn close(&mut self) {
            LOGGER
                .lock()
                .unwrap()
                .push(format!("MyDataSrc::close {}", self.id));
        }
        async fn create_data_conn_async(&mut self) -> errs::Result<Box<MyDataConn>> {
            LOGGER
                .lock()
                .unwrap()
                .push(format!("MyDataSrc::create_data_conn {}", self.id));
            let conn = MyDataConn::new();
            Ok(Box::new(conn))
        }
    }

    sabi::tokio::uses!("foo", MyDataSrc::new(1, false));
    sabi::tokio::uses!("bar", MyDataSrc::new(2, false));

    #[tokio::test]
    async fn test() {
        {
            let _auto_shutdown = sabi::tokio::setup_async().await.unwrap();
        }

        let logs = LOGGER.lock().unwrap();
        if logs[0] == "MyDataSrc::setup 1" {
            assert_eq!(
                *logs,
                &[
                    "MyDataSrc::setup 1",
                    "MyDataSrc::setup 2",
                    "MyDataSrc::close 2",
                    "MyDataSrc::drop 2",
                    "MyDataSrc::close 1",
                    "MyDataSrc::drop 1",
                ],
            );
        } else {
            assert_eq!(
                *logs,
                &[
                    "MyDataSrc::setup 2",
                    "MyDataSrc::setup 1",
                    "MyDataSrc::close 1",
                    "MyDataSrc::drop 1",
                    "MyDataSrc::close 2",
                    "MyDataSrc::drop 2",
                ],
            );
        }
    }
}
