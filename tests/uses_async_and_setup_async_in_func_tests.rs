#[cfg(feature = "tokio")]
#[cfg(test)]
mod uses_async_and_setup_async_in_func_tests {
    use sabi::tokio::{AsyncGroup, DataConn, DataSrc};
    use std::sync::{Arc, Mutex};

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
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl MyDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: bool) -> Self {
            Self {
                id,
                fail,
                logger: logger,
            }
        }
    }
    impl Drop for MyDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("MyDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<MyDataConn> for MyDataSrc {
        async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            if self.fail {
                logger.push(format!("MyDataSrc::setup {} failed", self.id));
                return Err(errs::Err::new("setup error".to_string()));
            }
            logger.push(format!("MyDataSrc::setup {}", self.id));
            Ok(())
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("MyDataSrc::close {}", self.id));
        }

        async fn create_data_conn_async(&mut self) -> errs::Result<Box<MyDataConn>> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("MyDataSrc::create_data_conn {}", self.id));
            let conn = MyDataConn::new();
            Ok(Box::new(conn))
        }
    }

    #[tokio::test]
    async fn test() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            sabi::tokio::uses_async("foo", MyDataSrc::new(1, logger.clone(), false)).await;
            sabi::tokio::uses_async("bar", MyDataSrc::new(2, logger.clone(), false)).await;

            let _auto_shutdown = sabi::tokio::setup_async().await.unwrap();
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "MyDataSrc::setup 1",
                "MyDataSrc::setup 2",
                "MyDataSrc::close 2",
                "MyDataSrc::drop 2",
                "MyDataSrc::close 1",
                "MyDataSrc::drop 1",
            ]
        );
    }
}
