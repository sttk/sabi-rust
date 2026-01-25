#[cfg(test)]
mod uses_and_setup_in_func_tests {
    use std::sync::{Arc, Mutex};

    struct MyDataConn {}
    impl MyDataConn {
        fn new() -> Self {
            Self {}
        }
    }
    impl sabi::DataConn for MyDataConn {
        fn commit(&mut self, _ag: &mut sabi::AsyncGroup) -> errs::Result<()> {
            Ok(())
        }
        fn rollback(&mut self, _ag: &mut sabi::AsyncGroup) {}
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
    impl sabi::DataSrc<MyDataConn> for MyDataSrc {
        fn setup(&mut self, _ag: &mut sabi::AsyncGroup) -> errs::Result<()> {
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

        fn create_data_conn(&mut self) -> errs::Result<Box<MyDataConn>> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("MyDataSrc::create_data_conn {}", self.id));
            let conn = MyDataConn::new();
            Ok(Box::new(conn))
        }
    }

    #[test]
    fn test() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            sabi::uses("foo", MyDataSrc::new(1, logger.clone(), false));
            sabi::uses("bar", MyDataSrc::new(2, logger.clone(), false));

            let _auto_shutdown = sabi::setup().unwrap();
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
