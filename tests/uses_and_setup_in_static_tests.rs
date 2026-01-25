#[cfg(test)]
mod uses_and_setup_in_static_tests {
    use std::sync::{LazyLock, Mutex};

    static LOGGER: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

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
    impl sabi::DataSrc<MyDataConn> for MyDataSrc {
        fn setup(&mut self, _ag: &mut sabi::AsyncGroup) -> errs::Result<()> {
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
        fn create_data_conn(&mut self) -> errs::Result<Box<MyDataConn>> {
            LOGGER
                .lock()
                .unwrap()
                .push(format!("MyDataSrc::create_data_conn {}", self.id));
            let conn = MyDataConn::new();
            Ok(Box::new(conn))
        }
    }

    sabi::uses!("foo", MyDataSrc::new(1, false));
    sabi::uses!("bar", MyDataSrc::new(2, false));

    #[test]
    fn test() {
        {
            let _auto_shutdown = sabi::setup().unwrap();
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
