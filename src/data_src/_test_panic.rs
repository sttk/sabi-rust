use super::_test_data::*;
use super::*;
use crate::data_conn::_test_data::*;

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

#[test]
fn test_of_setup_but_error() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
        manager.add("foo", ds1);

        let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::SetupPanicked);
        manager.add("bar", ds2);

        let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::Setup);
        manager.add("baz", ds3);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 3);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut errors = Vec::new();
        let setup_panic = catch_unwind(AssertUnwindSafe(|| manager.setup(&mut errors)));

        match setup_panic {
            Ok(()) => panic!(),
            Err(payload) => {
                let s = payload
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .unwrap();
                assert_eq!(s, "setup panic");
            }
        }

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);

        assert_eq!(errors.len(), 0);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2 panicked",
            "SyncDataSrc::close 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::drop 1",
        ],
    );
}

#[test]
fn test_of_setup_with_order_but_error() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::SetupPanicked);
        manager.add("foo", ds1);

        let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::SetupPanicked);
        manager.add("bar", ds2);

        let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::None);
        manager.add("baz", ds3);

        let ds4 = SyncDataSrc::new(4, logger.clone(), Fail::None);
        manager.add("qux", ds4);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 4);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut errors = Vec::new();
        let setup_panic = catch_unwind(AssertUnwindSafe(|| {
            manager.setup_with_order(&["qux", "baz", "foo"], &mut errors);
        }));

        match setup_panic {
            Ok(()) => panic!(),
            Err(payload) => {
                let s = payload
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .unwrap();
                assert_eq!(s, "setup panic");
            }
        }

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);

        assert_eq!(errors.len(), 0);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::new 4",
            "SyncDataSrc::setup 4",
            "SyncDataSrc::setup 3",
            "SyncDataSrc::setup 1 panicked",
            "SyncDataSrc::close 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::close 4",
            "SyncDataSrc::drop 4",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::drop 1",
        ],
    );
}
