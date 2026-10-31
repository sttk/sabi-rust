use super::_test_data::*;
use super::*;
use crate::data_conn::_test_data::*;

use std::sync::{Arc, Mutex};

#[test]
fn setup_no_data_src() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut errors = Vec::new();
        manager.setup(&mut errors);

        assert_eq!(errors.len(), 0);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);
    }

    assert_eq!(logger.lock().unwrap().len(), 0);
}

#[test]
fn test_of_setup_and_ok() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
        manager.add("foo", ds1);

        let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::None);
        manager.add("bar", ds2);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 2);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut vec = Vec::new();
        manager.setup(&mut vec);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 2);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ],
    );
}

#[test]
fn test_of_setup_but_error() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
        manager.add("foo", ds1);

        let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::Setup);
        manager.add("bar", ds2);

        let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::Setup);
        manager.add("bar", ds3);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 3);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut errors = Vec::new();
        manager.setup(&mut errors);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 3);
        assert_eq!(manager.vec_ready.len(), 0);

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].index, 1);
        assert_eq!(errors[0].name, "bar".into());
        #[cfg(unix)]
        assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = alloc::string::String \"XXX\", file = src/data_src/_test_data.rs, line = 41 }");
        #[cfg(windows)]
        assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = alloc::string::String \"XXX\", file = src\\data_src\\_test_data.rs, line = 41 }");
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2 failed",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::drop 1",
        ],
    );
}

#[test]
fn setup_with_order_no_data_src() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut errors = Vec::new();
        manager.setup_with_order(&["foo", "bar"], &mut errors);

        assert_eq!(errors.len(), 0);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);
    }

    assert_eq!(logger.lock().unwrap().len(), 0);
}

#[test]
fn test_of_setup_with_order_and_ok() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
        manager.add("foo", ds1);

        let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::None);
        manager.add("bar", ds2);

        let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::None);
        manager.add("baz", ds3);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 3);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut errors = Vec::new();
        manager.setup_with_order(&["baz", "foo"], &mut errors);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 3);

        assert_eq!(errors.len(), 0);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup 3",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
        ],
    );
}

#[test]
fn test_of_setup_with_order_but_fail() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::Setup);
        manager.add("foo", ds1);

        let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::Setup);
        manager.add("bar", ds2);

        let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::None);
        manager.add("baz", ds3);

        let ds4 = SyncDataSrc::new(4, logger.clone(), Fail::None);
        manager.add("qux", ds4);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 4);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut errors = Vec::new();
        manager.setup_with_order(&["qux", "baz", "foo"], &mut errors);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 4);
        assert_eq!(manager.vec_ready.len(), 0);

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].index, 0);
        assert_eq!(errors[0].name, "foo".into());
        #[cfg(unix)]
        assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = alloc::string::String \"XXX\", file = src/data_src/_test_data.rs, line = 41 }");
        #[cfg(windows)]
        assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = alloc::string::String \"XXX\", file = src\\data_src\\_test_data.rs, line = 41 }");
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
            "SyncDataSrc::setup 1 failed",
            "SyncDataSrc::close 3",
            "SyncDataSrc::close 4",
            "SyncDataSrc::drop 4",
            "SyncDataSrc::drop 3",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::drop 1",
        ],
    );
}

#[test]
fn test_of_setup_with_order_containing_duplicated_name_and_ok() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
        manager.add("foo", ds1);

        let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::None);
        manager.add("bar", ds2);

        let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::None);
        manager.add("baz", ds3);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 3);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut vec = Vec::new();
        manager.setup_with_order(&["baz", "baz", "foo"], &mut vec);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 3);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup 3",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
        ],
    );
}

#[test]
fn test_of_setup_with_order_containing_duplicated_name_and_ok_2() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
        manager.add("foo", ds1);

        let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::None);
        manager.add("bar", ds2);

        let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::None);
        manager.add("baz", ds3);

        let ds4 = SyncDataSrc::new(4, logger.clone(), Fail::None);
        manager.add("qux", ds4);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 4);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut vec = Vec::new();
        manager.setup_with_order(&["baz", "foo", "baz", "qux"], &mut vec);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 4);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::new 4",
            "SyncDataSrc::setup 3",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 4",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 4",
            "SyncDataSrc::drop 4",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
        ],
    );
}

#[test]
fn test_of_setup_with_order_buf_one_of_names_is_not_used() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
        manager.add("foo", ds1);

        let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::None);
        manager.add("bar", ds2);

        let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::None);
        manager.add("baz", ds3);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 3);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut vec = Vec::new();
        manager.setup_with_order(&["baz", "foo", "xxx"], &mut vec);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 3);
    }

    assert_eq!(
        *logger.lock().unwrap(),
        vec![
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::new 3",
            "SyncDataSrc::setup 3",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
            "SyncDataSrc::close 3",
            "SyncDataSrc::drop 3",
        ],
    );
}

#[test]
fn test_of_setup_with_order_buf_one_of_names_is_not_used_2() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
        manager.add("foo", ds1);

        let ds2 = SyncDataSrc::new(2, logger.clone(), Fail::None);
        manager.add("bar", ds2);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 2);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut errors = Vec::new();
        manager.setup_with_order(&["baz", "foo", "xxx"], &mut errors);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 2);

        assert!(errors.is_empty());
    }
}
