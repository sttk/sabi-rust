use super::_test_data::*;
use super::*;
use crate::tokio::data_conn::_test_data::*;

#[tokio::test]
async fn test_of_setup_async_and_ok() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

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
        manager.setup_async(&mut vec).await;

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 2);
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
    }
}

#[tokio::test]
async fn test_of_setup_but_error() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

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

        let mut vec = Vec::new();
        manager.setup_async(&mut vec).await;

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 3);
        assert_eq!(manager.vec_ready.len(), 0);
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 2 failed".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
    }
}

#[tokio::test]
async fn setup_with_order_no_data_src() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    {
        let mut manager = DataSrcManager::new(true);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);

        let mut errors = Vec::new();
        manager
            .setup_with_order_async(&["baz", "foo"], &mut errors)
            .await;

        assert_eq!(errors.len(), 0);

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);
    }

    {
        let locked = logger.lock().unwrap();
        assert_eq!(locked.len(), 0);
    }
}

#[tokio::test]
async fn test_of_setup_with_order_and_ok() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

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
        manager
            .setup_with_order_async(&["baz", "foo"], &mut errors)
            .await;

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 3);

        assert_eq!(errors.len(), 0);
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
    }
}

#[tokio::test]
async fn test_of_setup_with_order_but_fail() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

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
        manager
            .setup_with_order_async(&["qux", "baz", "foo"], &mut errors)
            .await;

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 4);
        assert_eq!(manager.vec_ready.len(), 0);

        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].index, 0);
        assert_eq!(errors[0].name, "foo".into());
        #[cfg(unix)]
        assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = alloc::string::String \"XXX\", file = src/tokio/data_src/_test_data.rs, line = 44 }");
        #[cfg(windows)]
        assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = alloc::string::String \"XXX\", file = src\\tokio\\data_src\\_test_data.rs, line = 44 }");
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 1 failed".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 4".to_string()));
    }
}

#[tokio::test]
async fn test_of_setup_with_order_containing_duplicated_name_and_ok() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

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
        manager
            .setup_with_order_async(&["baz", "baz", "foo"], &mut vec)
            .await;

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 3);
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
    }
}

#[tokio::test]
async fn test_of_setup_with_order_containing_duplicated_name_and_ok_2() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

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
        manager
            .setup_with_order_async(&["baz", "foo", "baz", "qux"], &mut vec)
            .await;

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 4);
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
    }
}

#[tokio::test]
async fn test_of_setup_with_order_buf_one_of_names_is_not_used() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

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
        manager
            .setup_with_order_async(&["baz", "foo", "xxx"], &mut vec)
            .await;

        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 3);
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let locked = logger.lock().unwrap();
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup_async 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
    }
}
