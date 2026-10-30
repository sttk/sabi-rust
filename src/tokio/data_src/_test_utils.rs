use super::_test_data::*;
use super::*;
use crate::tokio::data_conn::_test_data::*;

use std::collections::HashMap;
use std::sync::Arc;

#[tokio::test]
async fn test_of_copy_ds_ready_to_map() {
    let logger = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut errors = Vec::new();

    let mut index_map = HashMap::<Arc<str>, (bool, usize)>::new();

    let manager = DataSrcManager::new(true);
    manager.copy_ds_ready_to_map(&mut index_map);
    assert!(index_map.is_empty());

    let mut manager = DataSrcManager::new(true);
    let ds1 = SyncDataSrc::new(1, logger.clone(), Fail::None);
    manager.add("foo", ds1);
    manager.setup_async(&mut errors).await;
    assert!(errors.is_empty());
    manager.copy_ds_ready_to_map(&mut index_map);
    assert_eq!(index_map.len(), 1);
    assert_eq!(index_map.get("foo").unwrap(), &(true, 0));

    let mut manager = DataSrcManager::new(false);
    let ds2 = AsyncDataSrc::new(2, logger.clone(), Fail::None);
    let ds3 = SyncDataSrc::new(3, logger.clone(), Fail::None);
    manager.add("bar", ds2);
    manager.add("baz", ds3);
    manager.setup_async(&mut errors).await;
    assert!(errors.is_empty());
    manager.copy_ds_ready_to_map(&mut index_map);
    assert_eq!(index_map.len(), 3);
    assert_eq!(index_map.get("foo").unwrap(), &(true, 0));
    assert_eq!(index_map.get("bar").unwrap(), &(false, 0));
    assert_eq!(index_map.get("baz").unwrap(), &(false, 1));
}
