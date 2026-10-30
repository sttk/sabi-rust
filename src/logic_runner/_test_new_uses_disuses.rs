use crate::data_conn::_test_data::*;
use crate::data_src::_test_data::*;
use crate::{DataHub, LogicRunner};
use std::sync::{Arc, Mutex};

#[test]
fn test_new() {
    struct MyData;
    let hub = DataHub::<MyData>::new();
    let runner = LogicRunner::new(hub);

    assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.local);
    assert!(runner.hub.run.data_src_map.is_empty());
    assert!(runner.hub.run.data_conn_manager.vec.is_empty());
    assert!(runner.hub.run.data_conn_manager.index_map.is_empty());
    assert!(!runner.hub.run.fixed);
}

#[test]
fn test_uses() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct MyData;
    let hub = DataHub::<MyData>::new();
    let mut runner = LogicRunner::new(hub);

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.local);
    assert!(runner.hub.run.data_src_map.is_empty());
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);

    assert!(runner.hub.run.begin().is_ok());

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
    assert!(runner.hub.run.local_data_src_manager.local);
    assert_eq!(runner.hub.run.data_src_map.len(), 2);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(runner.hub.run.fixed);
}

#[test]
fn test_uses_but_already_fixed() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct MyData;
    let hub = DataHub::<MyData>::new();
    let mut runner = LogicRunner::new(hub);

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 1);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert!(runner.hub.run.local_data_src_manager.local);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);

    assert!(runner.hub.run.begin().is_ok());

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 1);
    assert!(runner.hub.run.local_data_src_manager.local);
    assert_eq!(runner.hub.run.data_src_map.len(), 1);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(runner.hub.run.fixed);

    runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 1);
    assert!(runner.hub.run.local_data_src_manager.local);
    assert_eq!(runner.hub.run.data_src_map.len(), 1);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(runner.hub.run.fixed);
}

#[test]
fn test_disuses_and_ok() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let hub = DataHub::<Abc>::new();
    let mut runner = LogicRunner::new(hub);

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.local);
    assert!(runner.hub.run.data_src_map.is_empty());
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);

    runner.disuses("foo");

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 1);
    assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.local);
    assert!(runner.hub.run.data_src_map.is_empty());
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);

    runner.disuses("bar");

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.local);
    assert!(runner.hub.run.data_src_map.is_empty());
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);
}

#[test]
fn test_disuses_and_fix() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let hub = DataHub::<Abc>::new();
    let mut runner = LogicRunner::new(hub);

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.local);
    assert!(runner.hub.run.data_src_map.is_empty());
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);

    runner.disuses("foo");

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 1);
    assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.local);
    assert!(runner.hub.run.data_src_map.is_empty());
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);

    runner.disuses("bar");

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.local);
    assert!(runner.hub.run.data_src_map.is_empty());
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

    assert!(runner.hub.run.begin().is_ok());

    assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
    assert!(runner.hub.run.local_data_src_manager.local);
    assert_eq!(runner.hub.run.data_src_map.len(), 2);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(runner.hub.run.fixed);

    runner.uses("baz", SyncDataSrc::new(3, logger.clone(), Fail::None));

    assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
    assert!(runner.hub.run.local_data_src_manager.local);
    assert_eq!(runner.hub.run.data_src_map.len(), 2);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(runner.hub.run.fixed);

    runner.disuses("bar");

    assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
    assert!(runner.hub.run.local_data_src_manager.local);
    assert_eq!(runner.hub.run.data_src_map.len(), 2);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(runner.hub.run.fixed);

    runner.hub.run.end();

    assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 2);
    assert!(runner.hub.run.local_data_src_manager.local);
    assert_eq!(runner.hub.run.data_src_map.len(), 2);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);

    runner.disuses("bar");

    assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 1);
    assert!(runner.hub.run.local_data_src_manager.local);
    assert_eq!(runner.hub.run.data_src_map.len(), 1);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);

    runner.disuses("foo");

    assert!(runner.hub.run.local_data_src_manager.vec_unready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.vec_ready.is_empty());
    assert!(runner.hub.run.local_data_src_manager.local);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert!(!runner.hub.run.fixed);
}
