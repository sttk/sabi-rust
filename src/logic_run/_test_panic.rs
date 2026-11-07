use crate::data_conn::_test_data::*;
use crate::data_src::_test_data::*;
use crate::{DataAcc, DataHub, LogicRunner};

use std::panic;
use std::sync::{Arc, Mutex};

#[test]
fn test_run_panic() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let mut runner = LogicRunner::new(DataHub::<Abc>::new());

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, false);

    match panic::catch_unwind(panic::AssertUnwindSafe(|| {
        runner
            .start()
            .run(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                logger
                    .lock()
                    .unwrap()
                    .push("Run the logic but panicked".to_string());
                panic!("run logic panic");
            })
            .end()
    })) {
        Err(payload) => {
            assert_eq!(*payload.downcast::<&str>().unwrap(), "run logic panic");
        }
        _ => panic!(),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::create_data_conn 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn 2",
            "SyncDataConn::new 2",
            "Run the logic but panicked",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[test]
fn test_run_force_panic() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let mut runner = LogicRunner::new(DataHub::<Abc>::new());

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, false);

    match panic::catch_unwind(panic::AssertUnwindSafe(|| {
        runner
            .start()
            .run_force(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                logger
                    .lock()
                    .unwrap()
                    .push("Run the logic but panicked".to_string());
                panic!("run logic panic");
            })
            .end()
    })) {
        Err(payload) => {
            assert_eq!(*payload.downcast::<&str>().unwrap(), "run logic panic");
        }
        _ => panic!(),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::create_data_conn 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn 2",
            "SyncDataConn::new 2",
            "Run the logic but panicked",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[test]
fn test_run_or_block_panic() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let mut runner = LogicRunner::new(DataHub::<Abc>::new());

    runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
    runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, false);

    match panic::catch_unwind(panic::AssertUnwindSafe(|| {
        runner
            .start()
            .run_or_block(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                logger
                    .lock()
                    .unwrap()
                    .push("Run the logic but panicked".to_string());
                panic!("run logic panic");
            })
            .end()
    })) {
        Err(payload) => {
            assert_eq!(*payload.downcast::<&str>().unwrap(), "run logic panic");
        }
        _ => panic!(),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::create_data_conn 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn 2",
            "SyncDataConn::new 2",
            "Run the logic but panicked",
            "SyncDataConn::close 2",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}

#[test]
fn test_close_panic() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    struct Abc;
    let mut runner = LogicRunner::new(DataHub::<Abc>::new());

    runner.uses(
        "foo",
        SyncDataSrc::new(1, logger.clone(), Fail::ClosePanicked),
    );
    runner.uses(
        "bar",
        SyncDataSrc::new(2, logger.clone(), Fail::ClosePanicked),
    );

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 2);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, false);

    match panic::catch_unwind(panic::AssertUnwindSafe(|| {
        runner
            .start()
            .run(|data| {
                let _conn1 = data.get_data_conn::<SyncDataConn>("foo")?;
                let _conn2 = data.get_data_conn::<SyncDataConn>("bar")?;
                logger.lock().unwrap().push("Run the logic".to_string());
                Ok(())
            })
            .end()
    })) {
        Err(payload) => {
            assert_eq!(*payload.downcast::<&str>().unwrap(), "close panicked");
        }
        _ => panic!(),
    }

    assert_eq!(runner.hub.run.local_data_src_manager.vec_unready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.vec_ready.len(), 0);
    assert_eq!(runner.hub.run.local_data_src_manager.local, true);
    assert_eq!(runner.hub.run.data_src_map.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.vec.len(), 0);
    assert_eq!(runner.hub.run.data_conn_manager.index_map.len(), 0);
    assert_eq!(runner.hub.run.fixed, true);

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup 1",
            "SyncDataSrc::setup 2",
            "SyncDataSrc::create_data_conn 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn 2",
            "SyncDataConn::new 2",
            "Run the logic",
            "SyncDataConn::close 2 panicked",
            "SyncDataConn::drop 2",
            "SyncDataConn::close 1 panicked",
            "SyncDataConn::drop 1",
            "SyncDataSrc::close 2",
            "SyncDataSrc::drop 2",
            "SyncDataSrc::close 1",
            "SyncDataSrc::drop 1",
        ]
    );
}
