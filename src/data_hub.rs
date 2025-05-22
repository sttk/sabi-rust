// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;
use std::collections::HashMap;
use std::sync;

use errs::Err;
use hashbrown;

use crate::data_conn::DataConnList;
use crate::data_src::DataSrcList;
use crate::{AsyncGroup, DataConn, DataConnContainer, DataSrc, DataSrcContainer};

static mut GLOBAL_DATA_SRC_LIST: DataSrcList = DataSrcList::new(false);
static GLOBAL_DATA_SRCS_FIXED: sync::OnceLock<()> = sync::OnceLock::new();

#[derive(Debug)]
pub enum DataHubError {
    FailToSetupGlobalDataSrcs {
        errors: HashMap<String, Err>,
    },
    FailToSetupLocalDataSrcs {
        errors: HashMap<String, Err>,
    },
    FailToCommitDataConn {
        errors: HashMap<String, Err>,
    },
    NoDataSrcToCreateDataConn {
        name: String,
        data_conn_type: &'static str,
    },
    FailToCastDataConn {
        name: String,
        cast_to_type: &'static str,
    },
}

pub fn uses<S, C>(name: &str, ds: S)
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    if GLOBAL_DATA_SRCS_FIXED.get().is_none() {
        #[allow(static_mut_refs)]
        unsafe {
            GLOBAL_DATA_SRC_LIST.add_data_src(name.to_string(), ds);
        }
    }
}

pub fn setup() -> Result<GlobalDataSrcsCloser, Err> {
    if let Ok(_) = GLOBAL_DATA_SRCS_FIXED.set(()) {
        #[allow(static_mut_refs)]
        unsafe {
            let err_map = GLOBAL_DATA_SRC_LIST.setup_data_srcs();
            if err_map.len() > 0 {
                return Err(Err::new(DataHubError::FailToSetupGlobalDataSrcs {
                    errors: err_map,
                }));
            }
            return Ok(GlobalDataSrcsCloser { should_close: true });
        }
    }

    Ok(GlobalDataSrcsCloser {
        should_close: false,
    })
}

pub struct GlobalDataSrcsCloser {
    should_close: bool,
}

impl Drop for GlobalDataSrcsCloser {
    fn drop(&mut self) {
        if self.should_close {
            #[allow(static_mut_refs)]
            unsafe {
                GLOBAL_DATA_SRC_LIST.close_data_srcs();
            }
        }
    }
}

pub struct DataHub {
    local_data_src_list: DataSrcList,
    data_src_map: hashbrown::HashMap<String, *mut DataSrcContainer>,
    data_conn_list: DataConnList,
    data_conn_map: hashbrown::HashMap<String, *mut DataConnContainer>,
    fixed: bool,
}

impl DataHub {
    pub fn new() -> Self {
        let _ = GLOBAL_DATA_SRCS_FIXED.set(());

        let mut data_src_map = hashbrown::HashMap::new();

        #[allow(static_mut_refs)]
        unsafe {
            GLOBAL_DATA_SRC_LIST.copy_container_ptrs_did_setup_into(&mut data_src_map);
        }

        Self {
            local_data_src_list: DataSrcList::new(true),
            data_src_map,
            data_conn_list: DataConnList::new(),
            data_conn_map: hashbrown::HashMap::new(),
            fixed: false,
        }
    }

    pub fn uses<S, C>(&mut self, name: &str, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        if self.fixed {
            return;
        }

        self.local_data_src_list.add_data_src(name.to_string(), ds);
    }

    pub fn disuses(&mut self, name: &str) {
        if self.fixed {
            return;
        }

        let itr = self
            .data_src_map
            .extract_if(|nm, p| unsafe { (*(*p)).local } && nm == name);
        for tpl in itr {
            let ptr = tpl.1;
            self.local_data_src_list.remove_container_ptr_did_setup(ptr);

            let close_fn = unsafe { (*ptr).close_fn };
            let drop_fn = unsafe { (*ptr).drop_fn };

            close_fn(ptr);
            drop_fn(ptr);
        }
    }

    fn close(&mut self) {
        self.data_src_map.clear();
        self.local_data_src_list.close_data_srcs();
    }

    fn begin(&mut self) -> Result<(), Err> {
        self.fixed = true;

        let err_map = self.local_data_src_list.setup_data_srcs();
        if err_map.len() > 0 {
            return Err(Err::new(DataHubError::FailToSetupLocalDataSrcs {
                errors: err_map,
            }));
        }

        self.local_data_src_list
            .copy_container_ptrs_did_setup_into(&mut self.data_src_map);

        Ok(())
    }

    fn commit(&mut self) -> Result<(), Err> {
        let mut ptr = self.data_conn_list.head();
        if ptr.is_null() {
            return Ok(());
        }

        let mut err_map = HashMap::new();
        let mut ag = AsyncGroup::new();
        while !ptr.is_null() {
            let commit_fn = unsafe { (*ptr).commit_fn };
            let name = unsafe { &(*ptr).name };
            let next = unsafe { (*ptr).next };

            ag.name = name;

            if let Err(err) = commit_fn(ptr, &mut ag) {
                err_map.insert(name.to_string(), err);
                break;
            }

            ptr = next;
        }
        ag.join_and_put_errors_into(&mut err_map);

        if err_map.is_empty() {
            return Ok(());
        }

        Err(Err::new(DataHubError::FailToCommitDataConn {
            errors: err_map,
        }))
    }

    fn rollback(&mut self) {
        let mut ptr = self.data_conn_list.head();
        if ptr.is_null() {
            return;
        }

        let mut ag = AsyncGroup::new();
        while !ptr.is_null() {
            let should_force_back_fn = unsafe { (*ptr).should_force_back_fn };
            let force_back_fn = unsafe { (*ptr).force_back_fn };
            let rollback_fn = unsafe { (*ptr).rollback_fn };
            let name = unsafe { &(*ptr).name };
            let next = unsafe { (*ptr).next };

            ag.name = name;

            if should_force_back_fn(ptr) {
                force_back_fn(ptr, &mut ag);
            } else {
                rollback_fn(ptr, &mut ag);
            }

            ptr = next;
        }

        ag.join_and_ignore_errors();
    }

    fn post_commit(&mut self) {
        let mut ptr = self.data_conn_list.head();
        if ptr.is_null() {
            return;
        }

        let mut ag = AsyncGroup::new();
        while !ptr.is_null() {
            let post_commit_fn = unsafe { (*ptr).post_commit_fn };
            let name = unsafe { &(*ptr).name };
            let next = unsafe { (*ptr).next };

            ag.name = name;

            post_commit_fn(ptr, &mut ag);

            ptr = next;
        }

        ag.join_and_ignore_errors();
    }

    fn end(&mut self) {
        let mut ptr = self.data_conn_list.last();
        while !ptr.is_null() {
            let close_fn = unsafe { (*ptr).close_fn };
            let prev = unsafe { (*ptr).prev };
            close_fn(ptr);
            ptr = prev;
        }

        self.data_conn_map.clear();
        self.data_conn_list.close();
        self.fixed = false;
    }

    pub fn get_data_conn<C>(&mut self, name: &str) -> Result<&C, Err>
    where
        C: DataConn + 'static,
    {
        match self.data_conn_map.get(name) {
            Some(conn_ptr) => {
                let type_id = any::TypeId::of::<C>();
                let is_fn = unsafe { (*(*conn_ptr)).is_fn };
                if !is_fn(type_id) {
                    return Err(Err::new(DataHubError::FailToCastDataConn {
                        name: name.to_string(),
                        cast_to_type: any::type_name::<C>(),
                    }));
                }
                let typed_ptr = (*conn_ptr) as *mut DataConnContainer<C>;
                return Ok(unsafe { &((*typed_ptr).data_conn) });
            }
            None => match self.data_src_map.get(name) {
                Some(src_ptr) => {
                    let type_id = any::TypeId::of::<C>();
                    let is_data_conn_fn = unsafe { (*(*src_ptr)).is_data_conn_fn };
                    if !is_data_conn_fn(type_id) {
                        return Err(Err::new(DataHubError::FailToCastDataConn {
                            name: name.to_string(),
                            cast_to_type: any::type_name::<C>(),
                        }));
                    }

                    let create_data_conn_fn = unsafe { (*(*src_ptr)).create_data_conn_fn };
                    let boxed = create_data_conn_fn(*src_ptr)?;
                    let raw_ptr = Box::into_raw(boxed);
                    let conn_ptr = raw_ptr.cast::<DataConnContainer>();

                    self.data_conn_list.append_container_ptr(conn_ptr);
                    self.data_conn_map.insert(name.to_string(), conn_ptr);

                    let typed_ptr = raw_ptr.cast::<DataConnContainer<C>>();
                    return Ok(unsafe { &(*typed_ptr).data_conn });
                }
                None => {
                    return Err(Err::new(DataHubError::NoDataSrcToCreateDataConn {
                        name: name.to_string(),
                        data_conn_type: any::type_name::<C>(),
                    }));
                }
            },
        }
    }
}

impl Drop for DataHub {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests_data_hub {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::time;

    struct Logger {
        log_vec: Vec<String>,
    }

    impl Logger {
        fn new() -> Self {
            Self {
                log_vec: Vec::new(),
            }
        }
        fn log(&mut self, s: &str) {
            self.log_vec.push(s.to_string());
        }
        fn assert_logs(&self, logs: &[&str]) {
            assert_eq!(self.log_vec.len(), logs.len());
            for i in 0..self.log_vec.len() {
                assert_eq!(self.log_vec[i], logs[i]);
            }
        }
        fn clear(&mut self) {
            self.log_vec.clear();
        }
    }

    struct SyncDataSrc {
        will_fail_ds: bool,
        will_fail_dc: bool,
        logger: Arc<Mutex<Logger>>,
    }

    impl SyncDataSrc {
        fn new(logger: Arc<Mutex<Logger>>, will_fail_ds: bool, will_fail_dc: bool) -> Self {
            Self {
                will_fail_ds,
                will_fail_dc,
                logger: logger,
            }
        }
    }

    impl Drop for SyncDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataSrc dropped");
        }
    }

    impl DataSrc<SyncDataConn> for SyncDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            let mut logger = self.logger.lock().unwrap();
            if self.will_fail_ds {
                logger.log("SyncDataSrc failed to setup");
                return Err(Err::new("XXX".to_string()));
            }
            logger.log("SyncDataSrc setupped");
            Ok(())
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataSrc closed");
        }

        fn create_data_conn(&mut self) -> Result<Box<SyncDataConn>, Err> {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataSrc created DataConn");
            let conn = SyncDataConn::new(self.logger.clone(), self.will_fail_dc.clone());
            Ok(Box::new(conn))
        }
    }

    struct AsyncDataSrc {
        logger: Arc<Mutex<Logger>>,
        will_fail_ds: bool,
        will_fail_dc: bool,
    }

    impl AsyncDataSrc {
        fn new(logger: Arc<Mutex<Logger>>, will_fail_ds: bool, will_fail_dc: bool) -> Self {
            Self {
                logger: logger,
                will_fail_ds,
                will_fail_dc,
            }
        }
    }

    impl Drop for AsyncDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("AsyncDataSrc dropped");
        }
    }

    impl DataSrc<AsyncDataConn> for AsyncDataSrc {
        fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> {
            let will_fail_ds = self.will_fail_ds;
            let logger = self.logger.clone();

            ag.add(async move || {
                // The `.await` mut be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;

                if will_fail_ds {
                    logger.lock().unwrap().log("AsyncDataSrc failed to setup");
                    return Err(Err::new("YYY".to_string()));
                }

                logger.lock().unwrap().log("AsyncDataSrc setupped");
                Ok(())
            });
            Ok(())
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("AsyncDataSrc closed");
        }

        fn create_data_conn(&mut self) -> Result<Box<AsyncDataConn>, Err> {
            let mut logger = self.logger.lock().unwrap();
            logger.log("AsyncDataSrc created DataConn");
            let conn = AsyncDataConn::new(self.logger.clone(), self.will_fail_dc);
            Ok(Box::new(conn))
        }
    }

    struct SyncDataConn {
        committed: bool,
        will_fail_dc: bool,
        logger: Arc<Mutex<Logger>>,
    }

    impl SyncDataConn {
        fn new(logger: Arc<Mutex<Logger>>, will_fail_dc: bool) -> Self {
            Self {
                committed: false,
                will_fail_dc,
                logger: logger,
            }
        }
    }

    impl Drop for SyncDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataConn dropped");
        }
    }

    impl DataConn for SyncDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            let mut logger = self.logger.lock().unwrap();
            if self.will_fail_dc {
                logger.log("SyncDataConn failed to commit");
                return Err(Err::new("XXX".to_string()));
            }
            self.committed = true;
            logger.log("SyncDataConn committed");
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataConn post committed");
        }
        fn should_force_back(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataConn rollbacked");
        }
        fn force_back(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataConn forced back");
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataConn closed");
        }
    }

    struct AsyncDataConn {
        committed: Arc<Mutex<bool>>,
        logger: Arc<Mutex<Logger>>,
        will_fail_dc: bool,
    }

    impl AsyncDataConn {
        fn new(logger: Arc<Mutex<Logger>>, will_fail_dc: bool) -> Self {
            Self {
                committed: Arc::new(Mutex::new(false)),
                logger: logger,
                will_fail_dc,
            }
        }
    }

    impl Drop for AsyncDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("AsyncDataConn dropped");
        }
    }

    impl DataConn for AsyncDataConn {
        fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> {
            let will_fail_dc = self.will_fail_dc;
            let logger = self.logger.clone();
            let committed = self.committed.clone();

            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;

                if will_fail_dc {
                    logger.lock().unwrap().log("AsyncDataConn failed to commit");
                    return Err(Err::new("YYY".to_string()));
                }

                let mut commit_flag = committed.lock().unwrap();
                *commit_flag = true;
                logger.lock().unwrap().log("AsyncDataConn committed");
                Ok(())
            });
            Ok(())
        }
        fn post_commit(&mut self, ag: &mut AsyncGroup) {
            let logger = self.logger.clone();
            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;
                logger.lock().unwrap().log("AsyncDataConn post committed");
                Ok(())
            });
        }
        fn should_force_back(&self) -> bool {
            *self.committed.lock().unwrap()
        }
        fn rollback(&mut self, ag: &mut AsyncGroup) {
            let logger = self.logger.clone();
            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;
                logger.lock().unwrap().log("AsyncDataConn rollbacked");
                Ok(())
            });
        }
        fn force_back(&mut self, ag: &mut AsyncGroup) {
            let logger = self.logger.clone();
            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;
                logger.lock().unwrap().log("AsyncDataConn forced back");
                Ok(())
            });
        }
        fn close(&mut self) {
            self.logger.lock().unwrap().log("AsyncDataConn closed");
        }
    }

    #[test]
    fn tests() {
        let logger = Arc::new(Mutex::new(Logger::new()));

        test_global_uses_and_setup_and_drop(logger.clone());
        test_hub_new_drop(logger.clone());
        test_hub_uses_disuses(logger.clone());
        test_hub_begin_end(logger.clone());
        test_hub_fail_to_begin_async(logger.clone());
        test_hub_fail_to_begin_sync(logger.clone());
        test_hub_commit(logger.clone());
        test_hub_fail_to_commit_async(logger.clone());
        test_hub_fail_to_commit_sync(logger.clone());
        test_hub_rollback(logger.clone());
        test_hub_force_back(logger.clone());
        test_hub_post_commit(logger.clone());
        test_hub_get_data_conn_create(logger.clone());
        test_hub_get_data_conn_reuse(logger.clone());
        test_hub_get_data_conn_create_fail(logger.clone());
        test_hub_get_data_conn_reuse_fail(logger.clone());
    }

    fn test_global_uses_and_setup_and_drop(logger: Arc<Mutex<Logger>>) {
        {
            uses("foo", AsyncDataSrc::new(logger.clone(), false, false));
            uses("bar", SyncDataSrc::new(logger.clone(), false, false));

            match setup() {
                Ok(_) => {}
                Err(_) => panic!(),
            }
        }

        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc setupped",
            "SyncDataSrc closed",
            "AsyncDataSrc closed",
        ]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_new_drop(_: Arc<Mutex<Logger>>) {
        let hub = DataHub::new();
        assert_eq!(hub.data_src_map.len(), 2);
        assert_eq!(hub.data_conn_map.len(), 0);
    }

    fn test_hub_uses_disuses(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, false));

            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.disuses("baz");

            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.disuses("qux");

            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.disuses("foo");

            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.disuses("bar");

            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);
        }

        logger
            .lock()
            .unwrap()
            .assert_logs(&["SyncDataSrc dropped", "AsyncDataSrc dropped"]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_begin_end(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, false));

            let result = hub.begin();
            assert!(result.is_ok());

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 0);

            if let Ok(conn) = hub.get_data_conn::<AsyncDataConn>("foo") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::AsyncDataConn"
                );
            } else {
                panic!();
            }

            if let Ok(conn) = hub.get_data_conn::<SyncDataConn>("qux") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::SyncDataConn"
                );
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            hub.end();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.disuses("baz");

            assert_eq!(hub.data_src_map.len(), 3);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.disuses("qux");

            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.disuses("foo");

            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.disuses("bar");

            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc setupped",
            "AsyncDataSrc created DataConn",
            "SyncDataSrc created DataConn",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
            "AsyncDataSrc closed",
            "AsyncDataSrc dropped",
            "SyncDataSrc closed",
            "SyncDataSrc dropped",
        ]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_fail_to_begin_async(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), true, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, false));

            if let Err(err) = hub.begin() {
                match err.reason::<DataHubError>() {
                    Ok(r) => match r {
                        DataHubError::FailToSetupLocalDataSrcs { errors } => {
                            assert_eq!(errors.len(), 1);
                        }
                        _ => panic!(),
                    },
                    _ => panic!(),
                }
            } else {
                panic!();
            }

            hub.end();

            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc failed to setup",
            "AsyncDataSrc dropped",
            "SyncDataSrc closed",
            "SyncDataSrc dropped",
        ]);
        logger.lock().unwrap().clear();
    }

    fn test_hub_fail_to_begin_sync(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), true, false));

            if let Err(err) = hub.begin() {
                match err.reason::<DataHubError>() {
                    Ok(r) => match r {
                        DataHubError::FailToSetupLocalDataSrcs { errors } => {
                            assert_eq!(errors.len(), 1);
                        }
                        _ => panic!(),
                    },
                    _ => panic!(),
                }
            } else {
                panic!();
            }

            hub.end();

            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc failed to setup",
            "AsyncDataSrc setupped",
            "SyncDataSrc dropped",
            "AsyncDataSrc closed",
            "AsyncDataSrc dropped",
        ]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_commit(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, false));

            let result = hub.begin();
            assert!(result.is_ok());

            if let Ok(conn) = hub.get_data_conn::<AsyncDataConn>("foo") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::AsyncDataConn"
                );
            } else {
                panic!();
            }

            if let Ok(conn) = hub.get_data_conn::<SyncDataConn>("qux") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::SyncDataConn"
                );
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            if let Err(_) = hub.commit() {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            hub.end();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc setupped",
            "AsyncDataSrc created DataConn",
            "SyncDataSrc created DataConn",
            "SyncDataConn committed",
            "AsyncDataConn committed",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
            "SyncDataSrc closed",
            "AsyncDataSrc closed",
            "SyncDataSrc dropped",
            "AsyncDataSrc dropped",
        ]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_fail_to_commit_async(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, true));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, false));

            let result = hub.begin();
            assert!(result.is_ok());

            if let Ok(conn) = hub.get_data_conn::<AsyncDataConn>("baz") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::AsyncDataConn"
                );
            } else {
                panic!();
            }

            if let Ok(conn) = hub.get_data_conn::<SyncDataConn>("qux") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::SyncDataConn"
                );
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            if let Err(err) = hub.commit() {
                match err.reason::<DataHubError>() {
                    Ok(r) => match r {
                        DataHubError::FailToCommitDataConn { errors } => {
                            match errors.get("baz") {
                                Some(e) => match e.reason::<String>() {
                                    Ok(s) => assert_eq!(s, "YYY"),
                                    Err(_) => panic!(),
                                },
                                None => panic!(),
                            };
                            match errors.get("qux") {
                                Some(_) => panic!(),
                                None => {}
                            }
                        }
                        _ => {}
                    },
                    Err(_) => panic!(),
                }
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            hub.end();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc setupped",
            "AsyncDataSrc created DataConn",
            "SyncDataSrc created DataConn",
            "SyncDataConn committed",
            "AsyncDataConn failed to commit",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
            "SyncDataSrc closed",
            "AsyncDataSrc closed",
            "SyncDataSrc dropped",
            "AsyncDataSrc dropped",
        ]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_fail_to_commit_sync(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, true));

            let result = hub.begin();
            assert!(result.is_ok());

            if let Ok(conn) = hub.get_data_conn::<AsyncDataConn>("baz") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::AsyncDataConn"
                );
            } else {
                panic!();
            }

            if let Ok(conn) = hub.get_data_conn::<SyncDataConn>("qux") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::SyncDataConn"
                );
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            if let Err(err) = hub.commit() {
                match err.reason::<DataHubError>() {
                    Ok(r) => match r {
                        DataHubError::FailToCommitDataConn { errors } => {
                            match errors.get("qux") {
                                Some(e) => match e.reason::<String>() {
                                    Ok(s) => assert_eq!(s, "XXX"),
                                    Err(_) => panic!(),
                                },
                                None => panic!(),
                            };
                            match errors.get("baz") {
                                Some(_) => panic!(),
                                None => {}
                            }
                        }
                        _ => {}
                    },
                    Err(_) => panic!(),
                }
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            hub.end();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc setupped",
            "AsyncDataSrc created DataConn",
            "SyncDataSrc created DataConn",
            "SyncDataConn failed to commit",
            "AsyncDataConn committed",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
            "SyncDataSrc closed",
            "AsyncDataSrc closed",
            "SyncDataSrc dropped",
            "AsyncDataSrc dropped",
        ]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_rollback(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, false));

            let result = hub.begin();
            assert!(result.is_ok());

            if let Ok(conn) = hub.get_data_conn::<AsyncDataConn>("foo") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::AsyncDataConn"
                );
            } else {
                panic!();
            }

            if let Ok(conn) = hub.get_data_conn::<SyncDataConn>("qux") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::SyncDataConn"
                );
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            hub.rollback();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            hub.end();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc setupped",
            "AsyncDataSrc created DataConn",
            "SyncDataSrc created DataConn",
            "SyncDataConn rollbacked",
            "AsyncDataConn rollbacked",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
            "SyncDataSrc closed",
            "AsyncDataSrc closed",
            "SyncDataSrc dropped",
            "AsyncDataSrc dropped",
        ]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_force_back(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, false));

            let result = hub.begin();
            assert!(result.is_ok());

            if let Ok(conn) = hub.get_data_conn::<AsyncDataConn>("foo") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::AsyncDataConn"
                );
            } else {
                panic!();
            }

            if let Ok(conn) = hub.get_data_conn::<SyncDataConn>("qux") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::SyncDataConn"
                );
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            if let Err(_) = hub.commit() {
                panic!();
            }
            hub.rollback();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            hub.end();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc setupped",
            "AsyncDataSrc created DataConn",
            "SyncDataSrc created DataConn",
            "SyncDataConn committed",
            "AsyncDataConn committed",
            "SyncDataConn forced back",
            "AsyncDataConn forced back",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
            "SyncDataSrc closed",
            "AsyncDataSrc closed",
            "SyncDataSrc dropped",
            "AsyncDataSrc dropped",
        ]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_post_commit(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, false));

            let result = hub.begin();
            assert!(result.is_ok());

            if let Ok(conn) = hub.get_data_conn::<AsyncDataConn>("foo") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::AsyncDataConn"
                );
            } else {
                panic!();
            }

            if let Ok(conn) = hub.get_data_conn::<SyncDataConn>("qux") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::SyncDataConn"
                );
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            if let Err(_) = hub.commit() {
                panic!();
            }
            hub.post_commit();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            hub.end();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc setupped",
            "AsyncDataSrc created DataConn",
            "SyncDataSrc created DataConn",
            "SyncDataConn committed",
            "AsyncDataConn committed",
            "SyncDataConn post committed",
            "AsyncDataConn post committed",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
            "SyncDataSrc closed",
            "AsyncDataSrc closed",
            "SyncDataSrc dropped",
            "AsyncDataSrc dropped",
        ]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_get_data_conn_create(_logger: Arc<Mutex<Logger>>) {
        //test_hub_get_data_conn_reuse(logger);
    }
    fn test_hub_get_data_conn_reuse(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, false));

            let result = hub.begin();
            assert!(result.is_ok());

            if let Ok(conn) = hub.get_data_conn::<AsyncDataConn>("foo") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::AsyncDataConn"
                );
            } else {
                panic!();
            }

            if let Ok(conn) = hub.get_data_conn::<SyncDataConn>("qux") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::SyncDataConn"
                );
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            if let Ok(conn) = hub.get_data_conn::<AsyncDataConn>("foo") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::AsyncDataConn"
                );
            } else {
                panic!();
            }

            if let Ok(conn) = hub.get_data_conn::<SyncDataConn>("qux") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::SyncDataConn"
                );
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            if let Err(_) = hub.commit() {
                panic!();
            }
            hub.post_commit();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            hub.end();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc setupped",
            "AsyncDataSrc created DataConn",
            "SyncDataSrc created DataConn",
            "SyncDataConn committed",
            "AsyncDataConn committed",
            "SyncDataConn post committed",
            "AsyncDataConn post committed",
            "SyncDataConn closed",
            "AsyncDataConn closed",
            "SyncDataConn dropped",
            "AsyncDataConn dropped",
            "SyncDataSrc closed",
            "AsyncDataSrc closed",
            "SyncDataSrc dropped",
            "AsyncDataSrc dropped",
        ]);

        logger.lock().unwrap().clear();
    }

    fn test_hub_get_data_conn_create_fail(_logger: Arc<Mutex<Logger>>) {
        //test_hub_get_data_conn_reuse_fail(logger);
    }

    fn test_hub_get_data_conn_reuse_fail(logger: Arc<Mutex<Logger>>) {
        {
            let mut hub = DataHub::new();
            assert_eq!(hub.data_src_map.len(), 2);
            assert_eq!(hub.data_conn_map.len(), 0);

            hub.uses("baz", AsyncDataSrc::new(logger.clone(), false, false));
            hub.uses("qux", SyncDataSrc::new(logger.clone(), false, false));

            let result = hub.begin();
            assert!(result.is_ok());

            if let Err(err) = hub.get_data_conn::<SyncDataConn>("foo") {
                match err.reason::<DataHubError>() {
                    Ok(r) => match r {
                        DataHubError::FailToCastDataConn { name, cast_to_type } => {
                            assert_eq!(name, "foo");
                            assert_eq!(
                                cast_to_type,
                                &"sabi::data_hub::tests_data_hub::SyncDataConn"
                            );
                        }
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                }
            } else {
                panic!();
            }

            if let Ok(conn) = hub.get_data_conn::<SyncDataConn>("qux") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::SyncDataConn"
                );
            } else {
                panic!();
            }

            if let Err(err) = hub.get_data_conn::<SyncDataConn>("xxx") {
                match err.reason::<DataHubError>() {
                    Ok(r) => match r {
                        DataHubError::NoDataSrcToCreateDataConn {
                            name,
                            data_conn_type,
                        } => {
                            assert_eq!(name, "xxx");
                            assert_eq!(
                                data_conn_type,
                                &"sabi::data_hub::tests_data_hub::SyncDataConn"
                            );
                        }
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                }
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 1);

            if let Ok(conn) = hub.get_data_conn::<AsyncDataConn>("foo") {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_data_hub::AsyncDataConn"
                );
            } else {
                panic!();
            }

            if let Err(err) = hub.get_data_conn::<AsyncDataConn>("qux") {
                match err.reason::<DataHubError>() {
                    Ok(r) => match r {
                        DataHubError::FailToCastDataConn { name, cast_to_type } => {
                            assert_eq!(name, "qux");
                            assert_eq!(
                                cast_to_type,
                                &"sabi::data_hub::tests_data_hub::AsyncDataConn"
                            );
                        }
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                }
            } else {
                panic!();
            }

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            if let Err(_) = hub.commit() {
                panic!();
            }
            hub.post_commit();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 2);

            hub.end();

            assert_eq!(hub.data_src_map.len(), 4);
            assert_eq!(hub.data_conn_map.len(), 0);
        }
        logger.lock().unwrap().assert_logs(&[
            "SyncDataSrc setupped",
            "AsyncDataSrc setupped",
            "SyncDataSrc created DataConn",
            "AsyncDataSrc created DataConn",
            "SyncDataConn committed",
            "AsyncDataConn committed",
            "SyncDataConn post committed",
            "AsyncDataConn post committed",
            "AsyncDataConn closed",
            "SyncDataConn closed",
            "AsyncDataConn dropped",
            "SyncDataConn dropped",
            "SyncDataSrc closed",
            "AsyncDataSrc closed",
            "SyncDataSrc dropped",
            "AsyncDataSrc dropped",
        ]);

        logger.lock().unwrap().clear();
    }

    /*
    mod tests_of_run {
        use super::*;
        use override_macro::{overridable, override_with};

        //どっちでもOK
        //fn sample_logic(data: &mut impl SampleData) -> Result<(), Err> {
        fn sample_logic(data: &mut dyn SampleData) -> Result<(), Err> {
            if let Ok(v) = data.get_value() {
                println!("{}", v);
            }
            Ok(())
        }

        #[overridable]
        trait SampleData {
            fn get_value(&self) -> Result<String, Err>;
        }

        #[overridable]
        trait SampleDataAcc: Acc {
            fn get_value(&self) -> Result<String, Err> {
                self.get_string()
            }
        }

        trait Acc {
            fn get_string(&self) -> Result<String, Err>;
        }

        struct Hub {}

        impl Hub {
            pub fn begin(&self) {}
            pub fn end(&self) {}
            pub fn get_string(&self) -> Result<String, Err> {
                Ok("ABC".to_string())
            }

            #[inline]
            pub fn run<F>(&mut self, f: F) -> Result<(), Err>
            where
                F: FnOnce(&mut Hub) -> Result<(), Err>,
            {
                let _ = self.begin();
                let r = f(self);
                self.end();
                r
            }
        }
        impl Acc for Hub {
            fn get_string(&self) -> Result<String, Err> {
                Hub::get_string(self)
            }
        }

        impl SampleDataAcc for Hub {}

        #[override_with(SampleDataAcc)]
        impl SampleData for Hub {}

        #[test]
        fn test() {
            let mut hub = Hub {};

            let _ = sample_logic(&mut hub);

            let _ = hub.run(|data| sample_logic(data));
        }
    }
    */
}
