// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;
use std::collections::HashMap;
use std::ptr;

use errs::Err;
use hashbrown;

use crate::{AsyncGroup, DataConn, DataConnContainer, DataSrc, DataSrcContainer};

impl<S, C> DataSrcContainer<S, C>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    pub(crate) fn new(local: bool, name: String, data_src: S) -> Self {
        Self {
            drop_fn: drop_data_src::<S, C>,

            setup_fn: setup_data_src::<S, C>,
            close_fn: close_data_src::<S, C>,
            create_data_conn_fn: create_data_conn::<S, C>,
            is_data_conn_fn: is_data_conn::<C>,

            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            local,
            name,
            data_src,
        }
    }
}

fn drop_data_src<S, C>(ptr: *const DataSrcContainer)
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S, C>;
    drop(unsafe { Box::from_raw(typed_ptr) });
}

fn setup_data_src<S, C>(ptr: *const DataSrcContainer, ag: &mut AsyncGroup) -> Result<(), Err>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S, C>;
    unsafe { (*typed_ptr).data_src.setup(ag) }
}

fn close_data_src<S, C>(ptr: *const DataSrcContainer)
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S, C>;
    unsafe { (*typed_ptr).data_src.close() }
}

fn create_data_conn<S, C>(ptr: *const DataSrcContainer) -> Result<Box<DataConnContainer<C>>, Err>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S, C>;
    let conn: Box<C> = unsafe { (*typed_ptr).data_src.create_data_conn() }?;
    let name = unsafe { &(*typed_ptr).name };
    Ok(Box::new(DataConnContainer::<C>::new(name, conn)))
}

fn is_data_conn<C>(type_id: any::TypeId) -> bool
where
    C: DataConn + 'static,
{
    any::TypeId::of::<C>() == type_id
}

pub(crate) struct DataSrcList {
    not_setup_head: *mut DataSrcContainer,
    not_setup_last: *mut DataSrcContainer,
    did_setup_head: *mut DataSrcContainer,
    did_setup_last: *mut DataSrcContainer,
    local: bool,
}

impl DataSrcList {
    pub(crate) const fn new(local: bool) -> Self {
        Self {
            not_setup_head: ptr::null_mut(),
            not_setup_last: ptr::null_mut(),
            did_setup_head: ptr::null_mut(),
            did_setup_last: ptr::null_mut(),
            local,
        }
    }

    fn append_container_ptr_not_setup(&mut self, ptr: *mut DataSrcContainer) {
        unsafe {
            (*ptr).next = ptr::null_mut();
        }

        if self.not_setup_last.is_null() {
            self.not_setup_head = ptr;
            self.not_setup_last = ptr;
            unsafe {
                (*ptr).prev = ptr::null_mut();
            }
        } else {
            unsafe {
                (*self.not_setup_last).next = ptr;
                (*ptr).prev = self.not_setup_last;
            }
            self.not_setup_last = ptr;
        }
    }

    fn remove_container_ptr_not_setup(&mut self, ptr: *mut DataSrcContainer) {
        let prev = unsafe { (*ptr).prev };
        let next = unsafe { (*ptr).next };

        if prev.is_null() && next.is_null() {
            self.not_setup_head = ptr::null_mut();
            self.not_setup_last = ptr::null_mut();
        } else if prev.is_null() {
            unsafe { (*next).prev = ptr::null_mut() };
            self.not_setup_head = next;
        } else if next.is_null() {
            unsafe { (*prev).next = ptr::null_mut() };
            self.not_setup_last = prev;
        } else {
            unsafe {
                (*next).prev = prev;
            }
            unsafe {
                (*prev).next = next;
            }
        }
    }

    fn append_container_ptr_did_setup(&mut self, ptr: *mut DataSrcContainer) {
        unsafe {
            (*ptr).next = ptr::null_mut();
        }

        if self.did_setup_last.is_null() {
            self.did_setup_head = ptr;
            self.did_setup_last = ptr;
            unsafe {
                (*ptr).prev = ptr::null_mut();
            }
        } else {
            unsafe {
                (*self.did_setup_last).next = ptr;
                (*ptr).prev = self.did_setup_last;
            }
            self.did_setup_last = ptr;
        }
    }

    pub(crate) fn remove_container_ptr_did_setup(&mut self, ptr: *mut DataSrcContainer) {
        let prev = unsafe { (*ptr).prev };
        let next = unsafe { (*ptr).next };

        if prev.is_null() && next.is_null() {
            self.did_setup_head = ptr::null_mut();
            self.did_setup_last = ptr::null_mut();
        } else if prev.is_null() {
            unsafe { (*next).prev = ptr::null_mut() };
            self.did_setup_head = next;
        } else if next.is_null() {
            unsafe { (*prev).next = ptr::null_mut() };
            self.did_setup_last = prev;
        } else {
            unsafe {
                (*next).prev = prev;
                (*prev).next = next;
            }
        }
    }

    pub(crate) fn copy_container_ptrs_did_setup_into(
        &self,
        map: &mut hashbrown::HashMap<String, *mut DataSrcContainer>,
    ) {
        let mut ptr = self.did_setup_head;
        while !ptr.is_null() {
            let next = unsafe { (*ptr).next };
            let name = unsafe { &(*ptr).name };
            map.insert(name.to_string(), ptr);
            ptr = next;
        }
    }

    pub(crate) fn add_data_src<S, C>(&mut self, name: String, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        let boxed = Box::new(DataSrcContainer::<S, C>::new(self.local, name, ds));
        let typed_ptr = Box::into_raw(boxed);
        let ptr = typed_ptr.cast::<DataSrcContainer>();
        self.append_container_ptr_not_setup(ptr);
    }

    pub(crate) fn setup_data_srcs(&mut self) -> HashMap<String, Err> {
        let mut err_map = HashMap::new();

        if self.not_setup_head.is_null() {
            return err_map;
        }

        let mut ag = AsyncGroup::new();

        let mut ptr = self.not_setup_head;
        while !ptr.is_null() {
            let setup_fn = unsafe { (*ptr).setup_fn };
            let next = unsafe { (*ptr).next };
            ag.name = unsafe { &(*ptr).name };
            if let Err(err) = setup_fn(ptr, &mut ag) {
                err_map.insert(ag.name.to_string(), err);
            }
            ptr = next;
        }

        ag.join_and_put_errors_into(&mut err_map);

        let mut ptr = self.not_setup_head;
        while !ptr.is_null() {
            let next = unsafe { (*ptr).next };
            let name = unsafe { &(*ptr).name };
            self.remove_container_ptr_not_setup(ptr);
            if !err_map.contains_key(name) {
                self.append_container_ptr_did_setup(ptr);
            } else {
                let drop_fn = unsafe { (*ptr).drop_fn };
                drop_fn(ptr);
            }
            ptr = next;
        }

        err_map
    }

    pub(crate) fn close_data_srcs(&mut self) {
        if self.did_setup_last.is_null() {
            return;
        }

        let mut ptr = self.did_setup_last;
        while !ptr.is_null() {
            let close_fn = unsafe { (*ptr).close_fn };
            let prev = unsafe { (*ptr).prev };
            close_fn(ptr);
            ptr = prev;
        }
    }
}

impl Drop for DataSrcList {
    fn drop(&mut self) {
        let mut ptr = self.did_setup_last;
        while !ptr.is_null() {
            let drop_fn = unsafe { (*ptr).drop_fn };
            let prev = unsafe { (*ptr).prev };
            drop_fn(ptr);
            ptr = prev;
        }
        let mut ptr = self.not_setup_last;
        while !ptr.is_null() {
            let drop_fn = unsafe { (*ptr).drop_fn };
            let prev = unsafe { (*ptr).prev };
            drop_fn(ptr);
            ptr = prev;
        }
        self.not_setup_head = ptr::null_mut();
        self.not_setup_last = ptr::null_mut();
        self.did_setup_head = ptr::null_mut();
        self.did_setup_last = ptr::null_mut();
    }
}

#[cfg(test)]
mod tests_of_data_src {
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
        will_fail: bool,
        logger: Arc<Mutex<Logger>>,
    }

    impl SyncDataSrc {
        fn new(logger: Arc<Mutex<Logger>>, will_fail: bool) -> Self {
            Self {
                will_fail,
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
            if self.will_fail {
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
            let conn = SyncDataConn::new(self.logger.clone(), false);
            Ok(Box::new(conn))
        }
    }

    struct AsyncDataSrc {
        logger: Arc<Mutex<Logger>>,
        will_fail: bool,
    }

    impl AsyncDataSrc {
        fn new(logger: Arc<Mutex<Logger>>, will_fail: bool) -> Self {
            Self {
                logger: logger,
                will_fail,
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
            let will_fail = self.will_fail;
            let logger = self.logger.clone();

            ag.add(async move || {
                // The `.await` mut be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;

                if will_fail {
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
            let conn = AsyncDataConn::new(self.logger.clone(), false);
            Ok(Box::new(conn))
        }
    }

    struct SyncDataConn {
        committed: bool,
        will_fail: bool,
        logger: Arc<Mutex<Logger>>,
    }

    impl SyncDataConn {
        fn new(logger: Arc<Mutex<Logger>>, will_fail: bool) -> Self {
            Self {
                committed: false,
                will_fail,
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
            if self.will_fail {
                logger.log("SyncDataConn failed to commit");
                return Err(Err::new("XXX".to_string()));
            }
            self.committed = true;
            logger.log("SyncDataConn committed");
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.log("SyncDataConn post-committed");
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
        will_fail: bool,
    }

    impl AsyncDataConn {
        fn new(logger: Arc<Mutex<Logger>>, will_fail: bool) -> Self {
            Self {
                committed: Arc::new(Mutex::new(false)),
                logger: logger,
                will_fail,
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
            let will_fail = self.will_fail;
            let logger = self.logger.clone();
            let committed = self.committed.clone();

            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;

                if will_fail {
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
                logger.lock().unwrap().log("AsyncDataConn post-committed");
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

    mod tests_of_data_src_list {
        use super::*;

        #[test]
        fn test_of_new() {
            let ds_list = DataSrcList::new(false);
            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());
        }

        #[test]
        fn test_of_append_container_ptr_not_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Logger::new()));

            let ds1 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr1);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr::null_mut());

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr2);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr::null_mut());

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_head_container_ptr_not_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Logger::new()));

            let ds1 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_not_setup(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr2);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr2).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_middle_container_ptr_not_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Logger::new()));

            let ds1 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_not_setup(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr1);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_last_container_ptr_not_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Logger::new()));

            let ds1 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_not_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr2);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_all_container_ptr_not_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Logger::new()));

            let ds1 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_not_setup(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr2);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr2).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_not_setup(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr3);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr3).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_not_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());
        }

        #[test]
        fn test_of_append_container_ptr_did_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Logger::new()));

            let ds1 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr1);
            assert_eq!(ds_list.did_setup_last, ptr1);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr::null_mut());

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr1);
            assert_eq!(ds_list.did_setup_last, ptr2);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr::null_mut());

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr1);
            assert_eq!(ds_list.did_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_middle_container_ptr_did_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Logger::new()));

            let ds1 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr1);
            assert_eq!(ds_list.did_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_did_setup(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr1);
            assert_eq!(ds_list.did_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr1);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_last_container_ptr_did_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Logger::new()));

            let ds1 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr1);
            assert_eq!(ds_list.did_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_did_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr1);
            assert_eq!(ds_list.did_setup_last, ptr2);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_all_container_ptr_did_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Logger::new()));

            let ds1 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr1);
            assert_eq!(ds_list.did_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_did_setup(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr2);
            assert_eq!(ds_list.did_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr2).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_did_setup(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr3);
            assert_eq!(ds_list.did_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr3).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_container_ptr_did_setup(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.did_setup_head, ptr::null_mut());
            assert_eq!(ds_list.did_setup_last, ptr::null_mut());
        }

        #[test]
        fn test_of_copy_container_ptrs_did_setup_into() {
            let mut ds_list = DataSrcList::new(false);

            let mut map = hashbrown::HashMap::<String, *mut DataSrcContainer>::new();
            ds_list.copy_container_ptrs_did_setup_into(&mut map);
            assert_eq!(map.len(), 0);

            let logger = Arc::new(Mutex::new(Logger::new()));

            let ds1 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr3);

            let mut map = hashbrown::HashMap::<String, *mut DataSrcContainer>::new();
            ds_list.copy_container_ptrs_did_setup_into(&mut map);
            assert_eq!(map.len(), 3);
            assert_eq!(map.get("foo").unwrap(), &ptr1);
            assert_eq!(map.get("bar").unwrap(), &ptr2);
            assert_eq!(map.get("baz").unwrap(), &ptr3);
        }

        #[test]
        fn test_setup_and_create_data_conn_and_close() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            {
                let mut data_src_list = DataSrcList::new(false);

                let ds_async = AsyncDataSrc::new(logger.clone(), false);
                data_src_list.add_data_src("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(logger.clone(), false);
                data_src_list.add_data_src("bar".to_string(), ds_sync);

                let err_map = data_src_list.setup_data_srcs();
                assert!(err_map.is_empty());

                let ptr = data_src_list.did_setup_head;
                let create_fn = unsafe { (*ptr).create_data_conn_fn };
                match create_fn(ptr) {
                    Ok(_) => {}
                    Err(_) => panic!(),
                }

                let ptr = unsafe { (*ptr).next };
                let create_fn = unsafe { (*ptr).create_data_conn_fn };
                match create_fn(ptr) {
                    Ok(_) => {}
                    Err(_) => panic!(),
                }

                data_src_list.close_data_srcs();
            }

            logger.lock().unwrap().assert_logs(&[
                "SyncDataSrc setupped",
                "AsyncDataSrc setupped",
                "AsyncDataSrc created DataConn",
                "SyncDataSrc created DataConn",
                "SyncDataSrc closed",
                "AsyncDataSrc closed",
                "SyncDataSrc dropped",
                "AsyncDataSrc dropped",
            ]);
            logger.lock().unwrap().clear();
        }

        #[test]
        fn test_fail_to_setup_sync_and_close() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            {
                let mut data_src_list = DataSrcList::new(true);

                let ds_async = AsyncDataSrc::new(logger.clone(), false);
                data_src_list.add_data_src("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(logger.clone(), true);
                data_src_list.add_data_src("bar".to_string(), ds_sync);

                let err_map = data_src_list.setup_data_srcs();
                assert_eq!(err_map.len(), 1);

                if let Some(err) = err_map.get("bar") {
                    if let Ok(r) = err.reason::<String>() {
                        assert_eq!(r, "XXX");
                    } else {
                        panic!();
                    }
                } else {
                    panic!();
                }

                data_src_list.close_data_srcs();
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

        #[test]
        fn test_fail_to_setup_async_and_close() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            {
                let mut data_src_list = DataSrcList::new(true);

                let ds_async = AsyncDataSrc::new(logger.clone(), true);
                data_src_list.add_data_src("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(logger.clone(), false);
                data_src_list.add_data_src("bar".to_string(), ds_sync);

                let err_map = data_src_list.setup_data_srcs();
                assert_eq!(err_map.len(), 1);

                if let Some(err) = err_map.get("foo") {
                    if let Ok(r) = err.reason::<String>() {
                        assert_eq!(r, "YYY");
                    } else {
                        panic!();
                    }
                } else {
                    panic!();
                }

                data_src_list.close_data_srcs();
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

        #[test]
        fn test_no_data_src() {
            const LOCAL: bool = true;
            let mut data_src_list = DataSrcList::new(LOCAL);

            let err_map = data_src_list.setup_data_srcs();
            assert!(err_map.is_empty());

            data_src_list.close_data_srcs();
        }
    }
}
