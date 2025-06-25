// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;
use std::collections::HashMap;
use std::ptr;

use errs::Err;

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

    #[cfg(test)]
    pub(crate) fn not_setup_head(&self) -> *mut DataSrcContainer {
        self.not_setup_head
    }

    #[cfg(test)]
    pub(crate) fn did_setup_head(&self) -> *mut DataSrcContainer {
        self.did_setup_head
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

    pub(crate) fn remove_and_drop_local_container_ptr_not_setup_by_name(&mut self, name: &str) {
        let mut ptr = self.not_setup_head;
        while !ptr.is_null() {
            let next = unsafe { (*ptr).next };
            let nm = unsafe { &(*ptr).name };
            let local = unsafe { (*ptr).local };

            if local && nm == name {
                let close_fn = unsafe { (*ptr).close_fn };
                let drop_fn = unsafe { (*ptr).drop_fn };

                self.remove_container_ptr_not_setup(ptr);

                close_fn(ptr);
                drop_fn(ptr);
            }

            ptr = next;
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

    pub(crate) fn remove_and_drop_local_container_ptr_did_setup_by_name(&mut self, name: &str) {
        let mut ptr = self.did_setup_head;
        while !ptr.is_null() {
            let next = unsafe { (*ptr).next };
            let nm = unsafe { &(*ptr).name };
            let local = unsafe { (*ptr).local };

            if local && nm == name {
                let close_fn = unsafe { (*ptr).close_fn };
                let drop_fn = unsafe { (*ptr).drop_fn };

                self.remove_container_ptr_did_setup(ptr);

                close_fn(ptr);
                drop_fn(ptr);
            }

            ptr = next;
        }
    }

    pub(crate) fn copy_container_ptrs_did_setup_into(
        &self,
        map: &mut HashMap<String, *mut DataSrcContainer>,
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
                break;
            }
            ptr = next;
        }

        ag.join_and_put_errors_into(&mut err_map);

        let first_ptr_not_setup_yet = ptr;

        ptr = self.not_setup_head;
        while !ptr.is_null() && ptr != first_ptr_not_setup_yet {
            let next = unsafe { (*ptr).next };
            let name = unsafe { &(*ptr).name };
            if !err_map.contains_key(name) {
                self.remove_container_ptr_not_setup(ptr);
                self.append_container_ptr_did_setup(ptr);
            }
            ptr = next;
        }

        err_map
    }

    pub(crate) fn close_and_drop_data_srcs(&mut self) {
        let mut ptr = self.did_setup_last;
        while !ptr.is_null() {
            let close_fn = unsafe { (*ptr).close_fn };
            let drop_fn = unsafe { (*ptr).drop_fn };
            let prev = unsafe { (*ptr).prev };
            close_fn(ptr);
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

impl Drop for DataSrcList {
    fn drop(&mut self) {
        self.close_and_drop_data_srcs();
    }
}

#[cfg(test)]
mod tests_of_data_src {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct SyncDataSrc {
        id: i8,
        will_fail: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl SyncDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, will_fail: bool) -> Self {
            Self {
                id,
                will_fail,
                logger: logger,
            }
        }
    }

    impl Drop for SyncDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataSrc {} dropped", self.id));
        }
    }

    impl DataSrc<SyncDataConn> for SyncDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            let mut logger = self.logger.lock().unwrap();
            if self.will_fail {
                logger.push(format!("SyncDataSrc {} failed to setup", self.id));
                return Err(Err::new("XXX".to_string()));
            }
            logger.push(format!("SyncDataSrc {} setupped", self.id));
            Ok(())
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataSrc {} closed", self.id));
        }

        fn create_data_conn(&mut self) -> Result<Box<SyncDataConn>, Err> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataSrc {} created DataConn", self.id));
            let conn = SyncDataConn::new();
            Ok(Box::new(conn))
        }
    }

    struct AsyncDataSrc {
        id: i8,
        will_fail: bool,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl AsyncDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, will_fail: bool) -> Self {
            Self {
                id,
                will_fail,
                logger,
            }
        }
    }

    impl Drop for AsyncDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataSrc {} dropped", self.id));
        }
    }

    impl DataSrc<AsyncDataConn> for AsyncDataSrc {
        fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> {
            let logger = self.logger.clone();
            let will_fail = self.will_fail;
            let id = self.id;
            ag.add(async move || {
                let mut logger = logger.lock().unwrap();
                if will_fail {
                    logger.push(format!("AsyncDataSrc {} failed to setup", id));
                    return Err(Err::new("XXX".to_string()));
                }
                logger.push(format!("AsyncDataSrc {} setupped", id));
                Ok(())
            });
            Ok(())
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataSrc {} closed", self.id));
        }

        fn create_data_conn(&mut self) -> Result<Box<AsyncDataConn>, Err> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataSrc {} created DataConn", self.id));
            let conn = AsyncDataConn::new();
            Ok(Box::new(conn))
        }
    }

    struct SyncDataConn {}

    impl SyncDataConn {
        fn new() -> Self {
            Self {}
        }
    }

    impl DataConn for SyncDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            Ok(())
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {}
        fn close(&mut self) {}
    }

    struct AsyncDataConn {}

    impl AsyncDataConn {
        fn new() -> Self {
            Self {}
        }
    }

    impl DataConn for AsyncDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            Ok(())
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {}
        fn close(&mut self) {}
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

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
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

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
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

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
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

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
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

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
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

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
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

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_not_setup(ptr2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
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

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
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

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
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

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
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
        fn test_of_remove_head_container_ptr_did_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
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
        }

        #[test]
        fn test_of_remove_middle_container_ptr_did_setup() {
            let mut ds_list = DataSrcList::new(false);

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
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

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
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

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
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

            let mut map = HashMap::<String, *mut DataSrcContainer>::new();
            ds_list.copy_container_ptrs_did_setup_into(&mut map);
            assert_eq!(map.len(), 0);

            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "foo".to_string(),
                ds1,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr1 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_container_ptr_did_setup(ptr3);

            let mut map = HashMap::<String, *mut DataSrcContainer>::new();
            ds_list.copy_container_ptrs_did_setup_into(&mut map);
            assert_eq!(map.len(), 3);
            assert_eq!(map.get("foo").unwrap(), &ptr1);
            assert_eq!(map.get("bar").unwrap(), &ptr2);
            assert_eq!(map.get("baz").unwrap(), &ptr3);
        }

        #[test]
        fn test_setup_and_create_data_conn_and_close() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data_src_list = DataSrcList::new(false);

                let ds_async = AsyncDataSrc::new(1, logger.clone(), false);
                data_src_list.add_data_src("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(2, logger.clone(), false);
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

                data_src_list.close_and_drop_data_srcs();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 setupped",
                    "AsyncDataSrc 1 created DataConn",
                    "SyncDataSrc 2 created DataConn",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                ]
            );
        }

        #[test]
        fn test_fail_to_setup_sync_and_close() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data_src_list = DataSrcList::new(true);

                let ds_async = AsyncDataSrc::new(1, logger.clone(), false);
                data_src_list.add_data_src("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(2, logger.clone(), true);
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

                data_src_list.close_and_drop_data_srcs();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 failed to setup",
                    "AsyncDataSrc 1 setupped",
                    "AsyncDataSrc 1 closed",
                    "AsyncDataSrc 1 dropped",
                    "SyncDataSrc 2 dropped",
                ],
            );
        }

        #[test]
        fn test_fail_to_setup_async_and_close() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data_src_list = DataSrcList::new(true);

                let ds_async = AsyncDataSrc::new(1, logger.clone(), true);
                data_src_list.add_data_src("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(2, logger.clone(), false);
                data_src_list.add_data_src("bar".to_string(), ds_sync);

                let err_map = data_src_list.setup_data_srcs();
                assert_eq!(err_map.len(), 1);

                if let Some(err) = err_map.get("foo") {
                    if let Ok(r) = err.reason::<String>() {
                        assert_eq!(r, "XXX");
                    } else {
                        panic!();
                    }
                } else {
                    panic!();
                }

                data_src_list.close_and_drop_data_srcs();
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc 2 setupped",
                    "AsyncDataSrc 1 failed to setup",
                    "SyncDataSrc 2 closed",
                    "SyncDataSrc 2 dropped",
                    "AsyncDataSrc 1 dropped",
                ],
            );
        }

        #[test]
        fn test_no_data_src() {
            const LOCAL: bool = true;
            let mut data_src_list = DataSrcList::new(LOCAL);

            let err_map = data_src_list.setup_data_srcs();
            assert!(err_map.is_empty());

            data_src_list.close_and_drop_data_srcs();

            assert!(data_src_list.not_setup_head.is_null());
            assert!(data_src_list.did_setup_head.is_null());
        }
    }
}
