// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;
use std::collections::HashMap;
use std::ptr;

use errs::Err;

use crate::async_group::AsyncGroup;
use crate::data_conn::{DataConn, NoopDataConn};

/// Is the enum type that represents the possible reasons for errors that can occur during data
/// source operations for external data services.
#[derive(Debug)]
pub enum DataSrcError {
    /// Indicates a failure during type casting when attempting to retrieve a struct that
    /// implements the `DataConn` trait.
    FailToCastDataConn {
        /// The name of the data service associated with the `DataConn` object for which the type
        /// cast failed.
        name: String,

        /// The name of the data type to which the cast was attempted.
        cast_to_type: &'static str,
    },

    /// Indicates that no `DataSrc` object was found--either globally or session-locally
    /// registered--that is associated with the specified name and capable of creating the
    /// requested `DataConn` instance.
    NoDataSrcToCreateDataConn {
        /// The name of the data service associated with the `DataSrc` object.
        name: String,

        /// The type name of the `DataConn` trait's implementing struct that was requested.
        data_conn_type: &'static str,
    },
}

/// Is the trait abstracts over data sources that provide connections to external data services
/// such as databases, file systems, or messaging services.
///
/// It handles the configuration needed to connect to these services and provides a way to create
/// per-session connections.
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    /// Performs global setup for the data source.
    /// This might involve establishing global connections or preparing resources needed for
    /// creating session-specific connections.
    ///
    /// # Parameters
    /// - `ag`: An `AsyncGroup` that can be used to manage asynchronous setup operations, especially
    ///         when the setup process is time-consuming and can be performed asynchronously.
    fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err>;

    /// Performs cleanup operations for the data source, such as closing global connections
    /// or releasing resources.
    fn close(&mut self);

    /// Creates a new data connection for a specific session.
    /// Each call to this method should return a fresh connection that can be used independently.
    ///
    /// # Returns
    /// - `Ok(Box<dyn DataConn>)`: A data connection used in this session.
    /// - `Err(Err)`: An `Err` object that holds the reason for the failure.
    fn create_data_conn(&mut self) -> Result<Box<C>, Err>;
}

pub(crate) struct NoopDataSrc {}

#[allow(dead_code)]
impl DataSrc<NoopDataConn> for NoopDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn close(&mut self) {}
    fn create_data_conn(&mut self) -> Result<Box<NoopDataConn>, Err> {
        Ok(Box::new(NoopDataConn {}))
    }
}

#[repr(C)]
pub(crate) struct DataSrcContainer<S = NoopDataSrc, C = NoopDataConn>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    drop_fn: fn(*const DataSrcContainer),
    setup_fn: fn(*const DataSrcContainer, ag: &mut AsyncGroup) -> Result<(), Err>,
    close_fn: fn(*const DataSrcContainer),
    create_data_conn_fn: fn(*const DataSrcContainer) -> Result<Box<C>, Err>,
    is_data_conn_fn: fn(any::TypeId) -> bool,

    prev: *mut DataSrcContainer,
    next: *mut DataSrcContainer,
    local: bool,
    name: String,

    data_src: S,
}

impl<S, C> DataSrcContainer<S, C>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    fn new(local: bool, name: String, data_src: S) -> Self {
        Self {
            drop_fn: drop_data_src::<S, C>,
            setup_fn: setup_data_src::<S, C>,
            close_fn: close_data_src::<S, C>,
            create_data_conn_fn: create_data_conn_from_data_src::<S, C>,
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

fn create_data_conn_from_data_src<S, C>(ptr: *const DataSrcContainer) -> Result<Box<C>, Err>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S, C>;
    unsafe { (*typed_ptr).data_src.create_data_conn() }
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
    has_setup_head: *mut DataSrcContainer,
    has_setup_last: *mut DataSrcContainer,
    local: bool,
}

impl DataSrcList {
    pub(crate) const fn new(local: bool) -> Self {
        Self {
            not_setup_head: ptr::null_mut(),
            not_setup_last: ptr::null_mut(),
            has_setup_head: ptr::null_mut(),
            has_setup_last: ptr::null_mut(),
            local,
        }
    }

    fn append_not_setup_ptr(&mut self, ptr: *mut DataSrcContainer) {
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

    fn remove_not_setup_ptr(&mut self, ptr: *mut DataSrcContainer) {
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

    fn append_has_setup_ptr(&mut self, ptr: *mut DataSrcContainer) {
        unsafe {
            (*ptr).next = ptr::null_mut();
        }

        if self.has_setup_last.is_null() {
            self.has_setup_head = ptr;
            self.has_setup_last = ptr;
            unsafe {
                (*ptr).prev = ptr::null_mut();
            }
        } else {
            unsafe {
                (*self.has_setup_last).next = ptr;
                (*ptr).prev = self.has_setup_last;
            }
            self.has_setup_last = ptr;
        }
    }

    fn remove_has_setup_ptr(&mut self, ptr: *mut DataSrcContainer) {
        let prev = unsafe { (*ptr).prev };
        let next = unsafe { (*ptr).next };

        if prev.is_null() && next.is_null() {
            self.has_setup_head = ptr::null_mut();
            self.has_setup_last = ptr::null_mut();
        } else if prev.is_null() {
            unsafe { (*next).prev = ptr::null_mut() };
            self.has_setup_head = next;
        } else if next.is_null() {
            unsafe { (*prev).next = ptr::null_mut() };
            self.has_setup_last = prev;
        } else {
            unsafe {
                (*next).prev = prev;
            }
            unsafe {
                (*prev).next = next;
            }
        }
    }

    pub(crate) fn add<S, C>(&mut self, name: String, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        let boxed = Box::new(DataSrcContainer::<S, C>::new(self.local, name, ds));
        let typed_ptr = Box::into_raw(boxed);
        let ptr = typed_ptr.cast::<DataSrcContainer>();
        self.append_not_setup_ptr(ptr);
    }

    pub(crate) fn setup(&mut self) -> HashMap<String, Err> {
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

        ag.join(&mut err_map);

        let mut ptr = self.not_setup_head;
        while !ptr.is_null() {
            let next = unsafe { (*ptr).next };
            let name = unsafe { &(*ptr).name };
            self.remove_not_setup_ptr(ptr);
            if !err_map.contains_key(name) {
                self.append_has_setup_ptr(ptr);
            } else {
                let drop_fn = unsafe { (*ptr).drop_fn };
                drop_fn(ptr);
            }
            ptr = next;
        }

        err_map
    }

    pub(crate) fn close(&mut self) {
        if self.has_setup_last.is_null() {
            return;
        }

        let mut ptr = self.has_setup_last;
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
        let mut ptr = self.has_setup_last;
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
        self.has_setup_head = ptr::null_mut();
        self.has_setup_last = ptr::null_mut();
    }
}

pub(crate) struct DataSrcMap {
    map: HashMap<String, *mut DataSrcContainer>,
}

impl DataSrcMap {
    pub(crate) fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub(crate) fn copy_from(&mut self, data_src_list: &DataSrcList) {
        let mut ptr = data_src_list.has_setup_head;
        while !ptr.is_null() {
            let next = unsafe { (*ptr).next };
            let name = unsafe { &(*ptr).name };
            self.map.insert(name.to_string(), ptr);
            ptr = next;
        }
    }

    pub(crate) fn remove(&mut self, name: &str, data_src_list: &mut DataSrcList) {
        if let Some(ptr) = self.map.remove(name) {
            let close_fn = unsafe { (*ptr).close_fn };
            let drop_fn = unsafe { (*ptr).drop_fn };

            data_src_list.remove_has_setup_ptr(ptr);

            close_fn(ptr);
            drop_fn(ptr);
        }
    }

    pub(crate) fn create_data_conn<C>(&mut self, name: &str) -> Result<&C, Err>
    where
        C: DataConn + 'static,
    {
        if let Some(ptr) = self.map.get_mut(name) {
            let type_id = any::TypeId::of::<C>();
            let is_fn = unsafe { (*(*ptr)).is_data_conn_fn };
            if !is_fn(type_id) {
                return Err(Err::new(DataSrcError::FailToCastDataConn {
                    name: name.to_string(),
                    cast_to_type: any::type_name::<C>(),
                }));
            }

            let create_fn = unsafe { (*(*ptr)).create_data_conn_fn };
            let conn = create_fn(*ptr)?;

            let ptr = Box::into_raw(conn);
            let typed_ptr = ptr as *mut C;
            return Ok(unsafe { &(*typed_ptr) });
        }

        Err(Err::new(DataSrcError::NoDataSrcToCreateDataConn {
            name: name.to_string(),
            data_conn_type: any::type_name::<C>(),
        }))
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
        fn is_committed(&self) -> bool {
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
        fn is_committed(&self) -> bool {
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
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());
        }

        #[test]
        fn test_of_append_not_setup() {
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

            ds_list.append_not_setup_ptr(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr1);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

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

            ds_list.append_not_setup_ptr(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr2);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

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

            ds_list.append_not_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_head_of_not_setup() {
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

            ds_list.append_not_setup_ptr(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_not_setup_ptr(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_not_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_not_setup_ptr(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr2);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr2).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_middle_of_not_setup() {
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

            ds_list.append_not_setup_ptr(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_not_setup_ptr(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_not_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_not_setup_ptr(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr1);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_last_of_not_setup() {
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

            ds_list.append_not_setup_ptr(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_not_setup_ptr(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_not_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_not_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr2);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_all_of_not_setup() {
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

            ds_list.append_not_setup_ptr(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_not_setup_ptr(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_not_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr1);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_not_setup_ptr(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr2);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr2).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_not_setup_ptr(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr3);
            assert_eq!(ds_list.not_setup_last, ptr3);
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());

            assert_eq!(unsafe { (*ptr3).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_not_setup_ptr(ptr3);
            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());
        }

        #[test]
        fn test_of_append_has_setup() {
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

            ds_list.append_has_setup_ptr(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr1);
            assert_eq!(ds_list.has_setup_last, ptr1);

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

            ds_list.append_has_setup_ptr(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr1);
            assert_eq!(ds_list.has_setup_last, ptr2);

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

            ds_list.append_has_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr1);
            assert_eq!(ds_list.has_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_head_of_has_setup() {
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

            ds_list.append_has_setup_ptr(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_has_setup_ptr(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_has_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr1);
            assert_eq!(ds_list.has_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_has_setup_ptr(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr2);
            assert_eq!(ds_list.has_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr2).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_middle_of_has_setup() {
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

            ds_list.append_has_setup_ptr(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_has_setup_ptr(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_has_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr1);
            assert_eq!(ds_list.has_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_has_setup_ptr(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr1);
            assert_eq!(ds_list.has_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr1);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_last_of_has_setup() {
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

            ds_list.append_has_setup_ptr(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_has_setup_ptr(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_has_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr1);
            assert_eq!(ds_list.has_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_has_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr1);
            assert_eq!(ds_list.has_setup_last, ptr2);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr::null_mut());
        }

        #[test]
        fn test_of_remove_all_of_has_setup() {
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

            ds_list.append_has_setup_ptr(ptr1);

            let ds2 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "bar".to_string(),
                ds2,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr2 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_has_setup_ptr(ptr2);

            let ds3 = SyncDataSrc::new(logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::<SyncDataSrc, SyncDataConn>::new(
                false,
                "baz".to_string(),
                ds3,
            ));
            let typed_ptr = Box::into_raw(boxed);
            let ptr3 = typed_ptr.cast::<DataSrcContainer>();

            ds_list.append_has_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr1);
            assert_eq!(ds_list.has_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr1).next }, ptr2);
            assert_eq!(unsafe { (*ptr2).prev }, ptr1);
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_has_setup_ptr(ptr1);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr2);
            assert_eq!(ds_list.has_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr2).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr2).next }, ptr3);
            assert_eq!(unsafe { (*ptr3).prev }, ptr2);
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_has_setup_ptr(ptr2);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr3);
            assert_eq!(ds_list.has_setup_last, ptr3);

            assert_eq!(unsafe { (*ptr3).prev }, ptr::null_mut());
            assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());

            ds_list.remove_has_setup_ptr(ptr3);

            assert_eq!(ds_list.local, false);
            assert_eq!(ds_list.not_setup_head, ptr::null_mut());
            assert_eq!(ds_list.not_setup_last, ptr::null_mut());
            assert_eq!(ds_list.has_setup_head, ptr::null_mut());
            assert_eq!(ds_list.has_setup_last, ptr::null_mut());
        }

        fn setup_and_create_data_conn_and_close(logger: Arc<Mutex<Logger>>) {
            const LOCAL: bool = false;
            let mut data_src_list = DataSrcList::new(LOCAL);

            let ds_async = AsyncDataSrc::new(logger.clone(), false);
            data_src_list.add("foo".to_string(), ds_async);

            let ds_sync = SyncDataSrc::new(logger.clone(), false);
            data_src_list.add("bar".to_string(), ds_sync);

            let err_map = data_src_list.setup();
            assert!(err_map.is_empty());

            let ptr = data_src_list.has_setup_head;
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

            data_src_list.close();
        }

        #[test]
        fn test_setup_and_create_data_conn_and_close() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            setup_and_create_data_conn_and_close(logger.clone());

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

        fn fail_to_setup_sync_and_close(logger: Arc<Mutex<Logger>>) {
            const LOCAL: bool = true;
            let mut data_src_list = DataSrcList::new(LOCAL);

            let ds_async = AsyncDataSrc::new(logger.clone(), false);
            data_src_list.add("foo".to_string(), ds_async);

            let ds_sync = SyncDataSrc::new(logger.clone(), true);
            data_src_list.add("bar".to_string(), ds_sync);

            let err_map = data_src_list.setup();
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

            data_src_list.close();
        }

        #[test]
        fn test_fail_to_setup_sync_and_close() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            fail_to_setup_sync_and_close(logger.clone());

            logger.lock().unwrap().assert_logs(&[
                "SyncDataSrc failed to setup",
                "AsyncDataSrc setupped",
                "SyncDataSrc dropped",
                "AsyncDataSrc closed",
                "AsyncDataSrc dropped",
            ]);
            logger.lock().unwrap().clear();
        }

        fn fail_to_setup_async_and_close(logger: Arc<Mutex<Logger>>) {
            const LOCAL: bool = true;
            let mut data_src_list = DataSrcList::new(LOCAL);

            let ds_async = AsyncDataSrc::new(logger.clone(), true);
            data_src_list.add("foo".to_string(), ds_async);

            let ds_sync = SyncDataSrc::new(logger.clone(), false);
            data_src_list.add("bar".to_string(), ds_sync);

            let err_map = data_src_list.setup();
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

            data_src_list.close();
        }

        #[test]
        fn test_fail_to_setup_async_and_close() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            fail_to_setup_async_and_close(logger.clone());

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

            let err_map = data_src_list.setup();
            assert!(err_map.is_empty());

            data_src_list.close();
        }
    }

    mod tests_of_data_src_map {
        use super::*;

        #[test]
        fn test_new() {
            let data_src_map = DataSrcMap::new();
            assert_eq!(data_src_map.map.len(), 0);

            const LOCAL: bool = true;
            let _data_src_list = DataSrcList::new(LOCAL);
        }

        #[test]
        fn test_dont_copy_ds_not_setupped() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            {
                const LOCAL: bool = true;
                let mut data_src_list = DataSrcList::new(LOCAL);
                let mut data_src_map = DataSrcMap::new();

                let ds_async = AsyncDataSrc::new(logger.clone(), false);
                data_src_list.add("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(logger.clone(), false);
                data_src_list.add("bar".to_string(), ds_sync);

                data_src_map.copy_from(&data_src_list);

                assert_eq!(data_src_map.map.len(), 0);

                data_src_list.close();
            }

            logger
                .lock()
                .unwrap()
                .assert_logs(&["SyncDataSrc dropped", "AsyncDataSrc dropped"]);
            logger.lock().unwrap().clear();
        }

        #[test]
        fn test_copy_from() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            {
                const LOCAL: bool = true;
                let mut data_src_list = DataSrcList::new(LOCAL);
                let mut data_src_map = DataSrcMap::new();

                let ds_async = AsyncDataSrc::new(logger.clone(), false);
                data_src_list.add("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(logger.clone(), false);
                data_src_list.add("bar".to_string(), ds_sync);

                let err_map = data_src_list.setup();
                assert_eq!(err_map.len(), 0);

                data_src_map.copy_from(&data_src_list);

                assert_eq!(data_src_map.map.len(), 2);
                assert!(data_src_map.map.contains_key("foo"));
                assert!(data_src_map.map.contains_key("bar"));

                data_src_list.close();
            }

            logger.lock().unwrap().assert_logs(&[
                "SyncDataSrc setupped",
                "AsyncDataSrc setupped",
                "SyncDataSrc closed",
                "AsyncDataSrc closed",
                "SyncDataSrc dropped",
                "AsyncDataSrc dropped",
            ]);
            logger.lock().unwrap().clear();
        }

        #[test]
        fn test_remove() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            {
                const LOCAL: bool = true;
                let mut data_src_list = DataSrcList::new(LOCAL);
                let mut data_src_map = DataSrcMap::new();

                let ds_async = AsyncDataSrc::new(logger.clone(), false);
                data_src_list.add("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(logger.clone(), false);
                data_src_list.add("bar".to_string(), ds_sync);

                let err_map = data_src_list.setup();
                assert_eq!(err_map.len(), 0);

                data_src_map.copy_from(&data_src_list);
                assert_eq!(data_src_map.map.len(), 2);

                data_src_map.remove("foo", &mut data_src_list);
                assert_eq!(data_src_map.map.len(), 1);

                data_src_map.remove("bar", &mut data_src_list);
                assert_eq!(data_src_map.map.len(), 0);

                logger.lock().unwrap().log("--");

                data_src_list.close();
            }

            logger.lock().unwrap().assert_logs(&[
                "SyncDataSrc setupped",
                "AsyncDataSrc setupped",
                "AsyncDataSrc closed",
                "AsyncDataSrc dropped",
                "SyncDataSrc closed",
                "SyncDataSrc dropped",
                "--",
            ]);
            logger.lock().unwrap().clear();
        }

        #[test]
        fn test_remove_but_no_ds() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            {
                const LOCAL: bool = true;
                let mut data_src_list = DataSrcList::new(LOCAL);
                let mut data_src_map = DataSrcMap::new();

                let ds_async = AsyncDataSrc::new(logger.clone(), false);
                data_src_list.add("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(logger.clone(), false);
                data_src_list.add("bar".to_string(), ds_sync);

                let err_map = data_src_list.setup();
                assert_eq!(err_map.len(), 0);

                data_src_map.copy_from(&data_src_list);
                assert_eq!(data_src_map.map.len(), 2);

                data_src_map.remove("baz", &mut data_src_list);
                assert_eq!(data_src_map.map.len(), 2);

                logger.lock().unwrap().log("--");

                data_src_list.close();
            }

            logger.lock().unwrap().assert_logs(&[
                "SyncDataSrc setupped",
                "AsyncDataSrc setupped",
                "--",
                "SyncDataSrc closed",
                "AsyncDataSrc closed",
                "SyncDataSrc dropped",
                "AsyncDataSrc dropped",
            ]);
            logger.lock().unwrap().clear();
        }

        #[test]
        fn test_create_data_conn() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            {
                const LOCAL: bool = true;
                let mut data_src_list = DataSrcList::new(LOCAL);
                let mut data_src_map = DataSrcMap::new();

                let ds_async = AsyncDataSrc::new(logger.clone(), false);
                data_src_list.add("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(logger.clone(), false);
                data_src_list.add("bar".to_string(), ds_sync);

                let err_map = data_src_list.setup();
                assert_eq!(err_map.len(), 0);

                data_src_map.copy_from(&data_src_list);

                assert_eq!(data_src_map.map.len(), 2);
                assert!(data_src_map.map.contains_key("foo"));
                assert!(data_src_map.map.contains_key("bar"));

                match data_src_map.create_data_conn::<AsyncDataConn>("foo") {
                    Ok(_) => {}
                    Err(_) => panic!(),
                }

                match data_src_map.create_data_conn::<SyncDataConn>("bar") {
                    Ok(_) => {}
                    Err(_) => panic!(),
                }

                data_src_list.close();
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
        fn test_create_data_conn_but_fail_to_cast() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            {
                const LOCAL: bool = true;
                let mut data_src_list = DataSrcList::new(LOCAL);
                let mut data_src_map = DataSrcMap::new();

                let ds_async = AsyncDataSrc::new(logger.clone(), false);
                data_src_list.add("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(logger.clone(), false);
                data_src_list.add("bar".to_string(), ds_sync);

                let err_map = data_src_list.setup();
                assert_eq!(err_map.len(), 0);

                data_src_map.copy_from(&data_src_list);

                assert_eq!(data_src_map.map.len(), 2);
                assert!(data_src_map.map.contains_key("foo"));
                assert!(data_src_map.map.contains_key("bar"));

                match data_src_map.create_data_conn::<SyncDataConn>("foo") {
                    Ok(_) => panic!(),
                    Err(err) => match err.reason::<DataSrcError>() {
                        Ok(r) => match r {
                            DataSrcError::FailToCastDataConn { name, cast_to_type } => {
                                assert_eq!(name, "foo");
                                assert_eq!(
                                    cast_to_type,
                                    &"sabi::data_src::tests_of_data_src::SyncDataConn"
                                );
                            }
                            _ => panic!(),
                        },
                        Err(_) => panic!(),
                    },
                }

                match data_src_map.create_data_conn::<AsyncDataConn>("bar") {
                    Ok(_) => panic!(),
                    Err(err) => match err.reason::<DataSrcError>() {
                        Ok(r) => match r {
                            DataSrcError::FailToCastDataConn { name, cast_to_type } => {
                                assert_eq!(name, "bar");
                                assert_eq!(
                                    cast_to_type,
                                    &"sabi::data_src::tests_of_data_src::AsyncDataConn"
                                );
                            }
                            _ => panic!(),
                        },
                        Err(_) => panic!(),
                    },
                }

                data_src_list.close();
            }

            logger.lock().unwrap().assert_logs(&[
                "SyncDataSrc setupped",
                "AsyncDataSrc setupped",
                "SyncDataSrc closed",
                "AsyncDataSrc closed",
                "SyncDataSrc dropped",
                "AsyncDataSrc dropped",
            ]);
            logger.lock().unwrap().clear();
        }

        #[test]
        fn test_create_data_conn_but_conn_not_found() {
            let logger = Arc::new(Mutex::new(Logger::new()));

            {
                const LOCAL: bool = true;
                let mut data_src_list = DataSrcList::new(LOCAL);
                let mut data_src_map = DataSrcMap::new();

                let ds_async = AsyncDataSrc::new(logger.clone(), false);
                data_src_list.add("foo".to_string(), ds_async);

                let ds_sync = SyncDataSrc::new(logger.clone(), false);
                data_src_list.add("bar".to_string(), ds_sync);

                let err_map = data_src_list.setup();
                assert_eq!(err_map.len(), 0);

                data_src_map.copy_from(&data_src_list);

                assert_eq!(data_src_map.map.len(), 2);
                assert!(data_src_map.map.contains_key("foo"));
                assert!(data_src_map.map.contains_key("bar"));

                match data_src_map.create_data_conn::<SyncDataConn>("baz") {
                    Ok(_) => panic!(),
                    Err(err) => match err.reason::<DataSrcError>() {
                        Ok(r) => match r {
                            DataSrcError::NoDataSrcToCreateDataConn {
                                name,
                                data_conn_type,
                            } => {
                                assert_eq!(name, "baz");
                                assert_eq!(
                                    data_conn_type,
                                    &"sabi::data_src::tests_of_data_src::SyncDataConn"
                                );
                            }
                            _ => panic!(),
                        },
                        Err(_) => panic!(),
                    },
                }

                data_src_list.close();
            }

            logger.lock().unwrap().assert_logs(&[
                "SyncDataSrc setupped",
                "AsyncDataSrc setupped",
                "SyncDataSrc closed",
                "AsyncDataSrc closed",
                "SyncDataSrc dropped",
                "AsyncDataSrc dropped",
            ]);
            logger.lock().unwrap().clear();
        }
    }
}
