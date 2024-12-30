// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::collections::HashMap;

use crate::async_group::AsyncGroupAsync;
use crate::errors;
use crate::AsyncGroup;
use crate::Err;

use std::ptr;

/// Represents a connection to a data store.
///
/// This trait declares methods: `commit`, `rollback`, `close`, etc. to work in a transaction
/// process.
pub trait DataConn {
    /// Commits the updates in a transaction.
    fn commit(&mut self, ag: &mut dyn AsyncGroup) -> Result<(), Err>;

    /// Checks whether updates are already committed.
    fn is_committed(&self) -> bool;

    /// Rollbacks updates in a transaction.
    fn rollback(&mut self, ag: &mut dyn AsyncGroup);

    /// Reverts updates forcely even if updates are already committed or this connection does not
    /// have rollback mechanism.
    fn force_back(&mut self, ag: &mut dyn AsyncGroup);

    /// Closes this connection.
    fn close(&mut self);
}

/// Represents a data source which creates connections to a data store like database, etc.
///
/// This trait declares methods: `setup`, `close`, and `create_data_conn`.
pub trait DataSrc {
    /// Connects to a data store and prepares to create `DataConn` instances.
    ///
    /// If the setup procedure is asynchronous, the `setup` method is implemented so as to use
    /// `AsyncGroup`.
    fn setup(&mut self, ag: &mut dyn AsyncGroup) -> Result<(), Err>;

    /// Disconnects to a data store.
    ///
    /// If the close procedure is asynchronous, the `close` method is implemented so as to use
    /// `AsyncGroup`.
    fn close(&mut self, ag: &mut dyn AsyncGroup);

    /// Creates a `DataConn` instance.
    fn create_data_conn(&self) -> Result<Box<dyn DataConn>, Err>;
}

struct NoopDataConn {}

impl DataConn for NoopDataConn {
    fn commit(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn is_committed(&self) -> bool {
        true
    }
    fn rollback(&mut self, _ag: &mut dyn AsyncGroup) {}
    fn force_back(&mut self, _ag: &mut dyn AsyncGroup) {}
    fn close(&mut self) {}
}

struct NoopDataSrc {}

impl DataSrc for NoopDataSrc {
    fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn close(&mut self, _ag: &mut dyn AsyncGroup) {}
    fn create_data_conn(&self) -> Result<Box<dyn DataConn>, Err> {
        Ok(Box::new(NoopDataConn {}))
    }
}

#[repr(C)]
struct DataSrcContainer<S = NoopDataSrc>
where
    S: DataSrc + 'static,
{
    drop_fn: fn(*const DataSrcContainer),
    setup_fn: fn(*const DataSrcContainer, ag: &mut dyn AsyncGroup) -> Result<(), Err>,
    close_fn: fn(*const DataSrcContainer, ag: &mut dyn AsyncGroup),
    prev: *mut DataSrcContainer,
    next: *mut DataSrcContainer,
    name: String,
    data_src: S,
}

impl<S> DataSrcContainer<S>
where
    S: DataSrc + 'static,
{
    fn new(name: String, data_src: S) -> Self {
        Self {
            drop_fn: drop_data_src_container::<S>,
            setup_fn: setup_data_src_container::<S>,
            close_fn: close_data_src_container::<S>,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            name,
            data_src,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn drop_data_src_container<S>(ptr: *const DataSrcContainer)
where
    S: DataSrc + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S>;
    drop(unsafe { Box::from_raw(typed_ptr) });
}

fn setup_data_src_container<S>(
    ptr: *const DataSrcContainer,
    ag: &mut dyn AsyncGroup,
) -> Result<(), Err>
where
    S: DataSrc + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S>;
    unsafe { (*typed_ptr).data_src.setup(ag) }
}

fn close_data_src_container<S>(ptr: *const DataSrcContainer, ag: &mut dyn AsyncGroup)
where
    S: DataSrc + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S>;
    unsafe { (*typed_ptr).data_src.close(ag) };
}

struct DataSrcList {
    head: *mut DataSrcContainer,
    last: *mut DataSrcContainer,
    fixed: bool,
}

impl DataSrcList {
    const fn new() -> Self {
        Self {
            head: ptr::null_mut(),
            last: ptr::null_mut(),
            fixed: false,
        }
    }

    fn add<S>(&mut self, name: String, ds: S)
    where
        S: DataSrc + 'static,
    {
        if self.fixed {
            return;
        }

        let boxed = Box::new(DataSrcContainer::<S>::new(name, ds));
        let typed_ptr = Box::into_raw(boxed);
        let ptr = typed_ptr.cast::<DataSrcContainer>();
        if self.last.is_null() {
            self.head = ptr;
            self.last = ptr;
        } else {
            unsafe {
                (*self.last).next = ptr;
                (*typed_ptr).prev = self.last;
            }
            self.last = ptr;
        }
    }

    fn fix(&mut self) {
        self.fixed = true;
    }

    fn setup(&mut self) -> HashMap<String, Err> {
        let mut err_map = HashMap::new();

        if self.head.is_null() {
            return err_map;
        }

        let mut ag = AsyncGroupAsync::new();

        let mut ptr = self.head;
        while !ptr.is_null() {
            let setup_fn = unsafe { (*ptr).setup_fn };
            let next = unsafe { (*ptr).next };
            ag.name = unsafe { &(*ptr).name };
            if let Err(err) = setup_fn(ptr, &mut ag) {
                err_map.insert(ag.name.to_string(), err);
            }
            ptr = next;
        }

        ag.wait(&mut err_map);

        err_map
    }

    fn close(&mut self) {
        let mut err_map = HashMap::new();

        if self.head.is_null() {
            return;
        }

        let mut ag = AsyncGroupAsync::new();

        let mut ptr = self.last;
        while !ptr.is_null() {
            let close_fn = unsafe { (*ptr).close_fn };
            let prev = unsafe { (*ptr).prev };
            close_fn(ptr, &mut ag);
            ptr = prev;
        }

        ag.wait(&mut err_map);
    }
}

impl Drop for DataSrcList {
    fn drop(&mut self) {
        let mut ptr = self.last;
        while !ptr.is_null() {
            let drop_fn = unsafe { (*ptr).drop_fn };
            let prev = unsafe { (*ptr).prev };
            drop_fn(ptr);
            ptr = prev;
        }
    }
}

static mut globalDataSrcList: DataSrcList = DataSrcList::new();

/// Registers a global `DataSrc` with its name.
///
/// By registering with this funciton, `DataSrc` becomes able to create a `DataConn` that can be
/// used within a transaction.
pub fn uses<S: DataSrc + 'static>(name: &str, ds: S) {
    unsafe {
        globalDataSrcList.add(name.to_string(), ds);
    }
}

/// Makes all globally registered DataSrc(s) usable.
///
/// This function forbids adding more global `DataSrc`(s), and calls each `setup` method of all
/// registered `DataSrc`(s)
pub fn setup() -> Result<(), Err> {
    unsafe {
        globalDataSrcList.fix();

        let err_map = globalDataSrcList.setup();
        if err_map.len() > 0 {
            return Err(Err::new(errors::DataSrc::FailToSetupGlobal {
                errors: err_map,
            }));
        }
    }

    Ok(())
}

/// Closes and frees each resource of registered global `DataSrc`(s).
///
// This function should always be called before an application ends.
pub fn close() {
    unsafe {
        globalDataSrcList.setup();
    }
}

/// Calls `setup` function, the argument function, and `close` function in order.
///
/// If `setup` function or the argument fucntion fails, this fucntion stops calling other functions
/// and return an `Err` containing the error reason.
pub fn start_app(app: fn() -> Result<(), Err>) -> Result<(), Err> {
    if let Err(err) = setup() {
        return Err(err);
    }

    if let Err(err) = app() {
        return Err(err);
    }

    close();

    Ok(())
}

#[cfg(test)]
mod tests_of_data_src_list {
    use super::*;
    use std::sync::LazyLock;
    use std::sync::Mutex;

    struct Logger {
        log_vec: Vec<String>,
    }

    impl Logger {
        fn new() -> Self {
            Self {
                log_vec: Vec::<String>::new(),
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
    }

    mod test_of_sync_setup {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        struct DataSrcA {}

        impl DataSrcA {
            fn new() -> Self {
                LOGGER.lock().unwrap().log("create DataSrcA");
                Self {}
            }
        }

        impl DataSrc for DataSrcA {
            fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
                LOGGER.lock().unwrap().log("setup DataSrcA");
                Ok(())
            }
            fn close(&mut self, _ag: &mut dyn AsyncGroup) {
                LOGGER.lock().unwrap().log("close DataSrcA");
            }
            fn create_data_conn(&self) -> Result<Box<dyn DataConn>, Err> {
                LOGGER.lock().unwrap().log("create DataConn of DataSrcA");
                Ok(Box::new(NoopDataConn {}))
            }
        }

        impl Drop for DataSrcA {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop DataSrcA");
            }
        }

        struct DataSrcB {}

        impl DataSrcB {
            fn new() -> Self {
                LOGGER.lock().unwrap().log("create DataSrcB");
                Self {}
            }
        }
        impl DataSrc for DataSrcB {
            fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
                LOGGER.lock().unwrap().log("setup DataSrcB");
                Ok(())
            }
            fn close(&mut self, _ag: &mut dyn AsyncGroup) {
                LOGGER.lock().unwrap().log("close DataSrcB");
            }
            fn create_data_conn(&self) -> Result<Box<dyn DataConn>, Err> {
                LOGGER.lock().unwrap().log("create DataConn of DataSrcB");
                Ok(Box::new(NoopDataConn {}))
            }
        }

        impl Drop for DataSrcB {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop DataSrcB");
            }
        }

        fn data_src_list() {
            let mut ds_list = DataSrcList::new();

            let ds_a = DataSrcA::new();
            ds_list.add("a".to_string(), ds_a);

            let ds_b = DataSrcB::new();
            ds_list.add("b".to_string(), ds_b);

            let err_map = ds_list.setup();
            assert!(err_map.is_empty());

            ds_list.close();
        }

        #[test]
        fn test() {
            data_src_list();

            LOGGER.lock().unwrap().assert_logs(&[
                "create DataSrcA",
                "create DataSrcB",
                "setup DataSrcA",
                "setup DataSrcB",
                "close DataSrcB",
                "close DataSrcA",
                "drop DataSrcB",
                "drop DataSrcA",
            ]);
        }
    }

    mod test_of_async_setup {
        use super::*;
        use std::thread;
        use std::time;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        struct DataSrcA {}

        impl DataSrcA {
            fn new() -> Self {
                LOGGER.lock().unwrap().log("create DataSrcA");
                Self {}
            }
        }

        impl DataSrc for DataSrcA {
            fn setup(&mut self, ag: &mut dyn AsyncGroup) -> Result<(), Err> {
                ag.add(|| {
                    LOGGER.lock().unwrap().log("setup DataSrcA: start");
                    thread::sleep(time::Duration::from_millis(100));
                    LOGGER.lock().unwrap().log("setup DataSrcA: end");
                    Ok(())
                });
                Ok(())
            }
            fn close(&mut self, ag: &mut dyn AsyncGroup) {
                ag.add(|| {
                    thread::sleep(time::Duration::from_millis(10));
                    LOGGER.lock().unwrap().log("close DataSrcA: start");
                    thread::sleep(time::Duration::from_millis(100));
                    LOGGER.lock().unwrap().log("close DataSrcA: end");
                    Ok(())
                });
            }
            fn create_data_conn(&self) -> Result<Box<dyn DataConn>, Err> {
                LOGGER.lock().unwrap().log("create DataConn of DataSrcA");
                Ok(Box::new(NoopDataConn {}))
            }
        }

        impl Drop for DataSrcA {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop DataSrcA");
            }
        }

        struct DataSrcB {}

        impl DataSrcB {
            fn new() -> Self {
                LOGGER.lock().unwrap().log("create DataSrcB");
                Self {}
            }
        }
        impl DataSrc for DataSrcB {
            fn setup(&mut self, ag: &mut dyn AsyncGroup) -> Result<(), Err> {
                ag.add(|| {
                    thread::sleep(time::Duration::from_millis(10));
                    LOGGER.lock().unwrap().log("setup DataSrcB: start");
                    thread::sleep(time::Duration::from_millis(20));
                    LOGGER.lock().unwrap().log("setup DataSrcB: end");
                    Ok(())
                });
                Ok(())
            }
            fn close(&mut self, ag: &mut dyn AsyncGroup) {
                ag.add(|| {
                    LOGGER.lock().unwrap().log("close DataSrcB: start");
                    thread::sleep(time::Duration::from_millis(20));
                    LOGGER.lock().unwrap().log("close DataSrcB: end");
                    Ok(())
                });
            }
            fn create_data_conn(&self) -> Result<Box<dyn DataConn>, Err> {
                LOGGER.lock().unwrap().log("create DataConn of DataSrcB");
                Ok(Box::new(NoopDataConn {}))
            }
        }

        impl Drop for DataSrcB {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop DataSrcB");
            }
        }

        fn data_src_list() {
            let mut ds_list = DataSrcList::new();

            let ds_a = DataSrcA::new();
            ds_list.add("a".to_string(), ds_a);

            let ds_b = DataSrcB::new();
            ds_list.add("b".to_string(), ds_b);

            let err_map = ds_list.setup();
            assert!(err_map.is_empty());

            ds_list.close();
        }

        #[test]
        fn test() {
            data_src_list();

            LOGGER.lock().unwrap().assert_logs(&[
                "create DataSrcA",
                "create DataSrcB",
                "setup DataSrcA: start",
                "setup DataSrcB: start",
                "setup DataSrcB: end",
                "setup DataSrcA: end",
                "close DataSrcB: start",
                "close DataSrcA: start",
                "close DataSrcB: end",
                "close DataSrcA: end",
                "drop DataSrcB",
                "drop DataSrcA",
            ]);
        }
    }
}
