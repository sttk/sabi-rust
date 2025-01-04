// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::collections::HashMap;
use std::ptr;

use crate::async_group::AsyncGroupAsync;
use crate::errors;
use crate::AsyncGroup;
use crate::Err;

/// Represents a connection to a data store.
///
/// This trait declares methods: `commit`, `rollback`, `close`, etc. to work in a transaction
/// process.
pub trait DaxConn {
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
/// This trait declares methods: `setup`, `close`, and `create_dax_conn`.
pub trait DaxSrc {
    /// Connects to a data store and prepares to create `DaxConn` instances.
    ///
    /// If the setup procedure is asynchronous, the `setup` method is implemented so as to use
    /// `AsyncGroup`.
    fn setup(&mut self, ag: &mut dyn AsyncGroup) -> Result<(), Err>;

    /// Disconnects to a data store.
    ///
    /// If the close procedure is asynchronous, the `close` method is implemented so as to use
    /// `AsyncGroup`.
    fn close(&mut self, ag: &mut dyn AsyncGroup);

    /// Creates a `DaxConn` instance.
    fn create_dax_conn(&self) -> Result<Box<dyn DaxConn>, Err>;
}

struct NoopDaxConn {}

impl DaxConn for NoopDaxConn {
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

struct NoopDaxSrc {}

impl DaxSrc for NoopDaxSrc {
    fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn close(&mut self, _ag: &mut dyn AsyncGroup) {}
    fn create_dax_conn(&self) -> Result<Box<dyn DaxConn>, Err> {
        Ok(Box::new(NoopDaxConn {}))
    }
}

#[repr(C)]
struct DaxSrcContainer<S = NoopDaxSrc>
where
    S: DaxSrc + 'static,
{
    drop_fn: fn(*const DaxSrcContainer),
    setup_fn: fn(*const DaxSrcContainer, ag: &mut dyn AsyncGroup) -> Result<(), Err>,
    close_fn: fn(*const DaxSrcContainer, ag: &mut dyn AsyncGroup),
    prev: *mut DaxSrcContainer,
    next: *mut DaxSrcContainer,
    name: String,
    dax_src: S,
}

impl<S> DaxSrcContainer<S>
where
    S: DaxSrc + 'static,
{
    fn new(name: String, dax_src: S) -> Self {
        Self {
            drop_fn: drop_dax_src_container::<S>,
            setup_fn: setup_dax_src_container::<S>,
            close_fn: close_dax_src_container::<S>,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            name,
            dax_src,
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn drop_dax_src_container<S>(ptr: *const DaxSrcContainer)
where
    S: DaxSrc + 'static,
{
    let typed_ptr = ptr as *mut DaxSrcContainer<S>;
    drop(unsafe { Box::from_raw(typed_ptr) });
}

fn setup_dax_src_container<S>(
    ptr: *const DaxSrcContainer,
    ag: &mut dyn AsyncGroup,
) -> Result<(), Err>
where
    S: DaxSrc + 'static,
{
    let typed_ptr = ptr as *mut DaxSrcContainer<S>;
    unsafe { (*typed_ptr).dax_src.setup(ag) }
}

fn close_dax_src_container<S>(ptr: *const DaxSrcContainer, ag: &mut dyn AsyncGroup)
where
    S: DaxSrc + 'static,
{
    let typed_ptr = ptr as *mut DaxSrcContainer<S>;
    unsafe { (*typed_ptr).dax_src.close(ag) };
}

struct DaxSrcList {
    head: *mut DaxSrcContainer,
    last: *mut DaxSrcContainer,
    fixed: bool,
}

impl DaxSrcList {
    const fn new() -> Self {
        Self {
            head: ptr::null_mut(),
            last: ptr::null_mut(),
            fixed: false,
        }
    }

    fn add<S>(&mut self, name: String, ds: S)
    where
        S: DaxSrc + 'static,
    {
        if self.fixed {
            return;
        }

        let boxed = Box::new(DaxSrcContainer::<S>::new(name, ds));
        let typed_ptr = Box::into_raw(boxed);
        let ptr = typed_ptr.cast::<DaxSrcContainer>();
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

impl Drop for DaxSrcList {
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

static mut GLOBAL_DAX_SRC_LIST: DaxSrcList = DaxSrcList::new();

/// Registers a global `DaxSrc` with its name.
///
/// By registering with this funciton, `DaxSrc` becomes able to create a `DaxConn` that can be
/// used within a transaction.
pub fn uses<S: DaxSrc + 'static>(name: &str, ds: S) {
    unsafe {
        GLOBAL_DAX_SRC_LIST.add(name.to_string(), ds);
    }
}

/// Makes all globally registered DaxSrc(s) usable.
///
/// This function forbids adding more global `DaxSrc`(s), and calls each `setup` method of all
/// registered `DaxSrc`(s)
pub fn setup() -> Result<(), Err> {
    unsafe {
        GLOBAL_DAX_SRC_LIST.fix();

        let err_map = GLOBAL_DAX_SRC_LIST.setup();
        if err_map.len() > 0 {
            return Err(Err::new(errors::DaxSrc::FailToSetupGlobal {
                errors: err_map,
            }));
        }
    }

    Ok(())
}

/// Closes and frees each resource of registered global `DaxSrc`(s).
///
// This function should always be called before an application ends.
pub fn close() {
    unsafe {
        GLOBAL_DAX_SRC_LIST.close();
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
mod tests_of_dax_src_list {
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

        struct DaxSrcA {}

        impl DaxSrcA {
            fn new() -> Self {
                LOGGER.lock().unwrap().log("create DaxSrcA");
                Self {}
            }
        }

        impl DaxSrc for DaxSrcA {
            fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
                LOGGER.lock().unwrap().log("setup DaxSrcA");
                Ok(())
            }
            fn close(&mut self, _ag: &mut dyn AsyncGroup) {
                LOGGER.lock().unwrap().log("close DaxSrcA");
            }
            fn create_dax_conn(&self) -> Result<Box<dyn DaxConn>, Err> {
                LOGGER.lock().unwrap().log("create DaxConn of DaxSrcA");
                Ok(Box::new(NoopDaxConn {}))
            }
        }

        impl Drop for DaxSrcA {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop DaxSrcA");
            }
        }

        struct DaxSrcB {}

        impl DaxSrcB {
            fn new() -> Self {
                LOGGER.lock().unwrap().log("create DaxSrcB");
                Self {}
            }
        }
        impl DaxSrc for DaxSrcB {
            fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
                LOGGER.lock().unwrap().log("setup DaxSrcB");
                Ok(())
            }
            fn close(&mut self, _ag: &mut dyn AsyncGroup) {
                LOGGER.lock().unwrap().log("close DaxSrcB");
            }
            fn create_dax_conn(&self) -> Result<Box<dyn DaxConn>, Err> {
                LOGGER.lock().unwrap().log("create DaxConn of DaxSrcB");
                Ok(Box::new(NoopDaxConn {}))
            }
        }

        impl Drop for DaxSrcB {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop DaxSrcB");
            }
        }

        fn dax_src_list() {
            let mut ds_list = DaxSrcList::new();

            let ds_a = DaxSrcA::new();
            ds_list.add("a".to_string(), ds_a);

            let ds_b = DaxSrcB::new();
            ds_list.add("b".to_string(), ds_b);

            let err_map = ds_list.setup();
            assert!(err_map.is_empty());

            ds_list.close();
        }

        #[test]
        fn test() {
            dax_src_list();

            LOGGER.lock().unwrap().assert_logs(&[
                "create DaxSrcA",
                "create DaxSrcB",
                "setup DaxSrcA",
                "setup DaxSrcB",
                "close DaxSrcB",
                "close DaxSrcA",
                "drop DaxSrcB",
                "drop DaxSrcA",
            ]);
        }
    }

    mod test_of_async_setup {
        use super::*;
        use std::thread;
        use std::time;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        struct DaxSrcA {}

        impl DaxSrcA {
            fn new() -> Self {
                LOGGER.lock().unwrap().log("create DaxSrcA");
                Self {}
            }
        }

        impl DaxSrc for DaxSrcA {
            fn setup(&mut self, ag: &mut dyn AsyncGroup) -> Result<(), Err> {
                ag.add(|| {
                    LOGGER.lock().unwrap().log("setup DaxSrcA: start");
                    thread::sleep(time::Duration::from_millis(100));
                    LOGGER.lock().unwrap().log("setup DaxSrcA: end");
                    Ok(())
                });
                Ok(())
            }
            fn close(&mut self, ag: &mut dyn AsyncGroup) {
                ag.add(|| {
                    thread::sleep(time::Duration::from_millis(10));
                    LOGGER.lock().unwrap().log("close DaxSrcA: start");
                    thread::sleep(time::Duration::from_millis(100));
                    LOGGER.lock().unwrap().log("close DaxSrcA: end");
                    Ok(())
                });
            }
            fn create_dax_conn(&self) -> Result<Box<dyn DaxConn>, Err> {
                LOGGER.lock().unwrap().log("create DaxConn of DaxSrcA");
                Ok(Box::new(NoopDaxConn {}))
            }
        }

        impl Drop for DaxSrcA {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop DaxSrcA");
            }
        }

        struct DaxSrcB {}

        impl DaxSrcB {
            fn new() -> Self {
                LOGGER.lock().unwrap().log("create DaxSrcB");
                Self {}
            }
        }
        impl DaxSrc for DaxSrcB {
            fn setup(&mut self, ag: &mut dyn AsyncGroup) -> Result<(), Err> {
                ag.add(|| {
                    thread::sleep(time::Duration::from_millis(10));
                    LOGGER.lock().unwrap().log("setup DaxSrcB: start");
                    thread::sleep(time::Duration::from_millis(20));
                    LOGGER.lock().unwrap().log("setup DaxSrcB: end");
                    Ok(())
                });
                Ok(())
            }
            fn close(&mut self, ag: &mut dyn AsyncGroup) {
                ag.add(|| {
                    LOGGER.lock().unwrap().log("close DaxSrcB: start");
                    thread::sleep(time::Duration::from_millis(20));
                    LOGGER.lock().unwrap().log("close DaxSrcB: end");
                    Ok(())
                });
            }
            fn create_dax_conn(&self) -> Result<Box<dyn DaxConn>, Err> {
                LOGGER.lock().unwrap().log("create DaxConn of DaxSrcB");
                Ok(Box::new(NoopDaxConn {}))
            }
        }

        impl Drop for DaxSrcB {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop DaxSrcB");
            }
        }

        fn dax_src_list() {
            let mut ds_list = DaxSrcList::new();

            let ds_a = DaxSrcA::new();
            ds_list.add("a".to_string(), ds_a);

            let ds_b = DaxSrcB::new();
            ds_list.add("b".to_string(), ds_b);

            let err_map = ds_list.setup();
            assert!(err_map.is_empty());

            ds_list.close();
        }

        #[test]
        fn test() {
            dax_src_list();

            LOGGER.lock().unwrap().assert_logs(&[
                "create DaxSrcA",
                "create DaxSrcB",
                "setup DaxSrcA: start",
                "setup DaxSrcB: start",
                "setup DaxSrcB: end",
                "setup DaxSrcA: end",
                "close DaxSrcB: start",
                "close DaxSrcA: start",
                "close DaxSrcB: end",
                "close DaxSrcA: end",
                "drop DaxSrcB",
                "drop DaxSrcA",
            ]);
        }
    }
}
