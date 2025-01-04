// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;
use std::collections::HashMap;
use std::ptr;

use crate::async_group::{AsyncGroup, AsyncGroupAsync};
use crate::errors;
use crate::errs::Err;

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

pub(crate) struct NoopDaxConn {}

impl DaxConn for NoopDaxConn {
    fn commit(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn is_committed(&self) -> bool {
        false
    }
    fn rollback(&mut self, _ag: &mut dyn AsyncGroup) {}
    fn force_back(&mut self, _ag: &mut dyn AsyncGroup) {}
    fn close(&mut self) {}
}

#[repr(C)]
struct DaxConnContainer<'a, C = NoopDaxConn>
where
    C: DaxConn + 'static,
{
    drop_fn: fn(*const DaxConnContainer),
    is_fn: fn(any::TypeId) -> bool,
    commit_fn: fn(*const DaxConnContainer, &mut dyn AsyncGroup) -> Result<(), Err>,
    is_committed_fn: fn(*const DaxConnContainer) -> bool,
    rollback_fn: fn(*const DaxConnContainer, &mut dyn AsyncGroup),
    force_back_fn: fn(*const DaxConnContainer, &mut dyn AsyncGroup),
    close_fn: fn(*const DaxConnContainer),
    prev: *mut DaxConnContainer<'a>,
    next: *mut DaxConnContainer<'a>,
    name: &'a str,
    dax_conn: Box<C>,
}

impl<'a, C> DaxConnContainer<'a, C>
where
    C: DaxConn + 'static,
{
    fn new(name: &'a str, dax_conn: Box<C>) -> Self {
        Self {
            drop_fn: drop_dax_conn::<C>,
            is_fn: is_dax_conn::<C>,
            commit_fn: commit_dax_conn::<C>,
            is_committed_fn: is_committed_dax_conn::<C>,
            rollback_fn: rollback_dax_conn::<C>,
            force_back_fn: force_back_dax_conn::<C>,
            close_fn: close_dax_conn::<C>,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            name,
            dax_conn,
        }
    }
}

fn drop_dax_conn<C>(ptr: *const DaxConnContainer)
where
    C: DaxConn + 'static,
{
    let typed_ptr = ptr as *mut DaxConnContainer<C>;
    unsafe {
        drop(Box::from_raw(typed_ptr));
    }
}

fn is_dax_conn<C>(type_id: any::TypeId) -> bool
where
    C: DaxConn + 'static,
{
    any::TypeId::of::<C>() == type_id
}

fn commit_dax_conn<C>(ptr: *const DaxConnContainer, ag: &mut dyn AsyncGroup) -> Result<(), Err>
where
    C: DaxConn + 'static,
{
    let typed_ptr = ptr as *mut DaxConnContainer<C>;
    unsafe { (*typed_ptr).dax_conn.commit(ag) }
}

fn is_committed_dax_conn<C>(ptr: *const DaxConnContainer) -> bool
where
    C: DaxConn + 'static,
{
    let typed_ptr = ptr as *mut DaxConnContainer<C>;
    unsafe { (*typed_ptr).dax_conn.is_committed() }
}

fn rollback_dax_conn<C>(ptr: *const DaxConnContainer, ag: &mut dyn AsyncGroup)
where
    C: DaxConn + 'static,
{
    let typed_ptr = ptr as *mut DaxConnContainer<C>;
    unsafe { (*typed_ptr).dax_conn.rollback(ag) }
}

fn force_back_dax_conn<C>(ptr: *const DaxConnContainer, ag: &mut dyn AsyncGroup)
where
    C: DaxConn + 'static,
{
    let typed_ptr = ptr as *mut DaxConnContainer<C>;
    unsafe { (*typed_ptr).dax_conn.force_back(ag) };
}

fn close_dax_conn<C>(ptr: *const DaxConnContainer)
where
    C: DaxConn + 'static,
{
    let typed_ptr = ptr as *mut DaxConnContainer<C>;
    unsafe { (*typed_ptr).dax_conn.close() };
}

pub(crate) struct DaxConnMap<'a> {
    head: *mut DaxConnContainer<'a>,
    last: *mut DaxConnContainer<'a>,
    map: HashMap<&'a str, *mut DaxConnContainer<'a>>,
}

impl<'a> DaxConnMap<'a> {
    pub(crate) fn new() -> Self {
        Self {
            head: ptr::null_mut(),
            last: ptr::null_mut(),
            map: HashMap::new(),
        }
    }

    pub(crate) fn insert<C>(&mut self, name: &'a str, conn: Box<C>)
    where
        C: DaxConn + 'static,
    {
        let boxed = Box::new(DaxConnContainer::<C>::new(name, conn));
        let typed_ptr = Box::into_raw(boxed);
        let ptr = typed_ptr.cast::<DaxConnContainer>();
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

        self.map.insert(name, ptr);
    }

    pub(crate) fn commit(&self) -> Result<(), Err> {
        if self.last.is_null() {
            return Ok(());
        }

        let mut err_map = HashMap::<String, Err>::new();
        let mut ag = AsyncGroupAsync::new();

        let mut ptr = self.head;
        while !ptr.is_null() {
            let commit_fn = unsafe { (*ptr).commit_fn };
            let name = unsafe { (*ptr).name };
            let next = unsafe { (*ptr).next };
            if let Err(err) = commit_fn(ptr, &mut ag) {
                err_map.insert(name.to_string(), err);
            }
            ptr = next;
        }

        ag.wait(&mut err_map);

        if err_map.is_empty() {
            return Ok(());
        }

        Err(Err::new(errors::DaxConn::FailToCommit { errors: err_map }))
    }

    pub(crate) fn rollback(&self) {
        if self.last.is_null() {
            return;
        }

        let mut ag = AsyncGroupAsync::new();

        let mut ptr = self.head;
        while !ptr.is_null() {
            let is_committed_fn = unsafe { (*ptr).is_committed_fn };
            let rollback_fn = unsafe { (*ptr).rollback_fn };
            let force_back_fn = unsafe { (*ptr).force_back_fn };
            let next = unsafe { (*ptr).next };
            if is_committed_fn(ptr) {
                force_back_fn(ptr, &mut ag);
            } else {
                rollback_fn(ptr, &mut ag);
            }
            ptr = next;
        }

        let mut err_map = HashMap::<String, Err>::new();
        ag.wait(&mut err_map);
    }

    pub(crate) fn close(&self) {
        if self.last.is_null() {
            return;
        }

        let mut ptr = self.last;
        while !ptr.is_null() {
            let close_fn = unsafe { (*ptr).close_fn };
            let prev = unsafe { (*ptr).prev };
            close_fn(ptr);
            ptr = prev;
        }
    }
}

impl Drop for DaxConnMap<'_> {
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

#[cfg(test)]
mod tests_of_dax_conn {
    use super::*;
    use std::sync::{LazyLock, Mutex};

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
        fn clear(&mut self) {
            self.log_vec.clear();
        }
    }

    mod test_of_dax_conn_map {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        struct DaxConnA {
            committed: bool,
        }

        impl DaxConnA {
            fn new() -> Self {
                Self { committed: false }
            }
        }

        impl DaxConn for DaxConnA {
            fn commit(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
                self.committed = true;
                LOGGER.lock().unwrap().log("DaxConnA commit");
                Ok(())
            }
            fn is_committed(&self) -> bool {
                self.committed
            }
            fn rollback(&mut self, _ag: &mut dyn AsyncGroup) {
                LOGGER.lock().unwrap().log("DaxConnA rollback");
            }
            fn force_back(&mut self, _ag: &mut dyn AsyncGroup) {
                LOGGER.lock().unwrap().log("DaxConnA force back");
            }
            fn close(&mut self) {
                LOGGER.lock().unwrap().log("DaxConnA close");
            }
        }

        impl Drop for DaxConnA {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("DaxConnA drop");
            }
        }

        struct DaxConnB {
            committed: bool,
        }

        impl DaxConnB {
            fn new() -> Self {
                Self { committed: false }
            }
        }

        impl DaxConn for DaxConnB {
            fn commit(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
                self.committed = true;
                LOGGER.lock().unwrap().log("DaxConnB commit");
                Ok(())
            }
            fn is_committed(&self) -> bool {
                self.committed
            }
            fn rollback(&mut self, _ag: &mut dyn AsyncGroup) {
                LOGGER.lock().unwrap().log("DaxConnB rollback");
            }
            fn force_back(&mut self, _ag: &mut dyn AsyncGroup) {
                LOGGER.lock().unwrap().log("DaxConnB force back");
            }
            fn close(&mut self) {
                LOGGER.lock().unwrap().log("DaxConnB close");
            }
        }

        impl Drop for DaxConnB {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("DaxConnB drop");
            }
        }

        fn create_dax_conn_a() -> Result<Box<dyn DaxConn>, Err> {
            Ok(Box::new(DaxConnA::new()))
        }

        fn create_dax_conn_b() -> Result<Box<dyn DaxConn>, Err> {
            Ok(Box::new(DaxConnB::new()))
        }

        fn commits() {
            let mut dax_conn_map = DaxConnMap::new();

            match create_dax_conn_a() {
                Ok(dax_conn) => {
                    let ptr = Box::into_raw(dax_conn);
                    let typed_ptr = ptr as *mut DaxConnA;
                    let boxed = unsafe { Box::from_raw(typed_ptr) };
                    dax_conn_map.insert("foo", boxed);
                }
                Err(_) => panic!(),
            }

            match create_dax_conn_b() {
                Ok(dax_conn) => {
                    let ptr = Box::into_raw(dax_conn);
                    let typed_ptr = ptr as *mut DaxConnB;
                    let boxed = unsafe { Box::from_raw(typed_ptr) };
                    dax_conn_map.insert("bar", boxed);
                }
                Err(_) => panic!(),
            }

            match dax_conn_map.commit() {
                Ok(_) => {}
                Err(_) => panic!(),
            }

            dax_conn_map.close();
        }

        fn rollbacks() {
            let mut dax_conn_map = DaxConnMap::new();

            match create_dax_conn_a() {
                Ok(dax_conn) => {
                    let ptr = Box::into_raw(dax_conn);
                    let typed_ptr = ptr as *mut DaxConnA;
                    let boxed = unsafe { Box::from_raw(typed_ptr) };
                    dax_conn_map.insert("foo", boxed);
                }
                Err(_) => panic!(),
            }

            match create_dax_conn_b() {
                Ok(dax_conn) => {
                    let ptr = Box::into_raw(dax_conn);
                    let typed_ptr = ptr as *mut DaxConnB;
                    let boxed = unsafe { Box::from_raw(typed_ptr) };
                    dax_conn_map.insert("bar", boxed);
                }
                Err(_) => panic!(),
            }

            dax_conn_map.rollback();
            dax_conn_map.close();
        }

        fn force_backs() {
            let mut dax_conn_map = DaxConnMap::new();

            match create_dax_conn_a() {
                Ok(dax_conn) => {
                    let ptr = Box::into_raw(dax_conn);
                    let typed_ptr = ptr as *mut DaxConnA;
                    let boxed = unsafe { Box::from_raw(typed_ptr) };
                    dax_conn_map.insert("foo", boxed);
                }
                Err(_) => panic!(),
            }

            match create_dax_conn_b() {
                Ok(dax_conn) => {
                    let ptr = Box::into_raw(dax_conn);
                    let typed_ptr = ptr as *mut DaxConnB;
                    let boxed = unsafe { Box::from_raw(typed_ptr) };
                    dax_conn_map.insert("bar", boxed);
                }
                Err(_) => panic!(),
            }

            match dax_conn_map.commit() {
                Ok(_) => {}
                Err(_) => panic!(),
            }

            dax_conn_map.rollback();
            dax_conn_map.close();
        }

        #[test]
        fn test() {
            commits();

            LOGGER.lock().unwrap().assert_logs(&[
                "DaxConnA commit",
                "DaxConnB commit",
                "DaxConnB close",
                "DaxConnA close",
                "DaxConnB drop",
                "DaxConnA drop",
            ]);

            LOGGER.lock().unwrap().clear();

            rollbacks();

            LOGGER.lock().unwrap().assert_logs(&[
                "DaxConnA rollback",
                "DaxConnB rollback",
                "DaxConnB close",
                "DaxConnA close",
                "DaxConnB drop",
                "DaxConnA drop",
            ]);

            LOGGER.lock().unwrap().clear();

            force_backs();

            LOGGER.lock().unwrap().assert_logs(&[
                "DaxConnA commit",
                "DaxConnB commit",
                "DaxConnA force back",
                "DaxConnB force back",
                "DaxConnB close",
                "DaxConnA close",
                "DaxConnB drop",
                "DaxConnA drop",
            ]);
        }
    }
}
