// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod base;

pub use base::DaxBaseImpl;
pub use base::{close, setup, start_app, uses};

use std::any;
use std::collections::HashMap;
use std::ptr;

use crate::async_group::AsyncGroup;
use crate::errors;
use crate::{DaxConn, DaxSrc, NoopDaxConn, NoopDaxSrc};

use errs::Err;

#[repr(C)]
struct DaxSrcContainer<S = NoopDaxSrc>
where
    S: DaxSrc + 'static,
{
    drop_fn: fn(*const DaxSrcContainer),
    setup_fn: fn(*const DaxSrcContainer, ag: &mut AsyncGroup) -> Result<(), Err>,
    close_fn: fn(*const DaxSrcContainer, ag: &mut AsyncGroup),
    create_dax_conn_fn: fn(*const DaxSrcContainer) -> Result<Box<dyn DaxConn>, Err>,
    prev: *mut DaxSrcContainer,
    next: *mut DaxSrcContainer,
    local: bool,
    name: String,
    dax_src: S,
}

impl<S> DaxSrcContainer<S>
where
    S: DaxSrc + 'static,
{
    fn new(local: bool, name: String, dax_src: S) -> Self {
        Self {
            drop_fn: drop_dax_src::<S>,
            setup_fn: setup_dax_src::<S>,
            close_fn: close_dax_src::<S>,
            create_dax_conn_fn: create_dax_conn_from_dax_src::<S>,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            local,
            name,
            dax_src,
        }
    }
}

fn drop_dax_src<S>(ptr: *const DaxSrcContainer)
where
    S: DaxSrc + 'static,
{
    let typed_ptr = ptr as *mut DaxSrcContainer<S>;
    drop(unsafe { Box::from_raw(typed_ptr) });
}

fn setup_dax_src<S>(ptr: *const DaxSrcContainer, ag: &mut AsyncGroup) -> Result<(), Err>
where
    S: DaxSrc + 'static,
{
    let typed_ptr = ptr as *mut DaxSrcContainer<S>;
    unsafe { (*typed_ptr).dax_src.setup(ag) }
}

fn close_dax_src<S>(ptr: *const DaxSrcContainer, ag: &mut AsyncGroup)
where
    S: DaxSrc + 'static,
{
    let typed_ptr = ptr as *mut DaxSrcContainer<S>;
    unsafe { (*typed_ptr).dax_src.close(ag) };
}

fn create_dax_conn_from_dax_src<S>(ptr: *const DaxSrcContainer) -> Result<Box<dyn DaxConn>, Err>
where
    S: DaxSrc + 'static,
{
    let typed_ptr = ptr as *mut DaxSrcContainer<S>;
    unsafe { (*typed_ptr).dax_src.create_dax_conn() }
}

#[repr(C)]
struct DaxConnContainer<C = NoopDaxConn>
where
    C: DaxConn + 'static,
{
    drop_fn: fn(*const DaxConnContainer),
    is_fn: fn(any::TypeId) -> bool,
    commit_fn: fn(*const DaxConnContainer, &mut AsyncGroup) -> Result<(), Err>,
    is_committed_fn: fn(*const DaxConnContainer) -> bool,
    rollback_fn: fn(*const DaxConnContainer, &mut AsyncGroup),
    force_back_fn: fn(*const DaxConnContainer, &mut AsyncGroup),
    close_fn: fn(*const DaxConnContainer),
    prev: *mut DaxConnContainer,
    next: *mut DaxConnContainer,
    name: String,
    dax_conn: Box<C>,
}

impl<C> DaxConnContainer<C>
where
    C: DaxConn + 'static,
{
    fn new(name: &str, dax_conn: Box<C>) -> Self {
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
            name: name.to_string(),
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

fn commit_dax_conn<C>(ptr: *const DaxConnContainer, ag: &mut AsyncGroup) -> Result<(), Err>
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

fn rollback_dax_conn<C>(ptr: *const DaxConnContainer, ag: &mut AsyncGroup)
where
    C: DaxConn + 'static,
{
    let typed_ptr = ptr as *mut DaxConnContainer<C>;
    unsafe { (*typed_ptr).dax_conn.rollback(ag) }
}

fn force_back_dax_conn<C>(ptr: *const DaxConnContainer, ag: &mut AsyncGroup)
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

struct DaxSrcList {
    head: *mut DaxSrcContainer,
    last: *mut DaxSrcContainer,
    fixed: bool,
    local: bool,
}

impl DaxSrcList {
    const fn new(local: bool) -> Self {
        Self {
            head: ptr::null_mut(),
            last: ptr::null_mut(),
            fixed: false,
            local,
        }
    }

    fn add<S>(&mut self, name: String, ds: S)
    where
        S: DaxSrc + 'static,
    {
        if self.fixed {
            return;
        }

        let boxed = Box::new(DaxSrcContainer::<S>::new(self.local, name, ds));
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

        let mut ag = AsyncGroup::new();

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

        ag.join(&mut err_map);

        err_map
    }

    fn close(&mut self) {
        if self.head.is_null() {
            return;
        }

        let mut ag = AsyncGroup::new();

        let mut ptr = self.last;
        while !ptr.is_null() {
            let close_fn = unsafe { (*ptr).close_fn };
            let prev = unsafe { (*ptr).prev };
            close_fn(ptr, &mut ag);
            ptr = prev;
        }

        let mut err_map = HashMap::new();
        ag.join(&mut err_map);
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

struct DaxConnMap {
    head: *mut DaxConnContainer,
    last: *mut DaxConnContainer,
    map: HashMap<String, *mut DaxConnContainer>,
}

impl DaxConnMap {
    fn new() -> Self {
        Self {
            head: ptr::null_mut(),
            last: ptr::null_mut(),
            map: HashMap::new(),
        }
    }

    fn insert<C>(&mut self, name: &str, conn: Box<C>)
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

        self.map.insert(name.to_string(), ptr);
    }

    fn commit(&self) -> Result<(), Err> {
        if self.last.is_null() {
            return Ok(());
        }

        let mut ag = AsyncGroup::new();
        let mut err_map = HashMap::new();

        let mut ptr = self.head;
        while !ptr.is_null() {
            let commit_fn = unsafe { (*ptr).commit_fn };
            let name = unsafe { &(*ptr).name };
            let next = unsafe { (*ptr).next };
            if let Err(err) = commit_fn(ptr, &mut ag) {
                err_map.insert(name.to_string(), err);
            }
            ptr = next;
        }

        ag.join(&mut err_map);

        if err_map.is_empty() {
            return Ok(());
        }

        Err(Err::new(errors::DaxConn::FailToCommit { errors: err_map }))
    }

    fn rollback(&self) {
        if self.last.is_null() {
            return;
        }

        let mut ag = AsyncGroup::new();

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

        let mut err_map = HashMap::new();
        ag.join(&err_map);
    }

    fn close(&self) {
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

impl Drop for DaxConnMap {
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
mod tests_of_dax {
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
        fn assert_log(&self, index: usize, log: &str) {
            //assert!(self.log_vec.len() > index);
            //assert_eq!(self.log_vec[index], log);
        }
        fn assert_log_either(&self, index: usize, candidates: &[&str]) {
            //assert!(self.log_vec.len() > index);
            //assert!(candidates.contains(&&self.log_vec[index].as_str()));
        }
        fn clear(&mut self) {
            self.log_vec.clear();
        }
    }

    mod tests_of_dax_src_list {
        use super::*;

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
                fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
                    LOGGER.lock().unwrap().log("setup DaxSrcA");
                    Ok(())
                }
                fn close(&mut self, _ag: &mut AsyncGroup) {
                    LOGGER.lock().unwrap().log("close DaxSrcA");
                }
                fn create_dax_conn(&mut self) -> Result<Box<dyn DaxConn>, Err> {
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
                fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
                    LOGGER.lock().unwrap().log("setup DaxSrcB");
                    Ok(())
                }
                fn close(&mut self, _ag: &mut AsyncGroup) {
                    LOGGER.lock().unwrap().log("close DaxSrcB");
                }
                fn create_dax_conn(&mut self) -> Result<Box<dyn DaxConn>, Err> {
                    LOGGER.lock().unwrap().log("create DaxConn of DaxSrcB");
                    Ok(Box::new(NoopDaxConn {}))
                }
            }

            impl Drop for DaxSrcB {
                fn drop(&mut self) {
                    LOGGER.lock().unwrap().log("drop DaxSrcB");
                }
            }

            fn test_sub() {
                let mut ds_list = DaxSrcList::new(false);

                let ds_a = DaxSrcA::new();
                ds_list.add("a".to_string(), ds_a);

                let ds_b = DaxSrcB::new();
                ds_list.add("b".to_string(), ds_b);

                let err_map = ds_list.setup();
                assert!(err_map.is_empty());

                ds_list.close();
            }

            #[test]
            fn test_main() {
                test_sub();

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
                fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> {
                    //ag.add(|| {
                    //    LOGGER.lock().unwrap().log("setup DaxSrcA: start");
                    //    thread::sleep(time::Duration::from_millis(100));
                    //    LOGGER.lock().unwrap().log("setup DaxSrcA: end");
                    //    Ok(())
                    //});
                    Ok(())
                }
                fn close(&mut self, ag: &mut AsyncGroup) {
                    //ag.add(|| {
                    //    LOGGER.lock().unwrap().log("close DaxSrcA: start");
                    //    thread::sleep(time::Duration::from_millis(100));
                    //    LOGGER.lock().unwrap().log("close DaxSrcA: end");
                    //    Ok(())
                    //});
                }
                fn create_dax_conn(&mut self) -> Result<Box<dyn DaxConn>, Err> {
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
                fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> {
                    //ag.add(|| {
                    //    LOGGER.lock().unwrap().log("setup DaxSrcB: start");
                    //    thread::sleep(time::Duration::from_millis(20));
                    //    LOGGER.lock().unwrap().log("setup DaxSrcB: end");
                    //    Ok(())
                    //});
                    Ok(())
                }
                fn close(&mut self, ag: &mut AsyncGroup) {
                    //ag.add(|| {
                    //    LOGGER.lock().unwrap().log("close DaxSrcB: start");
                    //    thread::sleep(time::Duration::from_millis(20));
                    //    LOGGER.lock().unwrap().log("close DaxSrcB: end");
                    //    Ok(())
                    //});
                }
                fn create_dax_conn(&mut self) -> Result<Box<dyn DaxConn>, Err> {
                    LOGGER.lock().unwrap().log("create DaxConn of DaxSrcB");
                    Ok(Box::new(NoopDaxConn {}))
                }
            }

            impl Drop for DaxSrcB {
                fn drop(&mut self) {
                    LOGGER.lock().unwrap().log("drop DaxSrcB");
                }
            }

            fn test_sub() {
                let mut ds_list = DaxSrcList::new(false);

                let ds_a = DaxSrcA::new();
                ds_list.add("a".to_string(), ds_a);

                let ds_b = DaxSrcB::new();
                ds_list.add("b".to_string(), ds_b);

                let err_map = ds_list.setup();
                assert!(err_map.is_empty());

                ds_list.close();
            }

            #[test]
            fn test_main() {
                test_sub();

                let logger = LOGGER.lock().unwrap();
                logger.assert_log(0, "create DaxSrcA");
                logger.assert_log(1, "create DaxSrcB");
                logger.assert_log_either(2, &["setup DaxSrcA: start", "setup DaxSrcB: start"]);
                logger.assert_log_either(3, &["setup DaxSrcA: start", "setup DaxSrcB: start"]);
                logger.assert_log_either(4, &["setup DaxSrcB: end", "setup DaxSrcA: end"]);
                logger.assert_log_either(5, &["setup DaxSrcB: end", "setup DaxSrcA: end"]);
                logger.assert_log_either(6, &["close DaxSrcB: start", "close DaxSrcA: start"]);
                logger.assert_log_either(7, &["close DaxSrcB: start", "close DaxSrcA: start"]);
                logger.assert_log_either(8, &["close DaxSrcB: end", "close DaxSrcA: end"]);
                logger.assert_log_either(9, &["close DaxSrcB: end", "close DaxSrcA: end"]);
                logger.assert_log(10, "drop DaxSrcB");
                logger.assert_log(11, "drop DaxSrcA");
            }
        }
    }

    mod tests_of_dax_conn_map {
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
            fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
                self.committed = true;
                LOGGER.lock().unwrap().log("DaxConnA commit");
                Ok(())
            }
            fn is_committed(&self) -> bool {
                self.committed
            }
            fn rollback(&mut self, _ag: &mut AsyncGroup) {
                LOGGER.lock().unwrap().log("DaxConnA rollback");
            }
            fn force_back(&mut self, _ag: &mut AsyncGroup) {
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
            fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
                self.committed = true;
                LOGGER.lock().unwrap().log("DaxConnB commit");
                Ok(())
            }
            fn is_committed(&self) -> bool {
                self.committed
            }
            fn rollback(&mut self, _ag: &mut AsyncGroup) {
                LOGGER.lock().unwrap().log("DaxConnB rollback");
            }
            fn force_back(&mut self, _ag: &mut AsyncGroup) {
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

        fn test_commits() {
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

        fn test_rollbacks() {
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

        fn test_force_backs() {
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
        fn test_main() {
            test_commits();

            LOGGER.lock().unwrap().assert_logs(&[
                "DaxConnA commit",
                "DaxConnB commit",
                "DaxConnB close",
                "DaxConnA close",
                "DaxConnB drop",
                "DaxConnA drop",
            ]);

            LOGGER.lock().unwrap().clear();

            test_rollbacks();

            LOGGER.lock().unwrap().assert_logs(&[
                "DaxConnA rollback",
                "DaxConnB rollback",
                "DaxConnB close",
                "DaxConnA close",
                "DaxConnB drop",
                "DaxConnA drop",
            ]);

            LOGGER.lock().unwrap().clear();

            test_force_backs();

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
