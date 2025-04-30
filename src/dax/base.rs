// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;

use super::*;
use crate::async_group::AsyncGroupSync;
use crate::errors;
use crate::{Dax, DaxConn, DaxSrc};

use errs::Err;

static mut GLOBAL_DAX_SRC_LIST: DaxSrcList = DaxSrcList::new(false);

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

/// Provides the methods to manage `DaxSrc`(s) and to operate `DaxConn`(s) in a transaction.
pub struct DaxBaseImpl {
    local_dax_src_list: DaxSrcList,
    dax_src_map: HashMap<String, *mut DaxSrcContainer>,
    dax_conn_map: DaxConnMap,
    fixed: bool,
}

impl DaxBaseImpl {
    /// Creates a `DaxBase` instance.
    pub fn new() -> Self {
        unsafe {
            GLOBAL_DAX_SRC_LIST.fix();
        }

        let mut map = HashMap::<String, *mut DaxSrcContainer>::new();

        let mut ptr = unsafe { GLOBAL_DAX_SRC_LIST.head };
        while !ptr.is_null() {
            let next = unsafe { (*ptr).next };
            let name = unsafe { &(*ptr).name };
            map.insert(name.to_string(), ptr);
            ptr = next;
        }

        Self {
            local_dax_src_list: DaxSrcList::new(true),
            dax_src_map: map,
            dax_conn_map: DaxConnMap::new(),
            fixed: false,
        }
    }

    /// Adds a `DaxSrc` with a registration name into this `DaxBase` and setup it.
    pub fn uses<S: DaxSrc + 'static>(&mut self, name: &str, ds: S) -> Result<(), Err> {
        if self.fixed {
            return Ok(());
        }

        self.local_dax_src_list.add(name.to_string(), ds);

        let ptr = self.local_dax_src_list.last;
        let name = unsafe { &(*ptr).name };
        self.dax_src_map.insert(name.to_string(), ptr);

        let setup_fn = unsafe { (*ptr).setup_fn };
        let mut ag = AsyncGroupSync::new();
        if let Err(err) = setup_fn(ptr, &mut ag) {
            return Err(err);
        }
        if let Some(err) = ag.err {
            return Err(err);
        }

        Ok(())
    }

    /// Removes a `DaxSrc` from this `DaxBase` and close it.
    pub fn disuses(&mut self, name: &str) {
        if self.fixed {
            return;
        }

        if let Some(ptr) = self.dax_src_map.remove(name) {
            let close_fn = unsafe { (*ptr).close_fn };
            let drop_fn = unsafe { (*ptr).drop_fn };

            if unsafe { (*ptr).local } {
                let prev = unsafe { (*ptr).prev };
                let next = unsafe { (*ptr).next };
                if prev.is_null() && next.is_null() {
                    self.local_dax_src_list.head = ptr::null_mut();
                    self.local_dax_src_list.last = ptr::null_mut();
                } else if prev.is_null() {
                    unsafe { (*next).prev = ptr::null_mut() };
                    self.local_dax_src_list.head = next;
                } else if next.is_null() {
                    unsafe { (*prev).next = ptr::null_mut() };
                    self.local_dax_src_list.last = prev;
                } else {
                    unsafe { (*next).prev = prev };
                    unsafe { (*prev).next = next };
                }
            }

            let mut ag = AsyncGroupSync::new();
            close_fn(ptr, &mut ag);
            drop_fn(ptr);
        }
    }

    /// Closes all `DaxSrc`(s) in this `DaxBase`.
    pub fn close(&mut self) {
        if self.fixed {
            return;
        }

        self.local_dax_src_list.close();
    }

    /// Begins a transaction.
    ///
    /// This method prevents to add new `DaxSrc` into this `DaxBase`.
    pub fn begin(&mut self) {
        self.fixed = true;
    }

    /// Commits all `DaxConn`(s) created in this `DaxBase`.
    pub fn commit(&mut self) -> Result<(), Err> {
        self.dax_conn_map.commit()
    }

    /// Rollbacks all `DaxConn`(s) created in this `DaxBase`.
    pub fn rollback(&mut self) {
        self.dax_conn_map.rollback();
    }

    /// Ends a transaction.
    ///
    /// This method closes all `DaxConn`(s) created in this `DaxBase` and re-allow to add a new
    /// `DaxSrc`.
    pub fn end(&mut self) {
        self.dax_conn_map.close();
        self.fixed = false;
    }
}

impl Drop for DaxBaseImpl {
    fn drop(&mut self) {
        self.close();
    }
}

impl Dax for DaxBaseImpl {
    fn get_dax_conn<C: DaxConn + 'static>(&mut self, name: &str) -> Result<&C, Err> {
        if let Some(ptr) = self.dax_conn_map.map.get(name) {
            let type_id = any::TypeId::of::<C>();
            let is_fn = unsafe { (*(*ptr)).is_fn };
            if is_fn(type_id) {
                let typed_ptr = (*ptr) as *const DaxConnContainer<C>;
                return Ok(unsafe { &((*typed_ptr).dax_conn) });
            }

            return Err(Err::new(errors::DaxBase::FailToCastDaxConn {
                name: name.to_string(),
                to_type: any::type_name::<C>(),
            }));
        }

        if let Some(ptr) = self.dax_src_map.get_mut(name) {
            let create_dax_conn = unsafe { (*(*ptr)).create_dax_conn_fn };
            return match create_dax_conn(*ptr) {
                Ok(dax_conn) => {
                    let ptr = Box::into_raw(dax_conn);
                    let typed_ptr = ptr as *mut C;
                    let boxed = unsafe { Box::from_raw(typed_ptr) };
                    self.dax_conn_map.insert(name, boxed);
                    if let Some(ptr) = self.dax_conn_map.map.get(name) {
                        let typed_ptr = (*ptr) as *const DaxConnContainer<C>;
                        return Ok(unsafe { &((*typed_ptr).dax_conn) });
                    }
                    return Err(Err::new(errors::DaxBase::FailToGetDaxConn {
                        name: name.to_string(),
                        to_type: any::type_name::<C>(),
                    }));
                }
                Err(err) => Err(err),
            };
        }

        Err(Err::new(errors::DaxBase::FailToGetDaxConn {
            name: name.to_string(),
            to_type: any::type_name::<C>(),
        }))
    }
}

#[cfg(test)]
mod tests_dax_base_impl {
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
            assert!(self.log_vec.len() > index);
            assert_eq!(self.log_vec[index], log);
        }
        fn assert_log_either(&self, index: usize, candidates: &[&str]) {
            assert!(self.log_vec.len() > index);
            assert!(candidates.contains(&&self.log_vec[index].as_str()));
        }
        fn clear(&mut self) {
            self.log_vec.clear();
        }
    }

    static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

    struct FooDaxConn {
        committed: bool,
    }

    impl FooDaxConn {
        fn new() -> Self {
            LOGGER.lock().unwrap().log("create FooDaxConn");
            Self { committed: false }
        }

        fn get_bool(&self) -> bool {
            true
        }
    }

    impl DaxConn for FooDaxConn {
        fn commit(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
            self.committed = true;
            LOGGER.lock().unwrap().log("commit FooDaxConn");
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut dyn AsyncGroup) {
            LOGGER.lock().unwrap().log("rollback FooDaxConn");
        }
        fn force_back(&mut self, _ag: &mut dyn AsyncGroup) {
            LOGGER.lock().unwrap().log("force back FooDaxConn");
        }
        fn close(&mut self) {
            LOGGER.lock().unwrap().log("close FooDaxConn");
        }
    }

    struct FooDaxSrc {}

    impl FooDaxSrc {
        fn new() -> Self {
            LOGGER.lock().unwrap().log("create FooDaxSrc");
            Self {}
        }
    }

    impl DaxSrc for FooDaxSrc {
        fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
            LOGGER.lock().unwrap().log("setup FooDaxSrc");
            Ok(())
        }
        fn close(&mut self, _ag: &mut dyn AsyncGroup) {
            LOGGER.lock().unwrap().log("close FooDaxSrc");
        }
        fn create_dax_conn(&mut self) -> Result<Box<dyn DaxConn>, Err> {
            LOGGER.lock().unwrap().log("create FooDaxConn of FooDaxSrc");
            Ok(Box::new(FooDaxConn::new()))
        }
    }

    struct BarDaxConn {
        committed: bool,
    }

    impl BarDaxConn {
        fn new() -> Self {
            LOGGER.lock().unwrap().log("create BarDaxConn");
            Self { committed: false }
        }

        fn get_num(&self) -> u64 {
            123
        }
    }

    impl DaxConn for BarDaxConn {
        fn commit(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
            self.committed = true;
            LOGGER.lock().unwrap().log("commit BarDaxConn");
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut dyn AsyncGroup) {
            LOGGER.lock().unwrap().log("rollback BarDaxConn");
        }
        fn force_back(&mut self, _ag: &mut dyn AsyncGroup) {
            LOGGER.lock().unwrap().log("force back BarDaxConn");
        }
        fn close(&mut self) {
            LOGGER.lock().unwrap().log("close BarDaxConn");
        }
    }

    struct BarDaxSrc {}

    impl BarDaxSrc {
        fn new() -> Self {
            LOGGER.lock().unwrap().log("create BarDaxSrc");
            Self {}
        }
    }

    impl DaxSrc for BarDaxSrc {
        fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
            LOGGER.lock().unwrap().log("setup BarDaxSrc");
            Ok(())
        }
        fn close(&mut self, _ag: &mut dyn AsyncGroup) {
            LOGGER.lock().unwrap().log("close BarDaxSrc");
        }
        fn create_dax_conn(&mut self) -> Result<Box<dyn DaxConn>, Err> {
            LOGGER.lock().unwrap().log("create BarDaxConn of BarDaxSrc");
            Ok(Box::new(BarDaxConn::new()))
        }
    }

    fn test_dax_base_with_no_dax_conn() {
        let mut base = DaxBaseImpl::new();

        let _ = base.uses("bar", BarDaxSrc::new());

        base.begin();
        base.commit().unwrap();
        base.end();
    }

    fn test_dax_base_with_dax_conns() {
        let mut base = DaxBaseImpl::new();

        let _ = base.uses("bar", BarDaxSrc::new());

        base.begin();

        let foo_conn: &FooDaxConn = base.get_dax_conn::<FooDaxConn>("foo").unwrap();
        assert_eq!(foo_conn.get_bool(), true);

        let bar_conn: &BarDaxConn = base.get_dax_conn::<BarDaxConn>("bar").unwrap();
        assert_eq!(bar_conn.get_num(), 123);

        base.commit().unwrap();
        base.end();
    }

    #[test]
    fn test() {
        uses("foo", FooDaxSrc::new());
        let _ = setup().unwrap();

        test_dax_base_with_no_dax_conn();

        LOGGER.lock().unwrap().assert_logs(&[
            "create FooDaxSrc",
            "setup FooDaxSrc",
            "create BarDaxSrc",
            "setup BarDaxSrc",
            "close BarDaxSrc",
        ]);

        LOGGER.lock().unwrap().clear();

        test_dax_base_with_dax_conns();

        close();

        LOGGER.lock().unwrap().assert_logs(&[
            "create BarDaxSrc",
            "setup BarDaxSrc",
            "create FooDaxConn of FooDaxSrc",
            "create FooDaxConn",
            "create BarDaxConn of BarDaxSrc",
            "create BarDaxConn",
            "commit FooDaxConn",
            "commit BarDaxConn",
            "close BarDaxConn",
            "close FooDaxConn",
            "close BarDaxSrc",
            "close FooDaxSrc",
        ]);
    }
}

#[cfg(test)]
mod tests_of_dax {
    use super::*;

    struct FooDaxConn {
        committed: bool,
    }
    impl FooDaxConn {
        fn new() -> Self {
            Self { committed: false }
        }
        fn get_string(&self) -> Result<String, Err> {
            Ok(String::from("aaa"))
        }
        fn get_bool(&self) -> bool {
            true
        }
    }
    impl DaxConn for FooDaxConn {
        fn commit(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
            self.committed = true;
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut dyn AsyncGroup) {}
        fn force_back(&mut self, _ag: &mut dyn AsyncGroup) {}
        fn close(&mut self) {}
    }

    struct FooDaxSrc {}
    impl FooDaxSrc {
        fn new() -> Self {
            Self {}
        }
    }
    impl DaxSrc for FooDaxSrc {
        fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
            Ok(())
        }
        fn close(&mut self, _ag: &mut dyn AsyncGroup) {}
        fn create_dax_conn(&mut self) -> Result<Box<dyn DaxConn>, Err> {
            Ok(Box::new(FooDaxConn::new()))
        }
    }

    struct BarDaxConn {
        committed: bool,
    }
    impl BarDaxConn {
        fn new() -> Self {
            Self { committed: false }
        }
        fn get_num(&self) -> i64 {
            -123
        }
    }
    impl DaxConn for BarDaxConn {
        fn commit(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
            self.committed = true;
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut dyn AsyncGroup) {}
        fn force_back(&mut self, _ag: &mut dyn AsyncGroup) {}
        fn close(&mut self) {}
    }

    struct BarDaxSrc {}
    impl BarDaxSrc {
        fn new() -> Self {
            Self {}
        }
    }
    impl DaxSrc for BarDaxSrc {
        fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
            Ok(())
        }
        fn close(&mut self, _ag: &mut dyn AsyncGroup) {}
        fn create_dax_conn(&mut self) -> Result<Box<dyn DaxConn>, Err> {
            Ok(Box::new(BarDaxConn::new()))
        }
    }

    trait FooDax: Dax {
        fn m01(&mut self) -> Result<String, Err> {
            let conn = self.get_dax_conn::<FooDaxConn>("foo").unwrap();
            conn.get_string()
        }
        fn m03(&mut self) -> Result<bool, Err> {
            let conn = self.get_dax_conn::<FooDaxConn>("foo").unwrap();
            Ok(conn.get_bool())
        }
    }

    trait BarDax: Dax {
        fn m02(&mut self) -> Result<i64, Err> {
            let conn = self.get_dax_conn::<BarDaxConn>("bar").unwrap();
            Ok(conn.get_num())
        }
    }

    trait HogeLogicDax {
        fn m01(&mut self) -> Result<String, Err>;
        fn m02(&mut self) -> Result<i64, Err>;
    }

    trait FugaLogicDax {
        fn m02(&mut self) -> Result<i64, Err>;
        fn m03(&mut self) -> Result<bool, Err>;
    }

    struct DaxBase {
        pub(crate) Impl: DaxBaseImpl,
    }
    impl DaxBase {
        fn new() -> Self {
            Self {
                Impl: DaxBaseImpl::new(),
            }
        }
        fn uses<S: DaxSrc + 'static>(&mut self, name: &str, ds: S) -> Result<(), Err> {
            self.Impl.uses(name, ds)
        }
        fn disuses(&mut self, name: &str) {
            self.Impl.disuses(name);
        }
    }
    impl Dax for DaxBase {
        fn get_dax_conn<C: DaxConn + 'static>(&mut self, name: &str) -> Result<&C, Err> {
            self.Impl.get_dax_conn::<C>(name)
        }
    }
    impl FooDax for DaxBase {}
    impl BarDax for DaxBase {}
    impl HogeLogicDax for DaxBase {
        fn m01(&mut self) -> Result<String, Err> {
            FooDax::m01(self)
        }
        fn m02(&mut self) -> Result<i64, Err> {
            BarDax::m02(self)
        }
    }
    impl FugaLogicDax for DaxBase {
        fn m02(&mut self) -> Result<i64, Err> {
            BarDax::m02(self)
        }
        fn m03(&mut self) -> Result<bool, Err> {
            FooDax::m03(self)
        }
    }

    fn hoge_logic(dax: &mut dyn HogeLogicDax) -> Result<(), Err> {
        let s = dax.m01()?;
        assert_eq!(s, "aaa");

        let n = dax.m02()?;
        assert_eq!(n, -123);

        Ok(())
    }

    fn fuga_logic(dax: &mut dyn FugaLogicDax) -> Result<(), Err> {
        let n = dax.m02()?;
        assert_eq!(n, -123);

        let b = dax.m03()?;
        assert_eq!(b, true);

        Ok(())
    }

    #[test]
    fn test() {
        let mut dax = DaxBase::new();

        let _ = dax.uses("foo", FooDaxSrc::new()).unwrap();
        let _ = dax.uses("bar", BarDaxSrc::new()).unwrap();

        hoge_logic(&mut dax);
        let _ = dax.Impl.commit().unwrap();
        dax.Impl.end();

        fuga_logic(&mut dax);
        let _ = dax.Impl.commit().unwrap();
        dax.Impl.end();
    }
}
