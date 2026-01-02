// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{
    AsyncGroup, AutoShutdown, DataConn, DataConnContainer, DataSrc, DataSrcContainer,
    SendSyncNonNull,
};

use setup_read_cleanup::{PhasedCell, PhasedError};
use std::borrow::Cow;
use std::collections::HashMap;
use std::{any, marker, ptr};

#[derive(Debug)]
pub enum DataSrcError {
    FailToSetupGlobalDataSrcs { errors: HashMap<String, errs::Err> },
}

unsafe impl<T: Send + Sync> Send for SendSyncNonNull<T> {}
unsafe impl<T: Send + Sync> Sync for SendSyncNonNull<T> {}

impl<T: Send + Sync> SendSyncNonNull<T> {
    pub(crate) fn new(non_null_ptr: ptr::NonNull<T>) -> Self {
        Self {
            non_null_ptr,
            _phantom: marker::PhantomData,
        }
    }
}

impl<S, C> DataSrcContainer<S, C>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    pub(crate) fn new(name: impl Into<Cow<'static, str>>, data_src: S, local: bool) -> Self {
        Self {
            drop_fn: drop_data_src::<S, C>,
            setup_fn: setup_data_src::<S, C>,
            close_fn: close_data_src::<S, C>,
            create_data_conn_fn: create_data_conn::<S, C>,
            is_data_conn_fn: is_data_conn::<C>,

            local,
            name: name.into(),
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

fn setup_data_src<S, C>(ptr: *const DataSrcContainer, ag: &mut AsyncGroup) -> errs::Result<()>
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

fn create_data_conn<S, C>(ptr: *const DataSrcContainer) -> errs::Result<Box<DataConnContainer<C>>>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S, C>;
    let conn: Box<C> = unsafe { (*typed_ptr).data_src.create_data_conn() }?;
    let name = unsafe { &(*typed_ptr).name };
    Ok(Box::new(DataConnContainer::<C>::new(
        name.to_string(),
        conn,
    )))
}

fn is_data_conn<C>(type_id: any::TypeId) -> bool
where
    C: DataConn + 'static,
{
    any::TypeId::of::<C>() == type_id
}

static DS_VEC: PhasedCell<Vec<SendSyncNonNull<DataSrcContainer>>> = PhasedCell::new(Vec::new());

const NOOP: fn(&mut Vec<SendSyncNonNull<DataSrcContainer>>) -> Result<(), PhasedError> = |_| Ok(());

pub(crate) fn add_data_src<S, C>(
    data_src_vec: &mut Vec<SendSyncNonNull<DataSrcContainer>>,
    name: impl Into<Cow<'static, str>>,
    ds: S,
    local: bool,
) where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    let boxed = Box::new(DataSrcContainer::<S, C>::new(name, ds, local));
    let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
    data_src_vec.push(SendSyncNonNull::new(ptr));
}

pub(crate) fn setup_all_data_srcs(
    data_src_vec: &mut [SendSyncNonNull<DataSrcContainer>],
    err_map: &mut HashMap<String, errs::Err>,
) {
    if data_src_vec.is_empty() {
        return;
    }

    let mut done_vec = Vec::<*mut DataSrcContainer>::new();

    let mut ag = AsyncGroup::new();
    for ssnnptr in data_src_vec.iter() {
        let ptr = ssnnptr.non_null_ptr.as_ptr();
        let setup_fn = unsafe { (*ptr).setup_fn };
        ag.name = unsafe { &(*ptr).name };
        if let Err(err) = setup_fn(ptr, &mut ag) {
            err_map.insert(ag.name.to_string(), err);
            break;
        }
        done_vec.push(ptr);
    }
    ag.join_and_collect_errors(err_map);

    if !err_map.is_empty() {
        for ptr in done_vec {
            if !err_map.contains_key(unsafe { &(*ptr).name }.as_ref()) {
                let close_fn = unsafe { (*ptr).close_fn };
                close_fn(ptr);
            }
        }
        drop_all_data_srcs(data_src_vec);
    }
}

pub(crate) fn drop_all_data_srcs(data_src_vec: &[SendSyncNonNull<DataSrcContainer>]) {
    for ssnnptr in data_src_vec.iter().rev() {
        let ptr = ssnnptr.non_null_ptr.as_ptr();
        let drop_fn = unsafe { (*ptr).drop_fn };
        drop_fn(ptr);
    }
}

pub(crate) fn close_and_drop_all_data_srcs(data_src_vec: &[SendSyncNonNull<DataSrcContainer>]) {
    for ssnnptr in data_src_vec.iter().rev() {
        let ptr = ssnnptr.non_null_ptr.as_ptr();
        let close_fn = unsafe { (*ptr).close_fn };
        let drop_fn = unsafe { (*ptr).drop_fn };
        close_fn(ptr);
        drop_fn(ptr);
    }
}

pub(crate) fn copy_global_data_srcs_to_map(m: &mut HashMap<Cow<str>, *mut DataSrcContainer>) {
    if let Ok(static_ds_vec) = DS_VEC.read() {
        for ssnnptr in static_ds_vec {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            m.insert(unsafe { (*ptr).name.clone() }, ptr);
        }
    }
}

impl Drop for AutoShutdown {
    fn drop(&mut self) {
        let _ = DS_VEC.transition_to_cleanup(NOOP);
        match DS_VEC.get_mut_unlocked() {
            Ok(vec) => {
                close_and_drop_all_data_srcs(vec);
            }
            Err(e) => {
                eprintln!("ERROR(sabi): Fail to close and drop global DataSrc(s): {e:?}");
            }
        }
    }
}

pub fn uses<S, C>(name: impl Into<Cow<'static, str>>, ds: S)
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    match DS_VEC.get_mut_unlocked() {
        Ok(vec) => {
            add_data_src(vec, name, ds, false);
        }
        Err(e) => {
            eprintln!("ERROR(sabi): Fail to add a global DataSrc: {e:?}");
        }
    }
}

pub fn setup() -> errs::Result<AutoShutdown> {
    let mut errors = HashMap::new();
    let errors_ref_mut = &mut errors;

    let _ = DS_VEC.transition_to_read(move |vec| {
        let mut regs: Vec<_> = inventory::iter::<DataSrcRegistration>.into_iter().collect();
        regs.sort_unstable_by_key(|reg| reg.priority);

        let mut static_vec: Vec<SendSyncNonNull<DataSrcContainer>> = Vec::with_capacity(regs.len());
        for reg in regs {
            let any_container = (reg.factory)();
            static_vec.push(any_container.ssnnptr);
        }
        vec.splice(0..0, static_vec);

        setup_all_data_srcs(vec, errors_ref_mut);
        Ok::<(), PhasedError>(())
    });

    if errors.is_empty() {
        Ok(AutoShutdown {})
    } else {
        Err(errs::Err::new(DataSrcError::FailToSetupGlobalDataSrcs {
            errors,
        }))
    }
}

#[doc(hidden)]
pub struct AnyDataSrcContainer {
    ssnnptr: SendSyncNonNull<DataSrcContainer>,
}

#[doc(hidden)]
pub fn create_data_src_container<S, C>(name: &'static str, data_src: S) -> AnyDataSrcContainer
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    let boxed = Box::new(DataSrcContainer::<S, C>::new(name, data_src, false));
    let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
    AnyDataSrcContainer {
        ssnnptr: SendSyncNonNull::new(ptr),
    }
}

#[doc(hidden)]
pub struct DataSrcRegistration {
    factory: fn() -> AnyDataSrcContainer,
    priority: u32,
}
impl DataSrcRegistration {
    pub const fn new(factory: fn() -> AnyDataSrcContainer, priority: u32) -> Self {
        Self { factory, priority }
    }
}
inventory::collect!(DataSrcRegistration);

#[macro_export]
macro_rules! uses {
    ($name:tt, $data_src:expr, $priority:expr) => {
        const _: () = {
            inventory::submit! {
                $crate::DataSrcRegistration::new(|| {
                    $crate::create_data_src_container($name, $data_src)
                }, $priority)
            }
        };
    };
    ($name:tt, $data_src:expr) => {
        const _: () = {
            inventory::submit! {
                $crate::DataSrcRegistration::new(|| {
                    $crate::create_data_src_container($name, $data_src)
                }, u32::MAX)
            }
        };
    };
}

#[cfg(test)]
mod tests_of_data_src {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct SyncDataConn {}
    impl SyncDataConn {
        fn new() -> Self {
            Self {}
        }
    }
    impl DataConn for SyncDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
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
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            Ok(())
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {}
        fn close(&mut self) {}
    }

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
        fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            if self.will_fail {
                logger.push(format!("SyncDataSrc {} failed to setup", self.id));
                return Err(errs::Err::new("XXX".to_string()));
            }
            logger.push(format!("SyncDataSrc {} setupped", self.id));
            Ok(())
        }

        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataSrc {} closed", self.id));
        }

        fn create_data_conn(&mut self) -> errs::Result<Box<SyncDataConn>> {
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
        wait: u64,
    }
    impl AsyncDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, will_fail: bool, wait: u64) -> Self {
            Self {
                id,
                will_fail,
                logger,
                wait,
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
        fn setup(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
            let logger = self.logger.clone();
            let will_fail = self.will_fail;
            let id = self.id;
            let wait = self.wait;
            ag.add(move || {
                std::thread::sleep(std::time::Duration::from_millis(wait));
                let mut logger = logger.lock().unwrap();
                if will_fail {
                    logger.push(format!("AsyncDataSrc {} failed to setup", id));
                    return Err(errs::Err::new("XXX".to_string()));
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
        fn create_data_conn(&mut self) -> errs::Result<Box<AsyncDataConn>> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataSrc {} created DataConn", self.id));
            let conn = AsyncDataConn::new();
            Ok(Box::new(conn))
        }
    }

    #[test]
    fn test_of_add_data_src() {
        let ds_vec: PhasedCell<Vec<SendSyncNonNull<DataSrcContainer>>> =
            PhasedCell::new(Vec::new());

        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        let ds1 = SyncDataSrc::new(1, logger.clone(), false);
        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            add_data_src(vec, "foo", ds1, false);
        }
        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            assert_eq!(vec.len(), 1);
            let dsc =
                vec[0].non_null_ptr.as_ptr() as *mut DataSrcContainer<SyncDataSrc, SyncDataConn>;
            assert_eq!(unsafe { (*dsc).name.clone() }, "foo");
            assert_eq!(unsafe { (*dsc).local }, false);
        }

        let ds2 = SyncDataSrc::new(1, logger.clone(), false);
        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            add_data_src(vec, "bar", ds2, false);
        }
        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            let dsc =
                vec[0].non_null_ptr.as_ptr() as *mut DataSrcContainer<SyncDataSrc, SyncDataConn>;
            assert_eq!(unsafe { (*dsc).name.clone() }, "foo");
            assert_eq!(unsafe { (*dsc).local }, false);
            let dsc =
                vec[1].non_null_ptr.as_ptr() as *mut DataSrcContainer<SyncDataSrc, SyncDataConn>;
            assert_eq!(unsafe { (*dsc).name.clone() }, "bar");
            assert_eq!(unsafe { (*dsc).local }, false);
        }
    }

    #[test]
    fn test_of_setup_and_close_zero_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        let ds_vec: PhasedCell<Vec<SendSyncNonNull<DataSrcContainer>>> =
            PhasedCell::new(Vec::new());

        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            let mut err_map = HashMap::<String, errs::Err>::new();
            setup_all_data_srcs(vec, &mut err_map);
            if !err_map.is_empty() {
                panic!("{err_map:?}");
            }
        }

        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            close_and_drop_all_data_srcs(vec);
        }

        assert_eq!(logger.lock().unwrap().len(), 0);
    }

    #[test]
    fn test_of_setup_and_close_sync_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        let ds_vec: PhasedCell<Vec<SendSyncNonNull<DataSrcContainer>>> =
            PhasedCell::new(Vec::new());

        let ds1 = SyncDataSrc::new(1, logger.clone(), false);
        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            add_data_src(vec, "foo", ds1, false);
        }

        let ds2 = SyncDataSrc::new(2, logger.clone(), false);
        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            add_data_src(vec, "bar", ds2, false);
        }

        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            let mut err_map = HashMap::<String, errs::Err>::new();
            setup_all_data_srcs(vec, &mut err_map);
            if !err_map.is_empty() {
                panic!("{err_map:?}");
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec!["SyncDataSrc 1 setupped", "SyncDataSrc 2 setupped",]
        );

        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            close_and_drop_all_data_srcs(vec);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "SyncDataSrc 1 setupped",
                "SyncDataSrc 2 setupped",
                "SyncDataSrc 2 closed",
                "SyncDataSrc 2 dropped",
                "SyncDataSrc 1 closed",
                "SyncDataSrc 1 dropped",
            ]
        );
    }

    #[test]
    fn test_of_setup_and_close_async_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        let ds_vec: PhasedCell<Vec<SendSyncNonNull<DataSrcContainer>>> =
            PhasedCell::new(Vec::new());

        let ds1 = AsyncDataSrc::new(1, logger.clone(), false, 200);
        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            add_data_src(vec, "foo", ds1, false);
        }

        let ds2 = AsyncDataSrc::new(2, logger.clone(), false, 100);
        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            add_data_src(vec, "bar", ds2, false);
        }

        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            let mut err_map = HashMap::<String, errs::Err>::new();
            setup_all_data_srcs(vec, &mut err_map);
            if !err_map.is_empty() {
                panic!("{err_map:?}");
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec!["AsyncDataSrc 2 setupped", "AsyncDataSrc 1 setupped",]
        );

        {
            let vec = ds_vec.get_mut_unlocked().unwrap();
            close_and_drop_all_data_srcs(vec);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "AsyncDataSrc 2 setupped",
                "AsyncDataSrc 1 setupped",
                "AsyncDataSrc 2 closed",
                "AsyncDataSrc 2 dropped",
                "AsyncDataSrc 1 closed",
                "AsyncDataSrc 1 dropped",
            ]
        );
    }
}
