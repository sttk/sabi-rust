// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod global_setup;

pub(crate) use global_setup::{
    copy_global_data_srcs_to_map, create_data_conn_from_global_data_src,
};
pub use global_setup::{create_static_data_src_container, setup, setup_with_order, uses};

use crate::{
    AsyncGroup, DataConn, DataConnContainer, DataSrc, DataSrcContainer, DataSrcManager,
    SendSyncNonNull,
};

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, mem, ptr};

/// An enum type representing the reasons for errors that can occur within [`DataSrc`] operations.
#[derive(Debug)]
pub enum DataSrcError {
    /// Indicates a failure to register a global data source.
    /// This can happen if the global data source manager is in an invalid state.
    FailToRegisterGlobalDataSrc {
        /// The name of the data source that failed to register.
        name: Arc<str>,
    },

    /// Indicates a failure during the setup process of one or more global data sources.
    /// Contains a vector of data source names and their corresponding errors.
    FailToSetupGlobalDataSrcs {
        /// The vector contains errors that occurred in each [`DataSrc`] object.
        errors: Vec<(Arc<str>, errs::Err)>,
    },

    /// Indicates that a setup process for global data sources is currently ongoing.
    DuringSetupGlobalDataSrcs,

    /// Indicates that global data sources have already been set up.
    AlreadySetupGlobalDataSrcs,

    /// Indicates a failure to cast a retrieved [`DataConn`] to the expected type.
    FailToCastDataConn {
        /// The name of the data connection that failed to cast.
        name: Arc<str>,
        /// The type name to which the [`DataConn`] attempted to cast.
        target_type: &'static str,
    },

    /// Indicates a failure to create a [`DataConn`] object from its [`DataSrc`].
    FailToCreateDataConn {
        /// The name of the data source that failed to be created.
        name: Arc<str>,
        /// The type name of the [`DataConn`] that failed to be created.
        data_conn_type: &'static str,
    },

    /// Indicates that no [`DataSrc`] was found to create a [`DataConn`] for the specified name
    /// and type.
    NotFoundDataSrcToCreateDataConn {
        /// The name of the data source that could not be found.
        name: Arc<str>,
        /// The type name of the [`DataConn`] that was requested.
        data_conn_type: &'static str,
    },
}

impl<S, C> DataSrcContainer<S, C>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    pub(crate) fn new(name: impl Into<Arc<str>>, data_src: S, local: bool) -> Self {
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
    unsafe { (*typed_ptr).data_src.close() };
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

impl DataSrcManager {
    pub(crate) const fn new(local: bool) -> Self {
        Self {
            vec_unready: Vec::new(),
            vec_ready: Vec::new(),
            local,
        }
    }

    pub(crate) fn prepend(&mut self, vec: Vec<SendSyncNonNull<DataSrcContainer>>) {
        self.vec_unready.splice(0..0, vec);
    }

    pub(crate) fn add<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        let boxed = Box::new(DataSrcContainer::<S, C>::new(name, ds, self.local));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
        self.vec_unready.push(SendSyncNonNull::new(ptr));
    }

    pub(crate) fn remove(&mut self, name: impl AsRef<str>) {
        let extracted_vec: Vec<_> = self
            .vec_ready
            .extract_if(.., |ssnnptr| {
                unsafe { &(*ssnnptr.non_null_ptr.as_ptr()).name }.as_ref() == name.as_ref()
            })
            .collect();

        for ssnnptr in extracted_vec.iter().rev() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let close_fn = unsafe { (*ptr).close_fn };
            let drop_fn = unsafe { (*ptr).drop_fn };
            close_fn(ptr);
            drop_fn(ptr);
        }

        let extracted_vec: Vec<_> = self
            .vec_unready
            .extract_if(.., |ssnnptr| {
                unsafe { &(*ssnnptr.non_null_ptr.as_ptr()).name }.as_ref() == name.as_ref()
            })
            .collect();

        for ssnnptr in extracted_vec.iter().rev() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let drop_fn = unsafe { (*ptr).drop_fn };
            drop_fn(ptr);
        }
    }

    pub(crate) fn close(&mut self) {
        let vec = mem::take(&mut self.vec_ready);
        for ssnnptr in vec.into_iter().rev() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let close_fn = unsafe { (*ptr).close_fn };
            let drop_fn = unsafe { (*ptr).drop_fn };
            close_fn(ptr);
            drop_fn(ptr);
        }
        let vec = mem::take(&mut self.vec_unready);
        for ssnnptr in vec.into_iter().rev() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let drop_fn = unsafe { (*ptr).drop_fn };
            drop_fn(ptr);
        }
    }

    pub(crate) fn setup(&mut self, errors: &mut Vec<(Arc<str>, errs::Err)>) {
        if self.vec_unready.is_empty() {
            return;
        }

        let mut n_done = 0;
        let mut ag = AsyncGroup::new();
        for ssnnptr in self.vec_unready.iter() {
            n_done += 1;
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let setup_fn = unsafe { (*ptr).setup_fn };
            ag._name = unsafe { (*ptr).name.clone() };
            if let Err(err) = setup_fn(ptr, &mut ag) {
                errors.push((ag._name.clone(), err));
                break;
            }
        }
        ag.join_and_collect_errors(errors);

        if errors.is_empty() {
            self.vec_ready.append(&mut self.vec_unready);
        } else {
            for ssnnptr in self.vec_unready[0..n_done].iter().rev() {
                let ptr = ssnnptr.non_null_ptr.as_ptr();
                let close_fn = unsafe { (*ptr).close_fn };
                close_fn(ptr);
            }
        }
    }

    pub(crate) fn setup_with_order(
        &mut self,
        names: &[&str],
        errors: &mut Vec<(Arc<str>, errs::Err)>,
    ) {
        if self.vec_unready.is_empty() {
            return;
        }

        let mut index_map: HashMap<&str, usize> = HashMap::with_capacity(names.len());
        // To overwrite later indexed elements with eariler ones when names overlap
        for (i, nm) in names.iter().rev().enumerate() {
            index_map.insert(*nm, names.len() - 1 - i);
        }

        let vec_unready = mem::take(&mut self.vec_unready);

        let mut ordered_vec: Vec<Option<SendSyncNonNull<DataSrcContainer>>> =
            vec![None; index_map.len()];
        for ssnnptr in vec_unready.into_iter() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let name = unsafe { (*ptr).name.clone() };
            if let Some(index) = index_map.remove(name.as_ref()) {
                ordered_vec[index] = Some(ssnnptr);
            } else {
                ordered_vec.push(Some(ssnnptr));
            }
        }

        let mut n_done = 0;
        let mut ag = AsyncGroup::new();
        for ssnnptr_opt in ordered_vec.iter() {
            n_done += 1;
            if let Some(ssnnptr) = ssnnptr_opt {
                let ptr = ssnnptr.non_null_ptr.as_ptr();
                let setup_fn = unsafe { (*ptr).setup_fn };
                ag._name = unsafe { (*ptr).name.clone() };
                if let Err(err) = setup_fn(ptr, &mut ag) {
                    errors.push((ag._name.clone(), err));
                    break;
                }
            }
        }
        ag.join_and_collect_errors(errors);

        if errors.is_empty() {
            for ssnnptr in ordered_vec.into_iter().flatten() {
                self.vec_ready.push(ssnnptr);
            }
        } else {
            for ssnnptr in ordered_vec[0..n_done].iter().flatten().rev() {
                let ptr = ssnnptr.non_null_ptr.as_ptr();
                let close_fn = unsafe { (*ptr).close_fn };
                close_fn(ptr);
            }
            for ssnnptr in ordered_vec.into_iter().flatten() {
                self.vec_unready.push(ssnnptr);
            }
        }
    }

    pub(crate) fn copy_ds_ready_to_map(&self, index_map: &mut HashMap<Arc<str>, (bool, usize)>) {
        for (i, ssnnptr) in self.vec_ready.iter().enumerate() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let name = unsafe { (*ptr).name.clone() };
            index_map.insert(name, (self.local, i));
        }
    }

    pub(crate) fn create_data_conn<C>(
        &self,
        index: usize,
        name: impl AsRef<str>,
    ) -> errs::Result<Box<DataConnContainer>>
    where
        C: DataConn + 'static,
    {
        if let Some(ssnnptr) = self.vec_ready.get(index) {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let type_id = any::TypeId::of::<C>();
            let is_fn = unsafe { (*ptr).is_data_conn_fn };
            let create_data_conn_fn = unsafe { (*ptr).create_data_conn_fn };
            if !is_fn(type_id) {
                Err(errs::Err::new(DataSrcError::FailToCastDataConn {
                    name: name.as_ref().into(),
                    target_type: any::type_name::<C>(),
                }))
            } else {
                match create_data_conn_fn(ptr) {
                    Ok(boxed) => Ok(boxed),
                    Err(err) => Err(errs::Err::with_source(
                        DataSrcError::FailToCreateDataConn {
                            name: name.as_ref().into(),
                            data_conn_type: any::type_name::<C>(),
                        },
                        err,
                    )),
                }
            }
        } else {
            Err(errs::Err::new(
                DataSrcError::NotFoundDataSrcToCreateDataConn {
                    name: name.as_ref().into(),
                    data_conn_type: any::type_name::<C>(),
                },
            ))
        }
    }
}

impl Drop for DataSrcManager {
    fn drop(&mut self) {
        self.close();
    }
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
        logger: Arc<Mutex<Vec<String>>>,
        fail_to_setup: bool,
        fail_to_create_data_conn: bool,
    }
    impl SyncDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail_to_setup: bool) -> Self {
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataSrc::new {}", id));
            Self {
                id,
                logger: logger,
                fail_to_setup,
                fail_to_create_data_conn: false,
            }
        }
        fn new_for_fail_to_create_data_conn(id: i8, logger: Arc<Mutex<Vec<String>>>) -> Self {
            Self {
                id,
                logger: logger,
                fail_to_setup: false,
                fail_to_create_data_conn: true,
            }
        }
    }
    impl Drop for SyncDataSrc {
        fn drop(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<SyncDataConn> for SyncDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail_to_setup {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("SyncDataSrc::setup {} failed", self.id));
                return Err(errs::Err::new("XXX".to_string()));
            }
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataSrc::setup {}", self.id));
            Ok(())
        }
        fn close(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataSrc::close {}", self.id));
        }
        fn create_data_conn(&mut self) -> errs::Result<Box<SyncDataConn>> {
            {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("SyncDataSrc::create_data_conn {}", self.id));
            }
            if self.fail_to_create_data_conn {
                return Err(errs::Err::new("eeee".to_string()));
            }
            let conn = SyncDataConn::new();
            Ok(Box::new(conn))
        }
    }

    struct AsyncDataSrc {
        id: i8,
        fail: bool,
        logger: Arc<Mutex<Vec<String>>>,
        wait: u64,
    }
    impl AsyncDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: bool, wait: u64) -> Self {
            logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataSrc::new {}", id));
            Self {
                id,
                fail,
                logger,
                wait,
            }
        }
    }
    impl Drop for AsyncDataSrc {
        fn drop(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<AsyncDataConn> for AsyncDataSrc {
        fn setup(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
            let logger = self.logger.clone();
            let fail = self.fail;
            let id = self.id;
            let wait = self.wait;
            ag.add(move || {
                std::thread::sleep(std::time::Duration::from_millis(wait));
                let mut logger = logger.lock().unwrap();
                if fail {
                    logger.push(format!("AsyncDataSrc::setup {} failed to setup", id));
                    return Err(errs::Err::new("XXX".to_string()));
                }
                logger.push(format!("AsyncDataSrc::setup {}", id));
                Ok(())
            });
            Ok(())
        }
        fn close(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataSrc::close {}", self.id));
        }
        fn create_data_conn(&mut self) -> errs::Result<Box<AsyncDataConn>> {
            {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataSrc::create_data_conn {}", self.id));
            }
            let conn = AsyncDataConn::new();
            Ok(Box::new(conn))
        }
    }

    #[test]
    fn test_of_new() {
        let manager = DataSrcManager::new(true);
        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);

        let manager = DataSrcManager::new(false);
        assert!(!manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);
    }

    #[test]
    fn test_of_prepend() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut vec = Vec::<SendSyncNonNull<DataSrcContainer>>::new();

            let ds = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::new("foo", ds, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            vec.push(SendSyncNonNull::new(ptr));

            let ds = AsyncDataSrc::new(2, logger.clone(), false, 0);
            let boxed = Box::new(DataSrcContainer::new("bar", ds, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            vec.push(SendSyncNonNull::new(ptr));

            let mut manager = DataSrcManager::new(true);
            manager.prepend(vec);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 2);
            assert_eq!(manager.vec_ready.len(), 0);

            assert_eq!(
                unsafe { manager.vec_unready[0].non_null_ptr.as_ref().name.clone() },
                "foo".into()
            );
            assert_eq!(
                unsafe { manager.vec_unready[1].non_null_ptr.as_ref().name.clone() },
                "bar".into()
            );

            let mut vec = Vec::<SendSyncNonNull<DataSrcContainer>>::new();

            let ds = SyncDataSrc::new(3, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::new("baz", ds, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            vec.push(SendSyncNonNull::new(ptr));

            let ds = AsyncDataSrc::new(4, logger.clone(), false, 0);
            let boxed = Box::new(DataSrcContainer::new("qux", ds, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            vec.push(SendSyncNonNull::new(ptr));

            manager.prepend(vec);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 4);
            assert_eq!(manager.vec_ready.len(), 0);

            assert_eq!(
                unsafe { manager.vec_unready[0].non_null_ptr.as_ref().name.clone() },
                "baz".into()
            );
            assert_eq!(
                unsafe { manager.vec_unready[1].non_null_ptr.as_ref().name.clone() },
                "qux".into()
            );
            assert_eq!(
                unsafe { manager.vec_unready[2].non_null_ptr.as_ref().name.clone() },
                "foo".into()
            );
            assert_eq!(
                unsafe { manager.vec_unready[3].non_null_ptr.as_ref().name.clone() },
                "bar".into()
            );
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "SyncDataSrc::new 1",
                "AsyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "AsyncDataSrc::new 4",
                "AsyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
                "AsyncDataSrc::drop 4",
                "SyncDataSrc::drop 3",
            ],
        );
    }

    #[test]
    fn test_of_add() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut manager = DataSrcManager::new(true);

            let ds = SyncDataSrc::new(1, logger.clone(), false);
            manager.add("foo", ds);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 1);
            assert_eq!(manager.vec_ready.len(), 0);

            assert_eq!(
                unsafe { manager.vec_unready[0].non_null_ptr.as_ref().name.clone() },
                "foo".into()
            );

            let ds = AsyncDataSrc::new(2, logger.clone(), false, 0);
            manager.add("bar", ds);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 2);
            assert_eq!(manager.vec_ready.len(), 0);

            assert_eq!(
                unsafe { manager.vec_unready[0].non_null_ptr.as_ref().name.clone() },
                "foo".into()
            );
            assert_eq!(
                unsafe { manager.vec_unready[1].non_null_ptr.as_ref().name.clone() },
                "bar".into()
            );
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "SyncDataSrc::new 1",
                "AsyncDataSrc::new 2",
                "AsyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ],
        );
    }

    #[test]
    fn test_of_remove() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut manager = DataSrcManager::new(true);

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::new("foo", ds1, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            manager.vec_unready.push(SendSyncNonNull::new(ptr));

            let ds2 = AsyncDataSrc::new(2, logger.clone(), false, 0);
            let boxed = Box::new(DataSrcContainer::new("bar", ds2, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            manager.vec_unready.push(SendSyncNonNull::new(ptr));

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::new("baz", ds3, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            manager.vec_ready.push(SendSyncNonNull::new(ptr));

            let ds4 = AsyncDataSrc::new(4, logger.clone(), false, 0);
            let boxed = Box::new(DataSrcContainer::new("qux", ds4, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            manager.vec_ready.push(SendSyncNonNull::new(ptr));

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 2);
            assert_eq!(manager.vec_ready.len(), 2);

            manager.remove("baz");
            manager.remove("foo");
            manager.remove("qux");
            manager.remove("bar");
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "SyncDataSrc::new 1",
                "AsyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "AsyncDataSrc::new 4",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::drop 1",
                "AsyncDataSrc::close 4",
                "AsyncDataSrc::drop 4",
                "AsyncDataSrc::drop 2",
            ],
        );
    }

    #[test]
    fn test_of_close() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut manager = DataSrcManager::new(true);

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::new("foo", ds1, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            manager.vec_unready.push(SendSyncNonNull::new(ptr));

            let ds2 = AsyncDataSrc::new(2, logger.clone(), false, 0);
            let boxed = Box::new(DataSrcContainer::new("bar", ds2, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            manager.vec_unready.push(SendSyncNonNull::new(ptr));

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
            let boxed = Box::new(DataSrcContainer::new("baz", ds3, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            manager.vec_ready.push(SendSyncNonNull::new(ptr));

            let ds4 = AsyncDataSrc::new(4, logger.clone(), false, 0);
            let boxed = Box::new(DataSrcContainer::new("qux", ds4, true));
            let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
            manager.vec_ready.push(SendSyncNonNull::new(ptr));

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 2);
            assert_eq!(manager.vec_ready.len(), 2);

            manager.close();
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "SyncDataSrc::new 1",
                "AsyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "AsyncDataSrc::new 4",
                "AsyncDataSrc::close 4",
                "AsyncDataSrc::drop 4",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
                "AsyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ],
        );
    }

    #[test]
    fn test_of_setup_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut manager = DataSrcManager::new(true);

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            manager.add("foo", ds1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            manager.add("bar", ds2);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 2);
            assert_eq!(manager.vec_ready.len(), 0);

            let mut vec = Vec::new();
            manager.setup(&mut vec);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 0);
            assert_eq!(manager.vec_ready.len(), 2);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ],
        );
    }

    #[test]
    fn test_of_setup_but_error() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut manager = DataSrcManager::new(true);

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            manager.add("foo", ds1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), true);
            manager.add("bar", ds2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), true);
            manager.add("bar", ds3);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 3);
            assert_eq!(manager.vec_ready.len(), 0);

            let mut vec = Vec::new();
            manager.setup(&mut vec);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 3);
            assert_eq!(manager.vec_ready.len(), 0);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2 failed",
                "SyncDataSrc::close 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 3",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
            ],
        );
    }

    #[test]
    fn test_of_setup_with_order_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut manager = DataSrcManager::new(true);

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            manager.add("foo", ds1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            manager.add("bar", ds2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
            manager.add("baz", ds3);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 3);
            assert_eq!(manager.vec_ready.len(), 0);

            let mut vec = Vec::new();
            manager.setup_with_order(&["baz", "foo"], &mut vec);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 0);
            assert_eq!(manager.vec_ready.len(), 3);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
            ],
        );
    }

    #[test]
    fn test_of_setup_with_order_but_fail() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut manager = DataSrcManager::new(true);

            let ds1 = SyncDataSrc::new(1, logger.clone(), true);
            manager.add("foo", ds1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), true);
            manager.add("bar", ds2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
            manager.add("baz", ds3);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 3);
            assert_eq!(manager.vec_ready.len(), 0);

            let mut vec = Vec::new();
            manager.setup_with_order(&["baz", "foo"], &mut vec);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 3);
            assert_eq!(manager.vec_ready.len(), 0);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::setup 1 failed",
                "SyncDataSrc::close 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::drop 1",
                "SyncDataSrc::drop 3",
            ],
        );
    }

    #[test]
    fn test_of_setup_with_order_containing_duplicated_name_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));

        {
            let mut manager = DataSrcManager::new(true);

            let ds1 = SyncDataSrc::new(1, logger.clone(), false);
            manager.add("foo", ds1);

            let ds2 = SyncDataSrc::new(2, logger.clone(), false);
            manager.add("bar", ds2);

            let ds3 = SyncDataSrc::new(3, logger.clone(), false);
            manager.add("baz", ds3);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 3);
            assert_eq!(manager.vec_ready.len(), 0);

            let mut vec = Vec::new();
            manager.setup_with_order(&["baz", "foo", "baz"], &mut vec);

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 0);
            assert_eq!(manager.vec_ready.len(), 3);
        }

        assert_eq!(
            *logger.lock().unwrap(),
            vec![
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::new 3",
                "SyncDataSrc::setup 3",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
                "SyncDataSrc::close 3",
                "SyncDataSrc::drop 3",
            ],
        );
    }

    #[test]
    fn test_of_copy_ds_ready_to_map() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut errors = Vec::new();

        let mut index_map = HashMap::<Arc<str>, (bool, usize)>::new();

        let manager = DataSrcManager::new(true);
        manager.copy_ds_ready_to_map(&mut index_map);
        assert!(index_map.is_empty());

        let mut manager = DataSrcManager::new(true);
        let ds1 = SyncDataSrc::new(1, logger.clone(), false);
        manager.add("foo", ds1);
        manager.setup(&mut errors);
        assert!(errors.is_empty());
        manager.copy_ds_ready_to_map(&mut index_map);
        assert_eq!(index_map.len(), 1);
        assert_eq!(index_map.get("foo").unwrap(), &(true, 0));

        let mut manager = DataSrcManager::new(false);
        let ds2 = AsyncDataSrc::new(2, logger.clone(), false, 0);
        let ds3 = SyncDataSrc::new(3, logger.clone(), false);
        manager.add("bar", ds2);
        manager.add("baz", ds3);
        manager.setup(&mut errors);
        assert!(errors.is_empty());
        manager.copy_ds_ready_to_map(&mut index_map);
        assert_eq!(index_map.len(), 3);
        assert_eq!(index_map.get("foo").unwrap(), &(true, 0));
        assert_eq!(index_map.get("bar").unwrap(), &(false, 0));
        assert_eq!(index_map.get("baz").unwrap(), &(false, 1));
    }

    #[test]
    fn test_of_create_data_conn_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut errors = Vec::new();

        let mut manager = DataSrcManager::new(true);
        let ds1 = SyncDataSrc::new(1, logger.clone(), false);
        manager.add("foo", ds1);
        manager.setup(&mut errors);

        if let Ok(boxed) = manager.create_data_conn::<SyncDataConn>(0, "foo") {
            assert_eq!(boxed.name.clone(), "foo".into());
        } else {
            panic!();
        }
    }

    #[test]
    fn test_of_create_data_conn_but_not_found() {
        let mut errors = Vec::new();

        let mut manager = DataSrcManager::new(true);
        manager.setup(&mut errors);

        if let Err(err) = manager.create_data_conn::<SyncDataConn>(0, "foo") {
            match err.reason::<DataSrcError>() {
                Ok(DataSrcError::NotFoundDataSrcToCreateDataConn {
                    name,
                    data_conn_type,
                }) => {
                    assert_eq!(*name, "foo".into());
                    assert_eq!(
                        *data_conn_type,
                        "sabi::data_src::tests_of_data_src::SyncDataConn"
                    );
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn test_of_create_data_conn_but_fail_to_cast() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut errors = Vec::new();

        let mut manager = DataSrcManager::new(true);
        let ds1 = SyncDataSrc::new(1, logger.clone(), false);
        manager.add("foo", ds1);
        manager.setup(&mut errors);

        if let Err(err) = manager.create_data_conn::<AsyncDataConn>(0, "foo") {
            match err.reason::<DataSrcError>() {
                Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                    assert_eq!(*name, "foo".into());
                    assert_eq!(
                        *target_type,
                        "sabi::data_src::tests_of_data_src::AsyncDataConn"
                    );
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }

    #[test]
    fn test_of_create_data_conn_but_fail_to_create() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut errors = Vec::new();

        let mut manager = DataSrcManager::new(true);
        let ds1 = SyncDataSrc::new_for_fail_to_create_data_conn(1, logger.clone());
        manager.add("foo", ds1);
        manager.setup(&mut errors);

        if let Err(err) = manager.create_data_conn::<SyncDataConn>(0, "foo") {
            match err.reason::<DataSrcError>() {
                Ok(DataSrcError::FailToCreateDataConn {
                    name,
                    data_conn_type,
                }) => {
                    assert_eq!(*name, "foo".into());
                    assert_eq!(
                        *data_conn_type,
                        "sabi::data_src::tests_of_data_src::SyncDataConn"
                    );
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }
}
