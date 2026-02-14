// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod global_setup;

pub(crate) use global_setup::{
    copy_global_data_srcs_to_map, create_data_conn_from_global_data_src_async,
};
pub use global_setup::{
    create_static_data_src_container, setup_async, setup_with_order_async, uses_async,
};

use crate::tokio::{
    AsyncGroup, DataConn, DataConnContainer, DataSrc, DataSrcContainer, DataSrcManager,
    SendSyncNonNull,
};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{any, mem, ptr};

/// Represents errors that can occur during data source operations.
#[derive(Debug)]
pub enum DataSrcError {
    /// An error indicating that one or more global data sources failed during their setup process.
    FailToSetupGlobalDataSrcs {
        /// A vector of errors, each containing the name of the data source and the error itself.
        errors: Vec<(Arc<str>, errs::Err)>,
    },

    /// An error indicating that a global data source setup is currently in progress.
    DuringSetupGlobalDataSrcs,

    /// An error indicating that global data sources have already been set up.
    AlreadySetupGlobalDataSrcs,

    /// An error indicating that a data connection could not be cast to the target type.
    FailToCastDataConn {
        /// The name of the data source that failed to provide the correct connection type.
        name: Arc<str>,
        /// The string representation of the target data connection type that was requested.
        target_type: &'static str,
    },

    /// An error indicating that a data connection could not be created by its data source.
    FailToCreateDataConn {
        /// The name of the data source that failed to create a data connection.
        name: Arc<str>,
        /// The string representation of the data connection type that was requested.
        data_conn_type: &'static str,
    },

    /// An error indicating that no data source was found for the requested data connection.
    NotFoundDataSrcToCreateDataConn {
        /// The name of the data source that was not found.
        name: Arc<str>,
        /// The string representation of the data connection type that was requested.
        data_conn_type: &'static str,
    },
}

impl<S, C> DataSrcContainer<S, C>
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    pub(crate) fn new(name: impl Into<Arc<str>>, data_src: S, local: bool) -> Self {
        Self {
            drop_fn: drop_data_src::<S, C>,
            close_fn: close_data_src::<S, C>,
            is_data_conn_fn: is_data_conn::<C>,

            setup_fn: setup_data_src_async::<S, C>,
            create_data_conn_fn: create_data_conn_async::<S, C>,

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

fn close_data_src<S, C>(ptr: *const DataSrcContainer)
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S, C>;
    unsafe { (*typed_ptr).data_src.close() };
}

fn is_data_conn<C>(type_id: any::TypeId) -> bool
where
    C: DataConn + 'static,
{
    any::TypeId::of::<C>() == type_id
}

fn setup_data_src_async<S, C>(
    ptr: *const DataSrcContainer,
    ag: &mut AsyncGroup,
) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + '_>>
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S, C>;
    let data_src = unsafe { &mut (*typed_ptr).data_src };
    Box::pin(data_src.setup_async(ag))
}

#[allow(clippy::type_complexity)]
fn create_data_conn_async<'a, S, C>(
    ptr: *const DataSrcContainer,
) -> Pin<Box<dyn Future<Output = errs::Result<Box<DataConnContainer<C>>>> + 'a>>
where
    S: DataSrc<C> + 'a,
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataSrcContainer<S, C>;
    let data_src = unsafe { &mut (*typed_ptr).data_src };
    let name = unsafe { &(*typed_ptr).name };
    Box::pin(async move {
        let conn: Box<C> = data_src.create_data_conn_async().await?;
        Ok(Box::new(DataConnContainer::<C>::new(
            name.to_string(),
            conn,
        )))
    })
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
        S: DataSrc<C> + 'static,
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

    pub(crate) async fn setup_async(&mut self, errors: &mut Vec<(Arc<str>, errs::Err)>) {
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
            if let Err(err) = setup_fn(ptr, &mut ag).await {
                errors.push((ag._name.clone(), err));
                break;
            }
        }
        ag.join_and_collect_errors_async(errors).await;

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

    pub(crate) async fn setup_with_order_async(
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
                if let Err(err) = setup_fn(ptr, &mut ag).await {
                    errors.push((ag._name.clone(), err));
                    break;
                }
            }
        }
        ag.join_and_collect_errors_async(errors).await;

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

    pub(crate) async fn create_data_conn_async<C>(
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
                match create_data_conn_fn(ptr).await {
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
    use std::sync::Arc;
    use tokio::sync::Mutex;

    struct SyncDataConn {}
    impl SyncDataConn {
        fn new() -> Self {
            Self {}
        }
    }
    impl DataConn for SyncDataConn {
        async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            Ok(())
        }
        async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {}
        fn close(&mut self) {}
    }

    struct AsyncDataConn {}
    impl AsyncDataConn {
        fn new() -> Self {
            Self {}
        }
    }
    impl DataConn for AsyncDataConn {
        async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            Ok(())
        }
        async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {}
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
            let logger_clone = logger.clone();
            tokio::spawn(async move {
                logger_clone
                    .lock()
                    .await
                    .push(format!("SyncDataSrc::new {}", id));
            });
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
            let logger = self.logger.clone();
            let id = self.id;
            tokio::spawn(async move {
                logger
                    .lock()
                    .await
                    .push(format!("SyncDataSrc::drop {}", id));
            });
        }
    }
    impl DataSrc<SyncDataConn> for SyncDataSrc {
        async fn setup_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let fail = self.fail_to_setup;
            let id = self.id;
            let logger = self.logger.clone();

            if fail {
                logger
                    .lock()
                    .await
                    .push(format!("SyncDataSrc::setup {} failed", id));
                return Err(errs::Err::new("XXX".to_string()));
            }
            logger
                .lock()
                .await
                .push(format!("SyncDataSrc::setup {}", id));
            Ok(())
        }
        fn close(&mut self) {
            let logger = self.logger.clone();
            let id = self.id;
            tokio::spawn(async move {
                logger
                    .lock()
                    .await
                    .push(format!("SyncDataSrc::close {}", id));
            });
        }
        async fn create_data_conn_async(&mut self) -> errs::Result<Box<SyncDataConn>> {
            let id = self.id;
            let logger = self.logger.clone();
            {
                logger
                    .lock()
                    .await
                    .push(format!("SyncDataSrc::create_data_conn {}", id));
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
            let logger_clone = logger.clone();
            tokio::spawn(async move {
                logger_clone
                    .lock()
                    .await
                    .push(format!("AsyncDataSrc::new {}", id));
            });
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
            let logger = self.logger.clone();
            let id = self.id;
            tokio::spawn(async move {
                logger
                    .lock()
                    .await
                    .push(format!("AsyncDataSrc::drop {}", id));
            });
        }
    }
    impl DataSrc<AsyncDataConn> for AsyncDataSrc {
        async fn setup_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
            let logger = self.logger.clone();
            let fail = self.fail;
            let id = self.id;
            let wait = self.wait;

            ag.add(async move {
                tokio::time::sleep(std::time::Duration::from_millis(wait)).await;
                let mut logger = logger.lock().await;
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
            let logger = self.logger.clone();
            let id = self.id;
            tokio::spawn(async move {
                logger
                    .lock()
                    .await
                    .push(format!("AsyncDataSrc::close {}", id));
            });
        }
        async fn create_data_conn_async(&mut self) -> errs::Result<Box<AsyncDataConn>> {
            let logger = self.logger.clone();
            {
                logger
                    .lock()
                    .await
                    .push(format!("AsyncDataSrc::create_data_conn {}", self.id));
            }
            let conn = AsyncDataConn::new();
            Ok(Box::new(conn))
        }
    }

    #[tokio::test]
    async fn test_of_new() {
        let manager = DataSrcManager::new(true);
        assert!(manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);

        let manager = DataSrcManager::new(false);
        assert!(!manager.local);
        assert_eq!(manager.vec_unready.len(), 0);
        assert_eq!(manager.vec_ready.len(), 0);
    }

    #[tokio::test]
    async fn test_of_prepend() {
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

        // Give some time for drop to be called
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let locked = logger.lock().await;
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 4".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
    }

    #[tokio::test]
    async fn test_of_add() {
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

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let locked = logger.lock().await;
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
    }

    #[tokio::test]
    async fn test_of_remove() {
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

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let locked = logger.lock().await;
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::close 4".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 4".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 2".to_string()));
    }

    #[tokio::test]
    async fn test_of_close() {
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

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let locked = logger.lock().await;
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::new 4".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::close 4".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 4".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
        assert!(locked.contains(&"AsyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
    }

    #[tokio::test]
    async fn test_of_setup_async_and_ok() {
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
            manager.setup_async(&mut vec).await;

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 0);
            assert_eq!(manager.vec_ready.len(), 2);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let locked = logger.lock().await;
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
    }

    #[tokio::test]
    async fn test_of_setup_but_error() {
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
            manager.setup_async(&mut vec).await;

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 3);
            assert_eq!(manager.vec_ready.len(), 0);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let locked = logger.lock().await;
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 2 failed".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
    }

    #[tokio::test]
    async fn test_of_setup_with_order_and_ok() {
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
            manager
                .setup_with_order_async(&["baz", "foo"], &mut vec)
                .await;

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 0);
            assert_eq!(manager.vec_ready.len(), 3);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let locked = logger.lock().await;
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
    }

    #[tokio::test]
    async fn test_of_setup_with_order_but_fail() {
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
            manager
                .setup_with_order_async(&["baz", "foo"], &mut vec)
                .await;

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 3);
            assert_eq!(manager.vec_ready.len(), 0);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let locked = logger.lock().await;
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 1 failed".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
    }

    #[tokio::test]
    async fn test_of_setup_with_order_containing_duplicated_name_and_ok() {
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
            manager
                .setup_with_order_async(&["baz", "foo", "baz"], &mut vec)
                .await;

            assert!(manager.local);
            assert_eq!(manager.vec_unready.len(), 0);
            assert_eq!(manager.vec_ready.len(), 3);
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let locked = logger.lock().await;
        assert!(locked.contains(&"SyncDataSrc::new 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::new 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::setup 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 2".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 1".to_string()));
        assert!(locked.contains(&"SyncDataSrc::close 3".to_string()));
        assert!(locked.contains(&"SyncDataSrc::drop 3".to_string()));
    }

    #[tokio::test]
    async fn test_of_copy_ds_ready_to_map() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut errors = Vec::new();

        let mut index_map = HashMap::<Arc<str>, (bool, usize)>::new();

        let manager = DataSrcManager::new(true);
        manager.copy_ds_ready_to_map(&mut index_map);
        assert!(index_map.is_empty());

        let mut manager = DataSrcManager::new(true);
        let ds1 = SyncDataSrc::new(1, logger.clone(), false);
        manager.add("foo", ds1);
        manager.setup_async(&mut errors).await;
        assert!(errors.is_empty());
        manager.copy_ds_ready_to_map(&mut index_map);
        assert_eq!(index_map.len(), 1);
        assert_eq!(index_map.get("foo").unwrap(), &(true, 0));

        let mut manager = DataSrcManager::new(false);
        let ds2 = AsyncDataSrc::new(2, logger.clone(), false, 0);
        let ds3 = SyncDataSrc::new(3, logger.clone(), false);
        manager.add("bar", ds2);
        manager.add("baz", ds3);
        manager.setup_async(&mut errors).await;
        assert!(errors.is_empty());
        manager.copy_ds_ready_to_map(&mut index_map);
        assert_eq!(index_map.len(), 3);
        assert_eq!(index_map.get("foo").unwrap(), &(true, 0));
        assert_eq!(index_map.get("bar").unwrap(), &(false, 0));
        assert_eq!(index_map.get("baz").unwrap(), &(false, 1));
    }

    #[tokio::test]
    async fn test_of_create_data_conn_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut errors = Vec::new();

        let mut manager = DataSrcManager::new(true);
        let ds1 = SyncDataSrc::new(1, logger.clone(), false);
        manager.add("foo", ds1);
        manager.setup_async(&mut errors).await;

        if let Ok(boxed) = manager
            .create_data_conn_async::<SyncDataConn>(0, "foo")
            .await
        {
            assert_eq!(boxed.name.clone(), "foo".into());
        } else {
            panic!();
        }
    }

    #[tokio::test]
    async fn test_of_create_data_conn_but_not_found() {
        let mut errors = Vec::new();

        let mut manager = DataSrcManager::new(true);
        manager.setup_async(&mut errors).await;

        if let Err(err) = manager
            .create_data_conn_async::<SyncDataConn>(0, "foo")
            .await
        {
            match err.reason::<DataSrcError>() {
                Ok(DataSrcError::NotFoundDataSrcToCreateDataConn {
                    name,
                    data_conn_type,
                }) => {
                    assert_eq!(*name, "foo".into());
                    assert_eq!(
                        *data_conn_type,
                        "sabi::tokio::data_src::tests_of_data_src::SyncDataConn"
                    );
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }

    #[tokio::test]
    async fn test_of_create_data_conn_but_fail_to_cast() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut errors = Vec::new();

        let mut manager = DataSrcManager::new(true);
        let ds1 = SyncDataSrc::new(1, logger.clone(), false);
        manager.add("foo", ds1);
        manager.setup_async(&mut errors).await;

        if let Err(err) = manager
            .create_data_conn_async::<AsyncDataConn>(0, "foo")
            .await
        {
            match err.reason::<DataSrcError>() {
                Ok(DataSrcError::FailToCastDataConn { name, target_type }) => {
                    assert_eq!(*name, "foo".into());
                    assert_eq!(
                        *target_type,
                        "sabi::tokio::data_src::tests_of_data_src::AsyncDataConn"
                    );
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }

    #[tokio::test]
    async fn test_of_create_data_conn_but_fail_to_create() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        let mut errors = Vec::new();

        let mut manager = DataSrcManager::new(true);
        let ds1 = SyncDataSrc::new_for_fail_to_create_data_conn(1, logger.clone());
        manager.add("foo", ds1);
        manager.setup_async(&mut errors).await;

        if let Err(err) = manager
            .create_data_conn_async::<SyncDataConn>(0, "foo")
            .await
        {
            match err.reason::<DataSrcError>() {
                Ok(DataSrcError::FailToCreateDataConn {
                    name,
                    data_conn_type,
                }) => {
                    assert_eq!(*name, "foo".into());
                    assert_eq!(
                        *data_conn_type,
                        "sabi::tokio::data_src::tests_of_data_src::SyncDataConn"
                    );
                }
                _ => panic!(),
            }
        } else {
            panic!();
        }
    }
}
