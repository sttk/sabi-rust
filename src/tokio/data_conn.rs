// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::{AsyncGroup, DataConn, DataConnContainer, DataConnManager};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{any, mem, ptr};

/// Represents errors that can occur during data connection operations.
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum DataConnError {
    /// An error indicating that one or more data connections failed during the pre-commit phase.
    FailToPreCommitDataConn {
        /// A vector of errors, each containing the name of the data connection and the error itself.
        errors: Vec<(Arc<str>, errs::Err)>,
    },

    /// An error indicating that one or more data connections failed during the commit phase.
    FailToCommitDataConn {
        /// A vector of errors, each containing the name of the data connection and the error itself.
        errors: Vec<(Arc<str>, errs::Err)>,
    },

    /// An error indicating that a data connection could not be cast to the target type.
    FailToCastDataConn {
        /// The name of the data connection that failed to cast.
        name: Arc<str>,
        /// The string representation of the target type to which the connection could not be cast.
        target_type: &'static str,
    },
}

impl<C> DataConnContainer<C>
where
    C: DataConn + 'static,
{
    pub(crate) fn new(name: impl Into<Arc<str>>, data_conn: Box<C>) -> Self {
        Self {
            drop_fn: drop_data_conn::<C>,
            is_fn: is_data_conn::<C>,
            commit_fn: commit_data_conn_async::<C>,
            pre_commit_fn: pre_commit_data_conn_async::<C>,
            post_commit_fn: post_commit_data_conn_async::<C>,
            should_force_back_fn: should_force_back_data_conn::<C>,
            rollback_fn: rollback_data_conn_async::<C>,
            force_back_fn: force_back_data_conn_async::<C>,
            close_fn: close_data_conn::<C>,

            name: name.into(),
            data_conn,
        }
    }
}

fn drop_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    unsafe {
        drop(Box::from_raw(ptr as *mut DataConnContainer<C>));
    }
}

fn is_data_conn<C>(type_id: any::TypeId) -> bool
where
    C: DataConn + 'static,
{
    any::TypeId::of::<C>() == type_id
}

fn commit_data_conn_async<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
) -> Pin<Box<dyn Future<Output = errs::Result<()>> + '_>>
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    Box::pin(container.data_conn.commit_async(ag))
}

fn pre_commit_data_conn_async<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
) -> Pin<Box<dyn Future<Output = errs::Result<()>> + '_>>
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    Box::pin(container.data_conn.pre_commit_async(ag))
}

fn post_commit_data_conn_async<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
) -> Pin<Box<dyn Future<Output = ()> + '_>>
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    Box::pin(container.data_conn.post_commit_async(ag))
}

fn should_force_back_data_conn<C>(ptr: *const DataConnContainer) -> bool
where
    C: DataConn + 'static,
{
    let container = unsafe { &*(ptr as *const DataConnContainer<C>) };
    container.data_conn.should_force_back()
}

fn rollback_data_conn_async<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
) -> Pin<Box<dyn Future<Output = ()> + '_>>
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    Box::pin(container.data_conn.rollback_async(ag))
}

fn force_back_data_conn_async<C>(
    ptr: *const DataConnContainer,
    ag: &mut AsyncGroup,
) -> Pin<Box<dyn Future<Output = ()> + '_>>
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    Box::pin(container.data_conn.force_back_async(ag))
}

fn close_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    let container = unsafe { &mut *(ptr as *mut DataConnContainer<C>) };
    container.data_conn.close();
}

impl DataConnManager {
    pub(crate) fn new() -> Self {
        Self {
            vec: Vec::new(),
            index_map: HashMap::new(),
        }
    }

    pub(crate) fn with_commit_order(names: &[&str]) -> Self {
        let mut index_map = HashMap::with_capacity(names.len());
        // To overwrite later indexed elements with eariler ones when names overlap
        for (i, nm) in names.iter().rev().enumerate() {
            index_map.insert((*nm).into(), names.len() - 1 - i);
        }

        Self {
            vec: vec![None; names.len()],
            index_map,
        }
    }

    pub(crate) fn add(&mut self, nnptr: ptr::NonNull<DataConnContainer>) {
        let name = unsafe { (*nnptr.as_ptr()).name.clone() };
        if let Some(index) = self.index_map.get(&name) {
            self.vec[*index] = Some(nnptr);
        } else {
            let index = self.vec.len();
            self.vec.push(Some(nnptr));
            self.index_map.insert(name.clone(), index);
        }
    }

    pub(crate) fn find_by_name(
        &self,
        name: impl AsRef<str>,
    ) -> Option<ptr::NonNull<DataConnContainer>> {
        if let Some(index) = self.index_map.get(name.as_ref()) {
            if *index < self.vec.len() {
                if let Some(nnptr) = self.vec[*index] {
                    let ptr = nnptr.as_ptr();
                    let cont_name = unsafe { &(*ptr).name };
                    if cont_name.as_ref() == name.as_ref() {
                        return Some(nnptr);
                    }
                }
            }
        }

        None
    }

    pub(crate) fn to_typed_ptr<C>(
        nnptr: &ptr::NonNull<DataConnContainer>,
    ) -> errs::Result<*mut DataConnContainer<C>>
    where
        C: DataConn + 'static,
    {
        let ptr = nnptr.as_ptr();
        let name = unsafe { &(*ptr).name };
        let type_id = any::TypeId::of::<C>();
        let is_fn = unsafe { (*ptr).is_fn };

        if !is_fn(type_id) {
            return Err(errs::Err::new(DataConnError::FailToCastDataConn {
                name: name.clone(),
                target_type: any::type_name::<C>(),
            }));
        }

        let typed_ptr = ptr as *mut DataConnContainer<C>;
        Ok(typed_ptr)
    }

    pub(crate) async fn commit_async(&self) -> errs::Result<()> {
        let mut errors = Vec::new();

        let mut ag = AsyncGroup::new();
        for nnptr in self.vec.iter().flatten() {
            let ptr = nnptr.as_ptr();
            let pre_commit_fn = unsafe { (*ptr).pre_commit_fn };
            ag._name = unsafe { (*ptr).name.clone() };
            if let Err(err) = pre_commit_fn(ptr, &mut ag).await {
                errors.push((ag._name.clone(), err));
                break;
            }
        }
        ag.join_and_collect_errors_async(&mut errors).await;

        if !errors.is_empty() {
            return Err(errs::Err::new(DataConnError::FailToPreCommitDataConn {
                errors,
            }));
        }

        let mut ag = AsyncGroup::new();
        for nnptr in self.vec.iter().flatten() {
            let ptr = nnptr.as_ptr();
            let commit_fn = unsafe { (*ptr).commit_fn };
            ag._name = unsafe { (*ptr).name.clone() };
            if let Err(err) = commit_fn(ptr, &mut ag).await {
                errors.push((ag._name.clone(), err));
                break;
            }
        }
        ag.join_and_collect_errors_async(&mut errors).await;

        if !errors.is_empty() {
            return Err(errs::Err::new(DataConnError::FailToCommitDataConn {
                errors,
            }));
        }

        let mut ag = AsyncGroup::new();
        for nnptr in self.vec.iter().flatten() {
            let ptr = nnptr.as_ptr();
            let post_commit_fn = unsafe { (*ptr).post_commit_fn };
            ag._name = unsafe { (*ptr).name.clone() };
            post_commit_fn(ptr, &mut ag).await;
        }
        ag.join_and_ignore_errors_async().await;

        Ok(())
    }

    pub(crate) async fn rollback_async(&mut self) {
        let mut ag = AsyncGroup::new();
        for nnptr in self.vec.iter().flatten() {
            let ptr = nnptr.as_ptr();
            let should_force_back_fn = unsafe { (*ptr).should_force_back_fn };
            let force_back_fn = unsafe { (*ptr).force_back_fn };
            let rollback_fn = unsafe { (*ptr).rollback_fn };
            ag._name = unsafe { (*ptr).name.clone() };

            if should_force_back_fn(ptr) {
                force_back_fn(ptr, &mut ag).await;
            } else {
                rollback_fn(ptr, &mut ag).await;
            }
        }
        ag.join_and_ignore_errors_async().await;
    }

    pub(crate) fn close(&mut self) {
        self.index_map.clear();

        let vec: Vec<Option<ptr::NonNull<DataConnContainer>>> = mem::take(&mut self.vec);

        for nnptr in vec.iter().flatten() {
            let ptr = nnptr.as_ptr();
            let close_fn = unsafe { (*ptr).close_fn };
            let drop_fn = unsafe { (*ptr).drop_fn };
            close_fn(ptr);
            drop_fn(ptr);
        }
    }
}

impl Drop for DataConnManager {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests_of_data_conn {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };
    use tokio::time;

    #[derive(PartialEq, Copy, Clone)]
    enum Fail {
        Not,
        Commit,
        PreCommit,
    }

    struct SyncDataConn {
        id: i8,
        committed: AtomicBool,
        fail: Fail,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl SyncDataConn {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::new {}", id));
            Self {
                id,
                committed: AtomicBool::new(false),
                fail,
                logger,
            }
        }
    }
    impl Drop for SyncDataConn {
        fn drop(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::drop {}", self.id));
        }
    }
    impl DataConn for SyncDataConn {
        async fn commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let fail = self.fail;
            let id = self.id;
            let logger = self.logger.clone();
            let committed = &self.committed;

            if fail == Fail::Commit {
                logger
                    .lock()
                    .unwrap()
                    .push(format!("SyncDataConn::commit {} failed", id));
                return Err(errs::Err::new("ZZZ".to_string()));
            }
            committed.store(true, Ordering::Release);
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::commit {}", id));
            Ok(())
        }
        async fn pre_commit_async(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let fail = self.fail;
            let id = self.id;
            let logger = self.logger.clone();

            if fail == Fail::PreCommit {
                logger
                    .lock()
                    .unwrap()
                    .push(format!("SyncDataConn::pre_commit {} failed", id));
                return Err(errs::Err::new("zzz".to_string()));
            }
            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::pre_commit {}", id));
            Ok(())
        }
        async fn post_commit_async(&mut self, _ag: &mut AsyncGroup) {
            let id = self.id;
            let logger = self.logger.clone();

            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::post_commit {}", id));
        }
        fn should_force_back(&self) -> bool {
            self.committed.load(Ordering::Acquire)
        }
        async fn rollback_async(&mut self, _ag: &mut AsyncGroup) {
            let id = self.id;
            let logger = self.logger.clone();

            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::rollback {}", id));
        }
        async fn force_back_async(&mut self, _ag: &mut AsyncGroup) {
            let id = self.id;
            let logger = self.logger.clone();

            logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::force_back {}", id));
        }
        fn close(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("SyncDataConn::close {}", self.id));
        }
    }

    struct AsyncDataConn {
        id: i8,
        committed: Arc<AtomicBool>,
        fail: Fail,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl AsyncDataConn {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
            logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataConn::new {}", id));
            Self {
                id,
                committed: Arc::new(AtomicBool::new(false)),
                fail,
                logger,
            }
        }
    }
    impl Drop for AsyncDataConn {
        fn drop(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataConn::drop {}", self.id));
        }
    }
    impl DataConn for AsyncDataConn {
        async fn commit_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
            let fail = self.fail;
            let id = self.id;
            let logger = self.logger.clone();
            let committed = self.committed.clone();

            ag.add(async move {
                time::sleep(time::Duration::from_millis(100)).await;
                if fail == Fail::Commit {
                    logger
                        .lock()
                        .unwrap()
                        .push(format!("AsyncDataConn::commit {} failed", id));
                    return Err(errs::Err::new("YYY".to_string()));
                }
                committed.store(true, Ordering::Release);
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::commit {}", id));
                Ok(())
            });
            Ok(())
        }
        async fn pre_commit_async(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
            let fail = self.fail;
            let id = self.id;
            let logger = self.logger.clone();

            ag.add(async move {
                time::sleep(time::Duration::from_millis(100)).await;
                if fail == Fail::PreCommit {
                    logger
                        .lock()
                        .unwrap()
                        .push(format!("AsyncDataConn::pre_commit {} failed", id));
                    return Err(errs::Err::new("yyy".to_string()));
                }
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::pre_commit {}", id));
                Ok(())
            });
            Ok(())
        }
        async fn post_commit_async(&mut self, ag: &mut AsyncGroup) {
            let logger = self.logger.clone();
            let id = self.id;

            ag.add(async move {
                time::sleep(time::Duration::from_millis(100)).await;
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::post_commit {}", id));
                Ok(())
            });
        }
        fn should_force_back(&self) -> bool {
            self.committed.load(Ordering::Acquire)
        }
        async fn rollback_async(&mut self, ag: &mut AsyncGroup) {
            let logger = self.logger.clone();
            let id = self.id;

            ag.add(async move {
                time::sleep(time::Duration::from_millis(100)).await;
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::rollback {}", id));
                Ok(())
            });
        }
        async fn force_back_async(&mut self, ag: &mut AsyncGroup) {
            let logger = self.logger.clone();
            let id = self.id;

            ag.add(async move {
                time::sleep(time::Duration::from_millis(100)).await;
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataConn::force_back {}", id));
                Ok(())
            });
        }
        fn close(&mut self) {
            self.logger
                .lock()
                .unwrap()
                .push(format!("AsyncDataConn::close {}", self.id));
        }
    }

    mod tests_of_data_conn_manager {
        use super::*;

        #[tokio::test]
        async fn test_new() {
            let manager = DataConnManager::new();
            assert!(manager.vec.is_empty());
        }

        #[tokio::test]
        async fn test_with_commit_order() {
            let manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);
            assert_eq!(manager.vec, vec![None, None, None]);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
            assert_eq!(*manager.index_map.get("baz").unwrap(), 1);
        }

        #[test]
        fn test_new_and_add() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();
            assert!(manager.vec.is_empty());
            assert!(manager.index_map.is_empty());

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);
            assert_eq!(manager.vec.len(), 1);
            assert_eq!(manager.index_map.len(), 1);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 0);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);
            assert_eq!(manager.vec.len(), 2);
            assert_eq!(manager.index_map.len(), 2);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 0);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 1);
        }

        #[test]
        fn test_with_commit_order_and_add() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);
            assert_eq!(manager.vec, vec![None, None, None]);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
            assert_eq!(*manager.index_map.get("baz").unwrap(), 1);

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);
            assert_eq!(manager.vec.len(), 3);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);
            assert_eq!(manager.vec.len(), 3);
            assert_eq!(manager.index_map.len(), 3);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 0);

            let conn = SyncDataConn::new(3, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("qux", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);
            assert_eq!(manager.vec.len(), 4);
            assert_eq!(manager.index_map.len(), 4);
            assert_eq!(*manager.index_map.get("foo").unwrap(), 2);
            assert_eq!(*manager.index_map.get("bar").unwrap(), 0);
            assert_eq!(*manager.index_map.get("qux").unwrap(), 3);
        }

        #[test]
        fn test_find_by_name_but_none() {
            let manager = DataConnManager::new();
            assert!(manager.find_by_name("foo").is_none());
            assert!(manager.find_by_name("bar").is_none());
        }

        #[test]
        fn test_find_by_name_and_found() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);

            if let Some(nnptr) = manager.find_by_name("foo") {
                let name = unsafe { (*nnptr.as_ptr()).name.clone() };
                assert_eq!(name.as_ref(), "foo");
            } else {
                panic!();
            }

            if let Some(nnptr) = manager.find_by_name("bar") {
                let name = unsafe { (*nnptr.as_ptr()).name.clone() };
                assert_eq!(name.as_ref(), "bar");
            } else {
                panic!();
            }
        }

        #[test]
        fn test_to_typed_ptr() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);

            let nnptr = manager.find_by_name("foo").unwrap();
            if let Ok(typed_nnptr) = DataConnManager::to_typed_ptr::<SyncDataConn>(&nnptr) {
                assert_eq!(any::type_name_of_val(&typed_nnptr), "*mut sabi::tokio::DataConnContainer<sabi::tokio::data_conn::tests_of_data_conn::SyncDataConn>");
                assert_eq!(unsafe { (*typed_nnptr).name.clone() }, "foo".into());
            } else {
                panic!();
            }

            let nnptr = manager.find_by_name("bar").unwrap();
            if let Ok(typed_nnptr) = DataConnManager::to_typed_ptr::<AsyncDataConn>(&nnptr) {
                assert_eq!(any::type_name_of_val(&typed_nnptr), "*mut sabi::tokio::DataConnContainer<sabi::tokio::data_conn::tests_of_data_conn::AsyncDataConn>");
                assert_eq!(unsafe { (*typed_nnptr).name.clone() }, "bar".into());
            } else {
                panic!();
            }
        }

        #[test]
        fn test_to_typed_ptr_but_fail() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            let mut manager = DataConnManager::new();

            let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);

            let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
            let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
            let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
            manager.add(nnptr);

            let nnptr = manager.find_by_name("foo").unwrap();
            if let Err(err) = DataConnManager::to_typed_ptr::<AsyncDataConn>(&nnptr) {
                match err.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(name.as_ref(), "foo");
                        assert_eq!(
                            *target_type,
                            "sabi::tokio::data_conn::tests_of_data_conn::AsyncDataConn"
                        );
                    }
                    _ => panic!(),
                }
            } else {
                panic!();
            }

            let nnptr = manager.find_by_name("bar").unwrap();
            if let Err(err) = DataConnManager::to_typed_ptr::<SyncDataConn>(&nnptr) {
                match err.reason::<DataConnError>() {
                    Ok(DataConnError::FailToCastDataConn { name, target_type }) => {
                        assert_eq!(name.as_ref(), "bar");
                        assert_eq!(
                            *target_type,
                            "sabi::tokio::data_conn::tests_of_data_conn::SyncDataConn"
                        );
                    }
                    _ => panic!(),
                }
            } else {
                panic!();
            }
        }

        #[tokio::test]
        async fn test_commit_ok() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                assert!(manager.commit_async().await.is_ok());
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "AsyncDataConn::post_commit 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_with_order() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::with_commit_order(&["bar", "baz", "foo"]);

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = SyncDataConn::new(3, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("qux", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                assert!(manager.commit_async().await.is_ok());
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::new 3",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 3",
                    "AsyncDataConn::pre_commit 2", // because of async
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 3",
                    "AsyncDataConn::commit 2", // because of async
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::post_commit 3",
                    "AsyncDataConn::post_commit 2", // because of async
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataConn::close 3",
                    "SyncDataConn::drop 3",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_but_fail_first_sync_pre_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                if let Err(e) = manager.commit_async().await {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].0, "foo".into());
                            assert_eq!(errors[0].1.reason::<String>().unwrap(), "zzz");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1 failed",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_but_fail_first_async_pre_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                if let Err(e) = manager.commit_async().await {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].0, "foo".into());
                            assert_eq!(errors[0].1.reason::<String>().unwrap(), "zzz");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1 failed",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_but_fail_second_pre_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::PreCommit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                if let Err(e) = manager.commit_async().await {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToPreCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].0, "foo".into());
                            assert_eq!(errors[0].1.reason::<String>().unwrap(), "yyy");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 1",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 2",
                    "AsyncDataConn::pre_commit 1 failed",
                    "AsyncDataConn::close 1",
                    "AsyncDataConn::drop 1",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_but_fail_first_sync_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                if let Err(e) = manager.commit_async().await {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].0, "foo".into());
                            assert_eq!(errors[0].1.reason::<String>().unwrap(), "ZZZ");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1 failed",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_but_fail_first_async_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                if let Err(e) = manager.commit_async().await {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].0, "foo".into());
                            assert_eq!(errors[0].1.reason::<String>().unwrap(), "YYY");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 1",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 2",
                    "AsyncDataConn::pre_commit 1",
                    "SyncDataConn::commit 2",
                    "AsyncDataConn::commit 1 failed",
                    "AsyncDataConn::close 1",
                    "AsyncDataConn::drop 1",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_commit_but_fail_second_commit() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Commit);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                if let Err(e) = manager.commit_async().await {
                    match e.reason::<DataConnError>() {
                        Ok(DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].0, "bar".into());
                            assert_eq!(errors[0].1.reason::<String>().unwrap(), "YYY");
                        }
                        _ => panic!(),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2 failed",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_rollback_and_first_is_sync() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                manager.rollback_async().await;
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_rollback_and_first_is_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                manager.rollback_async().await;
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 1",
                    "SyncDataConn::new 2",
                    "SyncDataConn::rollback 2",
                    "AsyncDataConn::rollback 1",
                    "AsyncDataConn::close 1",
                    "AsyncDataConn::drop 1",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_force_back_and_first_is_sync() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = SyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo", Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = AsyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                assert!(manager.commit_async().await.is_ok());
                manager.rollback_async().await;
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataConn::new 1",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "AsyncDataConn::post_commit 2",
                    "SyncDataConn::force_back 1",
                    "AsyncDataConn::force_back 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                ]
            );
        }

        #[tokio::test]
        async fn test_force_back_and_first_is_async() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut manager = DataConnManager::new();

                let conn = AsyncDataConn::new(1, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("foo".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                let conn = SyncDataConn::new(2, logger.clone(), Fail::Not);
                let boxed = Box::new(DataConnContainer::new("bar".to_string(), Box::new(conn)));
                let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                manager.add(nnptr);

                assert!(manager.commit_async().await.is_ok());
                manager.rollback_async().await;
            }

            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "AsyncDataConn::new 1",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 2",
                    "AsyncDataConn::pre_commit 1",
                    "SyncDataConn::commit 2",
                    "AsyncDataConn::commit 1",
                    "SyncDataConn::post_commit 2",
                    "AsyncDataConn::post_commit 1",
                    "SyncDataConn::force_back 2",
                    "AsyncDataConn::force_back 1",
                    "AsyncDataConn::close 1",
                    "AsyncDataConn::drop 1",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                ]
            );
        }
    }
}
