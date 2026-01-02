// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::data_src::{
    add_data_src, close_and_drop_all_data_srcs, copy_global_data_srcs_to_map, drop_all_data_srcs,
    setup_all_data_srcs,
};
use crate::{AsyncGroup, DataConn, DataConnContainer, DataHub, DataSrc, SendSyncNonNull};

use std::borrow::Cow;
use std::collections::HashMap;
use std::{any, mem, ptr};

#[derive(Debug)]
pub enum DataHubError {
    FailToSetupLocalDataSrcs {
        errors: HashMap<String, errs::Err>,
    },
    FailToPreCommitDataConn {
        errors: HashMap<String, errs::Err>,
    },
    FailToCommitDataConn {
        errors: HashMap<String, errs::Err>,
    },
    FailToCastDataConn {
        name: String,
        target_type: &'static str,
    },
    NoDataSrcToCreateDataConn {
        name: String,
        data_conn_type: &'static str,
    },
    FailToCreateDataConn {
        name: String,
        data_conn_type: &'static str,
    },
}

impl DataHub {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let mut data_src_map = HashMap::new();
        copy_global_data_srcs_to_map(&mut data_src_map);

        Self {
            data_src_vec_unready: Vec::new(),
            data_src_vec_ready: Vec::new(),
            data_src_map,
            data_conn_vec: Vec::new(),
            data_conn_map: HashMap::new(),
            fixed: false,
        }
    }

    pub fn uses<S, C>(&mut self, name: impl Into<Cow<'static, str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        if self.fixed {
            return;
        }
        add_data_src(&mut self.data_src_vec_unready, name, ds, true);
    }

    pub fn disuses(&mut self, name: impl AsRef<str>) {
        if self.fixed {
            return;
        }
        self.data_src_map
            .retain(|nm, ptr| unsafe { !(*(*ptr)).local } || nm.as_ref() != name.as_ref());

        let extracted_vec: Vec<_> = self
            .data_src_vec_unready
            .extract_if(.., |ssnnptr| {
                unsafe { &(*ssnnptr.non_null_ptr.as_ptr()).name }.as_ref() == name.as_ref()
            })
            .collect();
        drop_all_data_srcs(&extracted_vec);

        let extracted_vec: Vec<_> = self
            .data_src_vec_ready
            .extract_if(.., |ssnnptr| {
                unsafe { &(*ssnnptr.non_null_ptr.as_ptr()).name }.as_ref() == name.as_ref()
            })
            .collect();
        close_and_drop_all_data_srcs(&extracted_vec);
    }

    fn begin(&mut self) -> errs::Result<()> {
        self.fixed = true;

        let mut errors = HashMap::new();
        let errors_ref_mut = &mut errors;

        let mut vec = mem::take(&mut self.data_src_vec_unready);

        setup_all_data_srcs(&mut vec, errors_ref_mut);

        if errors_ref_mut.is_empty() {
            self.data_src_vec_ready.append(&mut vec);
            for ssnnptr in self.data_src_vec_ready.iter() {
                let ptr = ssnnptr.non_null_ptr.as_ptr();
                let name = unsafe { &(*ptr).name }.to_string();
                self.data_src_map.insert(name.into(), ptr);
            }
            Ok(())
        } else {
            Err(errs::Err::new(DataHubError::FailToSetupLocalDataSrcs {
                errors,
            }))
        }
    }

    fn commit(&mut self) -> errs::Result<()> {
        let mut errors = HashMap::new();

        let mut ag = AsyncGroup::new();
        for ssnnptr in self.data_conn_vec.iter() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let pre_commit_fn = unsafe { (*ptr).pre_commit_fn };
            ag.name = unsafe { &(*ptr).name };
            if let Err(err) = pre_commit_fn(ptr, &mut ag) {
                errors.insert(ag.name.to_string(), err);
                break;
            }
        }
        ag.join_and_collect_errors(&mut errors);

        if !errors.is_empty() {
            return Err(errs::Err::new(DataHubError::FailToPreCommitDataConn {
                errors,
            }));
        }

        let mut ag = AsyncGroup::new();
        for ssnnptr in self.data_conn_vec.iter() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let commit_fn = unsafe { (*ptr).commit_fn };
            ag.name = unsafe { &(*ptr).name };
            if let Err(err) = commit_fn(ptr, &mut ag) {
                errors.insert(ag.name.to_string(), err);
                break;
            }
        }
        ag.join_and_collect_errors(&mut errors);

        if !errors.is_empty() {
            return Err(errs::Err::new(DataHubError::FailToCommitDataConn {
                errors,
            }));
        }

        let mut ag = AsyncGroup::new();
        for ssnnptr in self.data_conn_vec.iter() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let post_commit_fn = unsafe { (*ptr).post_commit_fn };
            ag.name = unsafe { &(*ptr).name };
            post_commit_fn(ptr, &mut ag);
        }
        ag.join_and_ignore_errors();

        Ok(())
    }

    fn rollback(&mut self) {
        let mut ag = AsyncGroup::new();
        for ssnnptr in self.data_conn_vec.iter() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let should_force_back_fn = unsafe { (*ptr).should_force_back_fn };
            let force_back_fn = unsafe { (*ptr).force_back_fn };
            let rollback_fn = unsafe { (*ptr).rollback_fn };
            ag.name = unsafe { &(*ptr).name };

            if should_force_back_fn(ptr) {
                force_back_fn(ptr, &mut ag);
            } else {
                rollback_fn(ptr, &mut ag);
            }
        }
        ag.join_and_ignore_errors();
    }

    fn end(&mut self) {
        self.data_conn_map.clear();

        let vec = mem::take(&mut self.data_conn_vec);
        for ssnnptr in vec.into_iter().rev() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let close_fn = unsafe { (*ptr).close_fn };
            let drop_fn = unsafe { (*ptr).drop_fn };
            close_fn(ptr);
            drop_fn(ptr);
        }

        self.fixed = false;
    }

    pub fn get_data_conn<C>(&mut self, name: impl AsRef<str>) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        if let Some(data_conn_ptr) = self.data_conn_map.get(name.as_ref()) {
            let type_id = any::TypeId::of::<C>();
            let is_fn = unsafe { (*(*data_conn_ptr)).is_fn };
            if !is_fn(type_id) {
                return Err(errs::Err::new(DataHubError::FailToCastDataConn {
                    name: name.as_ref().to_string(),
                    target_type: any::type_name::<C>(),
                }));
            }
            let typed_ptr = (*data_conn_ptr) as *mut DataConnContainer<C>;
            return Ok(unsafe { &mut ((*typed_ptr).data_conn) });
        }

        if let Some(data_src_ptr) = self.data_src_map.get(name.as_ref()) {
            let type_id = any::TypeId::of::<C>();
            let is_fn = unsafe { (*(*data_src_ptr)).is_data_conn_fn };
            if !is_fn(type_id) {
                return Err(errs::Err::new(DataHubError::FailToCastDataConn {
                    name: name.as_ref().to_string(),
                    target_type: any::type_name::<C>(),
                }));
            }

            let create_data_conn_fn = unsafe { (*(*data_src_ptr)).create_data_conn_fn };
            match create_data_conn_fn(*data_src_ptr) {
                Ok(boxed) => {
                    let nnptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataConnContainer>();
                    self.data_conn_vec.push(SendSyncNonNull::new(nnptr));

                    let ptr = nnptr.as_ptr();
                    self.data_conn_map
                        .insert(Cow::Owned(name.as_ref().to_string()), ptr);

                    let typed_ptr = ptr.cast::<DataConnContainer<C>>();
                    return Ok(unsafe { &mut (*typed_ptr).data_conn });
                }
                Err(err) => {
                    return Err(errs::Err::with_source(
                        DataHubError::FailToCreateDataConn {
                            name: name.as_ref().to_string(),
                            data_conn_type: any::type_name::<C>(),
                        },
                        err,
                    ));
                }
            }
        }

        Err(errs::Err::new(DataHubError::NoDataSrcToCreateDataConn {
            name: name.as_ref().to_string(),
            data_conn_type: any::type_name::<C>(),
        }))
    }

    pub fn run<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let mut r = self.begin();
        if r.is_ok() {
            r = logic_fn(self);
        }
        self.end();
        r
    }

    pub fn txn<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let mut r = self.begin();
        if r.is_ok() {
            r = logic_fn(self);
        }
        if r.is_ok() {
            r = self.commit();
        }
        if r.is_err() {
            self.rollback();
        }
        self.end();
        r
    }
}

impl Drop for DataHub {
    fn drop(&mut self) {
        self.end();
        self.data_src_map.clear();
        drop_all_data_srcs(&self.data_src_vec_unready);
        close_and_drop_all_data_srcs(&self.data_src_vec_ready);
    }
}

#[cfg(test)]
mod tests_of_data_hub {
    use super::*;
    use std::{
        sync::{Arc, Mutex},
        thread, time,
    };

    #[derive(PartialEq, Copy, Clone)]
    enum Fail {
        Not,
        Setup,
        CreateDataConn,
        PreCommit,
        Commit,
    }

    struct SyncDataSrc {
        id: i8,
        fail: Fail,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl SyncDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
            {
                let mut lg = logger.lock().unwrap();
                lg.push(format!("SyncDataSrc::new {}", id));
            }
            Self { id, fail, logger }
        }
    }
    impl Drop for SyncDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<SyncDataConn> for SyncDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail == Fail::Setup {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("SyncDataSrc::setup {} failed", self.id));
                }
                return Err(errs::Err::new("XXX".to_string()));
            }
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("SyncDataSrc::setup {}", self.id));
            }
            Ok(())
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataSrc::close {}", self.id));
        }
        fn create_data_conn(&mut self) -> errs::Result<Box<SyncDataConn>> {
            if self.fail == Fail::CreateDataConn {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("SyncDataSrc::create_data_conn {} failed", self.id));
                }
                return Err(errs::Err::new("xxx".to_string()));
            }
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("SyncDataSrc::create_data_conn {}", self.id));
            }
            let conn = SyncDataConn::new(self.id, self.logger.clone(), self.fail);
            Ok(Box::new(conn))
        }
    }

    struct AsyncDataSrc {
        id: i8,
        fail: Fail,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl AsyncDataSrc {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
            {
                let mut lg = logger.lock().unwrap();
                lg.push(format!("AsyncDataSrc::new {}", id));
            }
            Self { id, fail, logger }
        }
    }
    impl Drop for AsyncDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<AsyncDataConn> for AsyncDataSrc {
        fn setup(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
            let fail = self.fail;
            let logger = self.logger.clone();
            let id = self.id;
            ag.add(move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = thread::sleep(time::Duration::from_millis(100));
                if fail == Fail::Setup {
                    logger
                        .lock()
                        .unwrap()
                        .push(format!("AsyncDataSrc::setup {} failed", id));
                    return Err(errs::Err::new("YYY".to_string()));
                }
                logger
                    .lock()
                    .unwrap()
                    .push(format!("AsyncDataSrc::setup {}", id));
                Ok(())
            });
            Ok(())
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataSrc::close {}", self.id));
        }
        fn create_data_conn(&mut self) -> errs::Result<Box<AsyncDataConn>> {
            if self.fail == Fail::CreateDataConn {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("AsyncDataSrc::create_data_conn {} failed", self.id));
                }
                return Err(errs::Err::new("xxx".to_string()));
            }
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("AsyncDataSrc::create_data_conn {}", self.id));
            }
            let conn = AsyncDataConn::new(self.id, self.logger.clone(), self.fail);
            Ok(Box::new(conn))
        }
    }

    struct SyncDataConn {
        id: i8,
        committed: bool,
        fail: Fail,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl SyncDataConn {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
            {
                let mut lg = logger.lock().unwrap();
                lg.push(format!("SyncDataConn::new {}", id));
            }
            Self {
                id,
                committed: false,
                fail,
                logger,
            }
        }
    }
    impl Drop for SyncDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn::drop {}", self.id));
        }
    }
    impl DataConn for SyncDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail == Fail::Commit {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("SyncDataConn::commit {} failed", self.id));
                }
                return Err(errs::Err::new("ZZZ".to_string()));
            }
            self.committed = true;
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("SyncDataConn::commit {}", self.id));
            }
            Ok(())
        }
        fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail == Fail::PreCommit {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("SyncDataConn::pre_commit {} failed", self.id));
                }
                return Err(errs::Err::new("zzz".to_string()));
            }
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("SyncDataConn::pre_commit {}", self.id));
            }
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn::post_commit {}", self.id));
        }
        fn should_force_back(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn::rollback {}", self.id));
        }
        fn force_back(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn::force_back {}", self.id));
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("SyncDataConn::close {}", self.id));
        }
    }

    struct AsyncDataConn {
        id: i8,
        committed: bool,
        fail: Fail,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl AsyncDataConn {
        fn new(id: i8, logger: Arc<Mutex<Vec<String>>>, fail: Fail) -> Self {
            {
                let mut lg = logger.lock().unwrap();
                lg.push(format!("AsyncDataConn::new {}", id));
            }
            Self {
                id,
                committed: false,
                fail,
                logger,
            }
        }
    }
    impl Drop for AsyncDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataConn::drop {}", self.id));
        }
    }
    impl DataConn for AsyncDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail == Fail::Commit {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("AsyncDataConn::commit {} failed", self.id));
                }
                return Err(errs::Err::new("VVV".to_string()));
            }
            self.committed = true;
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("AsyncDataConn::commit {}", self.id));
            }
            Ok(())
        }
        fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.fail == Fail::PreCommit {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("AsyncDataConn::pre_commit {} failed", self.id));
                }
                return Err(errs::Err::new("vvv".to_string()));
            }
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("AsyncDataConn::pre_commit {}", self.id));
            }
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataConn::post_commit {}", self.id));
        }
        fn should_force_back(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataConn::rollback {}", self.id));
        }
        fn force_back(&mut self, _ag: &mut AsyncGroup) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataConn::force_back {}", self.id));
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("AsyncDataConn::close {}", self.id));
        }
    }

    #[test]
    fn test_new() {
        let hub = DataHub::new();
        assert_eq!(hub.data_src_vec_unready.len(), 0);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);
    }

    #[test]
    fn test_uses_and_disuses() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        assert_eq!(hub.data_src_vec_unready.len(), 0);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);

        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Not));
        assert_eq!(hub.data_src_vec_unready.len(), 1);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);

        {
            assert_eq!(*logger.lock().unwrap(), &["SyncDataSrc::new 1"]);
        }

        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Not),
        );
        assert_eq!(hub.data_src_vec_unready.len(), 2);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &["SyncDataSrc::new 1", "AsyncDataSrc::new 2",]
            );
        }

        hub.disuses("foo".to_string());
        assert_eq!(hub.data_src_vec_unready.len(), 1);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::drop 1",
                ]
            );
        }

        hub.disuses("bar");
        assert_eq!(hub.data_src_vec_unready.len(), 0);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::drop 1",
                    "AsyncDataSrc::drop 2",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_end_with_zero_data_src() {
        let mut hub = DataHub::new();
        assert_eq!(hub.data_src_vec_unready.len(), 0);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);

        let r = hub.begin();
        assert!(r.is_ok());

        assert_eq!(hub.data_src_vec_unready.len(), 0);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, true);

        hub.end();

        assert_eq!(hub.data_src_vec_unready.len(), 0);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);
    }

    #[test]
    fn test_begin_and_end_with_data_srcs() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        assert_eq!(hub.data_src_vec_unready.len(), 0);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);

        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Not));
        assert_eq!(hub.data_src_vec_unready.len(), 1);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);

        {
            assert_eq!(*logger.lock().unwrap(), &["SyncDataSrc::new 1"],);
        }

        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Not),
        );
        assert_eq!(hub.data_src_vec_unready.len(), 2);
        assert_eq!(hub.data_src_vec_ready.len(), 0);
        assert_eq!(hub.data_src_map.len(), 0);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &["SyncDataSrc::new 1", "AsyncDataSrc::new 2"]
            );
        }

        let r = hub.begin();
        assert!(r.is_ok());

        assert_eq!(hub.data_src_vec_unready.len(), 0);
        assert_eq!(hub.data_src_vec_ready.len(), 2);
        assert_eq!(hub.data_src_map.len(), 2);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, true);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2"
                ]
            );
        }

        hub.end();

        assert_eq!(hub.data_src_vec_unready.len(), 0);
        assert_eq!(hub.data_src_vec_ready.len(), 2);
        assert_eq!(hub.data_src_map.len(), 2);
        assert_eq!(hub.data_conn_vec.len(), 0);
        assert_eq!(hub.data_conn_map.len(), 0);
        assert_eq!(hub.fixed, false);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2"
                ]
            );
        }

        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2",
                    "AsyncDataSrc::close 2",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_end_with_data_srcs_but_fail_to_setup_sync() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Setup));
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Not),
        );

        let r = hub.begin();
        assert!(r.is_err());

        if let Err(err) = r {
            match err.reason::<DataHubError>().unwrap() {
                DataHubError::FailToSetupLocalDataSrcs { errors } => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(
                        errors.get("foo").unwrap().reason::<String>().unwrap(),
                        "XXX",
                    );
                }
                _ => panic!(),
            };
        } else {
            panic!();
        }

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1 failed",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_end_with_data_srcs_but_fail_to_setup_async() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Not));
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Setup),
        );

        let r = hub.begin();
        assert!(r.is_err());

        if let Err(err) = r {
            match err.reason::<DataHubError>().unwrap() {
                DataHubError::FailToSetupLocalDataSrcs { errors } => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(
                        errors.get("bar").unwrap().reason::<String>().unwrap(),
                        "YYY",
                    );
                }
                _ => panic!(),
            };
        } else {
            panic!();
        }

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2 failed",
                    "SyncDataSrc::close 1",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_get_data_conn_end() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Not));
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Not),
        );

        let r = hub.begin();
        assert!(r.is_ok());

        match hub.get_data_conn::<SyncDataConn>("foo") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::SyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.get_data_conn::<AsyncDataConn>("bar") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::AsyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "AsyncDataSrc::create_data_conn 2",
                    "AsyncDataConn::new 2",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataSrc::close 2",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_get_data_conn_end_but_fail_to_get_data_conn_sync() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses(
            "foo",
            SyncDataSrc::new(1, logger.clone(), Fail::CreateDataConn),
        );
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Not),
        );

        let r = hub.begin();
        assert!(r.is_ok());

        match hub.get_data_conn::<SyncDataConn>("foo") {
            Ok(_conn) => panic!(),
            Err(e) => match e.reason::<DataHubError>().unwrap() {
                DataHubError::FailToCreateDataConn {
                    name,
                    data_conn_type,
                } => {
                    assert_eq!(name, "foo");
                    assert_eq!(
                        *data_conn_type,
                        "sabi::data_hub::tests_of_data_hub::SyncDataConn"
                    );
                }
                _ => panic!(),
            },
        }

        match hub.get_data_conn::<AsyncDataConn>("bar") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::AsyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1 failed",
                    "AsyncDataSrc::create_data_conn 2",
                    "AsyncDataConn::new 2",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "AsyncDataSrc::close 2",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_get_data_conn_end_but_fail_to_get_data_conn_async() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Not));
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::CreateDataConn),
        );

        let r = hub.begin();
        assert!(r.is_ok());

        match hub.get_data_conn::<SyncDataConn>("foo") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::SyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.get_data_conn::<AsyncDataConn>("bar") {
            Ok(_conn) => panic!(),
            Err(e) => match e.reason::<DataHubError>().unwrap() {
                DataHubError::FailToCreateDataConn {
                    name,
                    data_conn_type,
                } => {
                    assert_eq!(name, "bar");
                    assert_eq!(
                        *data_conn_type,
                        "sabi::data_hub::tests_of_data_hub::AsyncDataConn"
                    );
                }
                _ => panic!(),
            },
        }

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "AsyncDataSrc::create_data_conn 2 failed",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataSrc::close 2",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_commit_end_with_no_data_conns() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Not));
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Not),
        );

        let r = hub.begin();
        assert!(r.is_ok());

        let r = hub.commit();
        assert!(r.is_ok());

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2",
                    "AsyncDataSrc::close 2",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_commit_end_with_data_conns() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Not));
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Not),
        );

        let r = hub.begin();
        assert!(r.is_ok());

        match hub.get_data_conn::<SyncDataConn>("foo") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::SyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.get_data_conn::<AsyncDataConn>("bar") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::AsyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        let r = hub.commit();
        if r.is_err() {
            panic!();
        }

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "AsyncDataSrc::create_data_conn 2",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "AsyncDataConn::post_commit 2",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataSrc::close 2",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_commit_end_with_data_conns_but_fail_to_pre_commit_sync() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::PreCommit));
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Not),
        );

        let r = hub.begin();
        assert!(r.is_ok());

        match hub.get_data_conn::<SyncDataConn>("foo") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::SyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.get_data_conn::<AsyncDataConn>("bar") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::AsyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.commit() {
            Err(e) => match e.reason::<DataHubError>().unwrap() {
                DataHubError::FailToPreCommitDataConn { errors } => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(
                        errors.get("foo").unwrap().reason::<String>().unwrap(),
                        "zzz",
                    );
                }
                _ => panic!(),
            },
            _ => panic!(),
        }

        hub.rollback();

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "AsyncDataSrc::create_data_conn 2",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataSrc::close 2",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_commit_end_with_data_srcs_but_fail_to_pre_commit_async() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Not));
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::PreCommit),
        );

        let r = hub.begin();
        assert!(r.is_ok());

        match hub.get_data_conn::<SyncDataConn>("foo") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::SyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.get_data_conn::<AsyncDataConn>("bar") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::AsyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.commit() {
            Err(e) => match e.reason::<DataHubError>().unwrap() {
                DataHubError::FailToPreCommitDataConn { errors } => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(
                        errors.get("bar").unwrap().reason::<String>().unwrap(),
                        "vvv",
                    );
                }
                _ => panic!(),
            },
            _ => panic!(),
        }

        hub.rollback();

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "AsyncDataSrc::create_data_conn 2",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataSrc::close 2",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_commit_end_with_data_srcs_but_fail_to_commit_sync() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Commit));
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Not),
        );

        let r = hub.begin();
        assert!(r.is_ok());

        match hub.get_data_conn::<SyncDataConn>("foo") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::SyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.get_data_conn::<AsyncDataConn>("bar") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::AsyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.commit() {
            Err(e) => match e.reason::<DataHubError>().unwrap() {
                DataHubError::FailToCommitDataConn { errors } => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(
                        errors.get("foo").unwrap().reason::<String>().unwrap(),
                        "ZZZ",
                    );
                }
                _ => panic!(),
            },
            _ => panic!(),
        }

        hub.rollback();

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "AsyncDataSrc::create_data_conn 2",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1 failed",
                    "SyncDataConn::rollback 1",
                    "AsyncDataConn::rollback 2",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataSrc::close 2",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }

    #[test]
    fn test_begin_and_commit_end_with_data_srcs_but_fail_to_commit_async() {
        let logger = Arc::new(Mutex::new(Vec::new()));

        let mut hub = DataHub::new();
        hub.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Not));
        hub.uses(
            "bar".to_string(),
            AsyncDataSrc::new(2, logger.clone(), Fail::Commit),
        );

        let r = hub.begin();
        assert!(r.is_ok());

        match hub.get_data_conn::<SyncDataConn>("foo") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::SyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.get_data_conn::<AsyncDataConn>("bar") {
            Ok(conn) => {
                assert_eq!(
                    any::type_name_of_val(conn),
                    "sabi::data_hub::tests_of_data_hub::AsyncDataConn"
                );
            }
            Err(e) => {
                panic!("{e:?}");
            }
        }

        match hub.commit() {
            Err(e) => match e.reason::<DataHubError>().unwrap() {
                DataHubError::FailToCommitDataConn { errors } => {
                    assert_eq!(errors.len(), 1);
                    assert_eq!(
                        errors.get("bar").unwrap().reason::<String>().unwrap(),
                        "VVV",
                    );
                }
                _ => panic!(),
            },
            _ => panic!(),
        }

        hub.rollback();

        hub.end();
        drop(hub);

        {
            assert_eq!(
                *logger.lock().unwrap(),
                &[
                    "SyncDataSrc::new 1",
                    "AsyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "AsyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "AsyncDataSrc::create_data_conn 2",
                    "AsyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "AsyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "AsyncDataConn::commit 2 failed",
                    "SyncDataConn::force_back 1",
                    "AsyncDataConn::rollback 2",
                    "AsyncDataConn::close 2",
                    "AsyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "AsyncDataSrc::close 2",
                    "AsyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ]
            );
        }
    }
}
