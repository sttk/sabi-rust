// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod async_group;
pub use async_group::AsyncGroupError;

mod data_src;
pub use data_src::{create_data_src_container, AnyDataSrcContainer, DataSrcRegistration};
pub use data_src::{setup, uses, DataSrcError};

mod data_hub;
pub use data_hub::DataHubError;

mod data_acc;
mod data_conn;

use std::borrow::Cow;
use std::collections::HashMap;
use std::{any, cell, marker, ptr, thread};

pub struct AsyncGroup<'a> {
    named_handlers: Vec<(String, thread::JoinHandle<errs::Result<()>>)>,
    pub(crate) name: &'a str,
}

#[allow(unused_variables)] // for rustdoc
pub trait DataConn {
    fn commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;
    fn pre_commit(&mut self, ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn post_commit(&mut self, ag: &mut AsyncGroup) {}
    fn should_force_back(&self) -> bool {
        false
    }
    fn rollback(&mut self, ag: &mut AsyncGroup);
    fn force_back(&mut self, ag: &mut AsyncGroup) {}
    fn close(&mut self);
}

struct NoopDataConn {}

impl DataConn for NoopDataConn {
    fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn rollback(&mut self, _ag: &mut AsyncGroup) {}
    fn close(&mut self) {}
}

#[repr(C)]
struct DataConnContainer<C = NoopDataConn>
where
    C: DataConn + 'static,
{
    drop_fn: fn(*const DataConnContainer),
    is_fn: fn(any::TypeId) -> bool,
    commit_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> errs::Result<()>,
    pre_commit_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> errs::Result<()>,
    post_commit_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    should_force_back_fn: fn(*const DataConnContainer) -> bool,
    rollback_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    force_back_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    close_fn: fn(*const DataConnContainer),

    name: String,
    data_conn: Box<C>,
}

#[allow(unused_variables)] // for rustdoc
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    fn setup(&mut self, ag: &mut AsyncGroup) -> errs::Result<()>;
    fn close(&mut self);
    fn create_data_conn(&mut self) -> errs::Result<Box<C>>;
}

struct NoopDataSrc {}

impl DataSrc<NoopDataConn> for NoopDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
        Ok(())
    }
    fn close(&mut self) {}
    fn create_data_conn(&mut self) -> errs::Result<Box<NoopDataConn>> {
        Ok(Box::new(NoopDataConn {}))
    }
}

#[repr(C)]
struct DataSrcContainer<S = NoopDataSrc, C = NoopDataConn>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    drop_fn: fn(*const DataSrcContainer),
    setup_fn: fn(*const DataSrcContainer, &mut AsyncGroup) -> errs::Result<()>,
    close_fn: fn(*const DataSrcContainer),
    create_data_conn_fn: fn(*const DataSrcContainer) -> errs::Result<Box<DataConnContainer<C>>>,
    is_data_conn_fn: fn(any::TypeId) -> bool,

    local: bool,
    name: Cow<'static, str>,
    data_src: S,
}

struct SendSyncNonNull<T: Send + Sync> {
    non_null_ptr: ptr::NonNull<T>,
    _phantom: marker::PhantomData<cell::Cell<T>>,
}

pub struct AutoShutdown {}

pub trait DataAcc {
    fn get_data_conn<C: DataConn + 'static>(
        &mut self,
        name: impl AsRef<str>,
    ) -> errs::Result<&mut C>;
}

pub struct DataHub {
    data_src_vec_unready: Vec<SendSyncNonNull<DataSrcContainer>>,
    data_src_vec_ready: Vec<SendSyncNonNull<DataSrcContainer>>,
    data_src_map: HashMap<Cow<'static, str>, *mut DataSrcContainer>,
    data_conn_vec: Vec<SendSyncNonNull<DataConnContainer>>,
    data_conn_map: HashMap<Cow<'static, str>, *mut DataConnContainer>,
    fixed: bool,
}
