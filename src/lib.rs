// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;

use errs::Err;

mod async_group;
pub use async_group::{AsyncGroup, AsyncGroupError};

mod data_acc;
mod data_conn;
mod data_hub;
mod data_src;
pub use data_acc::DataAcc;
pub use data_hub::{setup, shutdown, shutdown_later, uses, DataHub, DataHubError};

#[allow(unused_variables)] // for rustdoc
pub trait DataConn {
    fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err>;
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
    fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
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

    commit_fn: fn(*const DataConnContainer, &mut AsyncGroup) -> Result<(), Err>,
    post_commit_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    should_force_back_fn: fn(*const DataConnContainer) -> bool,
    rollback_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    force_back_fn: fn(*const DataConnContainer, &mut AsyncGroup),
    close_fn: fn(*const DataConnContainer),

    prev: *mut DataConnContainer,
    next: *mut DataConnContainer,
    name: String,
    data_conn: Box<C>,
}

#[allow(unused_variables)] // for rustdoc
pub trait DataSrc<C>
where
    C: DataConn + 'static,
{
    fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err>;
    fn close(&mut self);
    fn create_data_conn(&mut self) -> Result<Box<C>, Err>;
}

struct NoopDataSrc {}

impl DataSrc<NoopDataConn> for NoopDataSrc {
    fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn close(&mut self) {}
    fn create_data_conn(&mut self) -> Result<Box<NoopDataConn>, Err> {
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
    setup_fn: fn(*const DataSrcContainer, &mut AsyncGroup) -> Result<(), Err>,
    close_fn: fn(*const DataSrcContainer),
    create_data_conn_fn: fn(*const DataSrcContainer) -> Result<Box<DataConnContainer<C>>, Err>,
    is_data_conn_fn: fn(any::TypeId) -> bool,

    prev: *mut DataSrcContainer,
    next: *mut DataSrcContainer,
    local: bool,
    name: String,

    data_src: S,
}
