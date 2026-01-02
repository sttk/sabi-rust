// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{AsyncGroup, DataConn, DataConnContainer};

use std::any;

impl<C> DataConnContainer<C>
where
    C: DataConn + 'static,
{
    pub(crate) fn new(name: String, data_conn: Box<C>) -> Self {
        Self {
            drop_fn: drop_data_conn::<C>,
            is_fn: is_data_conn::<C>,
            commit_fn: commit_data_conn::<C>,
            pre_commit_fn: pre_commit_data_conn::<C>,
            post_commit_fn: post_commit_data_conn::<C>,
            should_force_back_fn: should_force_back_data_conn::<C>,
            rollback_fn: rollback_data_conn::<C>,
            force_back_fn: force_back_data_conn::<C>,
            close_fn: close_data_conn::<C>,

            name,
            data_conn,
        }
    }
}

fn drop_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        drop(Box::from_raw(typed_ptr));
    }
}

fn is_data_conn<C>(type_id: any::TypeId) -> bool
where
    C: DataConn + 'static,
{
    any::TypeId::of::<C>() == type_id
}

fn commit_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup) -> errs::Result<()>
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.commit(ag) }
}

fn pre_commit_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup) -> errs::Result<()>
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.pre_commit(ag) }
}

fn post_commit_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        (*typed_ptr).data_conn.post_commit(ag);
    }
}

fn should_force_back_data_conn<C>(ptr: *const DataConnContainer) -> bool
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.should_force_back() }
}

fn rollback_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        (*typed_ptr).data_conn.rollback(ag);
    }
}

fn force_back_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        (*typed_ptr).data_conn.force_back(ag);
    }
}

fn close_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        (*typed_ptr).data_conn.close();
    }
}
