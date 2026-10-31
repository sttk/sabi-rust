// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{
    AsyncGroup, DataConn, DataConnContainer, DataSrc, DataSrcContainer, DataSrcManager, ErrEntry,
    SendSyncNonNull,
};

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, mem, ptr};

#[derive(Debug)]
pub enum DataSrcError {
    FailToRegisterGlobalDataSrc {
        name: Arc<str>,
    },

    FailToSetupGlobalDataSrcs {
        errors: Vec<ErrEntry>,
    },

    DuringSetupGlobalDataSrcs,

    AlreadySetupGlobalDataSrcs,

    FailToCastDataConn {
        name: Arc<str>,
        target_type: &'static str,
    },

    FailToCreateDataConn {
        name: Arc<str>,
        data_conn_type: &'static str,
    },

    NoDataSrcToCreateDataConn {
        name: Arc<str>,
        data_conn_type: &'static str,
    },
}

// NOTE: Uses the thin-pointer approach (#[repr(C)] + function pointers).
//
// Different DataSrc<C> implementations, each with a different C, must be
// mixed together in a single Vec<T>. Making it dyn would fix T =
// dyn DataSrc<C>, which pins C to one concrete type; a separate Vec<T>
// would then be needed for each C, which defeats this requirement.
//
// Also, if the value produced by create_data_conn_fn were passed to
// DataConnContainer::new by the caller (DataSrcManager) to be type-erased
// there, the static type of the function pointer read out through an
// unsafe pointer (i.e. the default type arguments) would end up
// disagreeing with the actual runtime value, causing it to be erased
// under the wrong type — a bug that was actually observed in practice.
// Type erasure (wrapping) must always be completed inside the
// monomorphized function itself; this is another reason dyn cannot be
// used here.

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

    pub(crate) fn setup(&mut self, errors: &mut Vec<ErrEntry>) {
        if self.vec_unready.is_empty() {
            return;
        }

        let mut ag = AsyncGroup::new();
        for (i, ssnnptr) in self.vec_unready.iter().enumerate() {
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let setup_fn = unsafe { (*ptr).setup_fn };
            let name = unsafe { &(*ptr).name };
            ag.index = i;
            ag.name = name.clone();
            if let Err(err) = setup_fn(ptr, &mut ag) {
                errors.push(ErrEntry {
                    index: i,
                    name: name.clone(),
                    err,
                });
                break;
            }
        }
        let n_done = ag.index;
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

    pub(crate) fn setup_with_order(&mut self, names: &[&str], errors: &mut Vec<ErrEntry>) {
        if self.vec_unready.is_empty() {
            return;
        }

        let mut index_map: HashMap<&str, usize> = HashMap::with_capacity(names.len());
        // Using rev because earlier ones take precedence when names overlap
        for (i, nm) in names.iter().rev().enumerate() {
            index_map.insert(*nm, names.len() - 1 - i);
        }

        let mut ordered_indexes = Vec::<Option<usize>>::with_capacity(self.vec_unready.len());
        ordered_indexes.resize(names.len(), None);

        for vec_index in 0..self.vec_unready.len() {
            let ssnnptr = &self.vec_unready[vec_index];
            let ptr = ssnnptr.non_null_ptr.as_ptr();
            let name = unsafe { (*ptr).name.clone() };
            if let Some(order_index) = index_map.remove(name.as_ref()) {
                ordered_indexes[order_index] = Some(vec_index);
            } else {
                ordered_indexes.push(Some(vec_index));
            }
        }

        let mut ag = AsyncGroup::new();
        let mut n_done = 0;
        for vec_index_opt in ordered_indexes.iter() {
            if let Some(vec_index) = vec_index_opt {
                let ssnnptr = &self.vec_unready[*vec_index];
                let ptr = ssnnptr.non_null_ptr.as_ptr();
                let setup_fn = unsafe { (*ptr).setup_fn };
                let name = unsafe { &(*ptr).name };
                ag.index = *vec_index;
                ag.name = name.clone();
                if let Err(err) = setup_fn(ptr, &mut ag) {
                    errors.push(ErrEntry {
                        index: *vec_index,
                        name: name.clone(),
                        err,
                    });
                    break;
                }
            }
            n_done += 1;
        }
        ag.join_and_collect_errors(errors);

        if errors.is_empty() {
            let old_unready = mem::take(&mut self.vec_unready);
            // Maximizing performance by pre-allocating the required capacity.
            self.vec_ready
                .reserve(ordered_indexes.iter().flatten().count());
            for vec_index in ordered_indexes.iter().flatten() {
                self.vec_ready.push(old_unready[*vec_index]);
            }
        } else {
            for vec_index in ordered_indexes.iter().take(n_done).flatten().rev() {
                let ssnnptr = &self.vec_unready[*vec_index];
                let ptr = ssnnptr.non_null_ptr.as_ptr();
                let close_fn = unsafe { (*ptr).close_fn };
                close_fn(ptr);
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
            Err(errs::Err::new(DataSrcError::NoDataSrcToCreateDataConn {
                name: name.as_ref().into(),
                data_conn_type: any::type_name::<C>(),
            }))
        }
    }
}

impl Drop for DataSrcManager {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_create_data_conn;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
pub(crate) mod _test_data;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_new_add_remove_close;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_setup;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_utils;
