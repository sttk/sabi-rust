// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{
    DataConn, DataConnContainer, DataConnManager, DataHub, DataRun, DataSrc, DataSrcManager,
    ErrEntry, SendSyncNonNull, TxnFailureReport,
};

use crate::global_setup::{copy_global_data_srcs_to_map, create_data_conn_from_global_data_src};

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, marker, ptr};

#[derive(Debug)]
pub enum DataHubError {
    FailToSetupLocalDataSrcs {
        errors: Vec<ErrEntry>,
    },

    NoDataSrcToCreateDataConn {
        name: Arc<str>,
        data_conn_type: &'static str,
    },

    FailToRunLogic {
        errors: Vec<ErrEntry>,
    },
}

impl DataRun {
    fn new() -> Self {
        let mut data_src_map = HashMap::new();
        copy_global_data_srcs_to_map(&mut data_src_map);

        Self {
            local_data_src_manager: DataSrcManager::new(true),
            data_src_map,
            data_conn_manager: DataConnManager::new(),
            fixed: false,
        }
    }

    fn with_commit_order(names: &[&str]) -> Self {
        let mut data_src_map = HashMap::new();
        copy_global_data_srcs_to_map(&mut data_src_map);

        Self {
            local_data_src_manager: DataSrcManager::new(true),
            data_src_map,
            data_conn_manager: DataConnManager::with_commit_order(names),
            fixed: false,
        }
    }

    #[inline]
    pub(crate) fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        if self.fixed {
            return;
        }
        self.local_data_src_manager.add(name, ds);
    }

    #[inline]
    pub(crate) fn disuses(&mut self, name: impl AsRef<str>) {
        if self.fixed {
            return;
        }
        self.data_src_map.remove(name.as_ref());
        self.local_data_src_manager.remove(name);
    }

    pub(crate) fn begin(&mut self) -> errs::Result<()> {
        self.fixed = true;

        let mut errors = Vec::new();

        self.local_data_src_manager.setup(&mut errors);
        if errors.is_empty() {
            self.local_data_src_manager
                .copy_ds_ready_to_map(&mut self.data_src_map);
            Ok(())
        } else {
            self.fixed = false;
            Err(errs::Err::new(DataHubError::FailToSetupLocalDataSrcs {
                errors,
            }))
        }
    }

    #[inline]
    pub(crate) fn new_failure_reports(&self) -> Vec<TxnFailureReport> {
        self.data_conn_manager.new_failure_reports()
    }

    #[inline]
    pub(crate) fn commit(&mut self, reports: &mut [TxnFailureReport]) -> errs::Result<()> {
        self.data_conn_manager.commit(reports)
    }

    #[inline]
    pub(crate) fn rollback(&mut self, reports: Vec<TxnFailureReport>) {
        self.data_conn_manager.rollback(reports)
    }

    #[inline]
    pub(crate) fn end(&mut self) {
        self.data_conn_manager.close();
        self.fixed = false;
    }

    pub(crate) fn get_data_conn<C>(&mut self, name: &str) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        if let Some(ssnnptr) = self.data_conn_manager.find_by_name(name) {
            let typed_ssnnptr = DataConnManager::to_typed_ptr::<C>(&ssnnptr)?;
            return Ok(unsafe { &mut (*typed_ssnnptr).data_conn });
        }

        if let Some((local, index)) = self.data_src_map.get(name) {
            let boxed = if *local {
                self.local_data_src_manager
                    .create_data_conn::<C>(*index, name)?
            } else {
                create_data_conn_from_global_data_src::<C>(*index, name)?
            };

            let ptr = Box::into_raw(boxed);
            if let Some(nnptr) = ptr::NonNull::new(ptr) {
                let ssnnptr = SendSyncNonNull::new(nnptr);
                self.data_conn_manager.add(ssnnptr);

                let typed_ptr = ptr.cast::<DataConnContainer<C>>();
                return Ok(unsafe { &mut (*typed_ptr).data_conn });
            } // else { /* impossible case. */ }
        }

        Err(errs::Err::new(DataHubError::NoDataSrcToCreateDataConn {
            name: name.into(),
            data_conn_type: any::type_name::<C>(),
        }))
    }
}

impl<T: 'static> DataHub<T> {
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        Self {
            run: DataRun::new(),
            _phantom: marker::PhantomData,
        }
    }

    #[inline]
    pub fn with_commit_order(names: &[&str]) -> Self {
        Self {
            run: DataRun::with_commit_order(names),
            _phantom: marker::PhantomData,
        }
    }
}
