// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod logic_run;
mod logic_runner;
mod logic_txn;

use crate::tokio::{
    DataAcc, DataConn, DataConnContainer, DataConnManager, DataHub, DataRun, DataSrc,
    DataSrcManager, ErrEntry, LogicRun, LogicTxn, SendSyncNonNull, TxnFailureReport,
};

use crate::tokio::data_src::{
    copy_global_data_srcs_to_map, create_data_conn_from_global_data_src_async,
};

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{any, marker, ptr};

/// Represents errors that can occur during [`DataHub`](crate::tokio::DataHub) setup, connection
/// retrieval, or logic execution.
#[derive(Debug)]
pub enum DataHubError {
    /// Indicates that setting up local data sources failed.
    FailToSetupLocalDataSrcs {
        /// The list of error entries detailing which local data sources failed and their errors.
        errors: Vec<ErrEntry>,
    },

    /// Indicates that no data source with the specified name was found when attempting to create
    /// a data connection.
    NoDataSrcToCreateDataConn {
        /// The requested data connection name.
        name: Arc<str>,
        /// The requested data connection type name.
        data_conn_type: &'static str,
    },

    /// Indicates that an error occurred while executing a logic function.
    FailToRunLogic {
        /// The list of error entries detailing which logic executions failed and their errors.
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
    fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C> + 'static,
        C: DataConn + 'static,
    {
        if self.fixed {
            return;
        }
        self.local_data_src_manager.add(name, ds);
    }

    #[inline]
    fn disuses(&mut self, name: impl AsRef<str>) {
        if self.fixed {
            return;
        }
        self.data_src_map.remove(name.as_ref());
        self.local_data_src_manager.remove(name);
    }

    async fn begin_async(&mut self) -> errs::Result<()> {
        self.fixed = true;

        let mut errors = Vec::new();

        self.local_data_src_manager.setup_async(&mut errors).await;
        if errors.is_empty() {
            self.local_data_src_manager
                .copy_ds_ready_to_map(&mut self.data_src_map);
            Ok(())
        } else {
            Err(errs::Err::new(DataHubError::FailToSetupLocalDataSrcs {
                errors,
            }))
        }
    }

    #[inline]
    fn new_failure_reports(&self) -> Vec<TxnFailureReport> {
        self.data_conn_manager.new_failure_reports()
    }

    #[inline]
    async fn commit_async(&mut self, reports: &mut [TxnFailureReport]) -> errs::Result<()> {
        self.data_conn_manager.commit_async(reports).await
    }

    #[inline]
    async fn rollback_async(&mut self, reports: Vec<TxnFailureReport>) {
        self.data_conn_manager.rollback_async(reports).await
    }

    #[inline]
    fn end(&mut self) {
        self.data_conn_manager.close();
        self.fixed = false;
    }

    async fn get_data_conn_async<C>(&mut self, name: &str) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        if let Some(nnptr) = self.data_conn_manager.find_by_name(name) {
            let typed_nnptr = DataConnManager::to_typed_ptr::<C>(&nnptr)?;
            return Ok(unsafe { &mut (*typed_nnptr).data_conn });
        }

        if let Some((local, index)) = self.data_src_map.get(name) {
            let boxed = if *local {
                self.local_data_src_manager
                    .create_data_conn_async::<C>(*index, name)
                    .await?
            } else {
                create_data_conn_from_global_data_src_async::<C>(*index, name).await?
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

impl<T: 'static + Send> DataAcc for DataHub<T> {
    type D = T;

    #[inline]
    async fn get_data_conn_async<C>(&mut self, name: &str) -> errs::Result<&mut C>
    where
        C: DataConn + 'static,
    {
        self.run.get_data_conn_async(name).await
    }

    #[allow(clippy::doc_overindented_list_items)]
    async fn run_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let mut r = self.run.begin_async().await;
        if r.is_ok() {
            r = logic_fn(self).await;
        }
        self.run.end();
        r
    }

    #[inline]
    async fn start_async(&mut self) -> LogicRun<'_, T> {
        LogicRun::new_async(self, false).await
    }
}

impl<T: 'static + Send> DataHub<T> {
    /// Creates a new asynchronous `DataHub` instance with default commit order.
    ///
    /// # Returns
    ///
    /// A new [`DataHub`] initialized with available global data sources.
    #[allow(clippy::new_without_default)]
    #[inline]
    pub fn new() -> Self {
        Self {
            run: DataRun::new(),
            _phantom: marker::PhantomData,
        }
    }

    /// Creates a new asynchronous `DataHub` instance configured with an explicit commit order for
    /// data connections.
    ///
    /// When a transaction commits, data connections with names listed in `names` are committed
    /// in the specified sequence. Connections not listed are committed after them.
    ///
    /// # Parameters
    ///
    /// * `names`: A slice of data connection names defining the commit order.
    ///
    /// # Returns
    ///
    /// A new [`DataHub`] configured with the specified commit order.
    #[inline]
    pub fn with_commit_order(names: &[&str]) -> Self {
        Self {
            run: DataRun::with_commit_order(names),
            _phantom: marker::PhantomData,
        }
    }

    async fn _txn_async<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        for<'b> F: FnMut(
            &'b mut DataHub<T>,
        ) -> Pin<Box<dyn Future<Output = errs::Result<()>> + Send + 'b>>,
    {
        let mut r = self.run.begin_async().await;
        if r.is_ok() {
            r = logic_fn(self).await;
        }

        let mut reports = self.run.new_failure_reports();

        if r.is_ok() {
            r = self.run.commit_async(&mut reports).await;
        }
        if r.is_err() {
            self.run.rollback_async(reports).await;
        }

        self.run.end();
        r
    }

    #[inline]
    async fn _begin_txn_async(&mut self) -> LogicTxn<'_, T> {
        LogicTxn::new_async(self).await
    }
}
