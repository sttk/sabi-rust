// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::tokio::{
    AutoShutdown, DataConn, DataConnContainer, DataSrc, DataSrcContainer, DataSrcError,
    DataSrcManager, StaticDataSrcContainer, StaticDataSrcRegistration,
};

use crate::SendSyncNonNull;

use setup_read_cleanup::{PhasedCellAsync, PhasedError, PhasedErrorKind};

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, ptr};
use tokio::sync::Mutex;

pub(crate) static DS_MANAGER: PhasedCellAsync<DataSrcManager> =
    PhasedCellAsync::new(DataSrcManager::new(false));

const NOOP: fn(&mut DataSrcManager) -> Result<(), PhasedError> = |_| Ok(());

impl Drop for AutoShutdown {
    fn drop(&mut self) {
        let _ = DS_MANAGER.force_to_cleanup(|ds_m| {
            ds_m.close();
            Ok::<(), PhasedError>(())
        });
    }
}

/// Registers an asynchronous data source into the global registry asynchronously.
///
/// This registers an asynchronous [`DataSrc`] with a unique name so that it can later be
/// initialized via [`setup_async`] or [`setup_with_order_async`], and accessed globally by
/// [`DataHub`](crate::tokio::DataHub).
///
/// # Type Parameters
///
/// * `S`: The type of the data source implementing [`DataSrc<C>`].
/// * `C`: The type of the data connection implementing [`DataConn`].
///
/// # Parameters
///
/// * `name`: The unique identifier name for the data source.
/// * `ds`: The data source instance to register.
///
/// # Errors
///
/// Returns an error if the global data source registry has already transitioned past the
/// registration phase (for example, if setup has already been called).
pub async fn uses_async<S, C>(name: impl Into<Arc<str>>, ds: S) -> errs::Result<()>
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    match DS_MANAGER.lock_async().await {
        Ok(mut dsm) => {
            dsm.add(name, ds);
            Ok(())
        }
        Err(e) => Err(errs::Err::with_source(
            DataSrcError::FailToRegisterGlobalDataSrc { name: name.into() },
            e,
        )),
    }
}

/// Registers an asynchronous data source into the global registry synchronously.
///
/// This registers an asynchronous [`DataSrc`] with a unique name if the global registry lock
/// is immediately available without waiting. It can later be initialized via [`setup_async`] or
/// [`setup_with_order_async`], and accessed globally by [`DataHub`](crate::tokio::DataHub).
///
/// # Type Parameters
///
/// * `S`: The type of the data source implementing [`DataSrc<C>`].
/// * `C`: The type of the data connection implementing [`DataConn`].
///
/// # Parameters
///
/// * `name`: The unique identifier name for the data source.
/// * `ds`: The data source instance to register.
///
/// # Errors
///
/// Returns an error if the registry lock cannot be acquired immediately or if the global data
/// source registry has already transitioned past the registration phase.
pub fn uses<S, C>(name: impl Into<Arc<str>>, ds: S) -> errs::Result<()>
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    match DS_MANAGER.try_lock() {
        Ok(mut dsm) => {
            dsm.add(name, ds);
            Ok(())
        }
        Err(e) => Err(errs::Err::with_source(
            DataSrcError::FailToRegisterGlobalDataSrc { name: name.into() },
            e,
        )),
    }
}

fn collect_static_data_src_containers(dsm: &mut DataSrcManager) {
    let regs: Vec<_> = inventory::iter::<StaticDataSrcRegistration>
        .into_iter()
        .collect();

    let mut static_vec: Vec<SendSyncNonNull<DataSrcContainer>> = Vec::with_capacity(regs.len());
    for reg in regs {
        let any_container = (reg.factory)();
        static_vec.push(any_container.ssnnptr);
    }

    dsm.prepend(static_vec);
}

/// Initializes all globally registered data sources asynchronously and concurrently.
///
/// This transitions the global data source registry to the active read phase and runs the
/// setup logic for all registered data sources concurrently using
/// [`AsyncGroup`](crate::tokio::AsyncGroup).
///
/// # Returns
///
/// Returns an [`AutoShutdown`] instance that will automatically close and clean up all global
/// data sources when dropped.
///
/// # Errors
///
/// Returns an error if:
/// * The data sources have already been initialized.
/// * Initialization is currently in progress.
/// * Any data source fails during its
///   [`setup_async`](crate::tokio::DataSrc::setup_async) execution.
pub async fn setup_async() -> errs::Result<AutoShutdown> {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let errors_for_closure = Arc::clone(&errors);

    if let Err(e) = DS_MANAGER
        .transition_to_read_async(move |ds_m| {
            collect_static_data_src_containers(ds_m);
            let errors_for_future = Arc::clone(&errors_for_closure);
            Box::pin(async move {
                let mut lock = errors_for_future.lock().await;
                ds_m.setup_async(&mut lock).await;
                Ok::<(), PhasedError>(())
            })
        })
        .await
    {
        if e.kind() == PhasedErrorKind::DuringTransitionToRead {
            return Err(errs::Err::new(DataSrcError::DuringSetupGlobalDataSrcs));
        } else {
            return Err(errs::Err::new(DataSrcError::AlreadySetupGlobalDataSrcs));
        }
    }

    let errors = Arc::try_unwrap(errors).unwrap().into_inner();
    if errors.is_empty() {
        Ok(AutoShutdown {})
    } else {
        Err(errs::Err::new(DataSrcError::FailToSetupGlobalDataSrcs {
            errors,
        }))
    }
}

/// Initializes all globally registered data sources asynchronously in the specified order.
///
/// Data sources whose names are present in `names` are set up in the specified sequence. Any
/// registered data sources not listed in `names` are set up subsequently.
///
/// # Parameters
///
/// * `names`: A static slice of data source names defining the desired order of setup.
///
/// # Returns
///
/// Returns an [`AutoShutdown`] instance that will automatically close and clean up all global
/// data sources when dropped.
///
/// # Errors
///
/// Returns an error if:
/// * The data sources have already been initialized.
/// * Initialization is currently in progress.
/// * Any data source fails during its
///   [`setup_async`](crate::tokio::DataSrc::setup_async) execution.
pub async fn setup_with_order_async(names: &'static [&str]) -> errs::Result<AutoShutdown> {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let errors_for_closure = Arc::clone(&errors);

    if let Err(e) = DS_MANAGER
        .transition_to_read_async(move |ds_m| {
            collect_static_data_src_containers(ds_m);
            let errors_for_future = Arc::clone(&errors_for_closure);
            Box::pin(async move {
                let mut lock = errors_for_future.lock().await;
                ds_m.setup_with_order_async(names, &mut lock).await;
                Ok::<(), PhasedError>(())
            })
        })
        .await
    {
        if e.kind() == PhasedErrorKind::DuringTransitionToRead {
            return Err(errs::Err::new(DataSrcError::DuringSetupGlobalDataSrcs));
        } else {
            return Err(errs::Err::new(DataSrcError::AlreadySetupGlobalDataSrcs));
        }
    }

    let errors = Arc::try_unwrap(errors).unwrap().into_inner();
    if errors.is_empty() {
        Ok(AutoShutdown {})
    } else {
        Err(errs::Err::new(DataSrcError::FailToSetupGlobalDataSrcs {
            errors,
        }))
    }
}

pub(crate) fn copy_global_data_srcs_to_map(index_map: &mut HashMap<Arc<str>, (bool, usize)>) {
    if let Ok(ds_m) = DS_MANAGER.read_relaxed() {
        ds_m.copy_ds_ready_to_map(index_map);
    } else if (match DS_MANAGER.force_to_read(NOOP) {
        Ok(_) => Ok(()),
        Err(e) => match e.kind() {
            PhasedErrorKind::PhaseIsAlreadyCleanup => Ok(()),
            PhasedErrorKind::DuringTransitionToRead => Ok(()),
            _ => Err(()),
        },
    })
    .is_ok()
    {
        if let Ok(ds_m) = DS_MANAGER.read_relaxed() {
            ds_m.copy_ds_ready_to_map(index_map);
        }
    }
}

/// Creates a container for a statically registered asynchronous data source.
///
/// # Type Parameters
///
/// * `S`: The type of the data source implementing [`DataSrc<C>`].
/// * `C`: The type of the data connection implementing [`DataConn`].
///
/// # Parameters
///
/// * `name`: The name of the data source.
/// * `data_src`: The data source instance.
///
/// # Returns
///
/// A [`StaticDataSrcContainer`] holding the data source.
#[doc(hidden)]
pub fn create_static_data_src_container<S, C>(
    name: &'static str,
    data_src: S,
) -> StaticDataSrcContainer
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    let boxed = Box::new(DataSrcContainer::<S, C>::new(name, data_src, false));
    let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
    StaticDataSrcContainer {
        ssnnptr: SendSyncNonNull::new(ptr),
    }
}

impl StaticDataSrcRegistration {
    /// Creates a new static data source registration entry.
    ///
    /// # Parameters
    ///
    /// * `factory`: A function pointer that produces a [`StaticDataSrcContainer`].
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub const fn new(factory: fn() -> StaticDataSrcContainer) -> Self {
        Self { factory }
    }
}
inventory::collect!(StaticDataSrcRegistration);

/// Registers a global asynchronous data source statically at compile/link time.
///
/// This macro registers a static data source factory that is collected and initialized during
/// [`setup_async`] or [`setup_with_order_async`].
///
/// # Parameters
///
/// * `$name`: A string literal representing the data source name.
/// * `$data_src`: An expression evaluating to the data source instance.
#[macro_export]
#[doc(hidden)]
macro_rules! _uses_for_async {
    ($name:tt, $data_src:expr) => {
        const _: () = {
            inventory::submit! {
                $crate::tokio::StaticDataSrcRegistration::new(|| {
                    $crate::tokio::create_static_data_src_container($name, $data_src)
                })
            }
        };
    };
}

pub(crate) async fn create_data_conn_from_global_data_src_async<C>(
    index: usize,
    name: impl AsRef<str>,
) -> errs::Result<Box<DataConnContainer>>
where
    C: DataConn + 'static,
{
    match DS_MANAGER.read_relaxed() {
        Ok(ds_manager) => ds_manager.create_data_conn_async::<C>(index, name).await,
        Err(e) => Err(errs::Err::with_source(
            DataSrcError::FailToCreateDataConn {
                name: name.as_ref().into(),
                data_conn_type: any::type_name::<C>(),
            },
            e,
        )),
    }
}
