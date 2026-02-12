// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::DataSrcError;

use super::super::{
    AutoShutdown, DataConn, DataConnContainer, DataSrc, DataSrcContainer, DataSrcManager,
    StaticDataSrcContainer, StaticDataSrcRegistration,
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

/// Registers a global data source, making it available throughout the application.
///
/// Global data sources are managed by a singleton and can be set up once for the application's lifetime.
/// If `setup_async` or `setup_with_order_async` has already been called, this function will log an error.
///
/// # Arguments
///
/// * `name` - The name to associate with this data source.
/// * `ds` - The data source instance, which must implement `DataSrc` and have a `'static` lifetime.
///
/// # Type Parameters
///
/// * `S` - The type of the data source.
/// * `C` - The type of the data connection provided by the data source.
pub async fn uses_async<S, C>(name: impl Into<Arc<str>>, ds: S)
where
    S: DataSrc<C> + 'static,
    C: DataConn + 'static,
{
    match DS_MANAGER.lock_async().await {
        Ok(mut dsm) => dsm.add(name, ds),
        Err(e) => {
            eprintln!("ERROR(sabi): Fail to add a global DataSrc: {e:?}");
        }
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

/// Asynchronously sets up all globally registered data sources.
///
/// This function transitions the global data source manager into a "read" phase,
/// setting up all data sources that have been registered via `uses_async` or
/// `inventory::submit!`. It collects any setup errors.
///
/// # Returns
///
/// A `Result` which is `Ok` containing an `AutoShutdown` guard if all data sources
/// are set up successfully. If setup fails for any data source, it returns an `Err`
/// with `DataSrcError::FailToSetupGlobalDataSrcs`. If called when data sources are
/// already set up or in transition, it returns `DataSrcError::AlreadySetupGlobalDataSrcs`
/// or `DataSrcError::DuringSetupGlobalDataSrcs` respectively.
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

/// Asynchronously sets up all globally registered data sources with a specified order.
///
/// Similar to `setup_async`, but allows defining the order in which data sources
/// are set up. Data sources not specified in `names` will be set up after the
/// specified ones, in an undefined order.
///
/// # Arguments
///
/// * `names` - An array of string slices specifying the desired setup order by data source name.
///
/// # Returns
///
/// A `Result` which is `Ok` containing an `AutoShutdown` guard if all data sources
/// are set up successfully. If setup fails for any data source, it returns an `Err`
/// with `DataSrcError::FailToSetupGlobalDataSrcs`. If called when data sources are
/// already set up or in transition, it returns `DataSrcError::AlreadySetupGlobalDataSrcs`
/// or `DataSrcError::DuringSetupGlobalDataSrcs` respectively.
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

#[doc(hidden)]
/// Creates a `StaticDataSrcContainer` for a given data source. Internal use only.
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
    /// Creates a new `StaticDataSrcRegistration` with the provided factory function.
    ///
    /// This is used internally by the `inventory` crate to register static data sources.
    ///
    /// # Arguments
    ///
    /// * `factory` - A function that returns a `StaticDataSrcContainer`.
    pub const fn new(factory: fn() -> StaticDataSrcContainer) -> Self {
        Self { factory }
    }
}
inventory::collect!(StaticDataSrcRegistration);

/// Macro for registering a global data source using `inventory`.
///
/// This macro simplifies the process of making a `DataSrc` globally available
/// at compile time.
///
/// # Arguments
///
/// * `$name` - The name of the data source (must be a string literal).
/// * `$data_src` - The data source instance.
///
/// # Examples
///
/// ```ignore
/// uses_async!("my_global_source", MyDataSource::new());
/// ```
#[macro_export]
macro_rules! uses_async {
    ($name:tt, $data_src:expr) => {
        const _: () = {
            inventory::submit! {
                $crate::StaticDataSrcRegistration::new(async || {
                    $crate::create_static_data_src_container_async($name, $data_src).await
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
