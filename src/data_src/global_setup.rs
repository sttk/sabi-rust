// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::DataSrcError;

use crate::{
    AutoShutdown, DataConn, DataConnContainer, DataSrc, DataSrcContainer, DataSrcManager,
    SendSyncNonNull, StaticDataSrcContainer, StaticDataSrcRegistration,
};

#[allow(unused)] // for rustdoc
use crate::DataHub;

use setup_read_cleanup::{PhasedCell, PhasedError, PhasedErrorKind};

use std::collections::HashMap;
use std::sync::Arc;
use std::{any, ptr};

pub(crate) static DS_MANAGER: PhasedCell<DataSrcManager> =
    PhasedCell::new(DataSrcManager::new(false));

const NOOP: fn(&mut DataSrcManager) -> Result<(), PhasedError> = |_| Ok(());

impl Drop for AutoShutdown {
    fn drop(&mut self) {
        let _ = DS_MANAGER.transition_to_cleanup(NOOP);
        match DS_MANAGER.get_mut_unlocked() {
            Ok(ds_m) => ds_m.close(),
            Err(e) => {
                eprintln!("ERROR(sabi): Fail to close and drop global DataSrc(s): {e:?}");
            }
        }
    }
}

/// Registers a global data source dynamically at runtime.
///
/// This function associates a given [`DataSrc`] implementation with a unique name.
/// This name will later be used to retrieve session-specific [`DataConn`] instances
/// from this data source.
///
/// Global data sources added via this function can be set up via [`setup`] or [`setup_with_order`].
///
/// # Parameters
///
/// * `name`: The unique name for the data source.
/// * `ds`: The [`DataSrc`] instance to register.
pub fn uses<S, C>(name: impl Into<Arc<str>>, ds: S)
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    match DS_MANAGER.get_mut_unlocked() {
        Ok(dsm) => dsm.add(name, ds),
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

/// Executes the setup process for all globally registered data sources.
///
/// This setup typically involves tasks such as creating connection pools,
/// opening global connections, or performing initial configurations necessary
/// for creating session-specific connections.
///
/// If any data source fails to set up, this function returns an [`errs::Err`] with
/// [`DataSrcError::FailToSetupGlobalDataSrcs`], containing a vector of the names
/// of the failed data sources and their corresponding [`errs::Err`] objects. In such a case,
/// all global data sources that were successfully set up are also closed and dropped.
///
/// If all data source setups are successful, the [`Result::Ok`] which contains an
/// [`AutoShutdown`] object is returned. This object is designed to close and drop global
/// data sources when it's dropped.
/// Thanks to Rust's ownership mechanism, this ensures that the global data sources are
/// automatically cleaned up when the return value goes out of scope.
///
/// **NOTE:** Do not receive the [`Result`] or its inner object into an anonymous
/// variable using `let _ = ...`.
/// If you do, the inner object is dropped immediately at that point.
///
/// # Returns
///
/// * `Result<AutoShutdown, errs::Err>`: An [`AutoShutdown`] if all global data sources are
///   set up successfully, or an [`errs::Err`] if any setup fails.
pub fn setup() -> errs::Result<AutoShutdown> {
    let mut errors = Vec::new();
    let em = &mut errors;

    if let Err(e) = DS_MANAGER.transition_to_read(move |dsm| {
        collect_static_data_src_containers(dsm);
        dsm.setup(em);
        Ok::<(), PhasedError>(())
    }) {
        if e.kind() == PhasedErrorKind::DuringTransitionToRead {
            Err(errs::Err::new(DataSrcError::DuringSetupGlobalDataSrcs))
        } else {
            Err(errs::Err::new(DataSrcError::AlreadySetupGlobalDataSrcs))
        }
    } else if errors.is_empty() {
        Ok(AutoShutdown {})
    } else {
        Err(errs::Err::new(DataSrcError::FailToSetupGlobalDataSrcs {
            errors,
        }))
    }
}

/// Executes the setup process for all globally registered data sources, allowing for a specified
/// order of setup for a subset of data sources.
///
/// This function is similar to [`setup`], but it allows defining a specific order for the setup
/// of certain data sources identified by their names. Data sources not specified in `names` will
/// be set up after the named ones, in their order of acquisition.
///
/// This setup typically involves tasks such as creating connection pools,
/// opening global connections, or performing initial configurations necessary
/// for creating session-specific connections.
///
/// If any data source fails to set up, this function returns an [`errs::Err`] with
/// [`DataSrcError::FailToSetupGlobalDataSrcs`], containing a vector of the names
/// of the failed data sources and their corresponding [`errs::Err`] objects. In such a case,
/// all global data sources that were successfully set up are also closed and dropped.
///
/// If all data source setups are successful, the [`Result::Ok`] which contains an
/// [`AutoShutdown`] object is returned. This object is designed to close and drop global
/// data sources when it's dropped.
/// Thanks to Rust's ownership mechanism, this ensures that the global data sources are
/// automatically cleaned up when the return value goes out of scope.
///
/// **NOTE:** Do not receive the [`Result`] or its inner object into an anonymous
/// variable using `let _ = ...`.
/// If you do, the inner object is dropped immediately at that point.
///
/// # Parameters
///
/// * `names`: A slice of `&str` representing the names of data sources to set up in a specific order.
///
/// # Returns
///
/// * `Result<AutoShutdown, errs::Err>`: An [`AutoShutdown`] if all global data sources are
///   set up successfully, or an [`errs::Err`] if any setup fails.
pub fn setup_with_order(names: &[&str]) -> errs::Result<AutoShutdown> {
    let mut errors = Vec::new();
    let em = &mut errors;

    if let Err(e) = DS_MANAGER.transition_to_read(move |dsm| {
        collect_static_data_src_containers(dsm);
        dsm.setup_with_order(names, em);
        Ok::<(), PhasedError>(())
    }) {
        if e.kind() == PhasedErrorKind::DuringTransitionToRead {
            Err(errs::Err::new(DataSrcError::DuringSetupGlobalDataSrcs))
        } else {
            Err(errs::Err::new(DataSrcError::AlreadySetupGlobalDataSrcs))
        }
    } else if errors.is_empty() {
        Ok(AutoShutdown {})
    } else {
        Err(errs::Err::new(DataSrcError::FailToSetupGlobalDataSrcs {
            errors,
        }))
    }
}

#[doc(hidden)]
/// Helper function to create a [`StaticDataSrcContainer`] for static registration.
/// This function is used by the [`uses!`] macro.
pub fn create_static_data_src_container<S, C>(
    name: &'static str,
    data_src: S,
) -> StaticDataSrcContainer
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    let boxed = Box::new(DataSrcContainer::<S, C>::new(name, data_src, false));
    let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<DataSrcContainer>();
    StaticDataSrcContainer {
        ssnnptr: SendSyncNonNull::new(ptr),
    }
}

impl StaticDataSrcRegistration {
    pub const fn new(factory: fn() -> StaticDataSrcContainer) -> Self {
        Self { factory }
    }
}
inventory::collect!(StaticDataSrcRegistration);

/// Registers a global data source that can be used throughout the application.
///
/// This macro associates a given [`DataSrc`] implementation with a unique name.
/// This name will later be used to retrieve session-specific [`DataConn`] instances
/// from this data source.
///
/// Global data sources are set up once via the [`setup`] function and are available
/// to all [`DataHub`] instances.
///
/// # Parameters
///
/// * `name`: The unique name for the data source.
/// * `ds`: The [`DataSrc`] instance to register.
///
/// # Examples
///
/// ```ignore
/// use sabi::{DataSrc, DataConn, AsyncGroup, uses};
/// use errs::Err;
///
/// struct MyDataSrc;
/// impl DataSrc<MyDataConn> for MyDataSrc {
///     fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> { Ok(()) }
///     fn close(&mut self) {}
///     fn create_data_conn(&mut self) -> errs::Result<Box<MyDataConn>> {
///         Ok(Box::new(MyDataConn))
///     }
/// }
///
/// struct MyDataConn;
/// impl DataConn for MyDataConn {
///     fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> { Ok(()) }
///     fn rollback(&mut self, _ag: &mut AsyncGroup) {}
///     fn close(&mut self) {}
/// }
///
/// uses!("my_data_src", MyDataSrc);
/// ```
#[macro_export]
macro_rules! uses {
    ($name:tt, $data_src:expr) => {
        const _: () = {
            inventory::submit! {
                $crate::StaticDataSrcRegistration::new(|| {
                    $crate::create_static_data_src_container($name, $data_src)
                })
            }
        };
    };
}

pub(crate) fn copy_global_data_srcs_to_map(index_map: &mut HashMap<Arc<str>, (bool, usize)>) {
    if let Ok(ds_m) = DS_MANAGER.read_relaxed() {
        ds_m.copy_ds_ready_to_map(index_map);
    } else if (match DS_MANAGER.transition_to_read(NOOP) {
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

pub(crate) fn create_data_conn_from_global_data_src<C>(
    index: usize,
    name: impl AsRef<str>,
) -> errs::Result<Box<DataConnContainer>>
where
    C: DataConn + 'static,
{
    match DS_MANAGER.read_relaxed() {
        Ok(ds_manager) => ds_manager.create_data_conn::<C>(index, name),
        Err(e) => Err(errs::Err::with_source(
            DataSrcError::FailToCreateDataConn {
                name: name.as_ref().into(),
                data_conn_type: any::type_name::<C>(),
            },
            e,
        )),
    }
}
