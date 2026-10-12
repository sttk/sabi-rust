// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::DataSrcError;

use crate::{
    AutoShutdown, DataConn, DataConnContainer, DataSrc, DataSrcContainer, DataSrcManager,
    SendSyncNonNull, StaticDataSrcContainer, StaticDataSrcRegistration,
};

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

/// Registers a data source into the global registry.
///
/// This registers a [`DataSrc`] with a unique name so that it can later be initialized via
/// [`setup`] or [`setup_with_order`], and accessed globally by [`DataHub`](crate::DataHub).
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
pub fn uses<S, C>(name: impl Into<Arc<str>>, ds: S) -> errs::Result<()>
where
    S: DataSrc<C>,
    C: DataConn + 'static,
{
    match DS_MANAGER.get_mut_unlocked() {
        Ok(dsm) => {
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

/// Initializes all globally registered data sources concurrently.
///
/// This transitions the global data source registry to the active read phase and runs the
/// setup logic for all registered data sources concurrently using
/// [`AsyncGroup`](crate::AsyncGroup).
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
/// * Any data source fails during its [`setup`](crate::DataSrc::setup) execution.
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

/// Initializes all globally registered data sources, executing setup in the specified order.
///
/// Data sources whose names are present in `names` are set up in the specified sequence. Any
/// registered data sources not listed in `names` are set up subsequently.
///
/// # Parameters
///
/// * `names`: A slice of data source names defining the desired order of setup.
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
/// * Any data source fails during its [`setup`](crate::DataSrc::setup) execution.
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

/// Creates a container for a statically registered data source.
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

/// Registers a global data source statically at compile/link time.
///
/// This macro registers a static data source factory that is collected and initialized during
/// [`setup`] or [`setup_with_order`].
///
/// # Parameters
///
/// * `$name`: A string literal representing the data source name.
/// * `$data_src`: An expression evaluating to the data source instance.
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
