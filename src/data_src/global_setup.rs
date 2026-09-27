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
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub const fn new(factory: fn() -> StaticDataSrcContainer) -> Self {
        Self { factory }
    }
}
inventory::collect!(StaticDataSrcRegistration);

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
