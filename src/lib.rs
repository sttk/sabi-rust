// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod err;

use std::any;
use std::error;
use std::fmt;
use std::ptr;

/// `Err` is the struct that represents an error used commonly in across the Sabi
/// Framework.
///
/// It encapsulates the reason for the error, which can be any data type.
/// Typically, the reason is an enum variant, which makes it easy to uniquely identify
/// the error kind and location in the source code.
/// In addition, since an enum variant can store additional informations as their fields,
/// it is possible to provide more detailed information about the error.
///
/// The reason for the error can be distinguished with switch statements, and type
/// casting, so it is easy to handle the error in a type-safe manner.
///
/// This struct also contains an optional cause error, which is the error caused the
/// current error. This is useful for chaining errors.
///
/// This struct is implements the `std::errors::Error` trait, so it can be used as an
/// common error type in Rust programs.
pub struct Err {
    reason_container: ptr::NonNull<ReasonContainer>,
    source: Option<Box<dyn error::Error>>,
}

#[derive(Debug)]
struct DummyReason {}

#[repr(C)]
struct ReasonContainer<R = DummyReason>
where
    R: fmt::Debug + Send + Sync + 'static,
{
    is_fn: fn(any::TypeId) -> bool,
    drop_fn: fn(*const ReasonContainer),
    debug_fn: fn(*const ReasonContainer, f: &mut fmt::Formatter<'_>) -> fmt::Result,
    display_fn: fn(*const ReasonContainer, f: &mut fmt::Formatter<'_>) -> fmt::Result,
    reason: R,
}
