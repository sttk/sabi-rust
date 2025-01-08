// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::collections::HashMap;

use crate::Err;

/// The enum type for errors by `AsyncGroup`.
///
/// The variants of this enum indicates the possible errors that may occur with `AsyncGroup`.
#[derive(Debug)]
pub enum AsyncGroup {
    /// The enum variant which indicates that at least one thread to setup a `DaxSrc` has
    /// panicked.
    ThreadPanicked {
        /// The message which is the argument of `panic!`.
        message: String,
    },
}

/// The enum type for errors by `DaxSrc`.
///
/// The variants of this enum indicates the possible errors that may occur with `DaxSrc`
#[derive(Debug)]
pub enum DaxSrc {
    /// The error reason which indicates that some `DaxSrc`(s) failed to set up.
    FailToSetupGlobal {
        /// The map of which keys are the registered names of `DaxSrc`(s) that failed, and of which
        /// values are `Err` having their error reasons.
        errors: HashMap<String, Err>,
    },
}

/// The enum type for errors by `DaxConn`.
///
/// The variants of this enum indicates the possible errors that may occur with `DaxConn`
#[derive(Debug)]
pub enum DaxConn {
    /// The error reason which indicates that some `DaxConn`(s) failed to commit.
    FailToCommit {
        /// The map of which keys are the registered names of `DaxConn`(s) that failed, and of
        /// which values are `Err`(s) having their error reasons.
        errors: HashMap<String, Err>,
    },
}

#[derive(Debug)]
pub enum DaxBase {
    FailToCastDaxConn { name: String, to_type: &'static str },
    FailToGetDaxConn { name: String, to_type: &'static str },
}
