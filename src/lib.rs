// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(unused_features)]

mod async_group;

pub use async_group::AsyncGroupError;

use std::sync::Arc;
use std::thread;

#[derive(Debug)]
pub struct ErrEntry {
    pub index: usize,
    pub name: Arc<str>,
    pub err: errs::Err,
}

pub struct AsyncGroup {
    handlers: Vec<(usize, Arc<str>, thread::JoinHandle<errs::Result<()>>)>,
    index: usize,
    name: Arc<str>,
}
