// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use futures::future;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use tokio::runtime;

use errs::Err;

/// The enum type represents errors that can occur during asynchronous operations managed by an
/// `AsyncGroup`.
#[derive(Debug)]
pub enum AsyncGroupError {
    /// Indicates that it is failed to create an asynchronous runtime.
    FailToCreateAsyncRuntime,
}

/// Manages multiple asynchronous operations and wait for all of them to complete.
///
/// If an error occurs during any of these operations, it collects and stores the errors for lator
/// retrieval.
pub struct AsyncGroup<'a> {
    func_vec: VecDeque<Box<dyn Fn() -> future::BoxFuture<'static, Result<(), Err>>>>,
    name_vec: VecDeque<String>,
    pub(crate) name: &'a str,
}

impl<'a> AsyncGroup<'_> {
    /// Creates a new `AsyncGroup`.
    pub(crate) fn new() -> Self {
        Self {
            func_vec: VecDeque::new(),
            name_vec: VecDeque::new(),
            name: "",
        }
    }

    /// Adds a new asynchronous operation to this instance.
    ///
    /// The operation is provided as a function that returns an `Err`.
    /// If the function encounters an error, it is recorded internally.
    ///
    /// # Parameters
    /// - `func`: An async function that will be executed in a separate thread. It must return a
    ///   `Result<(), Err>`, where `Err` indicates a failure.
    pub fn add<F, Fut>(&mut self, func_ptr: F)
    where
        F: Fn() -> Fut + 'static,
        Fut: Future<Output = Result<(), Err>> + Send + Sync + 'static,
    {
        self.func_vec
            .push_back(Box::new(move || Box::pin(func_ptr())));
        self.name_vec.push_back(self.name.to_string());
    }

    pub(crate) fn join(&mut self, err_map: &mut HashMap<String, Err>) {
        let mut fut_vec = Vec::new();
        while let Some(func) = self.func_vec.pop_front() {
            fut_vec.push(func());
        }

        match runtime::Runtime::new() {
            Ok(rt) => {
                rt.block_on(async {
                    let result_all = future::join_all(fut_vec).await;
                    for result in result_all.into_iter() {
                        if let Some(name) = self.name_vec.pop_front() {
                            if let Err(err) = result {
                                err_map.insert(name, err);
                            }
                        }
                    }
                });
            }
            Err(err) => {
                err_map.insert(
                    "sabi::AsyncGroup".to_string(),
                    Err::with_source(AsyncGroupError::FailToCreateAsyncRuntime, err),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests_async_group {
    use super::*;
    use std::time::Duration;
    use tokio::time;

    #[test]
    fn zero() {
        let mut ag = AsyncGroup::new();
        let mut m = HashMap::<String, Err>::new();

        ag.join(&mut m);
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn ok() {
        let mut ag = AsyncGroup::new();
        let mut m = HashMap::<String, Err>::new();

        ag.name = "foo";
        ag.add(async || Ok(()));
        ag.join(&mut m);
        assert_eq!(m.len(), 0);
    }

    #[derive(Debug, PartialEq)]
    enum Reasons {
        BadNumber(u32),
        BadString(String),
        BadFlag(bool),
    }

    #[test]
    fn error() {
        let mut ag = AsyncGroup::new();
        let mut m = HashMap::<String, Err>::new();

        ag.name = "foo";
        ag.add(async || {
            let _ = time::sleep(Duration::from_millis(50)).await;
            Err(Err::new(Reasons::BadNumber(123u32)))
        });

        ag.join(&mut m);
        assert_eq!(m.len(), 1);
        #[cfg(unix)]
        assert_eq!(
            format!("{:?}", *(m.get("foo").unwrap())),
            "errs::Err { reason = sabi::async_group::tests_async_group::Reasons BadNumber(123), file = src/async_group.rs, line = 128 }"
        );
        #[cfg(windows)]
        assert_eq!(
            format!("{:?}", *(m.get("foo").unwrap())),
            "errs::Err { reason = sabi::async_group::tests_async_group::Reasons BadNumber(123), file = src\\async_group.rs, line = 128 }"
        );
    }

    #[test]
    fn multiple_errors() {
        let mut ag = AsyncGroup::new();
        let mut m = HashMap::<String, Err>::new();

        ag.name = "foo0";
        ag.add(async || {
            let _ = time::sleep(Duration::from_millis(800)).await;
            Err(Err::new(Reasons::BadNumber(123u32)))
        });
        ag.name = "foo1";
        ag.add(async || {
            let _ = time::sleep(Duration::from_millis(200)).await;
            Err(Err::new(Reasons::BadString("hello".to_string())))
        });
        ag.name = "foo2";
        ag.add(async || {
            let _ = time::sleep(Duration::from_millis(400)).await;
            Err(Err::new(Reasons::BadFlag(true)))
        });

        ag.join(&mut m);
        assert_eq!(m.len(), 3);

        #[cfg(unix)]
        assert_eq!(
            format!("{:?}", *(m.get("foo0").unwrap())),
            "errs::Err { reason = sabi::async_group::tests_async_group::Reasons BadNumber(123), file = src/async_group.rs, line = 153 }"
        );
        #[cfg(window)]
        assert_eq!(
            format!("{:?}", *(m.get("foo0").unwrap())),
            "errs::Err { reason = sabi::async_group::tests_async_group::Reasons BadNumber(123), file = src\\async_group.rs, line = 153 }"
        );

        #[cfg(unix)]
        assert_eq!(
            format!("{:?}", *(m.get("foo1").unwrap())),
            "errs::Err { reason = sabi::async_group::tests_async_group::Reasons BadString(\"hello\"), file = src/async_group.rs, line = 157 }"
        );
        #[cfg(windows)]
        assert_eq!(
            format!("{:?}", *(m.get("foo1").unwrap())),
            "errs::Err { reason = sabi::async_group::tests_async_group::Reasons BadString(\"hello\"), file = src\\async_group.rs, line = 157 }"
        );

        #[cfg(unix)]
        assert_eq!(
            format!("{:?}", *(m.get("foo2").unwrap())),
            "errs::Err { reason = sabi::async_group::tests_async_group::Reasons BadFlag(true), file = src/async_group.rs, line = 162 }"
        );
        #[cfg(windows)]
        assert_eq!(
            format!("{:?}", *(m.get("foo2").unwrap())),
            "errs::Err { reason = sabi::async_group::tests_async_group::Reasons BadFlag(true), file = src\\async_group.rs, line = 162 }"
        );
    }
}
