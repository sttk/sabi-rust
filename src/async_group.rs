// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::collections::HashMap;
use std::thread;

use crate::errors;
use crate::Err;

/// Manages multiple asynchronous operations and wait for all of them to complete.
///
/// If an error occurs during any of these operations, it collects and stores the errors for lator
/// retrieval.
pub struct AsyncGroup<'a> {
    join_handles: Vec<thread::JoinHandle<Result<(), Err>>>,
    names: Vec<String>,
    pub(crate) name: &'a str,
}

impl AsyncGroup<'_> {
    /// Creates a new `AsyncGroup`.
    pub(crate) fn new() -> Self {
        Self {
            join_handles: Vec::new(),
            names: Vec::new(),
            name: "",
        }
    }

    /// Adds a new asynchronous operation to this instance.
    ///
    /// The operation is provided as a function that returns an `Err`.
    /// If the function encounters an error, it is recorded internally.
    ///
    /// # Parameters
    /// - `func`: A function that will be executed in a separate thread. It must return a
    ///   `Result<(), Err>`, where `Err` indicates a failure.
    pub fn add(&mut self, func: fn() -> Result<(), Err>) {
        let handle = thread::spawn(move || match func() {
            Ok(()) => Ok(()),
            Err(err) => Err(err),
        });
        self.join_handles.push(handle);
        self.names.push(self.name.to_string());
    }

    pub(crate) fn join(&mut self) -> HashMap<String, Err> {
        let mut err_map = HashMap::<String, Err>::new();

        while self.join_handles.len() > 0 {
            let name = self.names.remove(0);
            match self.join_handles.remove(0).join() {
                Ok(r) => {
                    if let Err(e) = r {
                        err_map.insert(name, e);
                    }
                }
                Err(err) => {
                    let msg = match err.downcast_ref::<&'static str>() {
                        Some(s) => *s,
                        None => match err.downcast_ref::<String>() {
                            Some(s) => &s[..],
                            None => "Thread panicked!",
                        },
                    };
                    err_map.insert(
                        name,
                        Err::new(errors::AsyncGroup::ThreadPanicked {
                            message: msg.to_string(),
                        }),
                    );
                }
            }
        }

        err_map
    }
}

#[cfg(test)]
mod tests_async_group {
    use super::*;
    use std::thread::*;
    use std::time;

    #[test]
    fn zero() {
        let mut ag = AsyncGroup::new();

        let m = ag.join();
        assert_eq!(m.len(), 0);
    }

    #[test]
    fn ok() {
        let mut ag = AsyncGroup::new();

        ag.name = "foo";
        ag.add(|| Ok(()));
        let m = ag.join();
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

        ag.name = "foo";
        ag.add(|| {
            thread::sleep(time::Duration::from_millis(50));
            Err(Err::new(Reasons::BadNumber(123u32)))
        });

        let m = ag.join();
        assert_eq!(m.len(), 1);
        assert_eq!(
            format!("{:?}", *(m.get("foo").unwrap())),
            "sabi::Err { reason = sabi::async_group::tests_async_group::Reasons BadNumber(123) }"
        );
    }

    #[test]
    fn multiple_errors() {
        let mut ag = AsyncGroup::new();

        ag.name = "foo0";
        ag.add(|| {
            thread::sleep(time::Duration::from_millis(200));
            Err(Err::new(Reasons::BadNumber(123u32)))
        });
        ag.name = "foo1";
        ag.add(|| {
            thread::sleep(time::Duration::from_millis(400));
            Err(Err::new(Reasons::BadString("hello".to_string())))
        });
        ag.name = "foo2";
        ag.add(|| {
            thread::sleep(time::Duration::from_millis(800));
            Err(Err::new(Reasons::BadFlag(true)))
        });

        let m = ag.join();
        assert_eq!(m.len(), 3);
        assert_eq!(
            format!("{:?}", *(m.get("foo0").unwrap())),
            "sabi::Err { reason = sabi::async_group::tests_async_group::Reasons BadNumber(123) }"
        );
        assert_eq!(
            format!("{:?}", *(m.get("foo1").unwrap())),
            "sabi::Err { reason = sabi::async_group::tests_async_group::Reasons BadString(\"hello\") }"
        );
        assert_eq!(
            format!("{:?}", *(m.get("foo2").unwrap())),
            "sabi::Err { reason = sabi::async_group::tests_async_group::Reasons BadFlag(true) }"
        );
    }
}
