// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::collections::HashMap;
use std::thread;

use crate::errors;

use errs::Err;

/// Executes added functions asynchronously.
///
/// This trait is used as an argument of DaxSrc#setup, DaxConn#commit, DacConn#rollback, and
/// DaxConn#forceback.
pub trait AsyncGroup {
    /// Adds and starts to run a target function.
    fn add(&mut self, func: fn() -> Result<(), Err>);
}

pub(crate) struct AsyncGroupAsync<'a> {
    join_handles: Vec<thread::JoinHandle<Result<(), Err>>>,
    names: Vec<String>,
    pub(crate) name: &'a str,
}

impl AsyncGroup for AsyncGroupAsync<'_> {
    fn add(&mut self, func: fn() -> Result<(), Err>) {
        let handle = thread::spawn(move || match func() {
            Ok(()) => Ok(()),
            Err(err) => Err(err),
        });
        self.join_handles.push(handle);
        self.names.push(self.name.to_string());
    }
}

impl AsyncGroupAsync<'_> {
    pub(crate) fn new() -> Self {
        Self {
            join_handles: Vec::new(),
            names: Vec::new(),
            name: "",
        }
    }

    pub(crate) fn wait(&mut self, err_map: &mut HashMap<String, Err>) {
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
    }
}

pub(crate) struct AsyncGroupSync {
    pub(crate) err: Option<Err>,
}

impl AsyncGroup for AsyncGroupSync {
    fn add(&mut self, func: fn() -> Result<(), Err>) {
        match func() {
            Ok(_) => {}
            Err(err) => {
                self.err = Some(err);
            }
        }
    }
}

impl AsyncGroupSync {
    pub(crate) fn new() -> Self {
        Self { err: None }
    }
}

#[cfg(test)]
mod tests_async_group {
    use super::*;

    mod tests_async_group_async {
        use super::*;
        use std::thread;
        use std::time;

        #[test]
        fn when_zero_function() {
            let mut ag = AsyncGroupAsync::new();
            let mut hm = HashMap::new();
            ag.wait(&mut hm);
            assert_eq!(hm.len(), 0);
        }

        #[test]
        fn when_one_function() {
            let mut ag = AsyncGroupAsync::new();
            ag.name = "foo";
            ag.add(|| Ok(()));
            let mut hm = HashMap::new();
            ag.wait(&mut hm);
            assert_eq!(hm.len(), 0);
        }

        #[test]
        fn when_two_function() {
            let mut ag = AsyncGroupAsync::new();
            ag.name = "foo";
            ag.add(|| {
                thread::sleep(time::Duration::from_millis(20));
                Ok(())
            });
            ag.name = "bar";
            ag.add(|| {
                thread::sleep(time::Duration::from_millis(10));
                Ok(())
            });
            let mut hm = HashMap::new();
            ag.wait(&mut hm);
            assert_eq!(hm.len(), 0);
        }

        #[derive(Debug, PartialEq)]
        enum Reasons {
            BadNumber(u32),
            BadString(String),
        }

        #[test]
        fn when_one_function_and_error() {
            let mut ag = AsyncGroupAsync::new();
            ag.name = "foo";
            ag.add(|| Err(Err::new(Reasons::BadNumber(123u32))));
            let mut hm = HashMap::new();
            ag.wait(&mut hm);
            assert_eq!(hm.len(), 1);
            assert_eq!(
                *(hm.get("foo").unwrap().reason::<Reasons>().unwrap()),
                Reasons::BadNumber(123u32)
            );
        }

        #[test]
        fn when_two_function_and_error() {
            let mut ag = AsyncGroupAsync::new();
            ag.name = "foo";
            ag.add(|| {
                thread::sleep(time::Duration::from_millis(20));
                Err(Err::new(Reasons::BadNumber(123u32)))
            });
            ag.name = "bar";
            ag.add(|| {
                thread::sleep(time::Duration::from_millis(10));
                Err(Err::new(Reasons::BadString("hello".to_string())))
            });
            let mut hm = HashMap::new();
            ag.wait(&mut hm);
            assert_eq!(hm.len(), 2);
            assert_eq!(
                *(hm.get("foo").unwrap().reason::<Reasons>().unwrap()),
                Reasons::BadNumber(123u32)
            );
            assert_eq!(
                *(hm.get("bar").unwrap().reason::<Reasons>().unwrap()),
                Reasons::BadString("hello".to_string())
            );
        }

        #[test]
        fn when_three_function_and_panicked() {
            let mut ag = AsyncGroupAsync::new();
            ag.name = "foo";
            ag.add(|| {
                thread::sleep(time::Duration::from_millis(20));
                panic!("panic 1");
            });
            let mut hm = HashMap::new();
            ag.wait(&mut hm);
            assert_eq!(hm.len(), 1);

            match hm
                .get("foo")
                .unwrap()
                .reason::<errors::AsyncGroup>()
                .unwrap()
            {
                errors::AsyncGroup::ThreadPanicked { message } => assert_eq!(message, "panic 1"),
            }
        }
    }

    mod tests_async_group_sync {
        use super::*;

        #[test]
        fn when_zero_function() {
            let ag = AsyncGroupSync::new();
            assert!(ag.err.is_none());
        }

        #[test]
        fn when_one_function() {
            let mut ag = AsyncGroupSync::new();
            ag.add(|| Ok(()));
            assert!(ag.err.is_none());
        }

        #[test]
        fn when_two_function() {
            let mut ag = AsyncGroupSync::new();
            ag.add(|| Ok(()));

            fn func() -> Result<(), Err> {
                Ok(())
            }
            ag.add(func);
            assert!(ag.err.is_none());
        }

        #[test]
        fn when_one_function_and_error() {
            let mut ag = AsyncGroupSync::new();
            ag.add(|| Err(Err::new("async error.".to_string())));
            assert!(ag.err.is_some());

            match ag.err.unwrap().reason::<String>() {
                Ok(s) => assert_eq!(s, "async error."),
                Err(_) => panic!(),
            }
        }

        #[test]
        fn when_two_functions_and_error() {
            let mut ag = AsyncGroupSync::new();
            ag.add(|| Ok(()));
            assert!(ag.err.is_none());

            #[derive(Debug)]
            enum Reasons {
                BadNumber(u32),
            }

            fn fail() -> Result<(), Err> {
                Err(Err::new(Reasons::BadNumber(123)))
            }
            ag.add(fail);
            assert!(ag.err.is_some());

            match ag.err.unwrap().reason::<Reasons>() {
                Ok(r) => match r {
                    Reasons::BadNumber(n) => assert_eq!(*n, 123u32),
                },
                Err(_) => panic!(),
            }
        }
    }
}
