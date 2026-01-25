// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::AsyncGroup;

use std::sync::Arc;
use std::{fmt, mem, thread};

/// The enum type representing the reasons for errors that can occur within an [`AsyncGroup`].
#[derive(Debug)]
pub enum AsyncGroupError {
    /// Indicates that a spawned thread by [`AsyncGroup`] has panicked.
    /// Contains the panic message if available.
    ThreadPanicked(String),
}

impl AsyncGroup {
    pub(crate) fn new() -> Self {
        Self {
            handlers: Vec::new(),
            _name: "".into(),
        }
    }

    /// Adds a task (a closure) to the group to be executed concurrently.
    ///
    /// This provided closure is executed in a new `std::thread` concurrently
    /// with other added tasks.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the closure, which must be
    ///   `FnOnce() -> errs::Result<()> + Send + 'static`.
    ///
    /// # Parameters
    ///
    /// * `f`: The closure to be executed in a separate thread.
    pub fn add<F>(&mut self, f: F)
    where
        F: FnOnce() -> errs::Result<()> + Send + 'static,
    {
        self.handlers.push((self._name.clone(), thread::spawn(f)));
    }

    pub(crate) fn join_and_collect_errors(&mut self, errors: &mut Vec<(Arc<str>, errs::Err)>) {
        if self.handlers.is_empty() {
            return;
        }

        let vec = mem::take(&mut self.handlers);

        for h in vec.into_iter() {
            match h.1.join() {
                Ok(r) => {
                    if let Err(e) = r {
                        errors.push((h.0.clone(), e));
                    }
                }
                Err(e) => {
                    let s = match e.downcast_ref::<Box<dyn fmt::Debug>>() {
                        Some(d) => format!("{d:?}"),
                        None => "".to_string(),
                    };
                    let e = errs::Err::new(AsyncGroupError::ThreadPanicked(s));
                    errors.push((h.0.clone(), e));
                }
            }
        }
    }

    pub(crate) fn join_and_ignore_errors(&mut self) {
        if self.handlers.is_empty() {
            return;
        }

        let vec = mem::take(&mut self.handlers);

        for h in vec.into_iter() {
            let _ = h.1.join();
        }
    }
}

#[cfg(test)]
mod tests_of_async_group {
    use super::*;
    use std::{sync, time};

    const BASE_LINE: u32 = line!();

    #[derive(Debug, PartialEq)]
    enum Reasons {
        BadString(String),
    }

    struct MyStruct {
        string: sync::Arc<sync::Mutex<String>>,
        fail: bool,
    }

    impl MyStruct {
        fn new(s: String, fail: bool) -> Self {
            Self {
                string: sync::Arc::new(sync::Mutex::new(s)),
                fail,
            }
        }

        fn process(&self, ag: &mut AsyncGroup) {
            let s_mutex = self.string.clone();
            let fail = self.fail;
            ag.add(move || {
                let _ = thread::sleep(time::Duration::from_millis(100));
                {
                    let mut s = s_mutex.lock().unwrap();
                    if fail {
                        return Err(errs::Err::new(Reasons::BadString(s.to_string())));
                    }
                    *s = s.to_uppercase();
                }
                Ok(())
            });
        }

        fn process_multiple(&self, ag: &mut AsyncGroup) {
            let s_mutex = self.string.clone();
            let fail = self.fail;
            ag.add(move || {
                let _ = thread::sleep(time::Duration::from_millis(100));
                {
                    let mut s = s_mutex.lock().unwrap();
                    if fail {
                        return Err(errs::Err::new(Reasons::BadString(s.to_string())));
                    }
                    *s = s.to_uppercase();
                }
                Ok(())
            });

            let s_mutex = self.string.clone();
            let fail = self.fail;
            ag.add(move || {
                let _ = thread::sleep(time::Duration::from_millis(100));
                {
                    let mut s = s_mutex.lock().unwrap();
                    if fail {
                        return Err(errs::Err::new(Reasons::BadString(s.to_string())));
                    }
                    *s = s.to_uppercase();
                }
                Ok(())
            });
        }
    }

    mod tests_of_join_and_collect_errors {
        use super::*;

        #[test]
        fn zero() {
            let mut ag = AsyncGroup::new();

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors(&mut err_vec);

            assert!(err_vec.is_empty());
        }

        #[test]
        fn single_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().unwrap(), "a");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            let mut errors = Vec::new();
            ag.join_and_collect_errors(&mut errors);

            assert!(errors.is_empty());
            assert_eq!(*struct_a.string.lock().unwrap(), "A");
        }

        #[test]
        fn single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), true);
            assert_eq!(*struct_a.string.lock().unwrap(), "a");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            let mut errors = Vec::new();
            ag.join_and_collect_errors(&mut errors);

            assert_eq!(errors.len(), 1);
            assert_eq!(*struct_a.string.lock().unwrap(), "a");

            assert_eq!(errors[0].0, "foo".into());
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", errors[0].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"a\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 28).to_string() + " }"
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", errors[0].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"a\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 28).to_string() + " }"
            );
        }

        #[test]
        fn multiple_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().unwrap(), "a".to_string());

            let struct_b = MyStruct::new("b".to_string(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "b".to_string());

            let struct_c = MyStruct::new("c".to_string(), false);
            assert_eq!(*struct_c.string.lock().unwrap(), "c".to_string());

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors(&mut err_vec);

            assert_eq!(err_vec.len(), 0);

            assert_eq!(*struct_a.string.lock().unwrap(), "A");
            assert_eq!(*struct_b.string.lock().unwrap(), "B");
            assert_eq!(*struct_c.string.lock().unwrap(), "C");
        }

        #[test]
        fn multiple_processes_and_single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().unwrap(), "a");

            let struct_b = MyStruct::new("b".to_string(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "b");

            let struct_c = MyStruct::new("c".to_string(), false);
            assert_eq!(*struct_c.string.lock().unwrap(), "c");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors(&mut err_vec);

            assert_eq!(err_vec.len(), 1);
            assert_eq!(err_vec[0].0, "bar".into());
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"b\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 28).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"b\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 28).to_string() + " }",
            );

            assert_eq!(*struct_a.string.lock().unwrap(), "A");
            assert_eq!(*struct_b.string.lock().unwrap(), "b");
            assert_eq!(*struct_c.string.lock().unwrap(), "C");
        }

        #[test]
        fn multiple_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), true);
            assert_eq!(*struct_a.string.lock().unwrap(), "a");

            let struct_b = MyStruct::new("b".to_string(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "b");

            let struct_c = MyStruct::new("c".to_string(), true);
            assert_eq!(*struct_c.string.lock().unwrap(), "c");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors(&mut err_vec);

            assert_eq!(err_vec.len(), 3);

            assert_eq!(err_vec[0].0, "foo".into());
            assert_eq!(err_vec[1].0, "bar".into());
            assert_eq!(err_vec[2].0, "baz".into());

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"a\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 28).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"a\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 28).to_string() + " }",
            );
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[1].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"b\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 28).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[1].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"b\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 28).to_string() + " }",
            );
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[2].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"c\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 28).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[2].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"c\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 28).to_string() + " }",
            );

            assert_eq!(*struct_a.string.lock().unwrap(), "a");
            assert_eq!(*struct_b.string.lock().unwrap(), "b");
            assert_eq!(*struct_c.string.lock().unwrap(), "c");
        }

        #[test]
        fn data_src_execute_multiple_setup_handles() {
            let mut ag = AsyncGroup::new();

            let struct_d = MyStruct::new("d".to_string(), false);
            assert_eq!(*struct_d.string.lock().unwrap(), "d");

            ag._name = "foo".into();
            struct_d.process(&mut ag);

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors(&mut err_vec);

            assert_eq!(err_vec.len(), 0);

            assert_eq!(*struct_d.string.lock().unwrap(), "D");
        }

        #[test]
        fn collect_all_errors_if_data_src_executes_multiple_setup_handles() {
            let mut ag = AsyncGroup::new();

            let struct_d = MyStruct::new("d".to_string(), true);
            assert_eq!(*struct_d.string.lock().unwrap(), "d");

            ag._name = "foo".into();
            struct_d.process_multiple(&mut ag);

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors(&mut err_vec);

            assert_eq!(err_vec.len(), 2);

            assert_eq!(err_vec[0].0, "foo".into());
            assert_eq!(err_vec[1].0, "foo".into());

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"d\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 44).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"d\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 44).to_string() + " }"
            );

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[1].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"d\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 58).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[1].1),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"d\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 58).to_string() + " }"
            );

            assert_eq!(*struct_d.string.lock().unwrap(), "d");
        }
    }

    mod tests_of_join_and_ignore_errors {
        use super::*;

        #[test]
        fn zero() {
            let mut ag = AsyncGroup::new();

            ag.join_and_ignore_errors();
        }

        #[test]
        fn single_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().unwrap(), "a");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag.join_and_ignore_errors();
            assert_eq!(*struct_a.string.lock().unwrap(), "A");
        }

        #[test]
        fn single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), true);
            assert_eq!(*struct_a.string.lock().unwrap(), "a");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag.join_and_ignore_errors();
            assert_eq!(*struct_a.string.lock().unwrap(), "a");
        }

        #[test]
        fn multiple_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().unwrap(), "a");

            let struct_b = MyStruct::new("b".to_string(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "b");

            let struct_c = MyStruct::new("c".to_string(), false);
            assert_eq!(*struct_c.string.lock().unwrap(), "c");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors();

            assert_eq!(*struct_a.string.lock().unwrap(), "A");
            assert_eq!(*struct_b.string.lock().unwrap(), "B");
            assert_eq!(*struct_c.string.lock().unwrap(), "C");
        }

        #[test]
        fn multiple_processes_and_single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().unwrap(), "a");

            let struct_b = MyStruct::new("b".to_string(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "b");

            let struct_c = MyStruct::new("c".to_string(), false);
            assert_eq!(*struct_c.string.lock().unwrap(), "c");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors();

            assert_eq!(*struct_a.string.lock().unwrap(), "A");
            assert_eq!(*struct_b.string.lock().unwrap(), "b");
            assert_eq!(*struct_c.string.lock().unwrap(), "C");
        }

        #[test]
        fn multiple_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), true);
            assert_eq!(*struct_a.string.lock().unwrap(), "a");

            let struct_b = MyStruct::new("b".to_string(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "b");

            let struct_c = MyStruct::new("c".to_string(), true);
            assert_eq!(*struct_c.string.lock().unwrap(), "c");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors();

            assert_eq!(*struct_a.string.lock().unwrap(), "a");
            assert_eq!(*struct_b.string.lock().unwrap(), "b");
            assert_eq!(*struct_c.string.lock().unwrap(), "c");
        }
    }
}
