// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::AsyncGroup;

use std::collections::HashMap;
use std::{fmt, thread};

#[derive(Debug)]
pub enum AsyncGroupError {
    ThreadPanicked(String),
}

impl AsyncGroup<'_> {
    pub(crate) fn new() -> Self {
        Self {
            named_handlers: Vec::new(),
            name: "",
        }
    }

    pub fn add<F>(&mut self, f: F)
    where
        F: FnOnce() -> errs::Result<()> + Send + Sync + 'static,
    {
        self.named_handlers
            .push((self.name.to_string(), thread::spawn(f)));
    }

    pub(crate) fn join_and_collect_errors(&mut self, err_map: &mut HashMap<String, errs::Err>) {
        if self.named_handlers.is_empty() {
            return;
        }

        let vec = std::mem::take(&mut self.named_handlers);

        for h in vec.into_iter() {
            match h.1.join() {
                Ok(r) => {
                    if let Err(e) = r {
                        err_map.insert(h.0, e);
                    }
                }
                Err(e) => {
                    let s = match e.downcast_ref::<Box<dyn fmt::Debug>>() {
                        Some(d) => format!("{d:?}"),
                        None => "Thread panicked!".to_string(),
                    };
                    err_map.insert(h.0, errs::Err::new(AsyncGroupError::ThreadPanicked(s)));
                }
            }
        }
    }

    pub(crate) fn join_and_ignore_errors(&mut self) {
        if self.named_handlers.is_empty() {
            return;
        }

        let vec = std::mem::take(&mut self.named_handlers);

        for h in vec.into_iter() {
            let _ = h.1.join();
        }
    }
}

#[cfg(test)]
mod tests_of_async_group {
    use super::*;
    use std::sync;
    use std::time;

    const BASE_LINE: u32 = line!();

    #[derive(Debug, PartialEq)]
    enum Reasons {
        BadFlag(bool),
        BadString(String),
        BadNumber(i64),
    }

    struct StructA {
        flag: sync::Arc<sync::Mutex<bool>>,
        will_fail: bool,
    }

    impl StructA {
        fn new(will_fail: bool) -> Self {
            Self {
                flag: sync::Arc::new(sync::Mutex::new(false)),
                will_fail,
            }
        }

        fn process(&self, ag: &mut AsyncGroup) {
            let flag_mutex = self.flag.clone();
            let will_fail = self.will_fail;
            ag.add(move || {
                let _ = thread::sleep(time::Duration::from_millis(100));
                {
                    let mut flag = flag_mutex.lock().unwrap();
                    if will_fail {
                        return Err(errs::Err::new(Reasons::BadFlag(*flag)));
                    }
                    *flag = true;
                }
                Ok(())
            });
        }
    }

    struct StructB {
        string: sync::Arc<sync::Mutex<String>>,
        will_fail: bool,
    }

    impl StructB {
        fn new(will_fail: bool) -> Self {
            Self {
                string: sync::Arc::new(sync::Mutex::new("-".to_string())),
                will_fail,
            }
        }

        fn process(&self, ag: &mut AsyncGroup) {
            let s_mutex = self.string.clone();
            let will_fail = self.will_fail;
            ag.add(move || {
                let _ = thread::sleep(time::Duration::from_millis(100));
                {
                    let mut s = s_mutex.lock().unwrap();
                    if will_fail {
                        return Err(errs::Err::new(Reasons::BadString(s.to_string())));
                    }
                    *s = "hello".to_string();
                }
                Ok(())
            });
        }
    }

    struct StructC {
        number: sync::Arc<sync::Mutex<i64>>,
        will_fail: bool,
    }

    impl StructC {
        fn new(will_fail: bool) -> Self {
            Self {
                number: sync::Arc::new(sync::Mutex::new(123)),
                will_fail,
            }
        }

        fn process(&self, ag: &mut AsyncGroup) {
            let n_mutex = self.number.clone();
            let will_fail = self.will_fail;
            ag.add(move || {
                let _ = thread::sleep(time::Duration::from_millis(100));
                {
                    let mut n = n_mutex.lock().unwrap();
                    if will_fail {
                        return Err(errs::Err::new(Reasons::BadNumber(*n)));
                    }
                    *n = 987
                }
                Ok(())
            });
        }
    }

    mod tests_of_join_and_collect_errors {
        use super::*;

        #[test]
        fn zero() {
            let mut hm = HashMap::new();

            let mut ag = AsyncGroup::new();

            ag.join_and_collect_errors(&mut hm);
            assert_eq!(hm.len(), 0);
        }

        #[test]
        fn single_ok() {
            let mut ag = AsyncGroup::new();
            let mut hm = HashMap::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.join_and_collect_errors(&mut hm);
            assert_eq!(hm.len(), 0);
            assert_eq!(*struct_a.flag.lock().unwrap(), true);
        }

        #[test]
        fn single_fail() {
            let mut ag = AsyncGroup::new();
            let mut hm = HashMap::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.join_and_collect_errors(&mut hm);
            assert_eq!(hm.len(), 1);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(hm.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }"
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(hm.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }"
            );
        }

        #[test]
        fn multiple_ok() {
            let mut ag = AsyncGroup::new();
            let mut hm = HashMap::<String, errs::Err>::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(false);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_collect_errors(&mut hm);
            assert_eq!(hm.len(), 0);
            assert_eq!(*struct_a.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "hello".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[test]
        fn multiple_processes_and_single_fail() {
            let mut ag = AsyncGroup::new();
            let mut hm = HashMap::<String, errs::Err>::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(false);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_collect_errors(&mut hm);
            assert_eq!(hm.len(), 1);

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(hm.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 60).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(hm.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 60).to_string() + " }",
            );

            assert_eq!(*struct_a.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[test]
        fn multiple_fail() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, errs::Err>::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(true);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_collect_errors(&mut m);
            assert_eq!(m.len(), 3);

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }",
            );
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 60).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 60).to_string() + " }",
            );
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("baz").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadNumber(123), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 90).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("baz").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadNumber(123), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 90).to_string() + " }",
            );

            assert_eq!(*struct_a.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 123);
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

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.join_and_ignore_errors();
            assert_eq!(*struct_a.flag.lock().unwrap(), true);
        }

        #[test]
        fn single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.join_and_ignore_errors();
            assert_eq!(*struct_a.flag.lock().unwrap(), false);
        }

        #[test]
        fn multiple_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(false);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors();

            assert_eq!(*struct_a.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "hello".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[test]
        fn multiple_processes_and_single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(false);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors();

            assert_eq!(*struct_a.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[test]
        fn multiple_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(true);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors();

            assert_eq!(*struct_a.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 123);
        }
    }
}
