// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;

use errs::Err;
use futures::future;
use tokio::runtime;

/// The enum type representing the reasons for errors that can occur within an `AsyncGroup`.
#[derive(Debug)]
pub enum AsyncGroupError {
    /// Indicates a failure to create a new asynchronous runtime, which is necessary
    /// for executing the tasks within the `AsyncGroup`.
    FailToCreateAsyncRuntime,
}

/// The structure that allows for the asynchronous execution of multiple functions
/// and waits for all of them to complete.
///
/// Functions are added using the `add` method and are then run concurrently.
/// The `AsyncGroup` ensures that all tasks finish before proceeding,
/// and can collect any errors that occur.
pub struct AsyncGroup<'a> {
    task_vec: VecDeque<Pin<Box<dyn Future<Output = Result<(), Err>> + Send + 'static>>>,
    name_vec: VecDeque<String>,
    pub(crate) name: &'a str,
}

impl<'a> AsyncGroup<'_> {
    pub(crate) fn new() -> Self {
        Self {
            task_vec: VecDeque::new(),
            name_vec: VecDeque::new(),
            name: "",
        }
    }

    /// Adds an asynchronous function (a future-producing closure) to the group.
    ///
    /// This provided function is executed asynchronously with other added functions,
    /// awaiting completion and collecting errors internally.
    ///
    /// # Type Parameters
    ///
    /// * `F`: The type of the closure, which must be `FnOnce` and `Send + 'static`.
    /// * `Fut`: The type of the future returned by the closure, which must be
    ///   `Future<Output = Result<(), Err>> + Send + 'static`.
    ///
    /// # Parameters
    ///
    /// * `future_fn`: A closure that, when called, returns a future to be executed.
    pub fn add<F, Fut>(&mut self, future_fn: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), Err>> + Send + 'static,
    {
        self.task_vec.push_back(Box::pin(future_fn()));
        self.name_vec.push_back(self.name.to_string());
    }

    pub(crate) fn join_and_collect_errors(mut self, err_map: &mut HashMap<String, Err>) {
        match runtime::Runtime::new() {
            Ok(rt) => {
                rt.block_on(async {
                    let result_all = future::join_all(self.task_vec).await;
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

    pub(crate) fn join_and_ignore_errors(self) {
        match runtime::Runtime::new() {
            Ok(rt) => {
                rt.block_on(async {
                    let _ = future::join_all(self.task_vec).await;
                });
            }
            Err(err) => {
                let _ = Err::with_source(AsyncGroupError::FailToCreateAsyncRuntime, err);
            }
        }
    }

    pub(crate) async fn join_and_collect_errors_async(
        mut self,
        err_map: &mut HashMap<String, Err>,
    ) {
        let result_all = future::join_all(self.task_vec).await;
        for result in result_all.into_iter() {
            if let Some(name) = self.name_vec.pop_front() {
                if let Err(err) = result {
                    err_map.insert(name, err);
                }
            }
        }
    }

    pub(crate) async fn join_and_ignore_errors_async(self) {
        let _ = future::join_all(self.task_vec).await;
    }
}

#[cfg(test)]
mod tests_of_async_group {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::time;

    const BASE_LINE: u32 = line!();

    #[derive(Debug, PartialEq)]
    enum Reasons {
        BadFlag(bool),
        BadString(String),
        BadNumber(i64),
    }

    struct StructA {
        flag: Arc<Mutex<bool>>,
        will_fail: bool,
    }

    impl StructA {
        fn new(will_fail: bool) -> Self {
            Self {
                flag: Arc::new(Mutex::new(false)),
                will_fail,
            }
        }

        fn process(&self, ag: &mut AsyncGroup) {
            let flag_clone = self.flag.clone();
            let will_fail = self.will_fail;
            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;

                {
                    let mut flag = flag_clone.lock().unwrap();
                    if will_fail {
                        return Err(Err::new(Reasons::BadFlag(*flag)));
                    }
                    *flag = true;
                }

                Ok(())
            });
        }
    }

    struct StructB {
        string: Arc<Mutex<String>>,
        flag: Arc<Mutex<bool>>,
        will_fail: bool,
    }

    impl StructB {
        fn new(will_fail: bool) -> Self {
            Self {
                string: Arc::new(Mutex::new("-".to_string())),
                flag: Arc::new(Mutex::new(false)),
                will_fail,
            }
        }

        fn process(&self, ag: &mut AsyncGroup) {
            let string_clone = self.string.clone();
            let flag_clone = self.flag.clone();
            let will_fail = self.will_fail;
            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(300)).await;

                {
                    let mut string = string_clone.lock().unwrap();
                    let mut flag = flag_clone.lock().unwrap();
                    if will_fail {
                        return Err(Err::new(Reasons::BadString(string.to_string())));
                    }
                    *flag = true;
                    *string = "hello".to_string();
                }
                Ok(())
            });
        }
    }

    struct StructC {
        number: Arc<Mutex<i64>>,
        will_fail: bool,
    }

    impl StructC {
        fn new(will_fail: bool) -> Self {
            Self {
                number: Arc::new(Mutex::new(123)),
                will_fail,
            }
        }

        fn process(&self, ag: &mut AsyncGroup) {
            let number_clone = self.number.clone();
            let will_fail = self.will_fail;
            ag.add(async move || {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(50)).await;

                {
                    let mut number = number_clone.lock().unwrap();
                    if will_fail {
                        return Err(Err::new(Reasons::BadNumber(*number)));
                    }
                    *number = 987;
                }
                Ok(())
            });
        }
    }

    mod test_join_and_collect_errors {
        use super::*;

        #[test]
        fn zero() {
            let ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            ag.join_and_collect_errors(&mut m);
            assert_eq!(m.len(), 0);
        }

        #[test]
        fn single_ok() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.join_and_collect_errors(&mut m);
            assert_eq!(m.len(), 0);
            assert_eq!(*struct_a.flag.lock().unwrap(), true);
        }

        #[test]
        fn single_fail() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.join_and_collect_errors(&mut m);
            assert_eq!(m.len(), 1);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 32).to_string() + " }"
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 32).to_string() + " }"
            );
        }

        #[test]
        fn multiple_ok() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(false);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(false);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_collect_errors(&mut m);
            assert_eq!(m.len(), 0);
            assert_eq!(*struct_a.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "hello".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[test]
        fn multiple_processes_and_single_fail() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(false);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_collect_errors(&mut m);
            assert_eq!(m.len(), 1);

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 69).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 69).to_string() + " }",
            );

            assert_eq!(*struct_a.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[test]
        fn multiple_fail() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
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
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 32).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 32).to_string() + " }",
            );
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 69).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 69).to_string() + " }",
            );
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("baz").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadNumber(123), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 102).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("baz").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadNumber(123), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 102).to_string() + " }",
            );

            assert_eq!(*struct_a.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 123);
        }
    }

    mod test_join_and_ignore_errors {
        use super::*;

        #[test]
        fn zero() {
            let ag = AsyncGroup::new();

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
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
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
            assert_eq!(*struct_b.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "hello".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[test]
        fn multiple_processes_and_single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
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
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[test]
        fn multiple_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
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
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 123);
        }
    }

    mod test_join_and_collect_errors_async {
        use super::*;

        #[tokio::test]
        async fn zero() {
            let ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            ag.join_and_collect_errors_async(&mut m).await;
            assert_eq!(m.len(), 0);
        }

        #[tokio::test]
        async fn single_ok() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.join_and_collect_errors_async(&mut m).await;
            assert_eq!(m.len(), 0);
            assert_eq!(*struct_a.flag.lock().unwrap(), true);
        }

        #[tokio::test]
        async fn single_fail() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.join_and_collect_errors_async(&mut m).await;
            assert_eq!(m.len(), 1);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 32).to_string() + " }"
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 32).to_string() + " }"
            );
        }

        #[tokio::test]
        async fn multiple_ok() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(false);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(false);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_collect_errors_async(&mut m).await;
            assert_eq!(m.len(), 0);
            assert_eq!(*struct_a.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "hello".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[tokio::test]
        async fn multiple_processes_and_single_fail() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(false);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_collect_errors_async(&mut m).await;
            assert_eq!(m.len(), 1);

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 69).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 69).to_string() + " }",
            );

            assert_eq!(*struct_a.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[tokio::test]
        async fn multiple_fail() {
            let mut ag = AsyncGroup::new();
            let mut m = HashMap::<String, Err>::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(true);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_collect_errors_async(&mut m).await;
            assert_eq!(m.len(), 3);

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 32).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("foo").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadFlag(false), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 32).to_string() + " }",
            );
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 69).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("bar").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadString(\"-\"), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 69).to_string() + " }",
            );
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", *(m.get("baz").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadNumber(123), file = src/async_group.rs, line = ".to_string() + &(BASE_LINE + 102).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", *(m.get("baz").unwrap())),
                "errs::Err { reason = sabi::async_group::tests_of_async_group::Reasons BadNumber(123), file = src\\async_group.rs, line = ".to_string() + &(BASE_LINE + 102).to_string() + " }",
            );

            assert_eq!(*struct_a.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 123);
        }
    }

    mod test_join_and_ignore_errors_async {
        use super::*;

        #[tokio::test]
        async fn zero() {
            let ag = AsyncGroup::new();

            ag.join_and_ignore_errors_async().await;
        }

        #[tokio::test]
        async fn single_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.join_and_ignore_errors_async().await;
            assert_eq!(*struct_a.flag.lock().unwrap(), true);
        }

        #[tokio::test]
        async fn single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.join_and_ignore_errors_async().await;
            assert_eq!(*struct_a.flag.lock().unwrap(), false);
        }

        #[tokio::test]
        async fn multiple_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(false);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(false);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors_async().await;

            assert_eq!(*struct_a.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.string.lock().unwrap(), "hello".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[tokio::test]
        async fn multiple_processes_and_single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(false);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(false);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors_async().await;

            assert_eq!(*struct_a.flag.lock().unwrap(), true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 987);
        }

        #[tokio::test]
        async fn multiple_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = StructA::new(true);
            assert_eq!(*struct_a.flag.lock().unwrap(), false);

            let struct_b = StructB::new(true);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());

            let struct_c = StructC::new(true);
            assert_eq!(*struct_c.number.lock().unwrap(), 123);

            ag.name = "foo";
            struct_a.process(&mut ag);

            ag.name = "bar";
            struct_b.process(&mut ag);

            ag.name = "baz";
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors_async().await;

            assert_eq!(*struct_a.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.flag.lock().unwrap(), false);
            assert_eq!(*struct_b.string.lock().unwrap(), "-".to_string());
            assert_eq!(*struct_c.number.lock().unwrap(), 123);
        }
    }
}
