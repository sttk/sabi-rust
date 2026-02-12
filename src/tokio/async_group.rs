// Copyright (C) 2024-2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use super::AsyncGroup;

use futures::future;
use std::future::Future;
use std::sync::Arc;

impl AsyncGroup {
    pub(crate) fn new() -> Self {
        Self {
            tasks: Vec::new(),
            names: Vec::new(),
            _name: "".into(),
        }
    }

    /// Adds a future to the AsyncGroup to be executed concurrently.
    ///
    /// The provided future will be polled along with others in this group.
    ///
    /// # Arguments
    ///
    /// * `future` - The future to add. It must implement `Future<Output = errs::Result<()>>`,
    ///              `Send`, and have a `'static` lifetime.
    pub fn add<Fut>(&mut self, future: Fut)
    where
        Fut: Future<Output = errs::Result<()>> + Send + 'static,
    {
        self.tasks.push(Box::pin(future));
        self.names.push(self._name.clone());
    }

    pub(crate) async fn join_and_collect_errors_async(
        self,
        errors: &mut Vec<(Arc<str>, errs::Err)>,
    ) {
        if self.tasks.is_empty() {
            return;
        }

        let result_all = future::join_all(self.tasks).await;
        for (i, result) in result_all.into_iter().enumerate() {
            if let Err(err) = result {
                errors.push((self.names[i].clone(), err));
            }
        }
    }

    pub(crate) async fn join_and_ignore_errors_async(self) {
        let _ = future::join_all(self.tasks).await;
    }
}

#[cfg(test)]
mod tests_of_async_group {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use tokio::time;

    const BASE_LINE: u32 = line!();

    #[derive(Debug, PartialEq)]
    enum Reasons {
        BadString(String),
    }

    struct MyStruct {
        string: Arc<Mutex<String>>,
        fail: bool,
    }

    impl MyStruct {
        fn new(s: String, fail: bool) -> Self {
            Self {
                string: Arc::new(Mutex::new(s)),
                fail,
            }
        }

        fn process(&self, ag: &mut AsyncGroup) {
            let s_clone = self.string.clone();
            let fail = self.fail;
            ag.add(async move {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;

                {
                    let mut s = s_clone.lock().await;
                    if fail {
                        return Err(errs::Err::new(Reasons::BadString((*s).to_string())));
                    }
                    *s = s.to_uppercase();
                }

                Ok(())
            });
        }

        fn process_multiple(&self, ag: &mut AsyncGroup) {
            let s_clone = self.string.clone();
            let fail = self.fail;
            ag.add(async move {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;

                {
                    let mut s = s_clone.lock().await;
                    if fail {
                        return Err(errs::Err::new(Reasons::BadString((*s).to_string())));
                    }
                    *s = s.to_uppercase();
                }

                Ok(())
            });

            let s_clone = self.string.clone();
            let fail = self.fail;
            ag.add(async move {
                // The `.await` must be executed outside the Mutex lock.
                let _ = time::sleep(time::Duration::from_millis(100)).await;

                {
                    let mut s = s_clone.lock().await;
                    if fail {
                        return Err(errs::Err::new(Reasons::BadString((*s).to_string())));
                    }
                    *s = s.to_uppercase();
                }

                Ok(())
            });
        }
    }

    mod tests_of_join_and_collect_errors {
        use super::*;

        #[tokio::test]
        async fn zero() {
            let ag = AsyncGroup::new();

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors_async(&mut err_vec).await;

            assert!(err_vec.is_empty());
        }

        #[tokio::test]
        async fn single_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().await, "a");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            let mut errors = Vec::new();
            ag.join_and_collect_errors_async(&mut errors).await;

            assert!(errors.is_empty());
            assert_eq!(*struct_a.string.lock().await, "A");
        }

        #[tokio::test]
        async fn single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), true);
            assert_eq!(*struct_a.string.lock().await, "a");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            let mut errors = Vec::new();
            ag.join_and_collect_errors_async(&mut errors).await;

            assert_eq!(errors.len(), 1);
            assert_eq!(*struct_a.string.lock().await, "a");

            assert_eq!(errors[0].0, "foo".into());
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", errors[0].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"a\"), file = src/tokio/async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }"
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", errors[0].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"a\"), file = src\\tokio\\async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }"
            );
        }

        #[tokio::test]
        async fn multiple_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().await, "a".to_string());

            let struct_b = MyStruct::new("b".to_string(), false);
            assert_eq!(*struct_b.string.lock().await, "b".to_string());

            let struct_c = MyStruct::new("c".to_string(), false);
            assert_eq!(*struct_c.string.lock().await, "c".to_string());

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors_async(&mut err_vec).await;

            assert_eq!(err_vec.len(), 0);

            assert_eq!(*struct_a.string.lock().await, "A");
            assert_eq!(*struct_b.string.lock().await, "B");
            assert_eq!(*struct_c.string.lock().await, "C");
        }

        #[tokio::test]
        async fn multiple_processes_and_single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().await, "a");

            let struct_b = MyStruct::new("b".to_string(), true);
            assert_eq!(*struct_b.string.lock().await, "b");

            let struct_c = MyStruct::new("c".to_string(), false);
            assert_eq!(*struct_c.string.lock().await, "c");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors_async(&mut err_vec).await;

            assert_eq!(err_vec.len(), 1);
            assert_eq!(err_vec[0].0, "bar".into());
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"b\"), file = src/tokio/async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"b\"), file = src\\tokio\\async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }",
            );

            assert_eq!(*struct_a.string.lock().await, "A");
            assert_eq!(*struct_b.string.lock().await, "b");
            assert_eq!(*struct_c.string.lock().await, "C");
        }

        #[tokio::test]
        async fn multiple_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), true);
            assert_eq!(*struct_a.string.lock().await, "a");

            let struct_b = MyStruct::new("b".to_string(), true);
            assert_eq!(*struct_b.string.lock().await, "b");

            let struct_c = MyStruct::new("c".to_string(), true);
            assert_eq!(*struct_c.string.lock().await, "c");

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors_async(&mut err_vec).await;

            assert_eq!(err_vec.len(), 3);

            assert_eq!(err_vec[0].0, "foo".into());
            assert_eq!(err_vec[1].0, "bar".into());
            assert_eq!(err_vec[2].0, "baz".into());

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"a\"), file = src/tokio/async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"a\"), file = src\\tokio\\async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }",
            );
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[1].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"b\"), file = src/tokio/async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[1].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"b\"), file = src\\tokio\\async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }",
            );
            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[2].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"c\"), file = src/tokio/async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[2].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"c\"), file = src\\tokio\\async_group.rs, line = ".to_string() + &(BASE_LINE + 30).to_string() + " }",
            );

            assert_eq!(*struct_a.string.lock().await, "a");
            assert_eq!(*struct_b.string.lock().await, "b");
            assert_eq!(*struct_c.string.lock().await, "c");
        }

        #[tokio::test]
        async fn data_src_execute_multiple_setup_handles() {
            let mut ag = AsyncGroup::new();

            let struct_d = MyStruct::new("d".to_string(), false);
            assert_eq!(*struct_d.string.lock().await, "d");

            ag._name = "foo".into();
            struct_d.process(&mut ag);

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors_async(&mut err_vec).await;

            assert_eq!(err_vec.len(), 0);

            assert_eq!(*struct_d.string.lock().await, "D");
        }

        #[tokio::test]
        async fn collect_all_errors_if_data_src_executes_multiple_setup_handles() {
            let mut ag = AsyncGroup::new();

            let struct_d = MyStruct::new("d".to_string(), true);
            assert_eq!(*struct_d.string.lock().await, "d");

            ag._name = "foo".into();
            struct_d.process_multiple(&mut ag);

            let mut err_vec = Vec::new();
            ag.join_and_collect_errors_async(&mut err_vec).await;

            assert_eq!(err_vec.len(), 2);

            assert_eq!(err_vec[0].0, "foo".into());
            assert_eq!(err_vec[1].0, "foo".into());

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"d\"), file = src/tokio/async_group.rs, line = ".to_string() + &(BASE_LINE + 49).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[0].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"d\"), file = src\\tokio\\async_group.rs, line = ".to_string() + &(BASE_LINE + 49).to_string() + " }"
            );

            #[cfg(unix)]
            assert_eq!(
                format!("{:?}", err_vec[1].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"d\"), file = src/tokio/async_group.rs, line = ".to_string() + &(BASE_LINE + 66).to_string() + " }",
            );
            #[cfg(windows)]
            assert_eq!(
                format!("{:?}", err_vec[1].1),
                "errs::Err { reason = sabi::tokio::async_group::tests_of_async_group::Reasons BadString(\"d\"), file = src\\tokio\\async_group.rs, line = ".to_string() + &(BASE_LINE + 66).to_string() + " }"
            );

            assert_eq!(*struct_d.string.lock().await, "d");
        }
    }

    mod tests_join_and_ignore_errors_async {
        use super::*;

        #[tokio::test]
        async fn zero() {
            let ag = AsyncGroup::new();

            ag.join_and_ignore_errors_async().await;
        }

        #[tokio::test]
        async fn single_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().await, "a".to_string());

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag.join_and_ignore_errors_async().await;
            assert_eq!(*struct_a.string.lock().await, "A".to_string());
        }

        #[tokio::test]
        async fn single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), true);
            assert_eq!(*struct_a.string.lock().await, "a".to_string());

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag.join_and_ignore_errors_async().await;
            assert_eq!(*struct_a.string.lock().await, "a".to_string());
        }

        #[tokio::test]
        async fn multiple_ok() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().await, "a".to_string());

            let struct_b = MyStruct::new("b".to_string(), false);
            assert_eq!(*struct_b.string.lock().await, "b".to_string());

            let struct_c = MyStruct::new("c".to_string(), false);
            assert_eq!(*struct_c.string.lock().await, "c".to_string());

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors_async().await;

            assert_eq!(*struct_a.string.lock().await, "A");
            assert_eq!(*struct_b.string.lock().await, "B");
            assert_eq!(*struct_c.string.lock().await, "C");
        }

        #[tokio::test]
        async fn multiple_processes_and_single_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), false);
            assert_eq!(*struct_a.string.lock().await, "a".to_string());

            let struct_b = MyStruct::new("b".to_string(), true);
            assert_eq!(*struct_b.string.lock().await, "b".to_string());

            let struct_c = MyStruct::new("c".to_string(), false);
            assert_eq!(*struct_c.string.lock().await, "c".to_string());

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors_async().await;

            assert_eq!(*struct_a.string.lock().await, "A");
            assert_eq!(*struct_b.string.lock().await, "b");
            assert_eq!(*struct_c.string.lock().await, "C");
        }

        #[tokio::test]
        async fn multiple_fail() {
            let mut ag = AsyncGroup::new();

            let struct_a = MyStruct::new("a".to_string(), true);
            assert_eq!(*struct_a.string.lock().await, "a".to_string());

            let struct_b = MyStruct::new("b".to_string(), true);
            assert_eq!(*struct_b.string.lock().await, "b".to_string());

            let struct_c = MyStruct::new("c".to_string(), true);
            assert_eq!(*struct_c.string.lock().await, "c".to_string());

            ag._name = "foo".into();
            struct_a.process(&mut ag);

            ag._name = "bar".into();
            struct_b.process(&mut ag);

            ag._name = "baz".into();
            struct_c.process(&mut ag);

            ag.join_and_ignore_errors_async().await;

            assert_eq!(*struct_a.string.lock().await, "a");
            assert_eq!(*struct_b.string.lock().await, "b");
            assert_eq!(*struct_c.string.lock().await, "c");
        }
    }
}
