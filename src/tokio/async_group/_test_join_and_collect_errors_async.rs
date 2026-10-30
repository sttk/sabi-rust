use super::_test_data::*;
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

    ag.index = 12;
    ag.name = "foo".into();
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

    ag.index = 12;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    let mut errors = Vec::new();
    ag.join_and_collect_errors_async(&mut errors).await;

    assert_eq!(errors.len(), 1);
    assert_eq!(*struct_a.string.lock().await, "a");

    assert_eq!(errors[0].index, 12);
    assert_eq!(errors[0].name, "foo".into());
    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", errors[0].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"a\"), file = src/tokio/async_group/_test_data.rs, line = 34 }"
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", errors[0].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"a\"), file = src\\tokio\\async_group\\_test_data.rs, line = 34 }"
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

    ag.index = 12;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.index = 34;
    ag.name = "bar".into();
    struct_b.process(&mut ag);

    ag.index = 56;
    ag.name = "baz".into();
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

    ag.index = 12;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.index = 34;
    ag.name = "bar".into();
    struct_b.process(&mut ag);

    ag.index = 56;
    ag.name = "baz".into();
    struct_c.process(&mut ag);

    let mut err_vec = Vec::new();
    ag.join_and_collect_errors_async(&mut err_vec).await;

    assert_eq!(err_vec.len(), 1);
    assert_eq!(err_vec[0].index, 34);
    assert_eq!(err_vec[0].name, "bar".into());
    #[cfg(unix)]
    assert_eq!(
                format!("{:?}", err_vec[0].err),
                "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"b\"), file = src/tokio/async_group/_test_data.rs, line = 34 }",
            );
    #[cfg(windows)]
    assert_eq!(
                format!("{:?}", err_vec[0].err),
                "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"b\"), file = src\\tokio\\async_group\\_test_data.rs, line = 34 }",
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

    ag.index = 12;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.index = 34;
    ag.name = "bar".into();
    struct_b.process(&mut ag);

    ag.index = 56;
    ag.name = "baz".into();
    struct_c.process(&mut ag);

    let mut err_vec = Vec::new();
    ag.join_and_collect_errors_async(&mut err_vec).await;

    assert_eq!(err_vec.len(), 3);

    assert_eq!(err_vec[0].index, 12);
    assert_eq!(err_vec[1].index, 34);
    assert_eq!(err_vec[2].index, 56);

    assert_eq!(err_vec[0].name, "foo".into());
    assert_eq!(err_vec[1].name, "bar".into());
    assert_eq!(err_vec[2].name, "baz".into());

    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", err_vec[0].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"a\"), file = src/tokio/async_group/_test_data.rs, line = 34 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[0].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"a\"), file = src\\tokio\\async_group\\_test_data.rs, line = 34 }",
    );
    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", err_vec[1].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"b\"), file = src/tokio/async_group/_test_data.rs, line = 34 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[1].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"b\"), file = src\\tokio\\async_group\\_test_data.rs, line = 34 }",
    );
    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", err_vec[2].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"c\"), file = src/tokio/async_group/_test_data.rs, line = 34 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[2].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"c\"), file = src\\tokio\\async_group\\_test_data.rs, line = 34 }",
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

    ag.index = 12;
    ag.name = "foo".into();
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

    ag.index = 12;
    ag.name = "foo".into();
    struct_d.process_multiple(&mut ag);

    let mut err_vec = Vec::new();
    ag.join_and_collect_errors_async(&mut err_vec).await;

    assert_eq!(err_vec.len(), 2);

    assert_eq!(err_vec[0].index, 12);
    assert_eq!(err_vec[1].index, 12);

    assert_eq!(err_vec[0].name, "foo".into());
    assert_eq!(err_vec[1].name, "foo".into());

    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", err_vec[0].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"d\"), file = src/tokio/async_group/_test_data.rs, line = 53 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[0].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"d\"), file = src\\tokio\\async_group\\_test_data.rs, line = 53 }"
    );

    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", err_vec[1].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"d\"), file = src/tokio/async_group/_test_data.rs, line = 70 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[1].err),
        "errs::Err { reason = sabi::tokio::async_group::_test_data::Reasons BadString(\"d\"), file = src\\tokio\\async_group\\_test_data.rs, line = 70 }"
    );

    assert_eq!(*struct_d.string.lock().await, "d");
}
