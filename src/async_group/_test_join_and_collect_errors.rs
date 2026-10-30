use super::_test_data::MyStruct;
use super::*;
use std::panic::panic_any;

#[test]
fn zero() {
    let ag = AsyncGroup::new();

    let mut err_vec = Vec::new();
    ag.join_and_collect_errors(&mut err_vec);

    assert!(err_vec.is_empty());
}

#[test]
fn single_ok() {
    let mut ag = AsyncGroup::new();

    let struct_a = MyStruct::new("a".to_string(), false);
    assert_eq!(*struct_a.string.lock().unwrap(), "a");

    ag.index = 12;
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

    ag.index = 12;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    let mut errors = Vec::new();
    ag.join_and_collect_errors(&mut errors);

    assert_eq!(errors.len(), 1);
    assert_eq!(*struct_a.string.lock().unwrap(), "a");

    assert_eq!(errors[0].index, 12);
    assert_eq!(errors[0].name, "foo".into());
    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", errors[0].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"a\"), file = src/async_group/_test_data.rs, line = 30 }"
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", errors[0].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"a\"), file = src\\async_group\\_test_data.rs, line = 30 }"
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
    ag.join_and_collect_errors(&mut err_vec);

    assert_eq!(err_vec.len(), 1);
    assert_eq!(err_vec[0].index, 34);
    assert_eq!(err_vec[0].name, "bar".into());
    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", err_vec[0].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"b\"), file = src/async_group/_test_data.rs, line = 30 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[0].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"b\"), file = src\\async_group\\_test_data.rs, line = 30 }",
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
    ag.join_and_collect_errors(&mut err_vec);

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
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"a\"), file = src/async_group/_test_data.rs, line = 30 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[0].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"a\"), file = src\\async_group\\_test_data.rs, line = 30 }",
    );
    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", err_vec[1].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"b\"), file = src/async_group/_test_data.rs, line = 30 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[1].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"b\"), file = src\\async_group\\_test_data.rs, line = 30 }",
    );
    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", err_vec[2].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"c\"), file = src/async_group/_test_data.rs, line = 30 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[2].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"c\"), file = src\\async_group\\_test_data.rs, line = 30 }",
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

    ag.index = 123;
    ag.name = "foo".into();
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

    ag.index = 123;
    ag.name = "foo".into();
    struct_d.process_multiple(&mut ag);

    let mut err_vec = Vec::new();
    ag.join_and_collect_errors(&mut err_vec);

    assert_eq!(err_vec.len(), 2);

    assert_eq!(err_vec[0].index, 123);
    assert_eq!(err_vec[1].index, 123);

    assert_eq!(err_vec[0].name, "foo".into());
    assert_eq!(err_vec[1].name, "foo".into());

    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", err_vec[0].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"d\"), file = src/async_group/_test_data.rs, line = 46 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[0].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"d\"), file = src\\async_group\\_test_data.rs, line = 46 }"
    );

    #[cfg(unix)]
    assert_eq!(
        format!("{:?}", err_vec[1].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"d\"), file = src/async_group/_test_data.rs, line = 60 }",
    );
    #[cfg(windows)]
    assert_eq!(
        format!("{:?}", err_vec[1].err),
        "errs::Err { reason = sabi::async_group::_test_data::Reasons BadString(\"d\"), file = src\\async_group\\_test_data.rs, line = 60 }"
    );

    assert_eq!(*struct_d.string.lock().unwrap(), "d");
}

#[test]
fn async_code_calls_thread_panic_with_a_str() {
    let mut ag = AsyncGroup::new();

    ag.index = 123;
    ag.name = "foo".into();

    ag.add(|| {
        panic!("panic");
    });

    let mut errors = Vec::<ErrEntry>::new();
    ag.join_and_collect_errors(&mut errors);

    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].index, 123);
    assert_eq!(errors[0].name, "foo".into());
    #[cfg(unix)]
    assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = sabi::async_group::AsyncGroupError ThreadPanicked(\"panic\"), file = src/async_group.rs, line = 62 }");
    #[cfg(windows)]
    assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = sabi::async_group::AsyncGroupError ThreadPanicked(\"panic\"), file = src\\async_group.rs, line = 62 }");
}

#[test]
fn async_code_calls_thread_panic_with_a_string() {
    let mut ag = AsyncGroup::new();

    ag.index = 123;
    ag.name = "foo".into();

    ag.add(|| {
        panic_any("panic".to_string());
    });

    let mut errors = Vec::<ErrEntry>::new();
    ag.join_and_collect_errors(&mut errors);

    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].index, 123);
    assert_eq!(errors[0].name, "foo".into());
    #[cfg(unix)]
    assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = sabi::async_group::AsyncGroupError ThreadPanicked(\"panic\"), file = src/async_group.rs, line = 62 }");
    #[cfg(windows)]
    assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = sabi::async_group::AsyncGroupError ThreadPanicked(\"panic\"), file = src\\async_group.rs, line = 62 }");
}

#[test]
fn async_code_calls_thread_panic_with_an_arc_str_ref() {
    let mut ag = AsyncGroup::new();

    ag.index = 123;
    ag.name = "foo".into();

    ag.add(|| {
        panic_any(Arc::new("panic"));
    });

    let mut errors = Vec::<ErrEntry>::new();
    ag.join_and_collect_errors(&mut errors);

    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].index, 123);
    assert_eq!(errors[0].name, "foo".into());
    #[cfg(unix)]
    assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = sabi::async_group::AsyncGroupError ThreadPanicked(\"panic\"), file = src/async_group.rs, line = 62 }");
    #[cfg(windows)]
    assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = sabi::async_group::AsyncGroupError ThreadPanicked(\"panic\"), file = src\\async_group.rs, line = 62 }");
}

#[test]
fn async_code_calls_thread_panic_with_an_arc_str() {
    let mut ag = AsyncGroup::new();

    ag.index = 123;
    ag.name = "foo".into();

    ag.add(|| {
        panic_any(Arc::<str>::from("panic"));
    });

    let mut errors = Vec::<ErrEntry>::new();
    ag.join_and_collect_errors(&mut errors);

    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].index, 123);
    assert_eq!(errors[0].name, "foo".into());
    #[cfg(unix)]
    assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = sabi::async_group::AsyncGroupError ThreadPanicked(\"panic\"), file = src/async_group.rs, line = 62 }");
    #[cfg(windows)]
    assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = sabi::async_group::AsyncGroupError ThreadPanicked(\"panic\"), file = src\\async_group.rs, line = 62 }");
}

#[test]
fn async_code_calls_thread_panic_with_an_unknown_value() {
    let mut ag = AsyncGroup::new();

    ag.index = 123;
    ag.name = "foo".into();

    ag.add(|| {
        panic_any(987);
    });

    let mut errors = Vec::<ErrEntry>::new();
    ag.join_and_collect_errors(&mut errors);

    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].index, 123);
    assert_eq!(errors[0].name, "foo".into());
    #[cfg(unix)]
    assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = sabi::async_group::AsyncGroupError ThreadPanicked(\"Unknown panic payload\"), file = src/async_group.rs, line = 62 }");
    #[cfg(windows)]
    assert_eq!(format!("{:?}", errors[0].err), "errs::Err { reason = sabi::async_group::AsyncGroupError ThreadPanicked(\"Unknown panic payload\"), file = src\\async_group.rs, line = 62 }");
}
