use super::_test_data::*;
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

    ag.index = 12;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.join_and_ignore_errors_async().await;
    assert_eq!(*struct_a.string.lock().await, "A".to_string());
}

#[tokio::test]
async fn single_fail() {
    let mut ag = AsyncGroup::new();

    let struct_a = MyStruct::new("a".to_string(), true);
    assert_eq!(*struct_a.string.lock().await, "a".to_string());

    ag.index = 12;
    ag.name = "foo".into();
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

    ag.index = 12;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.index = 34;
    ag.name = "bar".into();
    struct_b.process(&mut ag);

    ag.index = 56;
    ag.name = "baz".into();
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

    ag.index = 12;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.index = 34;
    ag.name = "bar".into();
    struct_b.process(&mut ag);

    ag.index = 56;
    ag.name = "baz".into();
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

    ag.index = 12;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.index = 34;
    ag.name = "bar".into();
    struct_b.process(&mut ag);

    ag.index = 56;
    ag.name = "baz".into();
    struct_c.process(&mut ag);

    ag.join_and_ignore_errors_async().await;

    assert_eq!(*struct_a.string.lock().await, "a");
    assert_eq!(*struct_b.string.lock().await, "b");
    assert_eq!(*struct_c.string.lock().await, "c");
}
