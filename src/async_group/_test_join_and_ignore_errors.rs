use super::_test_data::MyStruct;
use super::*;

#[test]
fn zero() {
    let ag = AsyncGroup::new();

    ag.join_and_ignore_errors();
}

#[test]
fn single_ok() {
    let mut ag = AsyncGroup::new();

    let struct_a = MyStruct::new("a".to_string(), false);
    assert_eq!(*struct_a.string.lock().unwrap(), "a");

    ag.index = 123;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.join_and_ignore_errors();
    assert_eq!(*struct_a.string.lock().unwrap(), "A");
}

#[test]
fn single_fail() {
    let mut ag = AsyncGroup::new();

    let struct_a = MyStruct::new("a".to_string(), true);
    assert_eq!(*struct_a.string.lock().unwrap(), "a");

    ag.index = 123;
    ag.name = "foo".into();
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

    ag.index = 123;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.index = 456;
    ag.name = "bar".into();
    struct_b.process(&mut ag);

    ag.index = 789;
    ag.name = "baz".into();
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

    ag.index = 123;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.index = 456;
    ag.name = "bar".into();
    struct_b.process(&mut ag);

    ag.index = 789;
    ag.name = "baz".into();
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

    ag.index = 123;
    ag.name = "foo".into();
    struct_a.process(&mut ag);

    ag.index = 456;
    ag.name = "foo".into();
    struct_b.process(&mut ag);

    ag.index = 789;
    ag.name = "foo".into();
    struct_c.process(&mut ag);

    ag.join_and_ignore_errors();

    assert_eq!(*struct_a.string.lock().unwrap(), "a");
    assert_eq!(*struct_b.string.lock().unwrap(), "b");
    assert_eq!(*struct_c.string.lock().unwrap(), "c");
}
