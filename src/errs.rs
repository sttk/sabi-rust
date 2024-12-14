// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;
use std::error;
use std::fmt;
use std::ptr;

/// The error type used within the Sabi framework.
///
/// It holds a value of any type that represents the reason for the error.
/// By the reason, it is possible to identify the error or retrieve the detailed information about
/// the error.
pub struct Err {
    reason_container: ptr::NonNull<ReasonContainer>,
    source: Option<Box<dyn error::Error>>,
}

impl Err {
    /// Creates a new error with the value reprenents the reason.
    ///
    /// ```rust
    /// use sabi::Err;
    ///
    /// #[derive(Debug)]
    /// enum Reasons {
    ///     IllegalState { state: String },
    /// }
    ///
    /// let err = Err::new(Reasons::IllegalState { state: "bad state".to_string() });
    /// ```
    pub fn new<R>(reason: R) -> Self
    where
        R: fmt::Debug + Send + Sync + 'static,
    {
        let boxed = Box::new(ReasonContainer::<R>::new(reason));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<ReasonContainer>();
        Self {
            reason_container: ptr,
            source: None,
        }
    }

    /// Creates a new error with the value reprenents the reason and the source error that causes
    /// this error.
    ///
    /// ```rust
    /// use sabi::Err;
    /// use std::io;
    ///
    /// #[derive(Debug)]
    /// enum Reasons {
    ///     FailToDoSomething,
    /// }
    ///
    /// let io_error = io::Error::other("oh no!");
    ///
    /// let err = Err::with_source(Reasons::FailToDoSomething, io_error);
    /// ```
    pub fn with_source<R, E>(reason: R, source: E) -> Self
    where
        R: fmt::Debug + Send + Sync + 'static,
        E: error::Error + Send + Sync + 'static,
    {
        let boxed = Box::new(ReasonContainer::<R>::new(reason));
        let ptr = ptr::NonNull::from(Box::leak(boxed)).cast::<ReasonContainer>();
        Self {
            reason_container: ptr,
            source: Some(Box::new(source)),
        }
    }

    /// Checks if `R` is the type of the reason held by this error object.
    ///
    /// ```rust
    /// use sabi::Err;
    ///
    /// #[derive(Debug)]
    /// enum Reasons {
    ///     IllegalState { state: String },
    /// }
    ///
    /// let err = Err::new(Reasons::IllegalState { state: "bad state".to_string() });
    /// assert!(err.is_reason::<Reasons>());
    /// assert!(err.is_reason::<String>() == false);
    /// ```
    pub fn is_reason<R>(&self) -> bool
    where
        R: fmt::Debug + Send + Sync + 'static,
    {
        let type_id = any::TypeId::of::<R>();
        let ptr = self.reason_container.as_ptr();
        let is_fn = unsafe { (*ptr).is_fn };
        is_fn(type_id)
    }

    /// Gets the reason value if the type of the reason is `R`.
    ///
    /// ```rust
    /// use sabi::Err;
    ///
    /// #[derive(Debug)]
    /// enum Reasons {
    ///     IllegalState { state: String },
    /// }
    ///
    /// let err = Err::new(Reasons::IllegalState { state: "bad state".to_string() });
    /// match err.reason::<Reasons>() {
    ///   Ok(r) => match r {
    ///     Reasons::IllegalState { state } => println!("state = {state}"),
    ///     _ => { /* ... */ }
    ///   }
    ///   Err(err) => match err.reason::<String>() {
    ///      Ok(s) => println!("string reason = {s}"),
    ///      Err(_err) => { /* ... */ }
    ///   }
    /// }
    /// ```
    pub fn reason<R>(&self) -> Result<&R, &Self>
    where
        R: fmt::Debug + Send + Sync + 'static,
    {
        let type_id = any::TypeId::of::<R>();
        let ptr = self.reason_container.as_ptr();
        let is_fn = unsafe { (*ptr).is_fn };
        if is_fn(type_id) {
            let typed_ptr = ptr as *const ReasonContainer<R>;
            return Ok(unsafe { &((*typed_ptr).reason) });
        }

        Err(self)
    }

    /// Checks the type of the reason held by this object, and if it matches, executes the provided
    /// function.
    ///
    /// ```rust
    /// use sabi::Err;
    ///
    /// #[derive(Debug)]
    /// enum Reasons {
    ///     IllegalState { state: String },
    /// }
    ///
    /// let err = Err::new(Reasons::IllegalState { state: "bad state".to_string() });
    /// err.match_reason::<Reasons>(|r| match r {
    ///     Reasons::IllegalState { state } => println!("state = {state}"),
    ///     _ => { /* ... */ }
    /// })
    /// .match_reason::<String>(|s| {
    ///     println!("string reason = {s}");
    /// });
    /// ```
    pub fn match_reason<R>(&self, func: fn(&R)) -> &Self
    where
        R: fmt::Debug + Send + Sync + 'static,
    {
        let type_id = any::TypeId::of::<R>();
        let ptr = self.reason_container.as_ptr();
        let is_fn = unsafe { (*ptr).is_fn };
        if is_fn(type_id) {
            let typed_ptr = ptr as *const ReasonContainer<R>;
            func(unsafe { &((*typed_ptr).reason) });
        }

        self
    }
}

impl Drop for Err {
    fn drop(&mut self) {
        let ptr = self.reason_container.as_ptr();
        let drop_fn = unsafe { (*ptr).drop_fn };
        drop_fn(ptr);
    }
}

impl fmt::Debug for Err {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ptr = self.reason_container.as_ptr();
        let debug_fn = unsafe { (*ptr).debug_fn };
        debug_fn(ptr, f)
    }
}

impl fmt::Display for Err {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ptr = self.reason_container.as_ptr();
        let display_fn = unsafe { (*ptr).display_fn };
        display_fn(ptr, f)
    }
}

impl error::Error for Err {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.source.as_deref()
    }
}

#[repr(C)]
struct ReasonContainer<R = ()>
where
    R: fmt::Debug + Send + Sync + 'static,
{
    drop_fn: fn(*const ReasonContainer),
    debug_fn: fn(*const ReasonContainer, f: &mut fmt::Formatter<'_>) -> fmt::Result,
    display_fn: fn(*const ReasonContainer, f: &mut fmt::Formatter<'_>) -> fmt::Result,
    is_fn: fn(any::TypeId) -> bool,
    reason: R,
}

impl<R> ReasonContainer<R>
where
    R: fmt::Debug + Send + Sync + 'static,
{
    fn new(reason: R) -> Self {
        Self {
            drop_fn: drop_reason_container::<R>,
            debug_fn: debug_reason::<R>,
            display_fn: display_reason::<R>,
            is_fn: is_reason::<R>,
            reason,
        }
    }
}

fn drop_reason_container<R>(ptr: *const ReasonContainer)
where
    R: fmt::Debug + Send + Sync + 'static,
{
    let typed_ptr = ptr as *mut ReasonContainer<R>;
    unsafe {
        drop(Box::from_raw(typed_ptr));
    }
}

fn debug_reason<R>(ptr: *const ReasonContainer, f: &mut fmt::Formatter<'_>) -> fmt::Result
where
    R: fmt::Debug + Send + Sync + 'static,
{
    let typed_ptr = ptr as *mut ReasonContainer<R>;
    write!(
        f,
        "{} {{ reason: {} {:?} }}",
        any::type_name::<Err>(),
        any::type_name::<R>(),
        unsafe { &(*typed_ptr).reason }
    )
}

fn display_reason<R>(ptr: *const ReasonContainer, f: &mut fmt::Formatter<'_>) -> fmt::Result
where
    R: fmt::Debug + Send + Sync + 'static,
{
    let typed_ptr = ptr as *mut ReasonContainer<R>;
    write!(f, "{:?}", unsafe { &(*typed_ptr).reason })
}

fn is_reason<R>(type_id: any::TypeId) -> bool
where
    R: fmt::Debug + Send + Sync + 'static,
{
    any::TypeId::of::<R>() == type_id
}

#[cfg(test)]
mod tests_of_err_without_source {
    use super::*;
    use std::error::Error;
    use std::sync::{LazyLock, Mutex};

    struct Logger {
        log_vec: Vec<String>,
    }
    impl Logger {
        fn new() -> Self {
            Self {
                log_vec: Vec::<String>::new(),
            }
        }
        fn log(&mut self, s: &str) {
            self.log_vec.push(s.to_string());
        }
        fn assert_logs(&self, logs: &[&str]) {
            assert_eq!(self.log_vec.len(), logs.len());
            for i in 0..self.log_vec.len() {
                assert_eq!(self.log_vec[i], logs[i]);
            }
        }
    }

    mod reason_is_enum_with_no_data {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        #[allow(dead_code)]
        #[derive(Debug)]
        enum Enum0 {
            Reason0,
            Other,
        }
        impl Drop for Enum0 {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop Enum0");
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::new(Enum0::Reason0);
            LOGGER.lock().unwrap().log("created Enum0");
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<Enum0>());
            assert!(!err.is_reason::<String>());

            assert!(err.source().is_none()); // necessary `use std::error::Error;`

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<Enum0>() {
                    Ok(r) => match r {
                        Enum0::Reason0 => {}
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<Enum0>(|r| match r {
                Enum0::Reason0 => {}
                _ => panic!(),
            });

            assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: sabi::errs::tests_of_err_without_source::reason_is_enum_with_no_data::Enum0 Reason0 }");
            assert_eq!(format!("{err}"), "Reason0");

            LOGGER.lock().unwrap().log("consumed Enum0");
        }

        #[test]
        fn test_err_by_reaason0() {
            consume_err();
            LOGGER.lock().unwrap().log("end");

            LOGGER.lock().unwrap().assert_logs(&[
                "created Enum0",
                "consumed Enum0",
                "drop Enum0",
                "end",
            ]);
        }
    }

    mod reason_is_enum_with_one_value {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        #[allow(dead_code)]
        #[derive(Debug)]
        enum Enum1 {
            Reason1(String),
            Other,
        }
        impl Drop for Enum1 {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop Enum1");
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::new(Enum1::Reason1("hello".to_string()));
            LOGGER.lock().unwrap().log("created Enum1");
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<Enum1>());
            assert!(!err.is_reason::<String>());

            assert!(err.source().is_none()); // necessary `use std::error::Error;`

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<Enum1>() {
                    Ok(r) => match r {
                        Enum1::Reason1(s) => assert_eq!(s, "hello"),
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<Enum1>(|r| match r {
                Enum1::Reason1(s) => assert_eq!(s, "hello"),
                _ => panic!(),
            });

            assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: sabi::errs::tests_of_err_without_source::reason_is_enum_with_one_value::Enum1 Reason1(\"hello\") }");
            assert_eq!(format!("{err}"), "Reason1(\"hello\")");

            LOGGER.lock().unwrap().log("consumed Enum1");
        }

        #[test]
        fn test_err_by_reaason1() {
            consume_err();
            LOGGER.lock().unwrap().log("end");

            LOGGER.lock().unwrap().assert_logs(&[
                "created Enum1",
                "consumed Enum1",
                "drop Enum1",
                "end",
            ]);
        }
    }

    mod reason_is_enum_with_two_values {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        #[allow(dead_code)]
        #[derive(Debug)]
        enum Enum2 {
            Reason2(String, i32),
            Other,
        }
        impl Drop for Enum2 {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop Enum2");
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::new(Enum2::Reason2("hello".to_string(), 123));
            LOGGER.lock().unwrap().log("created Enum2");
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<Enum2>());
            assert!(!err.is_reason::<String>());

            assert!(err.source().is_none()); // necessary `use std::error::Error;`

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<Enum2>() {
                    Ok(r) => match r {
                        Enum2::Reason2(s, n) => {
                            assert_eq!(s, "hello");
                            assert_eq!(*n, 123);
                        }
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<Enum2>(|r| match r {
                Enum2::Reason2(s, n) => {
                    assert_eq!(s, "hello");
                    assert_eq!(*n, 123);
                }
                _ => panic!(),
            });

            assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: sabi::errs::tests_of_err_without_source::reason_is_enum_with_two_values::Enum2 Reason2(\"hello\", 123) }");
            assert_eq!(format!("{err}"), "Reason2(\"hello\", 123)");

            LOGGER.lock().unwrap().log("consumed Enum2");
        }

        #[test]
        fn test_err_by_reaason2() {
            consume_err();
            LOGGER.lock().unwrap().log("end");

            LOGGER.lock().unwrap().assert_logs(&[
                "created Enum2",
                "consumed Enum2",
                "drop Enum2",
                "end",
            ]);
        }
    }

    mod reason_is_enum_with_named_fields {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        #[allow(dead_code)]
        #[derive(Debug)]
        enum Enum3 {
            Reason3 { x: i32, y: i32 },
            Other,
        }
        impl Drop for Enum3 {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop Enum3");
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::new(Enum3::Reason3 { x: 123, y: 456 });
            LOGGER.lock().unwrap().log("created Enum3");
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<Enum3>());
            assert!(!err.is_reason::<String>());

            assert!(err.source().is_none()); // necessary `use std::error::Error;`

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<Enum3>() {
                    Ok(r) => match r {
                        Enum3::Reason3 { x, y } => {
                            assert_eq!(*x, 123);
                            assert_eq!(*y, 456);
                        }
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<Enum3>(|r| match r {
                Enum3::Reason3 { x, y } => {
                    assert_eq!(*x, 123);
                    assert_eq!(*y, 456);
                }
                _ => panic!(),
            });

            assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: sabi::errs::tests_of_err_without_source::reason_is_enum_with_named_fields::Enum3 Reason3 { x: 123, y: 456 } }");
            assert_eq!(format!("{err}"), "Reason3 { x: 123, y: 456 }");

            LOGGER.lock().unwrap().log("consumed Enum3");
        }

        #[test]
        fn test_err_by_reaason3() {
            consume_err();
            LOGGER.lock().unwrap().log("end");

            LOGGER.lock().unwrap().assert_logs(&[
                "created Enum3",
                "consumed Enum3",
                "drop Enum3",
                "end",
            ]);
        }
    }

    mod reason_is_number {
        use super::*;

        fn create_err() -> Result<(), Err> {
            let err = Err::new(123_456);
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<i32>());
            assert!(!err.is_reason::<String>());

            assert!(err.source().is_none()); // necessary `use std::error::Error;`

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<i32>() {
                    Ok(n) => assert_eq!(*n, 123_456),
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<i32>(|n| {
                assert_eq!(*n, 123_456);
            });

            assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: i32 123456 }");
            assert_eq!(format!("{err}"), "123456");
        }

        #[test]
        fn test_err_by_reaason4() {
            consume_err();
        }
    }

    mod reason_is_string {
        use super::*;

        fn create_err() -> Result<(), Err> {
            let err = Err::new("Hello, world!".to_string());
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(!err.is_reason::<i32>());
            assert!(err.is_reason::<String>());

            assert!(err.source().is_none()); // necessary `use std::error::Error;`

            match err.reason::<String>() {
                Ok(s) => assert_eq!(s, "Hello, world!"),
                Err(err) => match err.reason::<i32>() {
                    Ok(_n) => panic!(),
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|s| {
                assert_eq!(s, "Hello, world!");
            })
            .match_reason::<i32>(|_n| {
                panic!();
            });

            assert_eq!(
                format!("{err:?}"),
                "sabi::errs::Err { reason: alloc::string::String \"Hello, world!\" }"
            );
            assert_eq!(format!("{err}"), "\"Hello, world!\"");
        }

        #[test]
        fn test_err_by_reaason5() {
            consume_err();
        }
    }

    mod reason_is_struct {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        #[allow(dead_code)]
        #[derive(Debug)]
        struct Struct6 {
            s: String,
            b: bool,
            n: i64,
        }
        impl Drop for Struct6 {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop Struct6");
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::new(Struct6 {
                s: "hello".to_string(),
                b: true,
                n: 987,
            });
            LOGGER.lock().unwrap().log("created Struct6");
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<Struct6>());
            assert!(!err.is_reason::<String>());

            assert!(err.source().is_none()); // necessary `use std::error::Error;`

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<Struct6>() {
                    Ok(r) => {
                        assert_eq!(r.s, "hello");
                        assert_eq!(r.b, true);
                        assert_eq!(r.n, 987);
                    }
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<Struct6>(|r| {
                assert_eq!(r.s, "hello");
                assert_eq!(r.b, true);
                assert_eq!(r.n, 987);
            });

            assert_eq!(
                format!("{err:?}"),
                "sabi::errs::Err { reason: sabi::errs::tests_of_err_without_source::reason_is_struct::Struct6 Struct6 { s: \"hello\", b: true, n: 987 } }"
            );
            assert_eq!(
                format!("{err}"),
                "Struct6 { s: \"hello\", b: true, n: 987 }"
            );

            LOGGER.lock().unwrap().log("consumed Struct6");
        }

        #[test]
        fn test_err_by_reaason6() {
            consume_err();
            LOGGER.lock().unwrap().log("end");

            LOGGER.lock().unwrap().assert_logs(&[
                "created Struct6",
                "consumed Struct6",
                "drop Struct6",
                "end",
            ]);
        }
    }
}

#[cfg(test)]
mod tests_of_err_with_source {
    use super::*;
    use std::error::Error;
    use std::sync::{LazyLock, Mutex};

    struct Logger {
        log_vec: Vec<String>,
    }
    impl Logger {
        fn new() -> Self {
            Self {
                log_vec: Vec::<String>::new(),
            }
        }
        fn log(&mut self, s: &str) {
            self.log_vec.push(s.to_string());
        }
        fn assert_logs(&self, logs: &[&str]) {
            assert_eq!(self.log_vec.len(), logs.len());
            for i in 0..self.log_vec.len() {
                assert_eq!(self.log_vec[i], logs[i]);
            }
        }
    }

    mod reason_is_enum_with_no_data {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        #[allow(dead_code)]
        #[derive(Debug)]
        enum Enum0 {
            Reason0,
            Other,
        }
        impl Drop for Enum0 {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop Enum0");
            }
        }

        #[allow(dead_code)]
        #[derive(Debug)]
        struct MyError {
            message: String,
        }

        impl MyError {
            fn new(msg: &str) -> Self {
                Self {
                    message: msg.to_string(),
                }
            }
        }

        impl error::Error for MyError {}

        impl fmt::Display for MyError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, "MyError{{message:\"{}\"}}", self.message)
            }
        }

        impl Drop for MyError {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop MyError");
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::with_source(Enum0::Reason0, MyError::new("oh no!"));
            LOGGER.lock().unwrap().log("created Enum0");
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<Enum0>());
            assert!(!err.is_reason::<String>());

            // necessary `use std::error::Error;`
            if let Some(src) = err.source() {
                assert_eq!(format!("{:?}", src), "MyError { message: \"oh no!\" }");
            }

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<Enum0>() {
                    Ok(r) => match r {
                        Enum0::Reason0 => {}
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<Enum0>(|r| match r {
                Enum0::Reason0 => {}
                _ => panic!(),
            });

            assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: sabi::errs::tests_of_err_with_source::reason_is_enum_with_no_data::Enum0 Reason0 }");
            assert_eq!(format!("{err}"), "Reason0");

            LOGGER.lock().unwrap().log("consumed Enum0");
        }

        #[test]
        fn test_err_by_reaason0() {
            consume_err();
            LOGGER.lock().unwrap().log("end");

            LOGGER.lock().unwrap().assert_logs(&[
                "created Enum0",
                "consumed Enum0",
                "drop Enum0",
                "drop MyError",
                "end",
            ]);
        }
    }

    mod reason_is_enum_with_one_value {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        #[allow(dead_code)]
        #[derive(Debug)]
        enum Enum1 {
            Reason1(String),
            Other,
        }
        impl Drop for Enum1 {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop Enum1");
            }
        }

        #[allow(dead_code)]
        #[derive(Debug)]
        struct MyError {
            message: String,
        }

        impl MyError {
            fn new(msg: &str) -> Self {
                Self {
                    message: msg.to_string(),
                }
            }
        }

        impl error::Error for MyError {}

        impl fmt::Display for MyError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, "MyError{{message:\"{}\"}}", self.message)
            }
        }

        impl Drop for MyError {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop MyError");
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::with_source(Enum1::Reason1("hello".to_string()), MyError::new("oh no!"));
            LOGGER.lock().unwrap().log("created Enum1");
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<Enum1>());
            assert!(!err.is_reason::<String>());

            // necessary `use std::error::Error;`
            if let Some(src) = err.source() {
                assert_eq!(format!("{:?}", src), "MyError { message: \"oh no!\" }");
            }

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<Enum1>() {
                    Ok(r) => match r {
                        Enum1::Reason1(s) => assert_eq!(s, "hello"),
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<Enum1>(|r| match r {
                Enum1::Reason1(s) => assert_eq!(s, "hello"),
                _ => panic!(),
            });

            assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: sabi::errs::tests_of_err_with_source::reason_is_enum_with_one_value::Enum1 Reason1(\"hello\") }");
            assert_eq!(format!("{err}"), "Reason1(\"hello\")");

            LOGGER.lock().unwrap().log("consumed Enum1");
        }

        #[test]
        fn test_err_by_reaason1() {
            consume_err();
            LOGGER.lock().unwrap().log("end");

            LOGGER.lock().unwrap().assert_logs(&[
                "created Enum1",
                "consumed Enum1",
                "drop Enum1",
                "drop MyError",
                "end",
            ]);
        }
    }

    mod reason_is_enum_with_two_values {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        #[allow(dead_code)]
        #[derive(Debug)]
        enum Enum2 {
            Reason2(String, i32),
            Other,
        }
        impl Drop for Enum2 {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop Enum2");
            }
        }

        #[allow(dead_code)]
        #[derive(Debug)]
        struct MyError {
            message: String,
        }

        impl MyError {
            fn new(msg: &str) -> Self {
                Self {
                    message: msg.to_string(),
                }
            }
        }

        impl error::Error for MyError {}

        impl fmt::Display for MyError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, "MyError{{message:\"{}\"}}", self.message)
            }
        }

        impl Drop for MyError {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop MyError");
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::with_source(
                Enum2::Reason2("hello".to_string(), 123),
                MyError::new("oh no!"),
            );
            LOGGER.lock().unwrap().log("created Enum2");
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<Enum2>());
            assert!(!err.is_reason::<String>());

            // necessary `use std::error::Error;`
            if let Some(src) = err.source() {
                assert_eq!(format!("{:?}", src), "MyError { message: \"oh no!\" }");
            }

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<Enum2>() {
                    Ok(r) => match r {
                        Enum2::Reason2(s, n) => {
                            assert_eq!(s, "hello");
                            assert_eq!(*n, 123);
                        }
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<Enum2>(|r| match r {
                Enum2::Reason2(s, n) => {
                    assert_eq!(s, "hello");
                    assert_eq!(*n, 123);
                }
                _ => panic!(),
            });

            assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: sabi::errs::tests_of_err_with_source::reason_is_enum_with_two_values::Enum2 Reason2(\"hello\", 123) }");
            assert_eq!(format!("{err}"), "Reason2(\"hello\", 123)");

            LOGGER.lock().unwrap().log("consumed Enum2");
        }

        #[test]
        fn test_err_by_reaason2() {
            consume_err();
            LOGGER.lock().unwrap().log("end");

            LOGGER.lock().unwrap().assert_logs(&[
                "created Enum2",
                "consumed Enum2",
                "drop Enum2",
                "drop MyError",
                "end",
            ]);
        }
    }

    mod reason_is_enum_with_named_fields {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        #[allow(dead_code)]
        #[derive(Debug)]
        enum Enum3 {
            Reason3 { x: i32, y: i32 },
            Other,
        }
        impl Drop for Enum3 {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop Enum3");
            }
        }

        #[allow(dead_code)]
        #[derive(Debug)]
        struct MyError {
            message: String,
        }

        impl MyError {
            fn new(msg: &str) -> Self {
                Self {
                    message: msg.to_string(),
                }
            }
        }

        impl error::Error for MyError {}

        impl fmt::Display for MyError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, "MyError{{message:\"{}\"}}", self.message)
            }
        }

        impl Drop for MyError {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop MyError");
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::with_source(Enum3::Reason3 { x: 123, y: 456 }, MyError::new("oh no!"));
            LOGGER.lock().unwrap().log("created Enum3");
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<Enum3>());
            assert!(!err.is_reason::<String>());

            // necessary `use std::error::Error;`
            if let Some(src) = err.source() {
                assert_eq!(format!("{:?}", src), "MyError { message: \"oh no!\" }");
            }

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<Enum3>() {
                    Ok(r) => match r {
                        Enum3::Reason3 { x, y } => {
                            assert_eq!(*x, 123);
                            assert_eq!(*y, 456);
                        }
                        _ => panic!(),
                    },
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<Enum3>(|r| match r {
                Enum3::Reason3 { x, y } => {
                    assert_eq!(*x, 123);
                    assert_eq!(*y, 456);
                }
                _ => panic!(),
            });

            assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: sabi::errs::tests_of_err_with_source::reason_is_enum_with_named_fields::Enum3 Reason3 { x: 123, y: 456 } }");
            assert_eq!(format!("{err}"), "Reason3 { x: 123, y: 456 }");

            LOGGER.lock().unwrap().log("consumed Enum3");
        }

        #[test]
        fn test_err_by_reaason3() {
            consume_err();
            LOGGER.lock().unwrap().log("end");

            LOGGER.lock().unwrap().assert_logs(&[
                "created Enum3",
                "consumed Enum3",
                "drop Enum3",
                "drop MyError",
                "end",
            ]);
        }
    }

    mod reason_is_number {
        use super::*;

        #[allow(dead_code)]
        #[derive(Debug)]
        struct MyError {
            message: String,
        }

        impl MyError {
            fn new(msg: &str) -> Self {
                Self {
                    message: msg.to_string(),
                }
            }
        }

        impl error::Error for MyError {}

        impl fmt::Display for MyError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, "MyError{{message:\"{}\"}}", self.message)
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::with_source(123_456, MyError::new("oh no!"));
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<i32>());
            assert!(!err.is_reason::<String>());

            // necessary `use std::error::Error;`
            if let Some(src) = err.source() {
                assert_eq!(format!("{:?}", src), "MyError { message: \"oh no!\" }");
            }

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<i32>() {
                    Ok(n) => assert_eq!(*n, 123_456),
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<i32>(|n| {
                assert_eq!(*n, 123_456);
            });

            assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: i32 123456 }");
            assert_eq!(format!("{err}"), "123456");
        }

        #[test]
        fn test_err_by_reaason4() {
            consume_err();
        }
    }

    mod reason_is_string {
        use super::*;

        #[allow(dead_code)]
        #[derive(Debug)]
        struct MyError {
            message: String,
        }

        impl MyError {
            fn new(msg: &str) -> Self {
                Self {
                    message: msg.to_string(),
                }
            }
        }

        impl error::Error for MyError {}

        impl fmt::Display for MyError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, "MyError{{message:\"{}\"}}", self.message)
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::with_source("Hello, world!".to_string(), MyError::new("oh no!"));
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(!err.is_reason::<i32>());
            assert!(err.is_reason::<String>());

            // necessary `use std::error::Error;`
            if let Some(src) = err.source() {
                assert_eq!(format!("{:?}", src), "MyError { message: \"oh no!\" }");
            }

            match err.reason::<String>() {
                Ok(s) => assert_eq!(s, "Hello, world!"),
                Err(err) => match err.reason::<i32>() {
                    Ok(_n) => panic!(),
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|s| {
                assert_eq!(s, "Hello, world!");
            })
            .match_reason::<i32>(|_n| {
                panic!();
            });

            assert_eq!(
                format!("{err:?}"),
                "sabi::errs::Err { reason: alloc::string::String \"Hello, world!\" }"
            );
            assert_eq!(format!("{err}"), "\"Hello, world!\"");
        }

        #[test]
        fn test_err_by_reaason5() {
            consume_err();
        }
    }

    mod reason_is_struct {
        use super::*;

        static LOGGER: LazyLock<Mutex<Logger>> = LazyLock::new(|| Mutex::new(Logger::new()));

        #[allow(dead_code)]
        #[derive(Debug)]
        struct Struct6 {
            s: String,
            b: bool,
            n: i64,
        }
        impl Drop for Struct6 {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop Struct6");
            }
        }

        #[allow(dead_code)]
        #[derive(Debug)]
        struct MyError {
            message: String,
        }

        impl MyError {
            fn new(msg: &str) -> Self {
                Self {
                    message: msg.to_string(),
                }
            }
        }

        impl error::Error for MyError {}

        impl fmt::Display for MyError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                write!(f, "MyError{{message:\"{}\"}}", self.message)
            }
        }

        impl Drop for MyError {
            fn drop(&mut self) {
                LOGGER.lock().unwrap().log("drop MyError");
            }
        }

        fn create_err() -> Result<(), Err> {
            let err = Err::with_source(
                Struct6 {
                    s: "hello".to_string(),
                    b: true,
                    n: 987,
                },
                MyError::new("oh no!"),
            );
            LOGGER.lock().unwrap().log("created Struct6");
            Err(err)
        }

        fn consume_err() {
            let err = create_err().unwrap_err();

            assert!(err.is_reason::<Struct6>());
            assert!(!err.is_reason::<String>());

            // necessary `use std::error::Error;`
            if let Some(src) = err.source() {
                assert_eq!(format!("{:?}", src), "MyError { message: \"oh no!\" }");
            }

            match err.reason::<String>() {
                Ok(_) => panic!(),
                Err(err) => match err.reason::<Struct6>() {
                    Ok(r) => {
                        assert_eq!(r.s, "hello");
                        assert_eq!(r.b, true);
                        assert_eq!(r.n, 987);
                    }
                    Err(_) => panic!(),
                },
            }

            err.match_reason::<String>(|_s| {
                panic!();
            })
            .match_reason::<Struct6>(|r| {
                assert_eq!(r.s, "hello");
                assert_eq!(r.b, true);
                assert_eq!(r.n, 987);
            });

            assert_eq!(
                format!("{err:?}"),
                "sabi::errs::Err { reason: sabi::errs::tests_of_err_with_source::reason_is_struct::Struct6 Struct6 { s: \"hello\", b: true, n: 987 } }"
            );
            assert_eq!(
                format!("{err}"),
                "Struct6 { s: \"hello\", b: true, n: 987 }"
            );

            LOGGER.lock().unwrap().log("consumed Struct6");
        }

        #[test]
        fn test_err_by_reaason6() {
            consume_err();
            LOGGER.lock().unwrap().log("end");

            LOGGER.lock().unwrap().assert_logs(&[
                "created Struct6",
                "consumed Struct6",
                "drop Struct6",
                "drop MyError",
                "end",
            ]);
        }
    }
}
