// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use chrono;
use std::ptr;
use std::sync;
use std::thread;

/// Represents the content of an `Err` instance.
///
/// It contains the type string of the reason, the content string of the reason, and the optional
/// content string of the source error.
pub struct ErrContent {
    /// Type of the reason.
    pub reason_type: &'static str,

    /// String that describes the reason for the error.
    pub reason_string: String,

    /// Optional string containing the source of the error.
    pub source_string: Option<String>,
}

/// Represents the details of an error occurence, including the time it occureed, and the source
/// file and line number.
pub struct ErrOccasion {
    /// The name of the source file where the error occured.
    pub file: &'static str,

    /// The line number in the source file where the error occurred.
    pub line: u32,

    // The time when the error occurred, in UTC.
    pub time: chrono::DateTime<chrono::Utc>,
}

struct ErrHandler {
    handle: fn(cont: &ErrContent, occ: &ErrOccasion),
    next: *mut ErrHandler,
}

impl ErrHandler {
    fn new(handle: fn(cont: &ErrContent, occ: &ErrOccasion)) -> Self {
        Self {
            handle,
            next: ptr::null_mut(),
        }
    }
}

pub(crate) struct ErrNotifier {
    sync_list_head: *mut ErrHandler,
    sync_list_last: *mut ErrHandler,
    async_list_head: *mut ErrHandler,
    async_list_last: *mut ErrHandler,
    fixed: bool,
}

impl ErrNotifier {
    const fn new() -> Self {
        Self {
            sync_list_head: ptr::null_mut(),
            sync_list_last: ptr::null_mut(),
            async_list_head: ptr::null_mut(),
            async_list_last: ptr::null_mut(),
            fixed: false,
        }
    }

    fn add_sync(&mut self, handle: fn(cont: &ErrContent, occ: &ErrOccasion)) {
        if self.fixed {
            return;
        }

        let boxed = Box::new(ErrHandler::new(handle));
        let ptr = Box::into_raw(boxed);

        if self.sync_list_last.is_null() {
            self.sync_list_head = ptr;
            self.sync_list_last = ptr;
        } else {
            unsafe {
                (*self.sync_list_last).next = ptr;
            }
            self.sync_list_last = ptr;
        }
    }

    fn add_async(&mut self, handle: fn(cont: &ErrContent, occ: &ErrOccasion)) {
        if self.fixed {
            return;
        }

        let boxed = Box::new(ErrHandler::new(handle));
        let ptr = Box::into_raw(boxed);

        if self.async_list_last.is_null() {
            self.async_list_head = ptr;
            self.async_list_last = ptr;
        } else {
            unsafe {
                (*self.async_list_last).next = ptr;
            }
            self.async_list_last = ptr;
        }
    }

    fn fix(&mut self) {
        self.fixed = true;
    }

    #[warn(static_mut_refs)]
    pub(crate) fn will_notify(&self) -> bool {
        if !self.fixed {
            return false;
        }

        if self.sync_list_head.is_null() && self.async_list_head.is_null() {
            return false;
        }

        return true;
    }

    #[warn(static_mut_refs)]
    pub(crate) fn notify(&self, cont: ErrContent, occ: ErrOccasion) {
        if !self.fixed {
            return;
        }

        let mut ptr = self.sync_list_head;
        while !ptr.is_null() {
            let next = unsafe { (*ptr).next };
            unsafe {
                ((*ptr).handle)(&cont, &occ);
            }
            ptr = next;
        }

        let cont = sync::Arc::new(cont);
        let occ = sync::Arc::new(occ);

        let mut ptr = self.async_list_head;
        while !ptr.is_null() {
            let next = unsafe { (*ptr).next };
            let handle = unsafe { (*ptr).handle };
            let cont_a = sync::Arc::clone(&cont);
            let occ_a = sync::Arc::clone(&occ);
            let _ = thread::spawn(move || handle(&cont_a, &occ_a));
            ptr = next;
        }
    }
}

pub(crate) static mut ERR_NOTIFIER: ErrNotifier = ErrNotifier::new();

/// Adds a new synchronous error handler to the global handler list.
///
/// It will not add the handler if the handlers have been fixed using `fix_err_handlers`.
pub fn add_sync_err_handler(handle: fn(cont: &ErrContent, occ: &ErrOccasion)) {
    unsafe {
        ERR_NOTIFIER.add_sync(handle);
    }
}

/// Adds a new asynchronous error handler to the global handler list.
///
/// It will not add the handler if the handlers have been fixed using `fix_err_handlers`.
pub fn add_async_err_handler(handle: fn(cont: &ErrContent, occ: &ErrOccasion)) {
    unsafe {
        ERR_NOTIFIER.add_async(handle);
    }
}

/// Prevents modification of the error handler lists.
///
/// Before this is called, no `Err` is nofified to the handlers
/// After this is caled, no new handlers can be added, and `Err`(s) is notified to the handlers.
pub fn fix_err_handlers() {
    unsafe {
        ERR_NOTIFIER.fix();
    }
}

#[cfg(test)]
mod tests_of_notify {
    use super::*;
    use chrono::TimeZone;
    use std::sync::{LazyLock, Mutex};

    mod sync_handler {
        use super::*;

        #[test]
        fn dont_notify_if_not_fix() {
            static LOGGER: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

            let mut notifier = ErrNotifier::new();

            fn handle1(cont: &ErrContent, occ: &ErrOccasion) {
                LOGGER.lock().unwrap().push(format!(
                    "1: content = {{ {}, {} }}, occasion = {{ {}, {}, {} }}",
                    cont.reason_type, cont.reason_string, occ.file, occ.line, occ.time
                ));
            }
            fn handle2(cont: &ErrContent, occ: &ErrOccasion) {
                LOGGER.lock().unwrap().push(format!(
                    "2: content = {{ {}, {} }}, occasion = {{ {}, {}, {} }}",
                    cont.reason_type, cont.reason_string, occ.file, occ.line, occ.time
                ));
            }
            notifier.add_sync(handle1);
            notifier.add_sync(handle2);

            let cont = ErrContent {
                reason_type: "reason/type",
                reason_string: "ReasonValue".to_string(),
                source_string: None,
            };
            let occ = ErrOccasion {
                file: "a/b/c",
                line: 123,
                time: chrono::Utc
                    .with_ymd_and_hms(2025, 1, 19, 11, 22, 33)
                    .unwrap(),
            };

            notifier.notify(cont, occ);

            assert!(LOGGER.lock().unwrap().is_empty());
        }

        #[test]
        fn notify() {
            static LOGGER: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

            let mut notifier = ErrNotifier::new();

            fn handle1(cont: &ErrContent, occ: &ErrOccasion) {
                LOGGER.lock().unwrap().push(format!(
                    "1: content = {{ {}, {} }}, occasion = {{ {}, {}, {} }}",
                    cont.reason_type, cont.reason_string, occ.file, occ.line, occ.time
                ));
            }
            fn handle2(cont: &ErrContent, occ: &ErrOccasion) {
                LOGGER.lock().unwrap().push(format!(
                    "2: content = {{ {}, {} }}, occasion = {{ {}, {}, {} }}",
                    cont.reason_type, cont.reason_string, occ.file, occ.line, occ.time
                ));
            }
            notifier.add_sync(handle1);
            notifier.add_sync(handle2);

            let cont = ErrContent {
                reason_type: "reason/type",
                reason_string: "ReasonValue".to_string(),
                source_string: None,
            };
            let occ = ErrOccasion {
                file: "a/b/c",
                line: 123,
                time: chrono::Utc
                    .with_ymd_and_hms(2025, 1, 19, 11, 22, 33)
                    .unwrap(),
            };

            notifier.fix();

            notifier.notify(cont, occ);

            assert_eq!(LOGGER.lock().unwrap().len(), 2);
            assert_eq!(LOGGER.lock().unwrap()[0], "1: content = { reason/type, ReasonValue }, occasion = { a/b/c, 123, 2025-01-19 11:22:33 UTC }");
            assert_eq!(LOGGER.lock().unwrap()[1], "2: content = { reason/type, ReasonValue }, occasion = { a/b/c, 123, 2025-01-19 11:22:33 UTC }");
        }
    }

    mod async_handler {
        use super::*;
        use std::thread;
        use std::time;

        #[test]
        fn dont_notify_if_not_fix() {
            static LOGGER: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

            let mut notifier = ErrNotifier::new();

            fn handle1(cont: &ErrContent, occ: &ErrOccasion) {
                thread::sleep(time::Duration::from_millis(100));
                LOGGER.lock().unwrap().push(format!(
                    "1: content = {{ {}, {} }}, occasion = {{ {}, {}, {} }}",
                    cont.reason_type, cont.reason_string, occ.file, occ.line, occ.time
                ));
            }
            fn handle2(cont: &ErrContent, occ: &ErrOccasion) {
                thread::sleep(time::Duration::from_millis(10));
                LOGGER.lock().unwrap().push(format!(
                    "2: content = {{ {}, {} }}, occasion = {{ {}, {}, {} }}",
                    cont.reason_type, cont.reason_string, occ.file, occ.line, occ.time
                ));
            }
            notifier.add_async(handle1);
            notifier.add_async(handle2);

            let cont = ErrContent {
                reason_type: "reason/type",
                reason_string: "ReasonValue".to_string(),
                source_string: None,
            };
            let occ = ErrOccasion {
                file: "a/b/c",
                line: 123,
                time: chrono::Utc
                    .with_ymd_and_hms(2025, 1, 19, 11, 22, 33)
                    .unwrap(),
            };

            notifier.notify(cont, occ);

            assert!(LOGGER.lock().unwrap().is_empty());
        }

        #[test]
        fn notify() {
            static LOGGER: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

            let mut notifier = ErrNotifier::new();

            fn handle1(cont: &ErrContent, occ: &ErrOccasion) {
                thread::sleep(time::Duration::from_millis(100));
                LOGGER.lock().unwrap().push(format!(
                    "1: content = {{ {}, {} }}, occasion = {{ {}, {}, {} }}",
                    cont.reason_type, cont.reason_string, occ.file, occ.line, occ.time
                ));
            }
            fn handle2(cont: &ErrContent, occ: &ErrOccasion) {
                thread::sleep(time::Duration::from_millis(10));
                LOGGER.lock().unwrap().push(format!(
                    "2: content = {{ {}, {} }}, occasion = {{ {}, {}, {} }}",
                    cont.reason_type, cont.reason_string, occ.file, occ.line, occ.time
                ));
            }
            notifier.add_async(handle1);
            notifier.add_async(handle2);

            let cont = ErrContent {
                reason_type: "reason/type",
                reason_string: "ReasonValue".to_string(),
                source_string: None,
            };
            let occ = ErrOccasion {
                file: "a/b/c",
                line: 123,
                time: chrono::Utc
                    .with_ymd_and_hms(2025, 1, 19, 11, 22, 33)
                    .unwrap(),
            };

            notifier.fix();

            notifier.notify(cont, occ);

            thread::sleep(time::Duration::from_millis(200));

            assert_eq!(LOGGER.lock().unwrap().len(), 2);
            assert_eq!(LOGGER.lock().unwrap()[0], "2: content = { reason/type, ReasonValue }, occasion = { a/b/c, 123, 2025-01-19 11:22:33 UTC }");
            assert_eq!(LOGGER.lock().unwrap()[1], "1: content = { reason/type, ReasonValue }, occasion = { a/b/c, 123, 2025-01-19 11:22:33 UTC }");
        }
    }
}

#[cfg(test)]
mod tests_of_err {
    use super::*;
    use crate::Err;
    use std::io;
    use std::sync::{LazyLock, Mutex};
    use std::thread;
    use std::time;

    #[derive(Debug)]
    enum Errors {
        FailToDoSomething,
    }

    #[test]
    fn test() {
        static LOGGER: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

        fn handle1(cont: &ErrContent, occ: &ErrOccasion) {
            LOGGER.lock().unwrap().push(format!(
                "A: content = {{ {}, {} }}, occasion = {{ {}, {} }}",
                cont.reason_type, cont.reason_string, occ.file, occ.line,
            ));
        }

        fn handle2(cont: &ErrContent, occ: &ErrOccasion) {
            LOGGER.lock().unwrap().push(format!(
                "B: content = {{ {}, {} }}, occasion = {{ {}, {} }}",
                cont.reason_type, cont.reason_string, occ.file, occ.line,
            ));
        }

        add_sync_err_handler(handle1);
        add_async_err_handler(handle2);
        fix_err_handlers();

        let _ = Err::new(Errors::FailToDoSomething);

        let error = io::Error::new(io::ErrorKind::NotFound, "oh no!");
        let _ = Err::with_source(Errors::FailToDoSomething, error);

        thread::sleep(time::Duration::from_millis(200));

        assert_logs();

        #[cfg(unix)]
        fn assert_logs() {
            let n = LOGGER.lock().unwrap().len();
            assert_ne!(n, 0);

            assert!(LOGGER.lock().unwrap().contains(&"A: content = { sabi::err::notify::tests_of_err::Errors, FailToDoSomething }, occasion = { src/err/notify.rs, 409 }".to_string()));
            assert!(LOGGER.lock().unwrap().contains(&"B: content = { sabi::err::notify::tests_of_err::Errors, FailToDoSomething }, occasion = { src/err/notify.rs, 409 }".to_string()));
            assert!(LOGGER.lock().unwrap().contains(&"A: content = { sabi::err::notify::tests_of_err::Errors, FailToDoSomething }, occasion = { src/err/notify.rs, 412 }".to_string()));
            assert!(LOGGER.lock().unwrap().contains(&"B: content = { sabi::err::notify::tests_of_err::Errors, FailToDoSomething }, occasion = { src/err/notify.rs, 412 }".to_string()));
        }

        #[cfg(windows)]
        fn assert_logs() {
            let n = LOGGER.lock().unwrap().len();
            assert_ne!(n, 0);

            assert!(LOGGER.lock().unwrap().contains(&"A: content = { sabi::err::notify::tests_of_err::Errors, FailToDoSomething }, occasion = { src\\err\\notify.rs, 409 }".to_string()));
            assert!(LOGGER.lock().unwrap().contains(&"B: content = { sabi::err::notify::tests_of_err::Errors, FailToDoSomething }, occasion = { src\\err\\notify.rs, 409 }".to_string()));
            assert!(LOGGER.lock().unwrap().contains(&"A: content = { sabi::err::notify::tests_of_err::Errors, FailToDoSomething }, occasion = { src\\err\\notify.rs, 412 }".to_string()));
            assert!(LOGGER.lock().unwrap().contains(&"B: content = { sabi::err::notify::tests_of_err::Errors, FailToDoSomething }, occasion = { src\\err\\notify.rs, 412 }".to_string()));
        }
    }
}
