// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use chrono;
use futures::future;
use std::future::Future;
use std::pin::Pin;
use std::ptr;
use std::sync;
use std::thread;
use tokio::runtime;

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

static FIXED: sync::OnceLock<()> = sync::OnceLock::new();

static mut SYNC_LIST_HEAD: *mut ErrHandler = ptr::null_mut();
static mut SYNC_LIST_LAST: *mut ErrHandler = ptr::null_mut();
static mut ASYNC_LIST_HEAD: *mut ErrHandler = ptr::null_mut();
static mut ASYNC_LIST_LAST: *mut ErrHandler = ptr::null_mut();

/// Adds a new synchronous error handler to the global handler list.
///
/// It will not add the handler if the handlers have been fixed using `fix_err_handlers`.
pub fn add_sync_err_handler(handle: fn(cont: &ErrContent, occ: &ErrOccasion)) {
    if !FIXED.get().is_none() {
        return;
    }

    let boxed = Box::new(ErrHandler::new(handle));
    let ptr = Box::into_raw(boxed);

    unsafe {
        if SYNC_LIST_LAST.is_null() {
            SYNC_LIST_HEAD = ptr;
            SYNC_LIST_LAST = ptr;
        } else {
            (*SYNC_LIST_LAST).next = ptr;
            SYNC_LIST_LAST = ptr;
        }
    }
}

/// Adds a new asynchronous error handler to the global handler list.
///
/// It will not add the handler if the handlers have been fixed using `fix_err_handlers`.
pub fn add_async_err_handler(handle: fn(cont: &ErrContent, occ: &ErrOccasion)) {
    if !FIXED.get().is_none() {
        return;
    }

    let boxed = Box::new(ErrHandler::new(handle));
    let ptr = Box::into_raw(boxed);

    unsafe {
        if ASYNC_LIST_LAST.is_null() {
            ASYNC_LIST_HEAD = ptr;
            ASYNC_LIST_LAST = ptr;
        } else {
            (*ASYNC_LIST_LAST).next = ptr;
            ASYNC_LIST_LAST = ptr;
        }
    }
}

/// Prevents modification of the error handler lists.
///
/// Before this is called, no `Err` is nofified to the handlers
/// After this is caled, no new handlers can be added, and `Err`(s) is notified to the handlers.
pub fn fix_err_handlers() {
    let _ = FIXED.set(());
}

pub(crate) fn can_notify() -> bool {
    if FIXED.get().is_none() {
        return false;
    }

    unsafe {
        if SYNC_LIST_HEAD.is_null() && ASYNC_LIST_HEAD.is_null() {
            return false;
        }
    }

    return true;
}

pub(crate) fn notify_err(cont: ErrContent, occ: ErrOccasion) {
    if FIXED.get().is_none() {
        return;
    }

    unsafe {
        let mut ptr = SYNC_LIST_HEAD;
        while !ptr.is_null() {
            let next = (*ptr).next;
            ((*ptr).handle)(&cont, &occ);
            ptr = next;
        }

        if !ASYNC_LIST_HEAD.is_null() {
            // because there is no need to wait for finishing
            let _ = thread::spawn(move || {
                if let Ok(rt) = runtime::Runtime::new() {
                    let cont = sync::Arc::new(cont);
                    let occ = sync::Arc::new(occ);
                    rt.block_on(async {
                        let mut ptr = ASYNC_LIST_HEAD;
                        let mut fut_vec: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
                        while !ptr.is_null() {
                            let next = (*ptr).next;
                            let handle = (*ptr).handle;
                            let c = sync::Arc::clone(&cont);
                            let o = sync::Arc::clone(&occ);
                            let fut = Box::pin(async move {
                                handle(&c, &o);
                            });
                            fut_vec.push(fut);
                            ptr = next;
                        }
                        future::join_all(fut_vec).await;
                    });
                }
            });
        }
    }
}

#[cfg(test)]
mod tests_of_notify {
    use super::*;
    use crate::Err;
    use std::sync::{LazyLock, Mutex};

    static LOGGER: LazyLock<Mutex<Vec<String>>> = LazyLock::new(|| Mutex::new(Vec::new()));

    fn handle1(cont: &ErrContent, occ: &ErrOccasion) {
        LOGGER.lock().unwrap().push(format!(
            "1: content = {{ {}, {} }}, occasion = {{ {}, {} }}",
            cont.reason_type, cont.reason_string, occ.file, occ.line,
        ));
    }
    fn handle2(cont: &ErrContent, occ: &ErrOccasion) {
        LOGGER.lock().unwrap().push(format!(
            "2: content = {{ {}, {} }}, occasion = {{ {}, {} }}",
            cont.reason_type, cont.reason_string, occ.file, occ.line,
        ));
    }
    fn handle3(cont: &ErrContent, occ: &ErrOccasion) {
        LOGGER.lock().unwrap().push(format!(
            "3: content = {{ {}, {} }}, occasion = {{ {}, {} }}",
            cont.reason_type, cont.reason_string, occ.file, occ.line,
        ));
    }
    fn handle4(cont: &ErrContent, occ: &ErrOccasion) {
        LOGGER.lock().unwrap().push(format!(
            "4: content = {{ {}, {} }}, occasion = {{ {}, {} }}",
            cont.reason_type, cont.reason_string, occ.file, occ.line,
        ));
    }

    #[derive(Debug)]
    enum Errors {
        FailToDoSomething,
    }

    #[allow(static_mut_refs)]
    #[test]
    fn test() {
        unsafe {
            assert!(SYNC_LIST_HEAD.is_null());
            assert!(SYNC_LIST_LAST.is_null());
            assert!(ASYNC_LIST_HEAD.is_null());
            assert!(ASYNC_LIST_LAST.is_null());

            assert!(FIXED.get().is_none());
            assert!(!can_notify());
        }

        let _ = Err::new(Errors::FailToDoSomething);
        let n = LOGGER.lock().unwrap().len();
        assert_eq!(n, 0);

        ////

        add_sync_err_handler(handle1);

        unsafe {
            assert!(!SYNC_LIST_HEAD.is_null());
            assert!(!SYNC_LIST_LAST.is_null());

            assert_eq!(SYNC_LIST_HEAD, SYNC_LIST_LAST);
            assert!((*SYNC_LIST_HEAD).next.is_null());
            assert!((*SYNC_LIST_LAST).next.is_null());

            assert!(ASYNC_LIST_HEAD.is_null());
            assert!(ASYNC_LIST_LAST.is_null());

            assert!(FIXED.get().is_none());
            assert!(!can_notify());
        }

        let _ = Err::new(Errors::FailToDoSomething);
        let n = LOGGER.lock().unwrap().len();
        assert_eq!(n, 0);

        ////

        add_sync_err_handler(handle2);

        unsafe {
            assert!(!SYNC_LIST_HEAD.is_null());
            assert!(!SYNC_LIST_LAST.is_null());

            assert_eq!((*SYNC_LIST_HEAD).next, SYNC_LIST_LAST);
            assert!((*SYNC_LIST_LAST).next.is_null());

            assert!(ASYNC_LIST_HEAD.is_null());
            assert!(ASYNC_LIST_LAST.is_null());

            assert!(FIXED.get().is_none());
            assert!(!can_notify());
        }

        let _ = Err::new(Errors::FailToDoSomething);
        let n = LOGGER.lock().unwrap().len();
        assert_eq!(n, 0);

        ////

        add_async_err_handler(handle3);

        unsafe {
            assert!(!SYNC_LIST_HEAD.is_null());
            assert!(!SYNC_LIST_LAST.is_null());

            assert_eq!((*SYNC_LIST_HEAD).next, SYNC_LIST_LAST);
            assert!((*SYNC_LIST_LAST).next.is_null());

            assert!(!ASYNC_LIST_HEAD.is_null());
            assert!(!ASYNC_LIST_LAST.is_null());

            assert_eq!(ASYNC_LIST_HEAD, ASYNC_LIST_LAST);
            assert!((*ASYNC_LIST_HEAD).next.is_null());
            assert!((*ASYNC_LIST_LAST).next.is_null());

            assert!(FIXED.get().is_none());
            assert!(!can_notify());
        }

        let _ = Err::new(Errors::FailToDoSomething);
        let n = LOGGER.lock().unwrap().len();
        assert_eq!(n, 0);

        ////

        add_async_err_handler(handle4);

        unsafe {
            assert!(!SYNC_LIST_HEAD.is_null());
            assert!(!SYNC_LIST_LAST.is_null());

            let handle = SYNC_LIST_HEAD;
            assert_eq!((*handle).next, SYNC_LIST_LAST);
            assert!((*SYNC_LIST_LAST).next.is_null());

            assert!(!ASYNC_LIST_HEAD.is_null());
            assert!(!ASYNC_LIST_LAST.is_null());

            assert_eq!((*ASYNC_LIST_HEAD).next, ASYNC_LIST_LAST);
            assert!((*ASYNC_LIST_LAST).next.is_null());

            assert!(FIXED.get().is_none());
            assert!(!can_notify());
        }

        let _ = Err::new(Errors::FailToDoSomething);
        let n = LOGGER.lock().unwrap().len();
        assert_eq!(n, 0);

        ////

        fix_err_handlers();

        unsafe {
            assert!(!SYNC_LIST_HEAD.is_null());
            assert!(!SYNC_LIST_LAST.is_null());

            let handle = SYNC_LIST_HEAD;
            assert_eq!((*handle).next, SYNC_LIST_LAST);
            assert!((*SYNC_LIST_LAST).next.is_null());

            assert!(!ASYNC_LIST_HEAD.is_null());
            assert!(!ASYNC_LIST_LAST.is_null());

            assert_eq!((*ASYNC_LIST_HEAD).next, ASYNC_LIST_LAST);
            assert!((*ASYNC_LIST_LAST).next.is_null());

            assert!(!FIXED.get().is_none());
            assert!(can_notify());
        }

        let _ = Err::new(Errors::FailToDoSomething);
        let n = LOGGER.lock().unwrap().len();

        // Since tests are executed in parallel, errors from other tests may write to the logs
        assert_ne!(n, 0);

        #[cfg(unix)]
        {
            assert!(LOGGER.lock().unwrap().contains(&String::from("1: content = { sabi::err::notify::tests_of_notify::Errors, FailToDoSomething }, occasion = { src/err/notify.rs, 344 }")));
            assert!(LOGGER.lock().unwrap().contains(&String::from("2: content = { sabi::err::notify::tests_of_notify::Errors, FailToDoSomething }, occasion = { src/err/notify.rs, 344 }")));
        }
        #[cfg(windows)]
        {
            assert!(LOGGER.lock().unwrap().contains(&String::from("1: content = { sabi::err::notify::tests_of_notify::Errors, FailToDoSomething }, occasion = { src\\err\\notify.rs, 344 }")));
            assert!(LOGGER.lock().unwrap().contains(&String::from("2: content = { sabi::err::notify::tests_of_notify::Errors, FailToDoSomething }, occasion = { src\\err\\notify.rs, 344 }")));
        }

        thread::sleep(std::time::Duration::from_millis(200));

        #[cfg(unix)]
        {
            assert!(LOGGER.lock().unwrap().contains(&String::from("3: content = { sabi::err::notify::tests_of_notify::Errors, FailToDoSomething }, occasion = { src/err/notify.rs, 344 }")));
            assert!(LOGGER.lock().unwrap().contains(&String::from("4: content = { sabi::err::notify::tests_of_notify::Errors, FailToDoSomething }, occasion = { src/err/notify.rs, 344 }")));
        }
        #[cfg(windows)]
        {
            assert!(LOGGER.lock().unwrap().contains(&String::from("3: content = { sabi::err::notify::tests_of_notify::Errors, FailToDoSomething }, occasion = { src\\err\\notify.rs, 344 }")));
            assert!(LOGGER.lock().unwrap().contains(&String::from("4: content = { sabi::err::notify::tests_of_notify::Errors, FailToDoSomething }, occasion = { src\\err\\notify.rs, 344 }")));
        }

        ////

        unsafe {
            SYNC_LIST_HEAD = ptr::null_mut();
            SYNC_LIST_LAST = ptr::null_mut();
            ASYNC_LIST_HEAD = ptr::null_mut();
            ASYNC_LIST_LAST = ptr::null_mut();
        }
    }
}
