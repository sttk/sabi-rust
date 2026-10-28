use super::*;
use std::{sync, time};

#[derive(Debug, PartialEq)]
pub(crate) enum Reasons {
    BadString(String),
}

pub(crate) struct MyStruct {
    pub(crate) string: sync::Arc<sync::Mutex<String>>,
    pub(crate) fail: bool,
}

impl MyStruct {
    pub(crate) fn new(s: String, fail: bool) -> Self {
        Self {
            string: sync::Arc::new(sync::Mutex::new(s)),
            fail,
        }
    }

    pub(crate) fn process(&self, ag: &mut AsyncGroup) {
        let s_mutex = self.string.clone();
        let fail = self.fail;
        ag.add(move || {
            let _ = thread::sleep(time::Duration::from_millis(100));
            {
                let mut s = s_mutex.lock().unwrap();
                if fail {
                    return Err(errs::Err::new(Reasons::BadString(s.to_string())));
                }
                *s = s.to_uppercase();
            }
            Ok(())
        });
    }

    pub(crate) fn process_multiple(&self, ag: &mut AsyncGroup) {
        let s_mutex = self.string.clone();
        let fail = self.fail;
        ag.add(move || {
            let _ = thread::sleep(time::Duration::from_millis(100));
            {
                let mut s = s_mutex.lock().unwrap();
                if fail {
                    return Err(errs::Err::new(Reasons::BadString(s.to_string())));
                }
                *s = s.to_uppercase();
            }
            Ok(())
        });

        let s_mutex = self.string.clone();
        let fail = self.fail;
        ag.add(move || {
            let _ = thread::sleep(time::Duration::from_millis(100));
            {
                let mut s = s_mutex.lock().unwrap();
                if fail {
                    return Err(errs::Err::new(Reasons::BadString(s.to_string())));
                }
                *s = s.to_uppercase();
            }
            Ok(())
        });
    }
}
