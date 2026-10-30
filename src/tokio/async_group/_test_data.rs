use super::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time;

#[derive(Debug, PartialEq)]
pub(crate) enum Reasons {
    BadString(String),
}

pub(crate) struct MyStruct {
    pub(crate) string: Arc<Mutex<String>>,
    pub(crate) fail: bool,
}

impl MyStruct {
    pub(crate) fn new(s: String, fail: bool) -> Self {
        Self {
            string: Arc::new(Mutex::new(s)),
            fail,
        }
    }

    pub(crate) fn process(&self, ag: &mut AsyncGroup) {
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

    pub(crate) fn process_multiple(&self, ag: &mut AsyncGroup) {
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
