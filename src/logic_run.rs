// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{AsDataHub, DataHubError, ErrEntry, LogicErrAt, LogicRun};

use std::{mem, panic};

impl<'a, A: AsDataHub> LogicRun<'a, A> {
    pub(crate) fn new(hub: &'a mut A, nested: bool) -> LogicRun<'a, A> {
        if !nested {
            if let Err(err) = hub.as_hub().run.begin() {
                return Self {
                    hub,
                    err: LogicErrAt::Begin { err },
                    index: 0,
                    nested,
                };
            }
        }

        Self {
            hub,
            err: LogicErrAt::Run {
                errors: Vec::with_capacity(0),
            },
            index: 0,
            nested,
        }
    }

    pub fn run<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut A) -> errs::Result<()>,
    {
        let index = self.index;
        self.index = index + 1;

        if self.hub.as_hub().run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    match panic::catch_unwind(panic::AssertUnwindSafe(|| logic_fn(self.hub))) {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            errors.push(ErrEntry {
                                index,
                                name: format!("LogicRun#run(logic-{})", index).into(),
                                err,
                            });
                        }
                        Err(payload) => {
                            self.hub.as_hub().run.abend();
                            panic::resume_unwind(payload);
                        }
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub fn run_force<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut A) -> errs::Result<()>,
    {
        let index = self.index;
        self.index = index + 1;

        if self.hub.as_hub().run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                match panic::catch_unwind(panic::AssertUnwindSafe(|| logic_fn(self.hub))) {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicRun#run_force(logic-{})", index).into(),
                            err,
                        });
                    }
                    Err(payload) => {
                        self.hub.as_hub().run.abend();
                        panic::resume_unwind(payload);
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub fn run_or_block<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut A) -> errs::Result<()>,
    {
        let index = self.index;
        self.index = index + 1;

        if self.hub.as_hub().run.is_abnormal() {
            return self;
        }

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    match panic::catch_unwind(panic::AssertUnwindSafe(|| logic_fn(self.hub))) {
                        Ok(Ok(_)) => {}
                        Ok(Err(err)) => {
                            errors.push(ErrEntry {
                                index,
                                name: format!("LogicRun#run_or_block(logic-{})", index).into(),
                                err,
                            });
                            self.err = LogicErrAt::Block {
                                errors: mem::take(errors),
                            };
                        }
                        Err(payload) => {
                            self.hub.as_hub().run.abend();
                            panic::resume_unwind(payload);
                        }
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub fn end(self) -> errs::Result<()> {
        if self.hub.as_hub().run.is_abnormal() {
            return Ok(());
        }

        if !self.nested {
            match panic::catch_unwind(panic::AssertUnwindSafe(|| {
                self.hub.as_hub().run.end();
            })) {
                Ok(_) => {}
                Err(payload) => {
                    self.hub.as_hub().run.abend();
                    panic::resume_unwind(payload);
                }
            }
        }

        match self.err {
            LogicErrAt::Begin { err } => Err(err),
            LogicErrAt::Run { errors } => {
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
                }
            }
            LogicErrAt::Block { errors } => {
                Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
            }
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test;
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod _test_panic;
