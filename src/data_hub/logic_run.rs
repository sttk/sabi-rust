// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataHub, DataHubError, ErrEntry, LogicErrAt, LogicRun};

use std::mem;

impl<'a, T> LogicRun<'a, T> {
    pub(crate) fn new(hub: &'a mut DataHub<T>, nested: bool) -> LogicRun<'a, T> {
        if !nested {
            if let Err(err) = hub.run.begin() {
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
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub) {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicRun#run(logic-{})", index).into(),
                            err,
                        });
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub fn run_force<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if let Err(err) = logic_fn(self.hub) {
                    errors.push(ErrEntry {
                        index,
                        name: format!("LogicRun#run_force(logic-{})", index).into(),
                        err,
                    });
                }
                self
            }
            _ => self,
        }
    }

    pub fn run_or_block<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        let index = self.index;
        self.index = index + 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub) {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicRun#run_or_block(logic-{})", index).into(),
                            err,
                        });
                        self.err = LogicErrAt::Block {
                            errors: mem::take(errors),
                        };
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub fn end(self) -> errs::Result<()> {
        if !self.nested {
            self.hub.run.end();
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
