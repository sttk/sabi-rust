// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataHub, ErrEntry, LogicErrAt, LogicTxn};

impl<'a, T> LogicTxn<'a, T> {
    pub(crate) fn new(hub: &'a mut DataHub<T>) -> LogicTxn<'a, T> {
        if let Err(err) = hub.run.begin() {
            Self {
                hub,
                err: LogicErrAt::Begin { err },
                index: 0,
            }
        } else {
            Self {
                hub,
                err: LogicErrAt::Run {
                    errors: Vec::with_capacity(0),
                },
                index: 0,
            }
        }
    }

    pub fn run<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub<T>) -> errs::Result<()>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            LogicErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub) {
                        errors.push(ErrEntry {
                            index,
                            name: format!("LogicTxn#run(logic-{})", index).into(),
                            err,
                        });
                    }
                }
                self
            }
            _ => self,
        }
    }
}
