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

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_logic_run {
    use super::*;
    use crate::{DataAcc, LogicRunner};
    use crate::_test_commons::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_logic_run_and_ok() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();

            let result = runner
                .start()
                .run_or_block(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Ok(())
                })
                .run(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_force(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                    let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .end();

            assert!(result.is_ok());
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "execute logic-0",
                "SyncDataSrc::create_data_conn 2",
                "SyncDataConn::new 2",
                "execute logic-1",
                "execute logic-2",
                "SyncDataConn::close 2",
                "SyncDataConn::drop 2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_logic_run_but_fail_to_run_but_run_forc_runs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();
            let logger_clone_3 = logger.clone();

            let result = runner
                .start()
                .run(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
                .run(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_or_block(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .run_force(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                    logger_clone_3
                        .lock()
                        .unwrap()
                        .push("execute logic-3".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToRunLogic { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "LogicRun#run(logic-0)".into());
                        assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "execute logic-0",
                "execute logic-3",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_but_failed_to_run_or_block_then_skip_even_run_force() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());

            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();
            let logger_clone_3 = logger.clone();

            let result = runner
                .start()
                .run_or_block(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
                .run(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_force(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .run_or_block(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger_clone_3
                        .lock()
                        .unwrap()
                        .push("execute logic-3".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToRunLogic { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "LogicRun#run_or_block(logic-0)".into());
                        assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "execute logic-0",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }

    #[test]
    fn test_runner_and_failed_to_run_force_then_skip_run_but_run_force_runs() {
        let logger = Arc::new(Mutex::new(Vec::<String>::new()));
        {
            struct A;
            let mut runner = LogicRunner::new(DataHub::<A>::new());
            runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
            runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

            let logger_clone_0 = logger.clone();
            let logger_clone_1 = logger.clone();
            let logger_clone_2 = logger.clone();
            let logger_clone_3 = logger.clone();

            let result = runner
                .start()
                .run_force(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                    logger_clone_0
                        .lock()
                        .unwrap()
                        .push("execute logic-0".to_string());
                    Err(errs::Err::new("logic-0 failed"))
                })
                .run(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger_clone_1
                        .lock()
                        .unwrap()
                        .push("execute logic-1".to_string());
                    Ok(())
                })
                .run_force(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("foo")?;
                    logger_clone_2
                        .lock()
                        .unwrap()
                        .push("execute logic-2".to_string());
                    Ok(())
                })
                .run_or_block(move |data| {
                    let _conn = data.get_data_conn::<SyncDataConn>("bar")?;
                    logger_clone_3
                        .lock()
                        .unwrap()
                        .push("execute logic-3".to_string());
                    Ok(())
                })
                .end();

            if let Err(err) = result {
                match err.reason::<DataHubError>() {
                    Ok(DataHubError::FailToRunLogic { errors }) => {
                        assert_eq!(errors.len(), 1);
                        assert_eq!(errors[0].index, 0);
                        assert_eq!(errors[0].name, "LogicRun#run_force(logic-0)".into());
                        assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"logic-0 failed");
                    }
                    _ => panic!("{err:?}"),
                }
            } else {
                panic!();
            }
        }

        assert_eq!(
            *logger.lock().unwrap(),
            &[
                "SyncDataSrc::new 1",
                "SyncDataSrc::new 2",
                "SyncDataSrc::setup 1",
                "SyncDataSrc::setup 2",
                "SyncDataSrc::create_data_conn 1",
                "SyncDataConn::new 1",
                "execute logic-0",
                "execute logic-2",
                "SyncDataConn::close 1",
                "SyncDataConn::drop 1",
                "SyncDataSrc::close 2",
                "SyncDataSrc::drop 2",
                "SyncDataSrc::close 1",
                "SyncDataSrc::drop 1",
            ]
        );
    }
}
