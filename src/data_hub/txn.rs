// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{
    DataConn, DataHub, DataHubError, DataSrc, ErrEntry, Runner, RunnerErrAt, Txn, TxnDataHub,
};

use std::mem;
use std::sync::Arc;

impl TxnDataHub {
    pub(crate) fn new(hub: DataHub) -> Self {
        Self { hub }
    }

    /// Registers a session-local data source to the [`DataHub`] in this [`TxnDataHub`] instance.
    ///
    /// This method is similar to the global [`uses!`][crate::uses!] macro but registers a data
    /// source
    /// that is local to this specific [`DataHub`] session. Once the [`DataHub`]'s state in this
    /// [`TxnDataHub`] instance is "fixed" (while [`run`][TxnDataHub::run],
    /// [`start`][TxnDataHub::start], [`txn`][TxnDataHub::txn] or
    /// [`begin_txn`][TxnDataHub::begin_txn] method is executing),
    /// further calls to `uses` are ignored. However, after the method completes,
    /// the [`DataHub`]'s "fixed" state is reset, allowing for new data sources to be
    /// registered or removed via [`disuses`][TxnDataHub::disuses] method in subsequent operations.
    ///
    /// # Parameters
    ///
    /// * `name`: The unique name for the local data source.
    /// * `ds`: The [`DataSrc`] instance to register.
    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        self.hub.uses(name, ds)
    }

    /// Unregisters and drops a session-local data source by its name.
    ///
    /// This method removes a data source that was previously registered via
    /// [`uses`][TxnDataHub::uses].
    /// This operation is ignored if the [`DataHub`]'s state in this [`TxnDataHub`] instance is
    /// already "fixed".
    ///
    /// # Parameters
    ///
    /// * `name`: The name of the local data source to unregister.
    pub fn disuses(&mut self, name: impl AsRef<str>) {
        self.hub.disuses(name)
    }

    /// Executes a given logic function without transaction control.
    ///
    /// This method sets up local data sources, runs the provided closure,
    /// and then cleans up the [`TxnDataHub`]'s session resources. It does not
    /// perform commit or rollback operations.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to [`DataHub`] in this [`TxnDataHub`] instance as an
    ///   argument.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: The result of the logic function's execution,
    ///   or an error if executing `logic_fn` fails.
    pub fn run<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        self.hub.run(logic_fn)
    }

    /// Creates a [`Runner`] for executing multiple logic functions.
    ///
    /// The returned [`Runner`] can execute logic functions using method chaining and collect
    /// errors from their execution.
    ///
    /// # Returns
    ///
    /// * `Runner`: The struct that executes logic functions using method chaining.
    pub fn start(&mut self) -> Runner<'_> {
        self.hub.start()
    }

    /// Executes a given logic function within a managed transaction.
    ///
    /// This method starts by setting up local data sources, runs the provided closure,
    /// and then attempts to commit all open data connections in the session.
    ///
    /// If any error occurs during the execution of the closure or during the commit phase,
    /// it initiates a rollback on all data connections and reports the transaction failure details.
    /// Finally, it cleans up session resources.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to [`DataHub`] in this [`TxnDataHub`] instance as an
    ///   argument.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: `Ok(())` if the closure and the commit phase succeed,
    ///   or an [`errs::Err`] if any phase fails.
    pub fn txn<F>(&mut self, mut logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let mut r = self.hub.begin();
        if r.is_ok() {
            r = logic_fn(&mut self.hub);
        }

        let mut reports = self.hub.new_failure_reports();

        if r.is_ok() {
            r = self.hub.commit(&mut reports);
        }
        if r.is_err() {
            self.hub.rollback(reports);
        }

        self.hub.end();
        r
    }

    /// Creates a [`Txn`] for executing multiple logic functions within a managed transaction.
    ///
    /// The returned [`Txn`] can execute logic functions using method chaining and collect
    /// errors from their execution.
    ///
    /// # Returns
    ///
    /// * `Txn`: The struct that executes logic functions using method chaining.
    pub fn begin_txn(&mut self) -> Txn<'_> {
        Txn::new(self)
    }
}

impl<'a> Txn<'a> {
    pub(crate) fn new(txn_hub: &'a mut TxnDataHub) -> Txn<'a> {
        if let Err(err) = txn_hub.hub.begin() {
            Self {
                hub: &mut txn_hub.hub,
                err: RunnerErrAt::Begin { err },
                index: 0,
            }
        } else {
            Self {
                hub: &mut txn_hub.hub,
                err: RunnerErrAt::Run {
                    errors: Vec::with_capacity(0),
                },
                index: 0,
            }
        }
    }

    /// Executes a logic function if no previous logic function has failed.
    ///
    /// If the logic function fails, its error is collected and subsequent calls to
    /// [`run`][Txn::run] and [`run_or_block`][Txn::run_or_block] are skipped.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to [`DataHub`] in the [`TxnDataHub`] instance which created
    ///   this [`Txn`] as an argument.
    pub fn run<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            RunnerErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub) {
                        errors.push(ErrEntry {
                            index,
                            name: format!("Txn#run(logic-{})", index).into(),
                            err,
                        });
                    }
                }
                self
            }
            _ => self,
        }
    }

    /// Executes a logic function regardless of previous logic function errors.
    ///
    /// However, previous [`run_or_block`][Runner::run_or_block] failed, this execution is skipped.
    ///
    /// If the logic function fails, its error is collected, but subsequent calls to
    /// [`run`][Txn::run] and [`run_or_block`][Txn::run_or_block] are skipped.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to [`DataHub`] in the [`TxnDataHub`] instance which created
    ///   this [`Txn`] as an argument.
    pub fn run_force<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            RunnerErrAt::Run { ref mut errors } => {
                if let Err(err) = logic_fn(self.hub) {
                    errors.push(ErrEntry {
                        index,
                        name: format!("Txn#run_force(logic-{})", index).into(),
                        err,
                    });
                }
                self
            }
            _ => self,
        }
    }

    /// Blocks subsequent logic functions if the logic function fails.
    ///
    /// Executes a logic function if no previous logic function has failed.
    ///
    /// If the logic function fails, its error is collected and subsequent calls to
    /// [`run`][Txn::run], [`run_force`][Txn::run_force], and
    /// [`run_or_block`][Txn::run_or_block] are skipped.
    ///
    /// # Parameters
    ///
    /// * `logic_fn`: A closure that encapsulates the business logic to be executed.
    ///   It takes a mutable reference to [`DataHub`] in the [`TxnDataHub`] instance which created
    ///   this [`Txn`] as an argument.
    pub fn run_or_block<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            RunnerErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub) {
                        errors.push(ErrEntry {
                            index,
                            name: format!("Txn#run_or_block(logic-{})", index).into(),
                            err,
                        });
                        self.err = RunnerErrAt::Block {
                            errors: mem::take(errors),
                        };
                    }
                }
                self
            }
            _ => self,
        }
    }

    /// Ends the transaction and returns the transaction result.
    ///
    /// The transaction is committed if all logic functions succeed. Otherwise, it is rolled back.
    ///
    /// If one or more logic functions failed, their errors are returned as
    /// a [`DataHubError::FailToRunLogic`] error.
    ///
    /// # Returns
    ///
    /// * `errs::Result<()>`: The result of the logic function's execution,
    ///   or an error if executing `logic_fn` or transaction control fail.
    pub fn end_txn(self) -> errs::Result<()> {
        match self.err {
            RunnerErrAt::Begin { err } => {
                self.hub.end();
                Err(err)
            }
            RunnerErrAt::Run { errors } => {
                let mut reports = self.hub.new_failure_reports();
                if errors.is_empty() {
                    let result = self.hub.commit(&mut reports);
                    if result.is_err() {
                        self.hub.rollback(reports);
                    }
                    self.hub.end();
                    result
                } else {
                    self.hub.rollback(reports);
                    self.hub.end();
                    Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
                }
            }
            RunnerErrAt::Block { errors } => {
                let reports = self.hub.new_failure_reports();
                self.hub.rollback(reports);
                self.hub.end();
                Err(errs::Err::new(DataHubError::FailToRunLogic { errors }))
            }
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_txn_data_hub {
    use super::*;
    use crate::_test_commons::*;
    use crate::DataAcc;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_disuses() {
        let logger = Arc::new(Mutex::new(Vec::new()));
        let mut data = DataHub::for_txn();
        data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

        assert_eq!(data.hub.local_data_src_manager.vec_unready.len(), 2);
        data.disuses("foo");
        assert_eq!(data.hub.local_data_src_manager.vec_unready.len(), 1);
        data.disuses("bar");
        assert_eq!(data.hub.local_data_src_manager.vec_unready.len(), 0);
    }

    mod test_ok {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_ok)]
        trait HogeData {
            fn process(&mut self) -> errs::Result<()>;
        }

        #[overridable(mod = test_ok)]
        trait FugaData {
            fn get_value(&mut self) -> errs::Result<String>;
            fn set_value(&mut self, v: &str) -> errs::Result<()>;
        }

        fn hoge_logic(data: &mut impl HogeData) -> errs::Result<()> {
            data.process()?;
            Ok(())
        }

        fn fuga_logic(data: &mut impl FugaData) -> errs::Result<()> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Ok(())
        }

        #[overridable(mod = test_ok)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
                Ok("hello".to_string())
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_ok)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
                assert_eq!(text, "hello");
                Ok(())
            }
        }
        impl BarDataAcc for DataHub {}

        #[overridable(mod = test_ok)]
        trait BazDataAcc: DataAcc {
            fn process(&mut self) -> errs::Result<()> {
                self.run(fuga_logic)?;
                Ok(())
            }
        }
        impl BazDataAcc for DataHub {}

        #[override_with(test_ok::FooDataAcc, test_ok::BarDataAcc, test_ok::BazDataAcc)]
        impl test_ok::HogeData for DataHub {}

        #[override_with(test_ok::FooDataAcc, test_ok::BarDataAcc, test_ok::BazDataAcc)]
        impl test_ok::FugaData for DataHub {}

        #[test]
        fn test_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.run(fuga_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_run_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.run(fuga_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_start() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.start().run(fuga_logic).end() {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_start_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.start().run(hoge_logic).end() {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.txn(fuga_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::post_commit 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_txn_with_commit_order() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn_with_commit_order(&["bar", "foo"]);

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.txn(fuga_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::post_commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.txn(hoge_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::post_commit 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_txn_with_nested_run_with_commit_order() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn_with_commit_order(&["bar", "foo"]);

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(_) = data.txn(hoge_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::post_commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_begin_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.begin_txn().run(fuga_logic).end_txn() {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::post_commit 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_begin_txn_with_commir_order() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn_with_commit_order(&["bar", "foo"]);

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.begin_txn().run(fuga_logic).end_txn() {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::post_commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_begin_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.begin_txn().run(hoge_logic).end_txn() {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::post_commit 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_begin_txn_with_nested_run_with_commit_order() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn_with_commit_order(&["bar", "foo"]);

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.begin_txn().run(hoge_logic).end_txn() {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::post_commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }
    }

    mod test_fail_to_run {
        use super::*;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_fail_to_run)]
        trait HogeData {
            fn process(&mut self) -> errs::Result<()>;
        }

        #[overridable(mod = test_fail_to_run)]
        trait FugaData {
            fn get_value(&mut self) -> errs::Result<String>;
            fn set_value(&mut self, v: &str) -> errs::Result<()>;
        }

        fn hoge_logic(data: &mut impl HogeData) -> errs::Result<()> {
            data.process()?;
            Ok(())
        }

        fn fuga_logic(data: &mut impl FugaData) -> errs::Result<()> {
            let v = data.get_value()?;
            let _ = data.set_value(&v);
            Err(errs::Err::new("fail"))
        }

        #[overridable(mod = test_fail_to_run)]
        trait FooDataAcc: DataAcc {
            fn get_value(&mut self) -> errs::Result<String> {
                let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
                Ok("hello".to_string())
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_fail_to_run)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
                assert_eq!(text, "hello");
                Ok(())
            }
        }
        impl BarDataAcc for DataHub {}

        #[overridable(mod = test_fail_to_run)]
        trait BazDataAcc: DataAcc {
            fn process(&mut self) -> errs::Result<()> {
                self.run(fuga_logic)?;
                Ok(())
            }
        }
        impl BazDataAcc for DataHub {}

        #[override_with(
            test_fail_to_run::FooDataAcc,
            test_fail_to_run::BarDataAcc,
            test_fail_to_run::BazDataAcc
        )]
        impl test_fail_to_run::HogeData for DataHub {}

        #[override_with(
            test_fail_to_run::FooDataAcc,
            test_fail_to_run::BarDataAcc,
            test_fail_to_run::BazDataAcc
        )]
        impl test_fail_to_run::FugaData for DataHub {}

        #[test]
        fn test_start() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.start().run(fuga_logic).end() {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Runner#run(logic-0)".into());
                            assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"fail");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_start_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.start().run(hoge_logic).end() {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Runner#run(logic-0)".into());
                            assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"fail");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.txn(fuga_logic) {
                    assert_eq!(err.reason::<&str>().unwrap(), &"fail");
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::rollback 1",
                    "SyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.txn(hoge_logic) {
                    assert_eq!(err.reason::<&str>().unwrap(), &"fail");
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::rollback 1",
                    "SyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_begin_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.begin_txn().run(fuga_logic).end_txn() {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run(logic-0)".into());
                            assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"fail");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::rollback 1",
                    "SyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_begin_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data.begin_txn().run(hoge_logic).end_txn() {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run(logic-0)".into());
                            assert_eq!(errors[0].err.reason::<&str>().unwrap(), &"fail");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "SyncDataConn::rollback 1",
                    "SyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }
    }

    mod test_runner_and_txn {
        use super::*;
        use crate::DataHubError;
        use override_macro::{overridable, override_with};

        #[overridable(mod = test_runner_and_txn)]
        trait HogeData {
            fn hoge_log(&mut self, s: &str) -> errs::Result<()>;
        }

        #[overridable(mod = test_runner_and_txn)]
        trait FugaData {
            fn fuga_log(&mut self, s: &str) -> errs::Result<()>;
        }

        #[overridable(mod = test_runner_and_txn)]
        trait PiyoData {
            fn piyo_log(&mut self, s: &str) -> errs::Result<()>;
        }

        #[overridable(mod = test_runner_and_txn)]
        trait HogeraData {
            fn hogera_log(&mut self, s: &str) -> errs::Result<()>;
        }

        fn hoge_logic(data: &mut impl HogeData) -> errs::Result<()> {
            data.hoge_log("Hoge")
        }

        fn fuga_logic(data: &mut impl FugaData) -> errs::Result<()> {
            data.fuga_log("Fuga")
        }

        fn piyo_logic(data: &mut impl PiyoData) -> errs::Result<()> {
            data.piyo_log("Piyo")
        }

        fn hogera_logic(data: &mut impl HogeraData) -> errs::Result<()> {
            data.hogera_log("Hogera")
        }

        #[overridable(mod = test_runner_and_txn)]
        trait FooDataAcc: DataAcc {
            fn hoge_log(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<SyncDataConn>("foo")?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("foo/hoge/{}", s));
                Ok(())
            }
            fn hogera_log(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<SyncDataConn>("foo")?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("foo/hogera/{}", s));
                Err(errs::Err::new("hogera_logic failed"))
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_runner_and_txn)]
        trait BarDataAcc: DataAcc {
            fn fuga_log(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<SyncDataConn>("bar")?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("bar/fuga/{}", s));
                Ok(())
            }
            fn piyo_log(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<SyncDataConn>("bar")?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("bar/piyo/{}", s));
                Ok(())
            }
        }
        impl BarDataAcc for DataHub {}

        #[override_with(test_runner_and_txn::FooDataAcc, test_runner_and_txn::BarDataAcc)]
        impl test_runner_and_txn::HogeData for DataHub {}

        #[override_with(test_runner_and_txn::FooDataAcc, test_runner_and_txn::BarDataAcc)]
        impl test_runner_and_txn::FugaData for DataHub {}

        #[override_with(test_runner_and_txn::FooDataAcc, test_runner_and_txn::BarDataAcc)]
        impl test_runner_and_txn::PiyoData for DataHub {}

        #[override_with(test_runner_and_txn::FooDataAcc, test_runner_and_txn::BarDataAcc)]
        impl test_runner_and_txn::HogeraData for DataHub {}

        #[test]
        fn runner_ok() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .start()
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end()
                {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn runner_fail_to_start() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));

                if let Err(err) = data
                    .start()
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), &"XXX");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2 failed",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn runner_fail_to_run() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .start()
                    .run(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Runner#run(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn runner_fail_to_run_force() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .start()
                    .run_force(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Runner#run_force(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn runner_fail_to_run_or_block() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .start()
                    .run_or_block(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Runner#run_or_block(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_ok() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn()
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    panic!("{err:?}");
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::post_commit 2",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_fail_to_begin_txn() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));

                if let Err(err) = data
                    .begin_txn()
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), &"XXX");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2 failed",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_fail_to_run() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn()
                    .run(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "SyncDataConn::rollback 1",
                    "SyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_fail_to_run_force() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn()
                    .run_force(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run_force(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "SyncDataConn::rollback 1",
                    "SyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_fail_to_run_or_block() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn()
                    .run_or_block(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 0);
                            assert_eq!(errors[0].name, "Txn#run_or_block(logic-0)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hogera/Hogera",
                    "SyncDataConn::rollback 1",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_fail_to_begin() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Setup));

                if let Err(err) = data.txn(hoge_logic) {
                    match err.reason::<crate::DataHubError>() {
                        Ok(crate::DataHubError::FailToSetupLocalDataSrcs { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), &"XXX");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2 failed",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_fail_to_commit() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::Commit));

                if let Err(err) = data
                    .begin_txn()
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<crate::DataConnError>() {
                        Ok(crate::DataConnError::FailToCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "ZZZ");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 2 failed",
                    "SyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/_test_commons.rs, line = 59 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src/_test_commons.rs, line = 59 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 2 failed",
                    "SyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\_test_commons.rs, line = 59 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: CommitFailure(errs::Err { reason = alloc::string::String \"ZZZ\", file = src\\_test_commons.rs, line = 59 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
        }

        #[test]
        fn txn_fail_to_pre_commit() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PreCommit));

                if let Err(err) = data
                    .begin_txn()
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<crate::DataConnError>() {
                        Ok(crate::DataConnError::FailToPreCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "zzz");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2 failed",
                    "SyncDataConn::rollback 1",
                    "SyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/_test_commons.rs, line = 75 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src/_test_commons.rs, line = 75 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2 failed",
                    "SyncDataConn::rollback 1",
                    "SyncDataConn::rollback 2",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\_test_commons.rs, line = 75 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: NoneByRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: LogicFailure(errs::Err { reason = alloc::string::String \"zzz\", file = src\\_test_commons.rs, line = 75 }), rollback: NoneByRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
        }

        #[test]
        fn txn_fail_to_post_commit() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::PostCommit));

                if let Err(err) = data
                    .begin_txn()
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<crate::DataConnError>() {
                        Ok(crate::DataConnError::FailToPostCommitDataConn { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "bar".into());
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), "!!!");
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::post_commit 2 failed",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src/_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "SyncDataSrc::create_data_conn 2",
                    "SyncDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "SyncDataConn::pre_commit 1",
                    "SyncDataConn::pre_commit 2",
                    "SyncDataConn::commit 1",
                    "SyncDataConn::commit 2",
                    "SyncDataConn::post_commit 1",
                    "SyncDataConn::post_commit 2 failed",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }]",
                    "SyncDataConn::on_txn_failure 2",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByCommitted, rollback: NoneByNotRolledBack }, TxnFailureReport { data_conn_name: \"bar\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: PostCommitFailure(errs::Err { reason = alloc::string::String \"!!!\", file = src\\_test_commons.rs, line = 93 }), rollback: NoneByNotRolledBack }]",
                    "SyncDataConn::close 2",
                    "SyncDataConn::drop 2",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
        }

        #[test]
        fn txn_fail_to_rollback() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = DataHub::for_txn();

                data.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::Rollback));
                data.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));

                if let Err(err) = data
                    .begin_txn()
                    .run(hoge_logic)
                    .run_force(hogera_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<crate::DataHubError>() {
                        Ok(crate::DataHubError::FailToRunLogic { errors }) => {
                            assert_eq!(errors.len(), 1);
                            assert_eq!(errors[0].index, 1);
                            assert_eq!(errors[0].name, "Txn#run_force(logic-1)".into());
                            assert_eq!(
                                errors[0].err.reason::<&str>().unwrap(),
                                &"hogera_logic failed"
                            );
                        }
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            #[cfg(unix)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "foo/hogera/Hogera",
                    "SyncDataConn::rollback 1 failed",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src/_test_commons.rs, line = 112 }) }]",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
            #[cfg(windows)]
            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "SyncDataSrc::new 1",
                    "SyncDataSrc::new 2",
                    "SyncDataSrc::setup 1",
                    "SyncDataSrc::setup 2",
                    "SyncDataSrc::create_data_conn 1",
                    "SyncDataConn::new 1",
                    "foo/hoge/Hoge",
                    "foo/hogera/Hogera",
                    "SyncDataConn::rollback 1 failed",
                    "SyncDataConn::on_txn_failure 1",
                    "TxnFailureReports=[TxnFailureReport { data_conn_name: \"foo\", data_conn_type: \"sabi::_test_commons::SyncDataConn\", cause: NoneByUncommitted, rollback: RollbackFailure(errs::Err { reason = alloc::string::String \"???\", file = src\\_test_commons.rs, line = 112 }) }]",
                    "SyncDataConn::close 1",
                    "SyncDataConn::drop 1",
                    "SyncDataSrc::close 2",
                    "SyncDataSrc::drop 2",
                    "SyncDataSrc::close 1",
                    "SyncDataSrc::drop 1"
                ],
            );
        }
    }
}
