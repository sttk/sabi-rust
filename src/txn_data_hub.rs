// Copyright (C) 2026 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use crate::{DataConn, DataHub, DataSrc, ErrEntry, Runner, TxnDataHub};

use std::mem;
use std::sync::Arc;

#[derive(Debug)]
pub enum TxnError {
    FailToRunLogics { errors: Vec<ErrEntry> },
}

impl TxnDataHub {
    pub fn new(hub: DataHub) -> Self {
        Self { hub }
    }

    pub fn uses<S, C>(&mut self, name: impl Into<Arc<str>>, ds: S)
    where
        S: DataSrc<C>,
        C: DataConn + 'static,
    {
        self.hub.uses(name, ds)
    }

    pub fn disuses(&mut self, name: impl AsRef<str>) {
        self.hub.disuses(name)
    }

    pub fn run<F>(&mut self, logic_fn: F) -> errs::Result<()>
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        self.hub.run(logic_fn)
    }

    pub fn start(&mut self) -> Runner<'_> {
        self.hub.start()
    }

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

    pub fn begin_txn(&mut self) -> Txn<'_> {
        Txn::new(self)
    }
}

enum TxnErrAt {
    Begin { err: errs::Err },
    Run { errors: Vec<ErrEntry> },
    Block { errors: Vec<ErrEntry> },
}

pub struct Txn<'a> {
    hub: &'a mut DataHub,
    err: TxnErrAt,
    index: usize,
}

impl<'a> Txn<'a> {
    pub fn new(txn_hub: &'a mut TxnDataHub) -> Txn<'a> {
        if let Err(err) = txn_hub.hub.begin() {
            Self {
                hub: &mut txn_hub.hub,
                err: TxnErrAt::Begin { err },
                index: 0,
            }
        } else {
            Self {
                hub: &mut txn_hub.hub,
                err: TxnErrAt::Run {
                    errors: Vec::with_capacity(0),
                },
                index: 0,
            }
        }
    }

    pub fn run<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            TxnErrAt::Run { ref mut errors } => {
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

    pub fn run_force<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            TxnErrAt::Run { ref mut errors } => {
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

    pub fn run_or_block<F>(mut self, mut logic_fn: F) -> Self
    where
        F: FnMut(&mut DataHub) -> errs::Result<()>,
    {
        let index = self.index;
        self.index += 1;

        match self.err {
            TxnErrAt::Run { ref mut errors } => {
                if errors.is_empty() {
                    if let Err(err) = logic_fn(self.hub) {
                        errors.push(ErrEntry {
                            index,
                            name: format!("Txn#run_or_block(logic-{})", index).into(),
                            err,
                        });
                        self.err = TxnErrAt::Block {
                            errors: mem::take(errors),
                        };
                    }
                }
                self
            }
            _ => self,
        }
    }

    pub fn end_txn(self) -> errs::Result<()> {
        match self.err {
            TxnErrAt::Begin { err } => {
                self.hub.end();
                Err(err)
            }
            TxnErrAt::Run { errors } => {
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
                    Err(errs::Err::new(TxnError::FailToRunLogics { errors }))
                }
            }
            TxnErrAt::Block { errors } => {
                let reports = self.hub.new_failure_reports();
                self.hub.rollback(reports);
                self.hub.end();
                Err(errs::Err::new(TxnError::FailToRunLogics { errors }))
            }
        }
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg(test)]
mod tests_of_txn_data_hub {
    use super::*;
    use crate::{AsyncGroup, DataAcc, DataConn, DataSrc, TxnFailureReport};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::{Arc, Mutex};

    #[derive(PartialEq, Clone, Copy)]
    enum Failure {
        None,
        Setup,
        PreCommit,
        Commit,
        PostCommit,
        Rollback,
    }

    struct FooDataConn {
        id: i8,
        text: String,
        committed: bool,
        failure: Failure,
        logger: Arc<Mutex<Vec<String>>>,
    }

    impl FooDataConn {
        fn new(id: i8, s: &str, f: Failure, logger: Arc<Mutex<Vec<String>>>) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("FooDataConn::new {}", id));
            }
            Self {
                id,
                text: s.to_string(),
                logger,
                committed: false,
                failure: f,
            }
        }
        fn get_text(&self) -> String {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::get_text {}", self.id));
            self.text.clone()
        }
    }
    impl Drop for FooDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::drop {}", self.id));
        }
    }
    impl DataConn for FooDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            if self.failure == Failure::Commit {
                logger.push(format!("FooDataConn::commit failed {}", self.id));
                return Err(errs::Err::new("commit error"));
            }
            self.committed = true;
            logger.push(format!("FooDataConn::commit {}", self.id));
            Ok(())
        }
        fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            if self.failure == Failure::PreCommit {
                logger.push(format!("FooDataConn::pre_commit failed {}", self.id));
                return Err(errs::Err::new("pre_commit error"));
            }
            logger.push(format!("FooDataConn::pre_commit {}", self.id));
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            if self.failure == Failure::PostCommit {
                logger.push(format!("FooDataConn::post_commit failed {}", self.id));
                return Err(errs::Err::new("post_commit error"));
            }
            logger.push(format!("FooDataConn::post_commit {}", self.id));
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            if self.failure == Failure::Rollback {
                logger.push(format!("FooDataConn::rollback failed {}", self.id));
                return Err(errs::Err::new("rollback error"));
            }
            logger.push(format!("FooDataConn::rollback {}", self.id));
            Ok(())
        }
        fn on_txn_failure(&mut self, _ag: &mut AsyncGroup, _reports: &[TxnFailureReport]) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::on_txn_failure {}", self.id));
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataConn::close {}", self.id));
        }
    }

    struct FooDataSrc {
        id: i8,
        failure: Failure,
        text: String,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl FooDataSrc {
        fn new(id: i8, s: &str, f: Failure, logger: Arc<Mutex<Vec<String>>>) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("FooDataSrc::new {}", id));
            }
            Self {
                id,
                logger,
                failure: f,
                text: s.to_string(),
            }
        }
    }
    impl Drop for FooDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<FooDataConn> for FooDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::Setup {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("FooDataSrc::setup {} failed", self.id));
                }
                return Err(errs::Err::new("XXX".to_string()));
            }
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("FooDataSrc::setup {}", self.id));
            }
            Ok(())
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("FooDataSrc::close {}", self.id));
        }
        fn create_data_conn(&mut self) -> errs::Result<Box<FooDataConn>> {
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("FooDataSrc::create_data_src {}", self.id));
            }
            let conn = FooDataConn::new(self.id, &self.text, self.failure, self.logger.clone());
            Ok(Box::new(conn))
        }
    }

    struct BarDataConn {
        id: i8,
        text: Option<String>,
        ds_text: Rc<RefCell<String>>,
        committed: bool,
        failure: Failure,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl BarDataConn {
        fn new(
            id: i8,
            ds_text: Rc<RefCell<String>>,
            f: Failure,
            logger: Arc<Mutex<Vec<String>>>,
        ) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("BarDataConn::new {}", id));
            }
            Self {
                id,
                text: None,
                ds_text,
                logger,
                committed: false,
                failure: f,
            }
        }
        fn set_text(&mut self, s: &str) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::set_text {}", self.id));
            self.text = Some(s.to_string());
        }
    }
    impl Drop for BarDataConn {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::drop {}", self.id));
        }
    }
    impl DataConn for BarDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::Commit {
                self.logger
                    .lock()
                    .unwrap()
                    .push(format!("BarDataConn::commit failed {}", self.id));
                return Err(errs::Err::new("commit error"));
            }
            self.committed = true;
            match &self.text {
                Some(s) => {
                    *self.ds_text.borrow_mut() = s.to_string();
                }
                None => {
                    *self.ds_text.borrow_mut() = "".to_string();
                }
            }
            self.logger
                .lock()
                .unwrap()
                .push(format!("BarDataConn::commit {}", self.id));
            Ok(())
        }
        fn pre_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::pre_commit {}", self.id));
            Ok(())
        }
        fn post_commit(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::post_commit {}", self.id));
            Ok(())
        }
        fn is_committed(&self) -> bool {
            self.committed
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::rollback {}", self.id));
            Ok(())
        }
        fn on_txn_failure(&mut self, _ag: &mut AsyncGroup, _reports: &[TxnFailureReport]) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn::on_txn_failure {}", self.id));
        }
        fn close(&mut self) {
            let text = self.text.clone().unwrap_or("".to_string());
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataConn.text = {}", text));
            logger.push(format!("BarDataConn::close {}", self.id));
        }
    }

    struct BarDataSrc {
        id: i8,
        text: Rc<RefCell<String>>,
        failure: Failure,
        logger: Arc<Mutex<Vec<String>>>,
    }
    impl BarDataSrc {
        fn new(id: i8, f: Failure, logger: Arc<Mutex<Vec<String>>>) -> Self {
            {
                let mut logger = logger.lock().unwrap();
                logger.push(format!("BarDataSrc::new {}", id));
            }
            Self {
                id,
                text: Rc::new(RefCell::new(String::new())),
                failure: f,
                logger,
            }
        }
    }
    impl Drop for BarDataSrc {
        fn drop(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc::drop {}", self.id));
        }
    }
    impl DataSrc<BarDataConn> for BarDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> errs::Result<()> {
            if self.failure == Failure::Setup {
                {
                    let mut logger = self.logger.lock().unwrap();
                    logger.push(format!("BarDataSrc::setup {} failed", self.id));
                }
                return Err(errs::Err::new("xxx".to_string()));
            }
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc::setup {}", self.id));
            Ok(())
        }
        fn close(&mut self) {
            let mut logger = self.logger.lock().unwrap();
            logger.push(format!("BarDataSrc.text = {}", self.text.borrow()));
            logger.push(format!("BarDataSrc::close {}", self.id));
        }
        fn create_data_conn(&mut self) -> errs::Result<Box<BarDataConn>> {
            {
                let mut logger = self.logger.lock().unwrap();
                logger.push(format!("BarDataSrc::create_data_src {}", self.id));
            }
            let conn = BarDataConn::new(
                self.id,
                self.text.clone(),
                self.failure,
                self.logger.clone(),
            );
            Ok(Box::new(conn))
        }
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
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_ok)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
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
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(_) = data.run(fuga_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_run_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(_) = data.run(fuga_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_start() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(_) = data.start().run(fuga_logic).end() {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_start_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(_) = data.start().run(hoge_logic).end() {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(_) = data.txn(fuga_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "FooDataConn::pre_commit 1",
                    "BarDataConn::pre_commit 2",
                    "FooDataConn::commit 1",
                    "BarDataConn::commit 2",
                    "FooDataConn::post_commit 1",
                    "BarDataConn::post_commit 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = hello", // because committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(_) = data.txn(hoge_logic) {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "FooDataConn::pre_commit 1",
                    "BarDataConn::pre_commit 2",
                    "FooDataConn::commit 1",
                    "BarDataConn::commit 2",
                    "FooDataConn::post_commit 1",
                    "BarDataConn::post_commit 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = hello", // because committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_begin_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(_) = data.begin_txn().run(fuga_logic).end_txn() {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "FooDataConn::pre_commit 1",
                    "BarDataConn::pre_commit 2",
                    "FooDataConn::commit 1",
                    "BarDataConn::commit 2",
                    "FooDataConn::post_commit 1",
                    "BarDataConn::post_commit 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = hello", // because committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_begin_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(_) = data.begin_txn().run(hoge_logic).end_txn() {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "FooDataConn::pre_commit 1",
                    "BarDataConn::pre_commit 2",
                    "FooDataConn::commit 1",
                    "BarDataConn::commit 2",
                    "FooDataConn::post_commit 1",
                    "BarDataConn::post_commit 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = hello", // because committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }
    }

    mod test_fail_to_run {
        use super::*;
        use crate::DataHubError;
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
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                Ok(conn.get_text())
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_fail_to_run)]
        trait BarDataAcc: DataAcc {
            fn set_value(&mut self, text: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                conn.set_text(text);
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
        fn test_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.run(fuga_logic) {
                    match err.reason::<&str>() {
                        Ok(s) => assert_eq!(s, &"fail"),
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_run_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.run(hoge_logic) {
                    match err.reason::<&str>() {
                        Ok(s) => assert_eq!(s, &"fail"),
                        _ => panic!("{err:?}"),
                    }
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_start() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.start().run(fuga_logic).end() {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_start_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.start().run(hoge_logic).end() {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.txn(fuga_logic) {
                    assert_eq!(err.reason::<&str>().unwrap(), &"fail");
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "FooDataConn::rollback 1",
                    "BarDataConn::rollback 2",
                    "FooDataConn::on_txn_failure 1",
                    "BarDataConn::on_txn_failure 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.txn(hoge_logic) {
                    assert_eq!(err.reason::<&str>().unwrap(), &"fail");
                } else {
                    panic!();
                }
            }

            assert_eq!(
                *logger.lock().unwrap(),
                vec![
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "FooDataConn::rollback 1",
                    "BarDataConn::rollback 2",
                    "FooDataConn::on_txn_failure 1",
                    "BarDataConn::on_txn_failure 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_begin_txn() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.begin_txn().run(fuga_logic).end_txn() {
                    match err.reason::<TxnError>() {
                        Ok(TxnError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "FooDataConn::rollback 1",
                    "BarDataConn::rollback 2",
                    "FooDataConn::on_txn_failure 1",
                    "BarDataConn::on_txn_failure 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn test_begin_txn_with_nested_run() {
            let logger = Arc::new(Mutex::new(Vec::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data.begin_txn().run(hoge_logic).end_txn() {
                    match err.reason::<TxnError>() {
                        Ok(TxnError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "FooDataConn::get_text 1",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "BarDataConn::set_text 2",
                    "FooDataConn::rollback 1",
                    "BarDataConn::rollback 2",
                    "FooDataConn::on_txn_failure 1",
                    "BarDataConn::on_txn_failure 2",
                    "BarDataConn.text = hello",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ", // because not committed
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
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
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("foo/hoge/{}", s));
                Ok(())
            }
            fn hogera_log(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<FooDataConn>("foo")?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("foo/hogera/{}", s));
                Err(errs::Err::new("hogera_logic failed"))
            }
        }
        impl FooDataAcc for DataHub {}

        #[overridable(mod = test_runner_and_txn)]
        trait BarDataAcc: DataAcc {
            fn fuga_log(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
                let mut logger = conn.logger.lock().unwrap();
                logger.push(format!("bar/fuga/{}", s));
                Ok(())
            }
            fn piyo_log(&mut self, s: &str) -> errs::Result<()> {
                let conn = self.get_data_conn::<BarDataConn>("bar")?;
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
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "foo/hoge/Hoge",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn runner_fail_to_start() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::Setup, logger.clone()));

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
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), &"xxx");
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2 failed",
                    "FooDataSrc::close 1",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn runner_fail_to_run() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data
                    .start()
                    .run(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn runner_fail_to_run_force() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data
                    .start()
                    .run_force(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn runner_fail_to_run_or_block() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data
                    .start()
                    .run_or_block(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end()
                {
                    match err.reason::<DataHubError>() {
                        Ok(DataHubError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_ok() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "foo/hoge/Hoge",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "bar/piyo/Piyo",
                    "FooDataConn::pre_commit 1",
                    "BarDataConn::pre_commit 2",
                    "FooDataConn::commit 1",
                    "BarDataConn::commit 2",
                    "FooDataConn::post_commit 1",
                    "BarDataConn::post_commit 2",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_fail_to_begin_txn() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::Setup, logger.clone()));

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
                            assert_eq!(errors[0].err.reason::<String>().unwrap(), &"xxx");
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2 failed",
                    "FooDataSrc::close 1",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_fail_to_run() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data
                    .begin_txn()
                    .run(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<TxnError>() {
                        Ok(TxnError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "FooDataConn::rollback 1",
                    "BarDataConn::rollback 2",
                    "FooDataConn::on_txn_failure 1",
                    "BarDataConn::on_txn_failure 2",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_fail_to_run_force() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data
                    .begin_txn()
                    .run_force(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<TxnError>() {
                        Ok(TxnError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "BarDataSrc::create_data_src 2",
                    "BarDataConn::new 2",
                    "bar/fuga/Fuga",
                    "FooDataConn::rollback 1",
                    "BarDataConn::rollback 2",
                    "FooDataConn::on_txn_failure 1",
                    "BarDataConn::on_txn_failure 2",
                    "BarDataConn.text = ",
                    "BarDataConn::close 2",
                    "BarDataConn::drop 2",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }

        #[test]
        fn txn_fail_to_run_or_block() {
            let logger = Arc::new(Mutex::new(Vec::<String>::new()));

            {
                let mut data = TxnDataHub::new(DataHub::new());

                data.uses(
                    "foo",
                    FooDataSrc::new(1, "hello", Failure::None, logger.clone()),
                );
                data.uses("bar", BarDataSrc::new(2, Failure::None, logger.clone()));

                if let Err(err) = data
                    .begin_txn()
                    .run_or_block(hogera_logic)
                    .run(hoge_logic)
                    .run_force(fuga_logic)
                    .run_or_block(piyo_logic)
                    .end_txn()
                {
                    match err.reason::<TxnError>() {
                        Ok(TxnError::FailToRunLogics { errors }) => {
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
                    "FooDataSrc::new 1",
                    "BarDataSrc::new 2",
                    "FooDataSrc::setup 1",
                    "BarDataSrc::setup 2",
                    "FooDataSrc::create_data_src 1",
                    "FooDataConn::new 1",
                    "foo/hogera/Hogera",
                    "FooDataConn::rollback 1",
                    "FooDataConn::on_txn_failure 1",
                    "FooDataConn::close 1",
                    "FooDataConn::drop 1",
                    "BarDataSrc.text = ",
                    "BarDataSrc::close 2",
                    "BarDataSrc::drop 2",
                    "FooDataSrc::close 1",
                    "FooDataSrc::drop 1",
                ],
            );
        }
    }
}
