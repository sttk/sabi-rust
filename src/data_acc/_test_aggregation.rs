use crate::data_conn::_test_data::*;
use crate::data_src::_test_data::*;
use crate::{DataAcc, DataHub, LogicRunner};
use override_macro::{overridable, override_with};
use std::sync::{Arc, Mutex};

#[overridable]
trait GettingDataAcc: DataAcc {
    fn get_text(&mut self) -> errs::Result<String> {
        let _conn = self.get_data_conn::<SyncDataConn>("foo")?;
        // getting a text by using _conn
        Ok("hello".to_string())
    }
}

#[overridable]
trait SettingDataAcc: DataAcc {
    fn set_text(&mut self, _text: String) -> errs::Result<()> {
        let _conn = self.get_data_conn::<SyncDataConn>("bar")?;
        // setting a text by using _conn
        Ok(())
    }
}

struct MyScope;
impl GettingDataAcc for DataHub<MyScope> {}
impl SettingDataAcc for DataHub<MyScope> {}

#[overridable]
trait HogeData {
    fn get_text(&mut self) -> errs::Result<String>;
    fn set_text(&mut self, text: String) -> errs::Result<()>;
}

#[override_with(GettingDataAcc, SettingDataAcc)]
impl HogeData for DataHub<MyScope> {}

fn hoge_logic(data: &mut impl HogeData) -> errs::Result<()> {
    let mut text = data.get_text()?;
    text = text.to_uppercase();
    data.set_text(text)?;
    Ok(())
}

#[test]
fn test_run() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut runner = LogicRunner::new(DataHub::<MyScope>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
        if let Err(err) = runner.run(hoge_logic) {
            panic!("{err:?}");
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
        ]
    );
}

#[test]
fn test_txn() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut runner = LogicRunner::new(DataHub::<MyScope>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
        if let Err(err) = runner.txn(hoge_logic) {
            panic!("{err:?}");
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
        ]
    );
}
