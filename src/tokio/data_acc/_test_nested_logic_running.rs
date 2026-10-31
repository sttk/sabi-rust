use crate::tokio::data_conn::_test_data::*;
use crate::tokio::data_src::_test_data::*;
use crate::tokio::*;

use override_macro::{overridable, override_with};
use std::sync::{Arc, Mutex};

#[overridable]
trait GettingDataAcc12: DataAcc {
    async fn get_text_async(&mut self) -> errs::Result<String> {
        let _conn = self.get_data_conn_async::<SyncDataConn>("foo").await?;
        // getting a text by using _conn
        Ok("hello".to_string())
    }
}

#[overridable]
trait SettingDataAcc12: DataAcc {
    async fn set_text_async(&mut self, _text: String) -> errs::Result<()> {
        let _conn = self.get_data_conn_async::<SyncDataConn>("bar").await?;
        // setting a text by using _conn
        Ok(())
    }
}

#[overridable]
trait GetSetDataAcc12: DataAcc + HogeData12 {
    async fn process_text_async(&mut self) -> errs::Result<()> {
        self.run_async(logic!(hoge_logic12)).await
    }
}

struct MyScope12;
impl GettingDataAcc12 for DataHub<MyScope12> {}
impl SettingDataAcc12 for DataHub<MyScope12> {}
impl GetSetDataAcc12 for DataHub<MyScope12> {}

#[trait_variant::make(Send)]
#[overridable]
trait HogeData12: Sized {
    async fn get_text_async(&mut self) -> errs::Result<String>;
    async fn set_text_async(&mut self, text: String) -> errs::Result<()>;
}

#[override_with(GettingDataAcc12, SettingDataAcc12)]
impl HogeData12 for DataHub<MyScope12> {}

async fn hoge_logic12(data: &mut impl HogeData12) -> errs::Result<()> {
    let mut text = data.get_text_async().await?;
    text = text.to_uppercase();
    data.set_text_async(text).await?;
    Ok(())
}

#[trait_variant::make(Send)]
#[overridable]
trait FugaData12 {
    async fn process_text_async(&mut self) -> errs::Result<()>;
}

#[override_with(GetSetDataAcc12)]
impl FugaData12 for DataHub<MyScope12> {}

async fn fuga_logic12(data: &mut impl FugaData12) -> errs::Result<()> {
    data.process_text_async().await
}

#[tokio::test]
async fn test_run_async() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut runner = LogicRunner::new(DataHub::<MyScope12>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
        if let Err(err) = runner.run_async(logic!(fuga_logic12)).await {
            panic!("{err:?}");
        }
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
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

#[tokio::test]
async fn test_txn() {
    let logger = Arc::new(Mutex::new(Vec::<String>::new()));

    {
        let mut runner = LogicRunner::new(DataHub::<MyScope12>::new());
        runner.uses("foo", SyncDataSrc::new(1, logger.clone(), Fail::None));
        runner.uses("bar", SyncDataSrc::new(2, logger.clone(), Fail::None));
        if let Err(err) = runner.txn_async(logic!(fuga_logic12)).await {
            panic!("{err:?}");
        }
    }

    assert_eq!(
        *logger.lock().unwrap(),
        &[
            "SyncDataSrc::new 1",
            "SyncDataSrc::new 2",
            "SyncDataSrc::setup_async 1",
            "SyncDataSrc::setup_async 2",
            "SyncDataSrc::create_data_conn_async 1",
            "SyncDataConn::new 1",
            "SyncDataSrc::create_data_conn_async 2",
            "SyncDataConn::new 2",
            "SyncDataConn::pre_commit_async 1",
            "SyncDataConn::pre_commit_async 2",
            "SyncDataConn::commit_async 1",
            "SyncDataConn::commit_async 2",
            "SyncDataConn::post_commit_async 1",
            "SyncDataConn::post_commit_async 2",
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
