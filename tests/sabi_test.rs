mod data_src {
    use errs::Err;
    use sabi::{AsyncGroup, DataConn, DataSrc};

    pub struct FooDataSrc {/* ... */}

    impl DataSrc<FooDataConn> for FooDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            /* ... */
            Ok(())
        }
        fn close(&mut self) { /* ... */
        }
        fn create_data_conn(&mut self) -> Result<Box<FooDataConn>, Err> {
            Ok(Box::new(FooDataConn{ /* ... */ }))
        }
    }

    pub struct FooDataConn {/* ... */}

    impl FooDataConn {
        /* ... */
    }

    impl DataConn for FooDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            /* ... */
            Ok(())
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) { /* ... */
        }
        fn close(&mut self) { /* ... */
        }
    }

    pub struct BarDataSrc {/* ... */}

    impl DataSrc<BarDataConn> for BarDataSrc {
        fn setup(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            /* ... */
            Ok(())
        }
        fn close(&mut self) { /* ... */
        }
        fn create_data_conn(&mut self) -> Result<Box<BarDataConn>, Err> {
            Ok(Box::new(BarDataConn{ /* ... */ }))
        }
    }

    pub struct BarDataConn {/* ... */}

    impl BarDataConn {
        /* ... */
    }

    impl DataConn for BarDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            /* ... */
            Ok(())
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) { /* ... */
        }
        fn close(&mut self) { /* ... */
        }
    }
}

mod logic_layer {
    use errs::Err;
    use override_macro::overridable;

    #[overridable]
    pub trait MyData {
        fn get_text(&mut self) -> Result<String, Err>;
        fn set_text(&mut self, text: String) -> Result<(), Err>;
    }

    pub fn my_logic(data: &mut impl MyData) -> Result<(), Err> {
        let text = data.get_text()?;
        data.set_text(text)?;
        Ok(())
    }
}

mod data_access_layer {
    use errs::Err;
    use override_macro::overridable;
    use sabi::DataAcc;

    use crate::data_src::{BarDataConn, FooDataConn};

    #[overridable]
    pub trait GettingDataAcc: DataAcc {
        fn get_text(&mut self) -> Result<String, Err> {
            let _conn = self.get_data_conn::<FooDataConn>("foo")?;
            /* ... */
            Ok("output text".to_string())
        }
    }

    #[overridable]
    pub trait SettingDataAcc: DataAcc {
        fn set_text(&mut self, text: String) -> Result<(), Err> {
            let _conn = self.get_data_conn::<BarDataConn>("bar")?;
            /* ... */
            assert_eq!("output text", text);
            Ok(())
        }
    }
}

mod hub {
    use errs::Err;
    use override_macro::override_with;
    use sabi::DataHub;

    use crate::data_access_layer::{GettingDataAcc, SettingDataAcc};
    use crate::logic_layer::MyData;

    impl GettingDataAcc for DataHub {}
    impl SettingDataAcc for DataHub {}

    #[override_with(GettingDataAcc, SettingDataAcc)]
    impl MyData for DataHub {}
}

mod app {
    use sabi::{setup, shutdown_later, uses, DataHub};

    use crate::data_src::{BarDataSrc, FooDataSrc};
    use crate::logic_layer::my_logic;

    #[test]
    fn test_datahub_transaction_flow() {
        // Register global DataSrc
        uses("foo", FooDataSrc {});
        // Set up the sabi framework
        let _ = setup().unwrap();
        // Automatically shut down DataSrc when the application exits
        let _later = shutdown_later();

        // Create a new instance of DataHub
        let mut data = DataHub::new();
        // Register session-local DataSrc with DataHub
        data.uses("bar", BarDataSrc {});

        // Execute application logic within a transaction
        // my_logic performs data operations via DataHub
        assert!(data.txn(my_logic).is_ok());
    }
}
