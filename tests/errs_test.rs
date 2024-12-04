#[cfg(test)]
mod integration_tests_of_err {
    use sabi;

    #[derive(Debug)]
    enum IoErrs {
        FileNotFound { path: String },
        NoPermission { path: String, r#mod: (u8, u8, u8) },
    }

    fn find_file() -> Result<(), sabi::Err> {
        let err = sabi::Err::new(IoErrs::FileNotFound {
            path: "/aaa/bbb/ccc".to_string(),
        });
        Err(err)
    }

    fn write_file() -> Result<(), sabi::Err> {
        let err = sabi::Err::new(IoErrs::NoPermission {
            path: "/aaa/bbb/ccc".to_string(),
            r#mod: (6, 6, 6),
        });
        Err(err)
    }

    #[test]
    fn should_create_err_and_identify_reason() {
        match find_file() {
            Ok(_) => panic!(),
            Err(err) => match err.reason::<IoErrs>() {
                Ok(r) => match r {
                    IoErrs::FileNotFound { path } => {
                        assert_eq!(path, "/aaa/bbb/ccc");
                    }
                    IoErrs::NoPermission { path: _, r#mod: _ } => panic!(),
                },
                _ => panic!(),
            },
        }

        match write_file() {
            Ok(_) => panic!(),
            Err(err) => match err.reason::<IoErrs>() {
                Ok(r) => match r {
                    IoErrs::FileNotFound { path: _ } => panic!(),
                    IoErrs::NoPermission { path, r#mod } => {
                        assert_eq!(path, "/aaa/bbb/ccc");
                        assert_eq!(*r#mod, (6, 6, 6));
                    }
                },
                _ => panic!(),
            },
        }
    }

    #[test]
    fn should_match_reason() {
        match find_file() {
            Ok(_) => panic!(),
            Err(err) => err.match_reason::<IoErrs>(|r| match r {
                IoErrs::FileNotFound { path } => {
                    assert_eq!(path, "/aaa/bbb/ccc");
                }
                IoErrs::NoPermission { path: _, r#mod: _ } => panic!(),
            }),
        };

        match write_file() {
            Ok(_) => panic!(),
            Err(err) => err.match_reason::<IoErrs>(|r| match r {
                IoErrs::FileNotFound { path: _ } => panic!(),
                IoErrs::NoPermission { path, r#mod } => {
                    assert_eq!(path, "/aaa/bbb/ccc");
                    assert_eq!(*r#mod, (6, 6, 6));
                }
            }),
        };
    }

    #[test]
    fn should_check_type_of_reason() {
        let err = find_file().unwrap_err();
        assert!(err.is_reason::<IoErrs>());
        assert!(!err.is_reason::<String>());

        let err = write_file().unwrap_err();
        assert!(err.is_reason::<IoErrs>());
        assert!(!err.is_reason::<String>());
    }

    #[test]
    fn should_output_err_in_debug_format() {
        let err = find_file().unwrap_err();
        //println!("{err:?}");
        assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: errs_test::integration_tests_of_err::IoErrs FileNotFound { path: \"/aaa/bbb/ccc\" } }");

        let err = write_file().unwrap_err();
        //println!("{err:?}");
        assert_eq!(format!("{err:?}"), "sabi::errs::Err { reason: errs_test::integration_tests_of_err::IoErrs NoPermission { path: \"/aaa/bbb/ccc\", mod: (6, 6, 6) } }");
    }

    #[test]
    fn should_output_err_in_display_format() {
        let err = find_file().unwrap_err();
        //println!("{err}");
        assert_eq!(format!("{err}"), "FileNotFound { path: \"/aaa/bbb/ccc\" }");

        let err = write_file().unwrap_err();
        //println!("{err}");
        assert_eq!(
            format!("{err}"),
            "NoPermission { path: \"/aaa/bbb/ccc\", mod: (6, 6, 6) }"
        );
    }
}

#[test]
fn compile_error_check() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_errors/*_errs.rs");
}
