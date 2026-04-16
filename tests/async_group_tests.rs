#[cfg(test)]
mod test_on_std {
    use sabi::AsyncGroup;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_ok() {
        let flag = Arc::new(Mutex::new(false));
        assert_eq!(*flag.lock().unwrap(), false);

        let mut ag = AsyncGroup::new();
        let flag_clone = flag.clone();
        ag.add(move || {
            let mut f = flag_clone.lock().unwrap();
            *f = true;
            Ok(())
        });
        let vec = ag.join();

        assert_eq!(vec.len(), 0);
        assert_eq!(*flag.lock().unwrap(), true);
    }

    #[test]
    fn test_err() {
        let mut ag = AsyncGroup::new();
        ag.add(|| Err(errs::Err::new("bad")));
        let vec = ag.join();

        assert_eq!(vec.len(), 1);
        assert_eq!(vec[0].0, "".into());
        assert_eq!(*vec[0].1.reason::<&str>().unwrap(), "bad");
    }
}

#[cfg(feature = "tokio")]
#[cfg(test)]
mod test_on_tokio {
    use sabi::tokio::AsyncGroup;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn test_ok() {
        let flag = Arc::new(Mutex::new(false));
        assert_eq!(*flag.lock().await, false);

        let flag_clone = flag.clone();

        let mut ag = AsyncGroup::new();
        ag.add(async move {
            let mut f = flag_clone.lock().await;
            *f = true;
            Ok(())
        });
        let vec = ag.join_async().await;

        assert_eq!(vec.len(), 0);
        assert_eq!(*flag.lock().await, true);
    }

    #[tokio::test]
    async fn test_err() {
        let mut ag = AsyncGroup::new();
        ag.add(async { Err(errs::Err::new("bad")) });
        let vec = ag.join_async().await;

        assert_eq!(vec.len(), 1);
        assert_eq!(vec[0].0, "".into());
        assert_eq!(*vec[0].1.reason::<&str>().unwrap(), "bad");
    }
}
