// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

use std::any;
use std::ptr;

use errs::Err;

use crate::{AsyncGroup, DataConn, DataConnContainer};

impl<C> DataConnContainer<C>
where
    C: DataConn + 'static,
{
    pub(crate) fn new(name: &str, data_conn: Box<C>) -> Self {
        Self {
            drop_fn: drop_data_conn::<C>,
            is_fn: is_data_conn::<C>,

            commit_fn: commit_data_conn::<C>,
            pre_commit_fn: pre_commit_data_conn::<C>,
            post_commit_fn: post_commit_data_conn::<C>,
            should_force_back_fn: should_force_back_data_conn::<C>,
            rollback_fn: rollback_data_conn::<C>,
            force_back_fn: force_back_data_conn::<C>,
            close_fn: close_data_conn::<C>,

            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            name: name.to_string(),
            data_conn,
        }
    }
}

fn drop_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        drop(Box::from_raw(typed_ptr));
    }
}

fn is_data_conn<C>(type_id: any::TypeId) -> bool
where
    C: DataConn + 'static,
{
    any::TypeId::of::<C>() == type_id
}

fn commit_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup) -> Result<(), Err>
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.commit(ag) }
}

fn pre_commit_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup) -> Result<(), Err>
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.pre_commit(ag) }
}

fn post_commit_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        (*typed_ptr).data_conn.post_commit(ag);
    }
}

fn should_force_back_data_conn<C>(ptr: *const DataConnContainer) -> bool
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe { (*typed_ptr).data_conn.should_force_back() }
}

fn rollback_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        (*typed_ptr).data_conn.rollback(ag);
    }
}

fn force_back_data_conn<C>(ptr: *const DataConnContainer, ag: &mut AsyncGroup)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        (*typed_ptr).data_conn.force_back(ag);
    }
}

fn close_data_conn<C>(ptr: *const DataConnContainer)
where
    C: DataConn + 'static,
{
    let typed_ptr = ptr as *mut DataConnContainer<C>;
    unsafe {
        (*typed_ptr).data_conn.close();
    }
}

pub(crate) struct DataConnList {
    head: *mut DataConnContainer,
    last: *mut DataConnContainer,
}

impl DataConnList {
    pub(crate) const fn new() -> Self {
        Self {
            head: ptr::null_mut(),
            last: ptr::null_mut(),
        }
    }

    #[inline]
    pub(crate) fn head(&self) -> *mut DataConnContainer {
        self.head
    }

    pub(crate) fn append_container_ptr(&mut self, ptr: *mut DataConnContainer) {
        unsafe {
            (*ptr).next = ptr::null_mut();
        }

        if self.last.is_null() {
            self.head = ptr;
            self.last = ptr;
            unsafe {
                (*ptr).prev = ptr::null_mut();
            }
        } else {
            unsafe {
                (*self.last).next = ptr;
                (*ptr).prev = self.last;
            }
            self.last = ptr;
        }
    }

    pub(crate) fn close_and_drop_data_conns(&mut self) {
        let mut ptr = self.last;
        while !ptr.is_null() {
            let close_fn = unsafe { (*ptr).close_fn };
            let drop_fn = unsafe { (*ptr).drop_fn };
            let prev = unsafe { (*ptr).prev };
            close_fn(ptr);
            drop_fn(ptr);
            ptr = prev;
        }
        self.head = ptr::null_mut();
        self.last = ptr::null_mut();
    }
}

impl Drop for DataConnList {
    fn drop(&mut self) {
        self.close_and_drop_data_conns();
    }
}

#[cfg(test)]
mod tests_of_data_conn {
    use super::*;
    use std::cell::RefCell;
    use std::rc::Rc;

    struct SampleDataConn {
        id: i8,
        logger: Rc<RefCell<Vec<String>>>,
    }

    impl<'a> SampleDataConn {
        fn new(id: i8, logger: Rc<RefCell<Vec<String>>>) -> Self {
            Self { id, logger }
        }
    }

    impl Drop for SampleDataConn {
        fn drop(&mut self) {
            self.logger
                .borrow_mut()
                .push(format!("SampleDataConn {} dropped", self.id));
        }
    }

    impl DataConn for SampleDataConn {
        fn commit(&mut self, _ag: &mut AsyncGroup) -> Result<(), Err> {
            Ok(())
        }
        fn rollback(&mut self, _ag: &mut AsyncGroup) {}
        fn close(&mut self) {}
    }

    mod tests_of_data_conn_list {
        use super::*;

        #[test]
        fn test_of_new() {
            let dc_list = DataConnList::new();

            assert_eq!(dc_list.head, ptr::null_mut());
            assert_eq!(dc_list.last, ptr::null_mut());
        }

        #[test]
        fn test_of_append_container_ptr() {
            let logger = Rc::new(RefCell::new(Vec::<String>::new()));
            {
                let mut dc_list = DataConnList::new();

                let dc1 = Box::new(SampleDataConn::new(1, logger.clone()));
                let boxed = Box::new(DataConnContainer::<SampleDataConn>::new("foo", dc1));
                let typed_ptr = Box::into_raw(boxed);
                let ptr1 = typed_ptr.cast::<DataConnContainer>();

                dc_list.append_container_ptr(ptr1);

                assert_eq!(dc_list.head, ptr1);
                assert_eq!(dc_list.last, ptr1);

                let dc2 = Box::new(SampleDataConn::new(2, logger.clone()));
                let boxed = Box::new(DataConnContainer::<SampleDataConn>::new("bar", dc2));
                let typed_ptr = Box::into_raw(boxed);
                let ptr2 = typed_ptr.cast::<DataConnContainer>();

                dc_list.append_container_ptr(ptr2);

                assert_eq!(dc_list.head, ptr1);
                assert_eq!(dc_list.last, ptr2);
                assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
                assert_eq!(unsafe { (*ptr1).next }, ptr2);
                assert_eq!(unsafe { (*ptr2).prev }, ptr1);
                assert_eq!(unsafe { (*ptr2).next }, ptr::null_mut());

                let dc3 = Box::new(SampleDataConn::new(3, logger.clone()));
                let boxed = Box::new(DataConnContainer::<SampleDataConn>::new("bar", dc3));
                let typed_ptr = Box::into_raw(boxed);
                let ptr3 = typed_ptr.cast::<DataConnContainer>();

                dc_list.append_container_ptr(ptr3);

                assert_eq!(dc_list.head, ptr1);
                assert_eq!(dc_list.last, ptr3);
                assert_eq!(unsafe { (*ptr1).prev }, ptr::null_mut());
                assert_eq!(unsafe { (*ptr1).next }, ptr2);
                assert_eq!(unsafe { (*ptr2).prev }, ptr1);
                assert_eq!(unsafe { (*ptr2).next }, ptr3);
                assert_eq!(unsafe { (*ptr3).prev }, ptr2);
                assert_eq!(unsafe { (*ptr3).next }, ptr::null_mut());
            }

            assert_eq!(
                *logger.borrow(),
                vec![
                    "SampleDataConn 3 dropped",
                    "SampleDataConn 2 dropped",
                    "SampleDataConn 1 dropped",
                ],
            );
        }
    }
}
