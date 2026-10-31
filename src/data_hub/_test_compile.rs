use crate::{DataAcc, DataHub};

#[test]
fn data_hub_implements_send_trait() {
    struct A;
    let mut data = DataHub::<A>::new();
    let handle = std::thread::spawn(move || {
        data.run(|_data| Ok(())).unwrap();
    });

    handle.join().unwrap();
}
