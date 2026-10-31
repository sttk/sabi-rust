use crate::tokio::*;

struct A;

trait Data {}
impl Data for DataHub<A> {}

async fn process_async(_data: &mut impl Data) -> errs::Result<()> {
    Ok(())
}

#[tokio::test]
async fn data_hub_implements_send_trait() {
    let handle = tokio::spawn(async {
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.run_async(logic!(process_async)).await.unwrap();
    });

    handle.await.unwrap();
}

#[tokio::test]
async fn txn_async_in_spawn() {
    let handle = tokio::spawn(async {
        let mut runner = LogicRunner::new(DataHub::<A>::new());
        runner.txn_async(logic!(process_async)).await.unwrap();
    });

    handle.await.unwrap();
}
