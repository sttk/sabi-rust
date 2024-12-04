use sabi::Err;

#[derive(Debug)]
enum Reasons {
  MyErr { msg: String, flag: bool },
}

fn return_err() -> Result<(), Err> {
  let err = Err::new(Reasons::MyErr { msg: "hello".to_string(), flag: true });
  Err(err)
}

fn consume_err(_err: Err) {}

fn main() {
  let err = return_err().unwrap_err();
  let reason = err.reason::<Reasons>().unwrap();
  println!("{:?}", reason);
  consume_err(err);
  println!("{:?}", reason);
}
