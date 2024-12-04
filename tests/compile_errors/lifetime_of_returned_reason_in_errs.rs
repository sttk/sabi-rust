use sabi::Err;

#[derive(Debug)]
enum Reasons {
  MyErr { msg: String, flag: bool },
}

fn return_reason() -> Result<(), Reasons> {
  Err(Reasons::MyErr { msg: "hello".to_string(), flag: true })
}

fn forward_reason() -> Result<(), Reasons> {
  return_reason()
}

fn return_err() -> Result<(), Err> {
  let err = Err::new(Reasons::MyErr { msg: "hello".to_string(), flag: true });
  Err(err)
}

fn forward_reason_in_err() -> Result<&'static Reasons, &'static Err> {
  let err = return_err().unwrap_err();
  err.reason::<Reasons>()
}

fn main() {
  let reason = forward_reason().unwrap_err();
  println!("{:?}", reason);

  let reason = forward_reason_in_err().unwrap_err();
  println!("{:?}", reason);
}
