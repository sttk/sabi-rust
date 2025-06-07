# [sabi-rust][repo-url] [![crate.io][crateio-img]][crateio-url] [![doc.rs][docrs-img]][docrs-url] [![CI Status][ci-img]][ci-url] [![MIT License][mit-img]][mit-url]

A small framework for Rust designed to separate logic from data access.

It achieves this by connecting the logic layer and the data access layer via traits, similar to traditional
Dependency Injection (DI).
This reduces the dependency between the two, allowing them to be implemented and tested independently.

However, traditional DI often presented an inconvenience in how methods were grouped.
Typically, methods were grouped by external data service (like a database) or by database table.
This meant the logic layer had to depend on units defined by the data access layer's concerns.
Furthermore, such traits often contained more methods than a specific piece of logic needed, making it
difficult to tell which methods were actually used in the logic without tracing the code.

This crate addresses that inconvenience.
The data access trait used by a logic function is unique to that logic, passed as an argument
to the logic function.
This trait declares all the data access methods that specific logic will use.

On the data access layer side, implementations can be provided in the form of default methods
on `DataAcc` derived traits.
This allows for implementation in any arbitrary unit — whether by external data service, by table,
or by functional concern.

This is achieved through the following mechanism:
* A `DataHub` struct aggregates all data access methods.
`DataAcc` derived traits are attached to `DataHub`, giving `DataHub` the implementations of
the data access methods.
* Additionally, the data access traits that logic functions take as arguments are also attached
to `DataHub`. But that alone wouldn't work in Rust because methods aren't overridden across traits,
even if they have the same name and arguments, leaving the logic-facing data access trait methods
without implementations.
* This is where the `override_macro` crate comes in: it adds the method implementations of the
logic-facing data access traits by calling the corresponding methods from the `DataAcc` derived traits.
(While it's possible to implement this by hand without `override_macro` crate, it becomes very
cumbersome for a large number of methods.)


## Install

In Cargo.toml, write this crate as a dependency:

```toml
[dependencies]
sabi = "0.0.2"
```

## Usage

### 1. Implementing `DataSrc` and `DataConn`

First, you'll define `DataSrc` which manages connections to external data services and creates
`DataConn`. 
Then, you'll define `DataConn` which represents a session-specific connection and implements
transactional operations.

```rust
use sabi::{AsyncGroup, DataSrc, DataConn};
use errs::Err;

struct FooDataSrc { /* ... */ }

impl DataSrc<FooDataConn> for FooDataSrc {
    fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn close(&mut self) { /* ... */ }
    fn create_data_conn(&mut self) -> Result<Box<FooDataConn>, Err> {
        Ok(Box::new(FooDataConn{ /* ... */ }))
    }
}

struct FooDataConn { /* ... */ }

impl FooDataConn { /* ... */ }

impl DataConn for FooDataConn {
    fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn rollback(&mut self, ag: &mut AsyncGroup) { /* ... */ }
    fn close(&mut self) { /* ... */ }
}

struct BarDataSrc { /* ... */ }

impl DataSrc<BarDataConn> for BarDataSrc {
    fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn close(&mut self) { /* ... */ }
    fn create_data_conn(&mut self) -> Result<Box<BarDataConn>, Err> {
        Ok(Box::new(BarDataConn{ /* ... */ }))
    }
}

struct BarDataConn { /* ... */ }

impl BarDataConn { /* ... */ }

impl DataConn for BarDataConn {
    fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn rollback(&mut self, ag: &mut AsyncGroup) { /* ... */ }
    fn close(&mut self) { /* ... */ }
}
```

### 2. Implementing logic Functions and data traits

Define traits and functions that express your application logic.
These traits are independent of specific data source implementations, improving testability.
The `#[overridable]` macro is used to allow these trait implementations to be overridden later.

```rust
use errs::Err;
use override_macro::overridable;

#[overridable]
trait MyData {
    fn get_text(&mut self) -> Result<String, Err>;
    fn set_text(&mut self, text: String) -> Result<(), Err>;
}

fn my_logic(data: &mut impl MyData) -> Result<(), Err> {
    let text = data.get_text()?;
    let _ = data.set_text(text)?;
    Ok(())
}
```

### 3. Implementing `DataAcc` derived traits

The `DataAcc` trait abstracts access to data connections.
The methods defined here will be used to obtain data connections via `DataHub` and perform
actual data operations.
The `#[overridable]` macro is also used here.

```rust
use sabi::DataAcc;
use errs::Err;
use override_macro::overridable;

use crate::data_src::{FooDataConn, BarDataConn};

#[overridable]
trait GettingDataAcc: DataAcc {
    fn get_text(&mut self) -> Result<String, Err> {
        let conn = self.get_data_conn::<FooDataConn>("foo")?;
        /* ... */
        Ok("output text".to_string())
    }
}

#[overridable]
trait SettingDataAcc: DataAcc {
    fn set_text(&mut self, text: String) -> Result<(), Err> {
        let conn = self.get_data_conn::<BarDataConn>("bar")?;
        /* ... */
        Ok(())
    }
}
```

### 4. Integrating data traits and `DataAcc` derived traits into `DataHub`

The `DataHub` is the central component that manages all `DataSrc` and `DataConn`,
providing access to them for your application logic.
By implementing the data traits (`MyData`) from step 2 and the `DataAcc` traits
from step 3 on `DataHub`, you integrate them.
The `#[override_with]` macro indicates that the methods of the `MyData` trait
will be provided by the corresponding methods of the `DataAcc` derived traits.

```rust
use sabi::DataHub;
use override_macro::override_with;

use crate::logic_layer::MyData;
use crate::data_access_layer::{GettingDataAcc, SettingDataAcc};

impl GettingDataAcc for DataHub {}
impl SettingDataAcc for DataHub {}

#[override_with(GettingDataAcc, SettingDataAcc)]
impl MyData for DataHub {}
```

### 5. Using logic functions and `DataHub`

Inside your `main` function, register your global `DataSrc` and setup the `sabi` framework.
Then, create an instance of `DataHub` and register the necessary local `DataSrc` using
the `uses` method.
Finally, use the `txn` method of `DataHub` to execute your defined application logic
function (`my_logic`) within a transaction.
This automatically handles transaction commits and rollbacks.

```rust
use sabi::{uses, setup, shutdown_later, DataHub};
use errs::Err;

use crate::data_src::{FooDataSrc, BarDataSrc};
use crate::logic_layer::my_logic;

fn main() {
    // Register global DataSrc
    uses("foo", FooDataSrc{});
    // Set up the sabi framework
    let _ = setup().unwrap();
    // Automatically shut down DataSrc when the application exits
    let _later = shutdown_later();

    // Create a new instance of DataHub
    let mut data = DataHub::new();
    // Register specific DataSrc with DataHub
    data.uses("bar", BarDataSrc{});

    // Execute application logic within a transaction
    // my_logic performs data operations via DataHub
    let _ = data.txn(my_logic).unwrap();
}
```

## Supporting Rust versions

This crate supports Rust 1.85.1 or later.

```bash
% cargo msrv find
  [Meta]   cargo-msrv 0.18.4
        ~~~~~~(omission)~~~~~
Result:
   Considered (min … max):   Rust 1.56.1 … Rust 1.87.0 
   Search method:            bisect                    
   MSRV:                     1.85.1                    
   Target:                   x86_64-apple-darwin
```

## License

Copyright (C) 2024-2025 Takayuki Sato

This program is free software under MIT License.<br>
See the file LICENSE in this distribution for more details.


[repo-url]: https://github.com/sttk/sabi-rust
[crateio-img]: https://img.shields.io/badge/crate.io-ver.0.0.2-fc8d62?logo=rust
[crateio-url]: https://crates.io/crates/sabi-rust
[docrs-img]: https://img.shields.io/badge/doc.rs-sabi_rust-66c2a5?logo=docs.rs
[docrs-url]: https://docs.rs/sabi-rust
[ci-img]: https://github.com/sttk/sabi-rust/actions/workflows/rust.yml/badge.svg?branch=main
[ci-url]: https://github.com/sttk/sabi-rust/actions?query=branch%3Amain
[mit-img]: https://img.shields.io/badge/license-MIT-green.svg
[mit-url]: https://opensource.org/licenses/MIT
