# [sabi][repo-url] [![crates.io][cratesio-img]][cratesio-url] [![doc.rs][docrs-img]][docrs-url] [![CI Status][ci-img]][ci-url] [![MIT License][mit-img]][mit-url]

A small framework for Rust designed to separate logic from data access.

It achieves this by connecting the logic layer and the data access layer via traits, similar to traditional
Dependency Injection (DI).
This reduces the dependency between the two, allowing them to be implemented and tested independently.

However, traditional DI often presented an inconvenience in how methods were grouped.
Typically, methods were grouped by external data service like a database or by database table.
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


## Installation

In Cargo.toml, write this crate as a dependency:

```toml
[dependencies]
sabi-rust = "0.6.0" # For synchronous APIs
```

For asynchronous APIs with `tokio` runtime, enable the `tokio` feature:

```toml
[dependencies]
sabi-rust = { version = "0.6.0", features = ["tokio"] }
tokio = { version = "1", features = ["full"] } # Required for tokio runtime
```

## Usage (Synchronous)

### 1. Implementing `DataSrc` and `DataConn`

First, you'll define `DataSrc` which manages connections to external data services and creates
`DataConn`. 
Then, you'll define `DataConn` which represents a session-specific connection and implements
transactional operations.

```rust
use sabi::{AsyncGroup, DataSrc, DataConn};
use errs::Err;

pub struct FooDataSrc { /* ... */ }

impl DataSrc<FooDataConn> for FooDataSrc {
    fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn close(&mut self) { /* ... */ }
    fn create_data_conn(&mut self) -> Result<Box<FooDataConn>, Err> {
        Ok(Box::new(FooDataConn{ /* ... */ }))
    }
}

pub struct FooDataConn { /* ... */ }

impl FooDataConn { /* ... */ }

impl DataConn for FooDataConn {
    fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn rollback(&mut self, ag: &mut AsyncGroup) { /* ... */ }
    fn close(&mut self) { /* ... */ }
}

pub struct BarDataSrc { /* ... */ }

impl DataSrc<BarDataConn> for BarDataSrc {
    fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn close(&mut self) { /* ... */ }
    fn create_data_conn(&mut self) -> Result<Box<BarDataConn>, Err> {
        Ok(Box::new(BarDataConn{ /* ... */ }))
    }
}

pub struct BarDataConn { /* ... */ }

impl BarDataConn { /* ... */ }

impl DataConn for BarDataConn {
    fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn rollback(&mut self, ag: &mut AsyncGroup) { /* ... */ }
    fn close(&mut self) { /* ... */ }
}

pub struct BazDataSrc { /* ... */ }

impl DataSrc<BazDataConn> for BazDataSrc {
    fn setup(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn close(&mut self) { /* ... */ }
    fn create_data_conn(&mut self) -> Result<Box<BazDataConn>, Err> {
        Ok(Box::new(BazDataConn{ /* ... */ }))
    }
}

pub struct BazDataConn { /* ... */ }

impl BazDataConn { /* ... */ }

impl DataConn for BazDataConn {
    fn commit(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn rollback(&mut self, ag: &mut AsyncGroup) { /* ... */ }
    fn close(&mut self) { /* ... */ }
}

```

### 2. Implementing logic functions and data traits

Define traits and functions that express your application logic.
These traits are independent of specific data source implementations, improving testability.
The `#[overridable]` macro is used to allow these trait implementations to be overridden later.

```rust
use errs::Err;
use override_macro::overridable;

#[overridable]
pub trait MyData {
    fn get_text(&mut self) -> Result<String, Err>;
    fn set_text(&mut self, text: String) -> Result<(), Err>;
    fn set_flag(&mut self, flag: bool) -> Result<(), Err>;
}

pub fn my_logic(data: &mut impl MyData) -> Result<(), Err> {
    let text = data.get_text()?;
    let _ = data.set_text(text)?;
    let _ = data.set_flag(true)?;
    Ok(())
}
```

### 3. Implementing `DataAcc` derived traits

The `DataAcc` trait in `sabi` provides a simple mechanism to retrieve `DataConn` objects. However, it's the *derived traits* (like `GettingDataAcc` and `SettingDataAcc` in this example) that define the application-specific methods for accessing data. These methods then use `DataAcc::get_data_conn` to obtain the appropriate `DataConn` and perform the actual data operations. The `#[overridable]` macro is also used here to allow these methods to be integrated with `DataHub`.

```rust
use sabi::DataAcc;
use errs::Err;
use override_macro::overridable;

use crate::data_src::{FooDataConn, BarDataConn};

#[overridable]
pub trait GettingDataAcc: DataAcc {
    fn get_text(&mut self) -> Result<String, Err> {
        let conn = self.get_data_conn::<FooDataConn>("foo")?;
        /* ... */
        Ok("output text".to_string())
    }
}

#[overridable]
pub trait SettingDataAcc: DataAcc {
    fn set_text(&mut self, text: String) -> Result<(), Err> {
        let conn = self.get_data_conn::<BarDataConn>("bar")?;
        /* ... */
        Ok(())
    }
}

#[overridable]
pub trait UpdatingDataAcc: DataAcc {
    fn set_flag(&mut self, flag: bool) -> Result<(), Err> {
        let conn = self.get_data_conn::<BazDataConn>("baz")?;
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
use errs::Err;

use crate::logic_layer::MyData;
use crate::data_access_layer::{GettingDataAcc, SettingDataAcc, UpdatingDataAcc};

impl GettingDataAcc for DataHub {}
impl SettingDataAcc for DataHub {}
impl UpdatingDataAcc for DataHub {}

#[override_with(GettingDataAcc, SettingDataAcc, UpdatingDataAcc)]
impl MyData for DataHub {}
```

### 5. Using logic functions and `DataHub`

Inside your `main` function, register your global `DataSrc` and setup the `sabi` framework.
Then, create an instance of `DataHub` and register the necessary local `DataSrc` using
the `uses` method.
Finally, use the `txn!` macro to execute your defined application logic
function (`my_logic`) within a transaction.
This automatically handles transaction commits and rollbacks.

```rust
use sabi::{uses, setup, DataHub};

use crate::data_src::{FooDataSrc, BarDataSrc, BazDataSrc};
use crate::logic_layer::my_logic;

// Register global DataSrc using the `sabi::uses!` macro.
// This makes `FooDataSrc` available throughout the application.
uses!("foo", FooDataSrc{});

fn main() {
    // Register global DataSrc using the `sabi::uses` function.
    // This makes `BazDataSrc` available throughout the application.
    uses("baz", BazDataSrc{}).unwrap();

    // Set up the sabi framework
    // _auto_shutdown automatically closes and drops global DataSrc at the end of the scope.
    // NOTE: Don't write as `let _ = ...` because the return variable is dropped immediately.
    let _auto_shutdown = setup().unwrap();

    // Create a new instance of DataHub
    let mut data = DataHub::new();
    // Register session-local DataSrc with DataHub using the `uses` method.
    // This makes `BarDataSrc` available only within this `DataHub` instance's session.
    data.uses("bar", BarDataSrc{});

    // Execute application logic within a transaction
    // my_logic performs data operations via DataHub by getting a text, setting it, and setting a flag.
    let _ = data.txn(my_logic).unwrap();

    // If you need to execute logic without transactional control (e.g., for read-only operations),
    // use the `run` method instead of `txn`.
    // let _ = data.run(my_logic).unwrap();
}
```

## Usage (Asynchronous with Tokio)

When the `tokio` feature is enabled, `sabi-rust` provides asynchronous counterparts for its core components and methods, allowing you to build non-blocking data access layers with the Tokio runtime.

The asynchronous APIs are available under the `sabi::tokio` module.

### 1. Implementing `DataSrc` and `DataConn` (Asynchronous)

Similar to the synchronous version, you'll define `DataSrc` and `DataConn`, but these will use asynchronous methods and return `Pin<Box<dyn Future>>` or be `async fn`. Remember that `DataSrc` and `DataConn` trait methods are now `async fn`.

```rust
use sabi::tokio::{AsyncGroup, DataSrc, DataConn};
use errs::Err;

pub struct FooDataSrc { /* ... */ }

impl DataSrc<FooDataConn> for FooDataSrc {
    async fn setup_async(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn close(&mut self) { /* ... */ }
    async fn create_data_conn_async(&mut self) -> Result<Box<FooDataConn>, Err> {
        Ok(Box::new(FooDataConn{ /* ... */ }))
    }
}

pub struct FooDataConn { /* ... */ }

impl FooDataConn { /* ... */ }

impl DataConn for FooDataConn {
    async fn commit_async(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    async fn rollback_async(&mut self, ag: &mut AsyncGroup) { /* ... */ }
    fn close(&mut self) { /* ... */ }
}

pub struct BarDataSrc { /* ... */ }

impl DataSrc<BarDataConn> for BarDataSrc {
    async fn setup_async(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    fn close(&mut self) { /* ... */ }
    async fn create_data_conn_async(&mut self) -> Result<Box<BarDataConn>, Err> {
        Ok(Box::new(BarDataConn{ /* ... */ }))
    }
}

pub struct BarDataConn { /* ... */ }

impl BarDataConn { /* ... */ }

impl DataConn for BarDataConn {
    async fn commit_async(&mut self, ag: &mut AsyncGroup) -> Result<(), Err> { /* ... */ Ok(()) }
    async fn rollback_async(&mut self, ag: &mut AsyncGroup) { /* ... */ }
    fn close(&mut self) { /* ... */ }
}
```

### 2. Implementing logic functions and data traits (Asynchronous)

Your application logic functions and their associated traits will now be `async`. The `#[overridable]` macro still functions the same way to allow trait implementations to be overridden.

```rust
use errs::Err;
use override_macro::overridable;

#[overridable]
pub trait MyAsyncData {
    async fn get_text_async(&mut self) -> Result<String, Err>;
    async fn set_text_async(&mut self, text: String) -> Result<(), Err>;
    async fn set_flag_async(&mut self, flag: bool) -> Result<(), Err>;
}

pub async fn my_async_logic(data: &mut impl MyAsyncData) -> Result<(), Err> {
    let text = data.get_text_async().await?;
    let _ = data.set_text_async(text).await?;
    let _ = data.set_flag_async(true).await?;
    Ok(())
}
```

### 3. Implementing `DataAcc` derived traits (Asynchronous)

The `DataAcc` trait and its derived traits will now also use `async` methods and rely on `DataAcc::get_data_conn_async` to retrieve asynchronous data connections.

```rust
use sabi::tokio::DataAcc;
use errs::Err;
use override_macro::overridable;

use crate::data_src::{FooDataConn, BarDataConn};

#[overridable]
pub trait GettingAsyncDataAcc: DataAcc {
    async fn get_text_async(&mut self) -> Result<String, Err> {
        let conn = self.get_data_conn_async::<FooDataConn>("foo").await?;
        // ... perform async operations with conn
        Ok("output text".to_string())
    }
}

#[overridable]
pub trait SettingAsyncDataAcc: DataAcc {
    async fn set_text_async(&mut self, text: String) -> Result<(), Err> {
        let conn = self.get_data_conn_async::<BarDataConn>("bar").await?;
        // ... perform async operations with conn
        Ok(())
    }
}
```

### 4. Integrating data traits and `DataAcc` derived traits into `DataHub` (Asynchronous)

The `sabi::tokio::DataHub` serves the same central role, but operates asynchronously. The integration with traits using `#[override_with]` remains conceptually similar.

```rust
use sabi::tokio::DataHub;
use override_macro::override_with;
use errs::Err;

use crate::logic_layer::MyAsyncData;
use crate::data_access_layer::{GettingAsyncDataAcc, SettingAsyncDataAcc};

impl GettingAsyncDataAcc for DataHub {}
impl SettingAsyncDataAcc for DataHub {}

#[override_with(GettingAsyncDataAcc, SettingAsyncDataAcc)]
impl MyAsyncData for DataHub {}
```

### 5. Using logic functions and `DataHub` (Asynchronous)

Use the `#[tokio::main]` macro to run your main asynchronous function. Register global `DataSrc` using `sabi::tokio::uses!`. Set up the framework with `sabi::tokio::setup_async`. Execute your logic with `data.txn_async` or `data.run_async`.

```rust
use sabi::tokio::{uses, uses_async, setup_async, DataHub, logic};
use tokio; // Ensure tokio is in scope for #[tokio::main]

use crate::data_src::{FooDataSrc, BarDataSrc};
use crate::logic_layer::my_async_logic;

// Register global DataSrc using the `sabi::tokio::uses!` macro.
uses!("foo", FooDataSrc{});

#[tokio::main]
async fn main() {
    // Register global DataSrc using the `sabi::tokio::uses_async` function.
    uses_async("bar", BarDataSrc{}).await.unwrap();
    // If there is no risk of conflict with other Tokio tasks, you can use the
    // `sabi::tokio::uses` function, which does not wait for the lock to be released.
    //uses("bar", BarDataSrc{}).unwrap();

    // Set up the sabi framework for async operations
    let _auto_shutdown = setup_async().await.unwrap();

    let mut data = DataHub::new();

    // Execute application logic within an asynchronous transaction
    // The `logic!` macro helps convert an async function into the required closure type.
    let _ = data.txn_async(logic!(my_async_logic)).await.unwrap();

    // If you need to execute logic without transactional control, use `run_async`.
    // let _ = data.run_async(logic!(my_async_logic)).await.unwrap();
}
```

## Supported Rust versions

This crate supports Rust 1.87.0 or later.

```bash
% ./build.sh msrv
  [Meta]   cargo-msrv 0.18.4  

Compatibility Check #1: Rust 1.75.0
  [FAIL]   Is incompatible 

Compatibility Check #2: Rust 1.84.1
  [FAIL]   Is incompatible 

Compatibility Check #3: Rust 1.89.0
  [OK]     Is compatible 

Compatibility Check #4: Rust 1.86.0
  [FAIL]   Is incompatible 

Compatibility Check #5: Rust 1.87.0
  [OK]     Is compatible 

Result:
   Considered (min … max):   Rust 1.56.1 … Rust 1.93.0 
   Search method:            bisect                    
   MSRV:                     1.87.0                    
   Target:                   x86_64-apple-darwin 
```

## License

Copyright (C) 2024-2026 Takayuki Sato

This program is free software under MIT License.<br>
See the file LICENSE in this distribution for more details.


[repo-url]: https://github.com/sttk/sabi-rust
[cratesio-img]: https://img.shields.io/badge/crates.io-ver.0.6.0-fc8d62?logo=rust
[cratesio-url]: https://crates.io/crates/sabi-rust
[docrs-img]: https://img.shields.io/badge/doc.rs-sabi_rust-66c2a5?logo=docs.rs
[docrs-url]: https://docs.rs/sabi-rust
[ci-img]: https://github.com/sttk/sabi-rust/actions/workflows/rust.yml/badge.svg?branch=main
[ci-url]: https://github.com/sttk/sabi-rust/actions?query=branch%3Amain
[mit-img]: https://img.shields.io/badge/license-MIT-green.svg
[mit-url]: https://opensource.org/licenses/MIT
