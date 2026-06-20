<div align="center">
  <a href="https://github.com/sttk/sabi-rust">
    <img src="./images/sabi-rust.png" width="250" height="auto" alt="Sabi"/>
  </a>

  <h2>
  "sabi" - A small framework to separate logics and data accesses
  </h2>
  <br>

  [![crates.io][cratesio-img]][cratesio-url] [![doc.rs][docrs-img]][docrs-url] [![CI Status][ci-img]][ci-url] [![MIT License][mit-img]][mit-url]
</div>

## Overview

**sabi** was developed with the goal of achieving a thorough separation between business logic and data access. However, it sets itself apart from conventional Dependency Injection (DI) frameworks that merely invert dependencies by sandwiching an interface between the two layers.

In particular, two key techniques elevate **sabi** into a highly advanced framework: the introduction of data access traits optimized for each specific piece of logic, and an approach that routes inputs and outputs from the controller layer directly into the data access layer via `DataSrc`, bypassing the logic layer entirely.

The former approach fully materializes the **Interface Segregation Principle (ISP)**, which has frequently become a mere formality within the SOLID principles. Furthermore, because adding methods to `DataAcc` or incorporating additional services via `DataSrc` does not affect existing logic that does not utilize them, capabilities can be extended without modifying existing code—thereby conceptually satisfying the **Open-Closed Principle (OCP)**.

Additionally, even if a `DataAcc` implementation providing a specific capability is replaced with an alternative implementation, or if responsibilities are rearranged among different `DataAcc` traits within the data access layer, the behavior of the logic layer remains unchanged as long as the underlying contract is maintained. Thus, the **Liskov Substitution Principle (LSP)** is realized at the contractual level.

Moreover, because both high-level logic and low-level data access depend on the `DataAcc` contract defined as an individual capability, the **Dependency Inversion Principle (DIP)** is also achieved at the architectural level. In this manner, rather than adhering strictly to the classical OOP context that presupposes inheritance, **sabi** realizes the core intent of each principle—"separation of concerns," "localization of change impact," "substitutability based on contracts," and "dependence on abstractions"—at the architectural level. It possesses a structure that conceptually aligns with all SOLID principles, an achievement that is exceptionally rare in the history of software design.

The latter approach—which routes inputs and outputs from the controller layer directly into the data access layer, bypassing the logic layer entirely—was conceived by breaking down the role of the controller layer into two distinct elements: "logic invocation" and "input/output data." In conventional architectures, because these two elements remained unseparated, a hierarchical structure dedicated solely to data flow (the so-called "data bucket brigade") was inevitable, requiring data to be transformed as it was passed from one layer to the next. However, by routing input/output data directly to the infrastructure layer (the data access department), this redundant data flow and the very hierarchical structure that supported it become entirely unnecessary.

As a result, the residual hierarchical dependency that typically persists between logic and data access is completely eliminated, elevating them into an equal relationship based on the contract defined by the trait's method signatures. This can be viewed as an evolutionary extension of the Dependency Inversion Principle (DIP)—pushing beyond conventional DIP, which merely inverts the direction of dependency, to minimize and localize the dependency itself within contractual boundaries.

This structure delivers the "restricted dependencies" and "localization of explicitly bounded contexts" required for AI-driven automated programming, making **sabi** a cutting-edge, AI-friendly, Capability-oriented framework for today's AI era.


## Install

In Cargo.toml, write this crate as a dependency:

```toml
[dependencies]
sabi-rust = "0.7.3" # For synchronous APIs
```

For asynchronous APIs with `tokio` runtime, enable the `tokio` feature:

```toml
[dependencies]
sabi-rust = { version = "0.7.3", features = ["tokio"] }
tokio = { version = "1", features = ["full"] } # Required for tokio runtime
```

## Usage (Synchronous)

### 1. Implementing a logic function and a data access trait

First, define a function that represents your application logic, along with its dedicated data access trait.
This trait is independent of specific data source implementations, improving testability.
The `#[overridable]` macro is used to allow this trait implementation to be overridden later.

```rust
use override_macro::overridable;

#[overridable]
pub trait MyData {
    fn get_text(&mut self) -> errs::Result<String>;
    fn set_text(&mut self, text: String) -> errs::Result<()>;
}

pub fn my_logic(data: &mut impl MyData) -> errs::Result<()> {
    let text = data.get_text()?;
    let _ = data.set_text(text)?;
    Ok(())
}
```

### 2. Implementing DataAcc derived traits

The `DataAcc` trait provides a simple mechanism to retrieve `DataConn` objects.
However, it's the derived traits (like `GettingDataAcc`, `RedisSettingDataAcc`, and `StdioPrintingDataAcc` in this example) that define the application-specific methods for accessing data.
These methods then use `DataAcc::get_data_conn` to obtain the appropriate `DataConn` and perform the actual data operations.
The `#[overridable]` macro is also used here to allow these methods to be integrated with `DataHub`.

```rust
use sabi::DataAcc;
use override_macro::overridable;
use sabi_redis::RedisDataConn;
use sabi_stdio::StdioDataConn;  // This is a conceptual, non-existent DataConn.

#[overridable]
pub trait GettingDataAcc: DataAcc {
    fn get_text(&mut self) -> errs::Result<String> {
        Ok("output text".to_string())
    }
}

#[overridable]
pub trait RedisSettingDataAcc: DataAcc {
    fn set_text(&mut self, text: String) -> errs::Result<()> {
        let redis_data_conn = self.get_data_conn::<RedisDataConn>("redis")?;
        let redis_conn = data_conn.get_connection();
        redis_conn
            .set("key", text)
            .map_err(|e| errs::Err::with_source("fail to set text to key", e))?;

        redis_data_conn.add_rollback(|redis_conn| {
            redis_conn
                .del("key")
                .map_err(|e| errs::Err::with_source("fail to delete key for rollback", e))
        });

        let stdio_data_conn = self.get_data_conn::<StdioDataConn>("stdio")?;
        stdio_data_conn.add_post_commit(|_stdin, stdout, _stderr| {
            stdout.println(text)
        });

        Ok(())
    }
}
```

### 3. Integrating data traits and DataAcc derived traits into DataHub

The `DataHub` is the central component that manages all `DataSrc` and `DataConn`, providing access to them for your application logic.
It implements `Send`.
By implementing the data traits (`MyData`) from step 1. and the `DataAcc` traits from step 2. on `DataHub`, you integrate them.
The `#[override_with]` macro indicates that the methods of the `MyData` trait will be provided by the corresponding methods of the `DataAcc` derived traits.

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

### 4. Using logic functions and DataHub

Inside your `main` function, register a global `DataSrc` and setup the `sabi` framework.
Then, create an instance of `DataHub` and register the necessary local `DataSrc` using the `uses` method.
Finally, use the `run` method or `txn` method of `DataHub` to execute your defined application logic function (`my_logic`) without or within a transaction.

```rust
use sabi::{uses, setup, DataHub};
use sabi_redis::RedisDataSrc;
use sabi_stdio::StdioDataSrc;  // This is a conceptual, non-existent DataConn.
use crate::logic_layer::my_logic;
use std::process::ExitCode;

// Register global DataSrc using the `sabi::uses!` macro.
// This makes `RedisDataSrc` available throughout the application.
uses!("redis", RedisDataSrc::new("redis://127.0.0.1:6379/0"));

fn main() -> errs::Result<()> {
    // Register global DataSrc using the `sabi::uses` function.
    // This makes `RedisDataSrc` available throughout the application.
    //uses("baz", RedisDataSrc{})?;

    // Set up the sabi framework
    // _auto_shutdown automatically closes and drops global DataSrc at the end of the scope.
    // NOTE: Don't write as `let _ = ...` because the return variable is dropped immediately.
    let _auto_shutdown = setup()?;

    run()?;
}

fn run() -> errs::Result<()> {
    let mut data = DataHub::new();

    // Register session-local DataSrc with DataHub using the `uses` method.
    // This makes `StdioDataSrc` available only within this `DataHub` instance's session.
    data.uses("stdio", StdioDataSrc::new());

    // Execute application logic without a transactional control.
    data.run(my_logic)?;

    // If you need to execute logic within a transaction, use the `txn` method instead of `run`.
    //data.txn(my_logic)?;
}
```

## Usage (Asynchronous)

When the `tokio` feature is enabled, `sabi-rust` provides asynchronous counterparts for its core components and methods, allowing you to build non-blocking data access layers with the Tokio runtime.

The asynchronous APIs are available under the `sabi::tokio` module.

### 1. Implementing logic functions and data traits (Asynchronous)

Your application logic functions and their associated traits will now be async.
The `#[overridable]` macro still functions the same way to allow trait implementations to be overridden.

Note that if the logic function is executed within a spawned task (e.g., via `tokio::spawn`), the data access trait argument must also implement `Send`.
This is because the future returned by the logic function captures the argument and must itself be `Send`.

```rust
use override_macro::overridable;

#[overridable]
pub trait MyAsyncData {
    async fn get_text_async(&mut self) -> Result<String, Err>;
    async fn set_text_async(&mut self, text: String) -> Result<(), Err>;
    async fn set_flag_async(&mut self, flag: bool) -> Result<(), Err>;
}

pub async fn my_async_logic(data: &mut (impl MyAsyncData + Send)) -> Result<(), Err> {
    let text = data.get_text_async().await?;
    let _ = data.set_text_async(text).await?;
    let _ = data.set_flag_async(true).await?;
    Ok(())
}
```

### 2. Implementing DataAcc derived traits (Asynchronous)

The `DataAcc` trait and its derived traits will now also use async methods and rely on `DataAcc::get_data_conn_async` to retrieve asynchronous data connections.

```rust
use sabi::tokio::DataAcc;
use override_macro::overridable;
use sabi_redis::RedisDataConnAsync;
use sabi_stdio::StdioDataConnAsync;  // This is a conceptual, non-existent DataConn.

#[overridable]
pub trait GettingDataAccAsync: DataAcc {
    async fn get_text_async(&mut self) -> errs::Result<String> {
        Ok("output text".to_string())
    }
}

#[overridable]
pub trait RedisSettingDataAccAsync: DataAcc {
    async fn set_text_async(&mut self, text: String) -> errs::Result<()> {
        let redis_data_conn = self.get_data_conn_async::<RedisDataConnAsync>("redis").await?;
        let redis_conn = data_conn.get_connection();
        redis_conn
            .set("key", text)
            .await
            .map_err(|e| errs::Err::with_source("fail to set text to key", e))?;

        redis_data_conn.add_rollback(|redis_conn| {
            redis_conn
                .del("key")
                .await
                .map_err(|e| errs::Err::with_source("fail to delete key for rollback", e))
        }).await;

        let stdio_data_conn = self.get_data_conn_async::<StdioDataConnAsync>("stdio").await?;
        stdio_data_conn.add_post_commit(|_stdin, stdout, _stderr| {
            stdout.println(text)
        }).await;

        Ok(())
    }
}
```

### 3. Integrating data traits and DataAcc derived traits into DataHub (Asynchronous)

The `sabi::tokio::DataHub` serves the same central role, but operates asynchronously.
It also implements `Send`.
The integration with traits using `#[override_with]` remains conceptually similar.

```rust
use sabi::tokio::DataHub;
use override_macro::override_with;

use crate::logic_layer::MyAsyncData;
use crate::data_access_layer::{GettingAsyncDataAcc, SettingAsyncDataAcc};

impl GettingAsyncDataAcc for DataHub {}
impl SettingAsyncDataAcc for DataHub {}

#[override_with(GettingAsyncDataAcc, SettingAsyncDataAcc)]
impl MyAsyncData for DataHub {}
```

### 4. Using logic functions and DataHub (Asynchronous)

Use the `#[tokio::main]` macro to run your main asynchronous function.
Register global `DataSrc` using `sabi::tokio::uses!`.
Set up the framework with `sabi::tokio::setup_async`.
Execute your logic with `data.txn_async` or `data.run_async`.

```rust
use sabi::tokio::{uses, uses_async, setup_async, DataHub, logic};
use tokio; // Ensure tokio is in scope for #[tokio::main]
use sabi_redis::RedisDataSrcAsync;
use sabi_stdio::StdioDataSrcAsync;
use crate::logic_layer::my_async_logic;

// Register global DataSrc using the `sabi::tokio::uses!` macro.
uses!("redis", RedisDataSrcAsync::new("redis://127.0.0.1:6379/0"));

#[tokio::main]
async fn main() -> errs::Result<()> {
    // Register global DataSrc using the `sabi::tokio::uses_async` function.
    //uses_async("redis", RedisDataSrcAsync::new("redis://127.0.0.1:6379/0")).await?;

    // If there is no risk of conflict with other Tokio tasks, you can use the
    // `sabi::tokio::uses` function, which does not wait for the lock to be released.
    //uses("redis", RedisDataSrcAsync::new("redis://127.0.0.1:6379/0"))?;

    // Set up the sabi framework for async operations
    let _auto_shutdown = setup_async().await.unwrap();

    run_async().await?;
}
    
async fn run_async() -> errs::Result<()> {
    tokio::spawn(async move {
        // Create a new instance of DataHub.
        // Since DataHub is Send, it can be moved into another task.
        let mut data = DataHub::new();

        // Register session-local DataSrc with DataHub using the `uses` method.
        // This makes `BarDataSrc` available only within this `DataHub` instance's session.
        // If this `DataHub` is moved between threads, `ds` must also implement `Send`.
        data.uses("stdio", StdioDataSrc::new());

        // Execute application logic without a transactional control.
        data.run_async(logic!(my_async_logic)).await?;

        // Execute application logic within an asynchronous transaction
        // The `logic!` macro helps convert an async function into the required closure type.
        // The resulting future is `Send`.
        data.run_async(logic!(my_async_logic)).await?;

        // If you need to execute logic within a transaction, use `txn_async` method instead of
        // `txn_async`.
        //data.txn_async(logic!(my_async_logic)).await?;
    })
    .await
}
```

## Related Links

### Data Sources
- [sabi_redis (Rust)](https://github.com/sttk/sabi_redis-rust) ... The DataSrc implementation for Redis

### Implementations in other languages
- [sabi (Golang)](https://github.com/sttk/sabi) ... The sabi implementation in Go
- [sabi (Java)](https://github.com/sttk/sabi-java) ... The sabi implementation in Java


## Supported Rust versions

This crate supports Rust 1.87.0 or later.

```bash
% ./build.sh msrv
  [Meta]   cargo-msrv 0.18.4

Compatibility Check #1: Rust 1.76.0
  [FAIL]   Is incompatible

Compatibility Check #2: Rust 1.86.0
  [FAIL]   Is incompatible

Compatibility Check #3: Rust 1.91.1
  [OK]     Is compatible

Compatibility Check #4: Rust 1.88.0
  [OK]     Is compatible

Compatibility Check #5: Rust 1.87.0
  [OK]     Is compatible

Result:
   Considered (min … max):   Rust 1.56.1 … Rust 1.95.0
   Search method:            bisect
   MSRV:                     1.87.0
   Target:                   x86_64-apple-darwin
```

## License

Copyright (C) 2024-2026 Takayuki Sato

This program is free software under MIT License.<br>
See the file LICENSE in this distribution for more details.

[repo-url]: https://github.com/sttk/sabi-rust
[cratesio-img]: https://img.shields.io/badge/crates.io-ver.0.7.3-fc8d62?logo=rust
[cratesio-url]: https://crates.io/crates/sabi-rust
[docrs-img]: https://img.shields.io/badge/doc.rs-sabi_rust-66c2a5?logo=docs.rs
[docrs-url]: https://docs.rs/sabi-rust
[ci-img]: https://github.com/sttk/sabi-rust/actions/workflows/rust.yml/badge.svg?branch=main
[ci-url]: https://github.com/sttk/sabi-rust/actions?query=branch%3Amain
[mit-img]: https://img.shields.io/badge/license-MIT-green.svg
[mit-url]: https://opensource.org/licenses/MIT
