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

**sabi** was developed with the goal of thoroughly separating business logic from data access. However, it differs from conventional Dependency Injection (DI) frameworks that merely invert dependencies by placing an interface between the two layers—**sabi** draws a clear line beyond that simple approach.

What elevates **sabi** to an advanced framework in particular are the following two key techniques: introducing a data-access interface optimized for each individual piece of logic, and routing input/output from the controller layer directly to the data access layer via `DataSrc`, completely bypassing the logic layer.

### Introducing Data-Access Traits Optimized Per Logic Unit

The former approach thoroughly embodies the Interface Segregation Principle (ISP)—one of the SOLID principles that has often ended up more nominal than real in practice. Each piece of logic (use case) defines, on the logic side, its own dedicated trait (interface) that specifies only the operations it truly needs. Meanwhile, the data access side implements traits based on its responsibility as a data provider. The `DataHub` then mediates and maps between the two, so that the logic side never needs to be aware of the data access side's structure, and the data access side never needs to depend on the structure of individual pieces of logic—each maintains its own independent responsibility.

This design is grounded in the philosophy that "the world does not exist as a single, fixed, objective reality, but rather reveals its meaning and form according to the questions and purposes held by the observing subject." The trait that logic should see should not be dictated by the constraints of the data access side, but should instead be defined based on the logic's own context and needs. The same holds true in reverse for the trait that data access should see. What matters to the logic is not the structure of the database or the ORM, but the "capability required to realize this particular use case." What matters to the data access side, on the other hand, is how to access storage or external services. By having both sides define their traits according to their own respective contexts, and having the `DataHub` bridge them together, true loose coupling is achieved—one where neither side depends on the other's internal structure.

Furthermore, because logic never needs to know any implementation details of data access, it can easily be swapped out for mocks that provide the necessary capabilities during testing, achieving high testability as well.

### A Structure That Routes Controller-Layer I/O Directly to the Data Access Layer, Bypassing the Logic Layer

The latter approach—routing input/output from the controller layer directly to the data access layer, completely bypassing the logic layer—was conceived by decomposing the controller layer's role into two elements: "invoking logic" and "input/output data." In conventional architectures, because these two elements were never separated, a layered structure whose sole responsibility was data flow (the so-called "data bucket brigade") became unavoidable, forcing data to be transformed every time it crossed a layer. However, by routing input/output data directly to the infrastructure layer (the data access division), this redundant data flow—and the layered structure that existed to support it—becomes entirely unnecessary.

As a result, the hierarchical dependency that would normally remain between logic and data access is completely eliminated, and is elevated instead into an equal relationship based on a contract defined by the trait's method signatures. This can be seen as an evolutionary extension of the Dependency Inversion Principle (DIP)—taking the conventional DIP, which merely reverses the direction of dependency, a step further by minimizing and localizing the dependency itself within the boundary of the contract.

### An AI-Friendly, Capability-Oriented Framework

Moreover, this structure is also well-suited to AI-driven automated programming. For AI-driven automated programming to be performed safely and accurately, the following conditions are required:

* **Localized dependencies**: The scope of dependencies an AI must grasp for a single change should be confined to a narrow portion of the system, not the system as a whole.
* **Explicit boundaries of side effects**: The extent to which side effects can propagate should be made explicit in the code itself, rather than relying on implicit call ordering.
* **Localized impact of change**: Adding features or swapping implementations should not ripple out into existing code that doesn't use them.
* **Substitutability through contract**: An implementation should be safely replaceable as long as it satisfies the contract (trait), without needing to know its implementation details.

**sabi** satisfies all of these conditions at the structural level by defining a dedicated data access trait for each piece of logic and making that logic depend only on that narrow trait—so that adding functionality to a particular `DataAcc` never affects existing logic, and swapping implementations can be done safely as long as the trait's contract is satisfied. In this way, **sabi** is not merely a DI framework, but an AI-friendly, capability-oriented framework suited to the modern AI era.

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
However, it's the derived traits (like `GettingDataAcc` and `SettingDataAcc` in this example) that define the application-specific methods for accessing data.
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
pub trait SettingDataAcc: DataAcc {
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
pub trait SettingDataAccAsync: DataAcc {
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
