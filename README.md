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

### Core Design Philosophy: Complete Separation of Logic and Data Access

`sabi` is a small-scale application framework rooted in the foundational philosophy of completely separating business logic from data access. However, its objective extends beyond the typical goals of conventional Dependency Injection (DI) frameworks—which merely insert an interface between logic and data access to allow implementation swapping and reduce direct coupling.

Instead, `sabi` requires the logic side to define its own dedicated interfaces and matches them against those provided by the data access side. Through this mechanism, it ensures that **"the logic side holds the ultimate initiative, achieving true independence from data access concerns."**

In traditional DI patterns, methods are frequently grouped based on the structure or convenience of the data access layer, such as database tables or external services (e.g., a `UserRepository` corresponding tightly to a `User` table). Consequently, the logic layer is dragged into and becomes dependent on these structural boundaries, often forcing interfaces to include redundant methods not required by a specific use case. This structure inherently tends to violate the Interface Segregation Principle (ISP).

`sabi` addresses this flaw by adopting a localized approach: **"defining data access interfaces (traits) precisely in units of individual logic components (use cases) that require them."** Because a logic function accepts only a trait declaring the bare minimum operations it actually needs, the logic side remains entirely agnostic of the underlying data access architecture.

Concurrently, the data access implementation can be provided with flexible granularity—whether table-centric or feature-centric—defined as default methods of traits derived from `DataAcc`.

Bridge-building between these distinct trait groups—the logic-driven minimum interfaces and the data-access-driven implementations, which are fundamentally difficult to connect directly in Rust—is orchestrated by `sabi` through a centralized `DataHub` struct and a method overriding (mapping) mechanism via `override_macro`.

Through this architecture, `sabi` thoroughly enforces the separation of concerns, powerfully driving the construction of clean, highly testable application architectures.

### Philosophical Background and Significance in Programming History

`sabi`’s approach of mapping logic-specific interfaces to data access implementations via the `DataHub` is rooted in an epistemological and relativistic premise: *“The appearance of an object changes depending on the context of the observing or utilizing subject.”*

The world does not exist as a single, fixed objective reality; rather, it manifests its meaning and form in response to the questions and purposes held by the subject. Mirroring this concept, interfaces visible to the logic should not be dictated by data-side constraints. Instead, they must be defined according to the inherent needs and context of the logic itself—a philosophy that `sabi` materializes into software design.

In many traditional architectures, database structures or external service models are established first, and the logic is subsequently tailored to conform to them. In `sabi`, however, the logic dictates only the operations it requires, and the data access layer responds by fulfilling them. In short, the system’s center of perspective shifts from "data" to "logic."

This philosophy of "complete separation of logic and data access" can be viewed, in a sense, as a return to the dawn of programming in the 1950s and 60s, when programs were composed of the barest essentials: "procedures" and "data." Yet, its realization is far from mere nostalgia. It integrates **Separation of Concerns** (Edsger W. Dijkstra, 1974) and **Dependency Control**, long pursued by modern software engineering, at an exceptionally high standard.

#### Separation of Concerns (Edsger W. Dijkstra, 1974)

Viewed through the lens of Dijkstra’s "Separation of Concerns," `sabi`’s structural isolation is uncompromisingly thorough.

* **Concerns of Business Logic**: Pure computation and domain knowledge—*what* data to process and evaluate under *which* rules.
* **Concerns of Data Access**: Environment-dependent persistence control—*where* to fetch data from and *where* to store it.

`sabi` strictly isolates these two concerns both spatially and logically via trait-bound boundaries, logic-specific traits, and the mapping mechanism powered by `DataHub` and `override_macro`.

Furthermore, responsibilities within the data access layer itself are granularly decomposed into:

* **`DataSrc`**: Configuration and connectivity to external services.
* **`DataConn`**: Connection lifecycle management and transaction control.
* **`DataAcc`**: The actual data operations.

This prevents multiple responsibilities from collapsing into a single component, ensuring highly localized reasons for change and superior maintainability.

#### Relationship with SOLID Principles

The design of `sabi` aligns seamlessly with the SOLID principles.

First, by entirely severing logic from data access, the **Single Responsibility Principle (SRP)** is satisfied with high precision. The logic layer changes strictly due to modifications in domain rules, while the data access layer changes solely due to infrastructure adjustments.

Second, because the logic depends exclusively on its own dedicated abstractions (traits), the **Open/Closed Principle (OCP)** and the **Liskov Substitution Principle (LSP)** are naturally preserved. Introducing new data sources or swapping in mocks for testing requires adding or modifying implementations of `DataSrc` or `DataConn` without impacting existing logic.

Third, defining dedicated traits per use case realizes the **Interface Segregation Principle (ISP)** in its purest form. Logic components never depend on massive, monolithic data access APIs; they depend strictly on the minimal set of operations they truly necessitate.

Most distinctively, `sabi` offers an extended interpretation of the **Dependency Inversion Principle (DIP)**.
Standard DIP dictates that *“high-level modules should not depend on low-level modules; both should depend on abstractions,”* which typically results in the low-level module conforming to abstractions defined by the high-level module (the inversion of dependency direction).

In `sabi`, however, the logic side and the data access side coexist with independent, parallel abstraction systems:

* **The Logic Side** depends on a "dedicated trait" born of its own context.
* **The Data Access Side** is implemented as a "provider trait" shaped by its own structure.
* **The `DataHub`** mediates and maps the two symmetrically as equal peers.

Consequently, there is no unidirectional hierarchy where one layer dominates the abstraction of the other. Both sides remain completely blind to each other's internal structures, preserving their context-specific abstractions while being symmetrically wired together by the `DataHub`. This structure represents a **"decentralization of dependency relationships,"** transcending simple dependency inversion to form a cornerstone of `sabi`’s identity.

#### Hexagonal Architecture (Alistair Cockburn, 2005)

Hexagonal Architecture positions the application core at the center, abstracting connections to the outside world via "Ports and Adapters." `sabi` inherits this philosophy but pushes it a step further.

In conventional Hexagonal Architecture, ports (interfaces) often end up designed as wide, system-wide contracts shared across the entire application. As a result, despite being application-centric, the granularity and shape of these interfaces frequently pull in structural baggage from the infrastructure (the adapter's constraints).

`sabi` disrupts this by redefining ports at the use-case level. Interfaces do not exist as "global contracts of the application core"; instead, they are formed locally according to "what this specific logic demands." This effectively evolves a core-centric Ports and Adapters model into a **use-case-centric architecture**.

Additionally, while the `DataHub` assumes a role akin to an Adapter Registry or a Composition Root in Hexagonal Architecture, it is more than a mere wiring mechanism. It dynamically reconstructs the relationship between logic traits and data access implementations. As a result, the primary architectural question shifts from *"Which external service are we consuming?"* to *"What does this logic inherently need?"*

In essence, `sabi` elevates Ports and Adapters from a model of "Boundary Design" to one of **"Perspective-Oriented Design."** Rather than locking external connections into static contracts, it alters how the world is viewed based on the logic's context, expanding Hexagonal Architecture into a higher dimension of abstraction.

#### Capability-Based Architecture (2010s–)

The essence of a Capability-Based Architecture lies in governing a system based on "what a subject is permitted to do," rather than "what identity a subject possesses."

The logic-specific traits in `sabi` function precisely as **Capabilities**. A logic block is never granted blanket, omnibus access to a massive Repository or ORM. Instead, it is explicitly handed only the bare minimum capabilities required to execute its specific use case.

This is not mere abstraction; it enforces a strict, type-level boundary over:

1. What can be accessed.
2. What can be known.
3. What can be executed.

In other words, a logic-specific trait in `sabi` is both an interface and a **Security Boundary**.

This design structurally insulates logic from unintended data access and side effects, naturally elevating security, testability, local reasoning, and concurrent safety.

From a Capability Security perspective, `sabi` "encapsulates data access privileges directly into the type system." The capabilities a logic block can wield are statically determined by the trait it receives, making any exceeding operations compile-time impossibilities. By embedding "privilege" into types rather than managing it as a runtime concern, `sabi` exhibits profound synergy with Rust’s ownership and borrowing semantics. It successfully distills a capability-oriented architecture into a practical, Rust-idiomatic framework.

#### CUPID Characteristics (Dan North, 2021)

Proposed as a modern complement or alternative to SOLID, Dan North’s CUPID characterization emphasizes software that exhibits the following traits:

* **Composable**
* **Unix Philosophy** (Small and simple)
* **Predictable**
* **Idiomatic** (Feels natural to the language)
* **Domain-based**

`sabi`’s architecture resonates profoundly with these criteria.

Because its logic-specific traits are micro-interfaces capturing only the bare essentials of a use case, each logic component remains small and highly decoupled. This aligns with being **Composable** and mirrors the **Unix Philosophy** of assembling overall behavior out of small, focused pieces.

Moreover, because data access pathways and transaction boundaries are unified under the `DataHub`, system behavior becomes highly traceable. Since what a logic component can see and execute is strictly constrained at the type level, the ripple effects of side effects and dependencies are easily contained, granting the system high **Predictability**.

Furthermore, `sabi` is naturally tailored to Rust’s core paradigms:

* Abstraction via traits.
* Boundaries of responsibility dictated by ownership and borrowing.
* Explicit dependency mapping using lifetimes.
* Execution efficiency preserved through zero-cost abstractions.

By leveraging these native concepts without fighting the language model, `sabi` stands as a thoroughly **Idiomatic** Rust architecture.

Additionally, because the initiative of interface design belongs to the use case rather than the data layer, the center of gravity always remains within the domain logic. The design originates from "what the logic requires" rather than infrastructure or persistence mechanisms, cementing its **Domain-based** nature.

Crucially, `sabi` does not aspire to be a "monolithic, all-encompassing abstraction framework." It eschews magical, multi-functional machinery in favor of a minimal framework designed to partition responsibilities cleanly. Each element is simple, constrained, and free of extraneous knowledge—a design philosophy closely aligned with the Unix maxim, *"Do One Thing Well."* Rather than masking complexity under heavy abstractions, `sabi` maintains systemic comprehensibility by decomposing and localizing responsibility.

#### AI Agent-Friendly Architecture (2024–)

In an era where Large Language Models (LLMs) and autonomous AI agents are deeply involved in generating, analyzing, and refactoring code, software architecture faces a new paradigm of requirements:

* Localized dependencies.
* Explicit side-effect boundaries.
* Small reasoning contexts.
* Explicit interface contracts.
* Context-contained code comprehension.

The structure of `sabi` is uniquely optimized for these modern demands.

Because logic components interact solely with dedicated traits, an AI agent does not need to parse the entire footprint of a massive Repository or ORM. It needs only to inspect the localized data capabilities presented to that specific logic block, making code completion, static analysis, automated test generation, and specification inference vastly more efficient.

Moreover, because the `DataHub` centralizes the side-effect boundaries, AI agents can effortlessly track exactly where the application interfaces with the external world. This structural predictability is vital for safe, autonomous code modifications and self-directed refactoring.

Historically, frameworks evolved to optimize for "how concisely a human could write code," frequently mistaking surface-level brevity for simplicity. However, this approach often fostered heavy reliance on implicit conventions and implicit framework internals, yielding codebases that are difficult to reason about from a localized snippet alone.

In the age of AI, relying on such vast, shared "implicit knowledge" becomes an architectural bottleneck. What matters now is **High Locality—the ability to reason about code entirely within its local context**. `sabi` structurally and philosophically anticipates this next wave of software design, delivering an architecture natively optimized for both humans and AI agents alike.

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
