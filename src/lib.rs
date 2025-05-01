// Copyright (C) 2024 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

mod async_group;
mod dax;

/// Enums for errors that can occur in this `sabi` crate.
pub mod errors;

pub use async_group::AsyncGroup;
pub use dax::DaxBaseImpl;
pub use dax::{close, setup, start_app, uses};

use errs::Err;

/// The trait for a set of data access methods.
///
/// This trait is inherited by `Dax` implementations for data stores, and each `Dax` implementation
/// defines data access methods to each data store.
/// In data access methods, `DaxConn` instances connected to data stores can be obtained with
/// `get_dax_conn` method.
pub trait Dax {
    /// Gets a `DaxConn` instances by the registered name and casts it to the specified type.
    fn get_dax_conn<C: DaxConn + 'static>(&mut self, name: &str) -> Result<&C, Err>;
}

/// Represents a data source which creates connections to a data store like database, etc.
pub trait DaxSrc {
    /// Connects to a data store and prepares to create `DaxConn` instances.
    ///
    /// If the setup procedure is asynchronous, use the `AsyncGroup` argument.
    fn setup(&mut self, ag: &mut dyn AsyncGroup) -> Result<(), Err>;

    /// Disconnects to a data store.
    ///
    /// If the closing procedure is asynchronous, use the `AsyncGroup` argument.
    fn close(&mut self, ag: &mut dyn AsyncGroup);

    /// Creates a `DaxConn` instance.
    fn create_dax_conn(&mut self) -> Result<Box<dyn DaxConn>, Err>;
}

struct NoopDaxSrc {}

impl DaxSrc for NoopDaxSrc {
    fn setup(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn close(&mut self, _ag: &mut dyn AsyncGroup) {}
    fn create_dax_conn(&mut self) -> Result<Box<dyn DaxConn>, Err> {
        Ok(Box::new(NoopDaxConn {}))
    }
}

/// Represents a connection to a data store.
///
/// This trait provides method interfaces to work in a transaction process.
pub trait DaxConn {
    /// Commits the updates in a transaction.
    fn commit(&mut self, ag: &mut dyn AsyncGroup) -> Result<(), Err>;

    /// Checks whether updates are already committed.
    fn is_committed(&self) -> bool;

    /// Rollbacks updates in a transaction.
    fn rollback(&mut self, ag: &mut dyn AsyncGroup);

    /// Reverts updates forcely even if updates are already committed or this connection does not
    /// have rollback mechanism.
    fn force_back(&mut self, ag: &mut dyn AsyncGroup);

    /// Closes this connection.
    fn close(&mut self);
}

struct NoopDaxConn {}

impl DaxConn for NoopDaxConn {
    fn commit(&mut self, _ag: &mut dyn AsyncGroup) -> Result<(), Err> {
        Ok(())
    }
    fn is_committed(&self) -> bool {
        false
    }
    fn rollback(&mut self, _ag: &mut dyn AsyncGroup) {}
    fn force_back(&mut self, _ag: &mut dyn AsyncGroup) {}
    fn close(&mut self) {}
}
