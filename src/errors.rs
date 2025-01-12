// Copyright (C) 2024-2025 Takayuki Sato. All Rights Reserved.
// This program is free software under MIT License.
// See the file LICENSE in this distribution for more details.

/// The enum type represents errors that can occur during asynchronous operations managed by an
/// `AsyncGroup`.
#[derive(Debug)]
pub enum AsyncGroup {
    /// Indicates that a thread panicked during execution.
    /// It holds a message describing the panic.
    ThreadPanicked { message: String },
}
