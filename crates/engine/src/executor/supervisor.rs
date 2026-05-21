//! Cross-platform supervised subprocess: spawn, capture, terminate.
//!
//! Extracted from `subprocess.rs` so the environment probe path can
//! reuse the same Job Object (Windows) / process-group (Unix) tear-
//! down semantics used by the regular subprocess node executor.

#![allow(unsafe_code)] // Win32 Job Object + Unix pre_exec(setsid) need raw FFI

use std::time::Duration;

/// Grace period between SIGTERM and SIGKILL on Unix. Long enough
/// for shells and Python interpreters to flush stdout, short
/// enough that a hung child doesn't block run finalization.
///
/// Windows cancellation uses `TerminateJobObject`, which is hard-
/// kill only — no grace window — so this constant is Unix-only in
/// practice. Defined unconditionally so the rest of the supervisor
/// machinery (moved here in later commits) can reference it without
/// cfg gymnastics; `pub(crate)` items aren't flagged by `dead_code`.
pub(crate) const CANCEL_GRACE: Duration = Duration::from_secs(2);
