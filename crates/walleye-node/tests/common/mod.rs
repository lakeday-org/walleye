//! What tests share, in two unrelated halves:
//!
//! - [`processes`]: nodes on runtimes of their own over one bucket, for the
//!   ownership tests to kill, pause and restart.
//! - [`stand_ins`]: the decision service, the drafting model and the
//!   embedder, served in-process so tests of judgements run in CI.
//!
//! Both are re-exported, so a test says `common::Proc` or `common::answer`
//! without caring which half it came from.
#![allow(dead_code, unused_imports)]

pub mod processes;
pub mod stand_ins;

pub use processes::*;
pub use stand_ins::*;
