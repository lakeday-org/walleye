//! A small, bounded JavaScript runtime for stream transforms.
//!
//! A worker is a module with a default export that takes an array of rows and
//! returns the rows to write:
//!
//! ```js
//! export default function (rows) {
//!   return rows.filter(r => r.amount > 0)
//!              .map(r => ({ ...r, cents: Math.round(r.amount * 100) }));
//! }
//! ```
//!
//! Everything a worker could use to reach the outside world is absent rather
//! than forbidden. There is no network, no clock beyond the built-ins, no
//! imports, and no way to keep state between batches, because each batch gets
//! its own isolate and the isolate is discarded afterwards. That is the same
//! doctrine the reference runtime settled on: persistent state belongs in
//! streams, not in JavaScript globals.
//!
//! Two limits are enforced rather than advertised. A heap ceiling is declared
//! when the isolate is built, and running out of it terminates the worker
//! instead of growing the host's memory. A deadline terminates a worker that
//! will not return. Both produce an error the caller can report against the
//! batch that caused it.
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

/// Default heap for one worker. The reference runtime uses the same figure,
/// which is a deliberately small failure domain: a worker that needs more
/// than this is doing something a stream transform should not.
pub const DEFAULT_HEAP_BYTES: usize = 128 * 1024 * 1024;
/// Default wall clock a single batch may take.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(15);

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub heap_bytes: usize,
    pub deadline: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            heap_bytes: DEFAULT_HEAP_BYTES,
            deadline: DEFAULT_DEADLINE,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    /// The worker could not be compiled, or is not shaped like a worker.
    Invalid(String),
    /// The worker threw.
    Threw(String),
    /// The worker ran past its deadline or its heap and was stopped.
    Stopped(String),
    /// The worker returned something that is not a batch of rows.
    Returned(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(detail) => write!(f, "worker is not usable: {detail}"),
            Self::Threw(detail) => write!(f, "worker threw: {detail}"),
            Self::Stopped(detail) => write!(f, "worker stopped: {detail}"),
            Self::Returned(detail) => write!(f, "worker returned {detail}"),
        }
    }
}
impl std::error::Error for Error {}

/// What the worker threw, as text.
///
/// A worker stopped by the deadline or the heap guard has not thrown
/// anything, so the caller reports that as a stop rather than dressing it up
/// as an exception the author could fix.
macro_rules! thrown {
    ($scope:expr) => {
        if $scope.has_caught() {
            $scope
                .exception()
                .map(|exception| exception.to_rust_string_lossy($scope))
        } else {
            None
        }
    };
}

/// Start V8 once for the process.
///
/// Call this before building a runtime whose threads may later run a worker.
/// V8's process-global setup has to precede the creation of any thread that
/// enters an isolate, and workers run on the blocking pool. Calling it more
/// than once is harmless; never calling it is not, so every entry point here
/// calls it too.
pub fn start() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform);
        v8::V8::initialize();
    });
}

/// Stops a worker that has run too long.
///
/// The thread outlives the call only until it is told the call finished, so
/// a fast worker does not leave a thread sleeping for the whole deadline.
struct Deadline {
    done: Arc<std::sync::atomic::AtomicBool>,
    fired: Arc<std::sync::atomic::AtomicBool>,
}
impl Deadline {
    fn arm(handle: v8::IsolateHandle, deadline: Duration) -> Self {
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let watching = Arc::clone(&done);
        let raised = Arc::clone(&fired);
        std::thread::spawn(move || {
            let step = Duration::from_millis(10);
            let mut left = deadline;
            while !watching.load(std::sync::atomic::Ordering::Acquire) {
                if left.is_zero() {
                    raised.store(true, std::sync::atomic::Ordering::Release);
                    handle.terminate_execution();
                    return;
                }
                let nap = step.min(left);
                std::thread::sleep(nap);
                left = left.saturating_sub(nap);
            }
        });
        Self { done, fired }
    }
    fn fired(&self) -> bool {
        self.fired.load(std::sync::atomic::Ordering::Acquire)
    }
}
impl Drop for Deadline {
    fn drop(&mut self) {
        self.done.store(true, std::sync::atomic::Ordering::Release);
    }
}

/// Run a worker over one batch of rows.
///
/// `rows` and the result are JSON arrays. Keeping the boundary at text means
/// no V8 type escapes this crate, and the caller never has to hold a handle
/// to an isolate.
///
/// This blocks the calling thread for as long as the worker runs, so call it
/// from a thread that is allowed to block.
pub fn run(worker: &str, rows: &str, limits: Limits) -> Result<String, Error> {
    start();
    let heap = limits.heap_bytes.max(8 * 1024 * 1024);
    let mut isolate = v8::Isolate::new(v8::CreateParams::default().heap_limits(0, heap));

    // Running out of heap has to stop the worker, not grow the host. The
    // small grant is headroom for V8 to unwind in; returning a bigger limit
    // and nothing else would let a worker keep allocating for ever.
    let over = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let raised = Arc::clone(&over);
    let terminator = isolate.thread_safe_handle();
    isolate.add_near_heap_limit_callback(
        near_heap_limit,
        Box::into_raw(Box::new(NearLimit { raised, terminator })) as *mut std::ffi::c_void,
    );

    let deadline = Deadline::arm(isolate.thread_safe_handle(), limits.deadline);
    let outcome = evaluate(&mut isolate, worker, rows);
    let stopped_late = deadline.fired();
    let stopped_big = over.load(std::sync::atomic::Ordering::Acquire);
    drop(deadline);

    match outcome {
        Err(Error::Threw(detail)) if stopped_late => Err(Error::Stopped(format!(
            "ran longer than {:?} ({detail})",
            limits.deadline
        ))),
        Err(Error::Threw(detail)) if stopped_big => Err(Error::Stopped(format!(
            "asked for more than {} MiB of heap ({detail})",
            heap / (1024 * 1024)
        ))),
        Err(Error::Invalid(detail)) if stopped_late => Err(Error::Stopped(format!(
            "ran longer than {:?} ({detail})",
            limits.deadline
        ))),
        other => other,
    }
}

struct NearLimit {
    raised: Arc<std::sync::atomic::AtomicBool>,
    terminator: v8::IsolateHandle,
}
extern "C" fn near_heap_limit(
    data: *mut std::ffi::c_void,
    current: usize,
    _initial: usize,
) -> usize {
    // Safety: the pointer is the box installed alongside this callback and
    // lives as long as the isolate it was installed on.
    let limit = unsafe { &*(data as *const NearLimit) };
    limit
        .raised
        .store(true, std::sync::atomic::Ordering::Release);
    limit.terminator.terminate_execution();
    // Enough room to unwind through, and no more.
    current + 16 * 1024 * 1024
}

fn evaluate(isolate: &mut v8::Isolate, worker: &str, rows: &str) -> Result<String, Error> {
    v8::scope!(let scope, isolate);
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);
    // TryCatch must not move once built, so it is pinned before use.
    let caught = std::pin::pin!(v8::TryCatch::new(scope));
    let scope = &mut caught.init();

    let source_text = v8::String::new(scope, worker)
        .ok_or_else(|| Error::Invalid("source is too large".into()))?;
    let name = v8::String::new(scope, "worker.js").expect("a short name");
    let origin = v8::ScriptOrigin::new(
        scope,
        name.into(),
        0,
        0,
        false,
        0,
        None,
        true,
        false,
        true,
        None,
    );
    let mut source = v8::script_compiler::Source::new(source_text, Some(&origin));
    let Some(module) = v8::script_compiler::compile_module(scope, &mut source) else {
        return Err(Error::Invalid(
            thrown!(scope).unwrap_or_else(|| "it could not be compiled".into()),
        ));
    };
    if module.instantiate_module(scope, refuse_import).is_none() {
        return Err(Error::Invalid(thrown!(scope).unwrap_or_else(|| {
            "it imports something, and a worker may not import".into()
        })));
    }
    if module.evaluate(scope).is_none() {
        return Err(Error::Threw(
            thrown!(scope).unwrap_or_else(|| "while first being run".into()),
        ));
    }

    let namespace = module.get_module_namespace();
    let namespace = namespace
        .to_object(scope)
        .ok_or_else(|| Error::Invalid("it exports nothing".into()))?;
    let key = v8::String::new(scope, "default").expect("a short name");
    let default = namespace
        .get(scope, key.into())
        .ok_or_else(|| Error::Invalid("it has no default export".into()))?;
    let transform: v8::Local<v8::Function> = default.try_into().map_err(|_| {
        Error::Invalid("its default export is not a function taking a batch of rows".into())
    })?;

    let input =
        v8::String::new(scope, rows).ok_or_else(|| Error::Invalid("batch too large".into()))?;
    let input = v8::json::parse(scope, input)
        .ok_or_else(|| Error::Invalid("the batch could not be given to it".into()))?;
    let receiver = v8::undefined(scope);
    let Some(returned) = transform.call(scope, receiver.into(), &[input]) else {
        return Err(Error::Threw(
            thrown!(scope).unwrap_or_else(|| "without a message".into()),
        ));
    };

    // One microtask checkpoint settles a promise that is already resolved,
    // which covers an async worker that never waited on anything. A worker
    // that is still waiting has nothing to wait for, because nothing here can
    // complete later.
    let returned = if returned.is_promise() {
        let promise: v8::Local<v8::Promise> = returned.try_into().expect("a promise");
        scope.perform_microtask_checkpoint();
        match promise.state() {
            v8::PromiseState::Fulfilled => promise.result(scope),
            v8::PromiseState::Rejected => {
                let reason = promise.result(scope).to_rust_string_lossy(scope);
                return Err(Error::Threw(reason));
            }
            v8::PromiseState::Pending => {
                return Err(Error::Returned(
                    "a promise that never settles; a worker cannot wait on anything".into(),
                ));
            }
        }
    } else {
        returned
    };

    if !returned.is_array() {
        return Err(Error::Returned(format!(
            "{}, but a worker must return an array of rows",
            describe(scope, returned)
        )));
    }
    let output = v8::json::stringify(scope, returned)
        .ok_or_else(|| Error::Returned("rows that cannot be written down".into()))?;
    Ok(output.to_rust_string_lossy(scope))
}

fn describe(scope: &mut v8::PinScope<'_, '_>, value: v8::Local<v8::Value>) -> String {
    if value.is_null_or_undefined() {
        return "nothing".into();
    }
    let rendered = value.to_rust_string_lossy(scope);
    let mut short: String = rendered.chars().take(60).collect();
    if rendered.chars().count() > 60 {
        short.push_str("...");
    }
    short
}

/// A worker is one file. Refusing every import is what keeps it that way, and
/// keeps it from reaching anything the host did not hand it.
fn refuse_import<'s>(
    _context: v8::Local<'s, v8::Context>,
    _specifier: v8::Local<'s, v8::String>,
    _attributes: v8::Local<'s, v8::FixedArray>,
    _referrer: v8::Local<'s, v8::Module>,
) -> Option<v8::Local<'s, v8::Module>> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_ok(worker: &str, rows: &str) -> String {
        run(worker, rows, Limits::default()).expect("worker ran")
    }

    #[test]
    fn a_worker_maps_a_batch_of_rows() {
        let out = run_ok(
            "export default function (rows) { return rows.map(r => ({ n: r.n * 2 })) }",
            r#"[{"n":1},{"n":2}]"#,
        );
        assert_eq!(out, r#"[{"n":2},{"n":4}]"#);
    }
    #[test]
    fn a_worker_may_drop_rows() {
        let out = run_ok(
            "export default rows => rows.filter(r => r.keep)",
            r#"[{"keep":true,"n":1},{"keep":false,"n":2}]"#,
        );
        assert_eq!(out, r#"[{"keep":true,"n":1}]"#);
    }
    #[test]
    fn an_async_worker_that_never_waits_still_answers() {
        let out = run_ok(
            "export default async function (rows) { return rows }",
            r#"[{"n":1}]"#,
        );
        assert_eq!(out, r#"[{"n":1}]"#);
    }
    #[test]
    fn a_worker_that_throws_reports_what_it_threw() {
        let error = run(
            "export default () => { throw new Error('no good') }",
            "[]",
            Limits::default(),
        )
        .expect_err("it threw");
        assert!(
            matches!(error, Error::Threw(ref detail) if detail.contains("no good")),
            "{error}"
        );
    }
    #[test]
    fn a_worker_that_returns_the_wrong_shape_is_refused() {
        let error = run("export default () => 42", "[]", Limits::default()).expect_err("not rows");
        assert!(
            matches!(error, Error::Returned(ref detail) if detail.contains("array of rows")),
            "{error}"
        );
    }
    #[test]
    fn a_worker_with_no_default_export_is_refused() {
        let error =
            run("export const other = 1", "[]", Limits::default()).expect_err("nothing to call");
        assert!(matches!(error, Error::Invalid(_)), "{error}");
    }
    #[test]
    fn a_worker_may_not_import() {
        let error = run(
            "import fs from 'fs'; export default rows => rows",
            "[]",
            Limits::default(),
        )
        .expect_err("imports are refused");
        assert!(matches!(error, Error::Invalid(_)), "{error}");
    }
    #[test]
    fn a_worker_cannot_reach_the_network_or_the_host() {
        for reach in ["fetch", "process", "require", "Deno", "globalThis.fetch"] {
            let out = run_ok(
                &format!("export default () => [{{ there: typeof {reach} }}]"),
                "[]",
            );
            assert_eq!(out, r#"[{"there":"undefined"}]"#, "{reach} is reachable");
        }
    }
    #[test]
    fn a_worker_that_will_not_return_is_stopped() {
        let error = run(
            "export default () => { while (true) {} }",
            "[]",
            Limits {
                heap_bytes: DEFAULT_HEAP_BYTES,
                deadline: Duration::from_millis(300),
            },
        )
        .expect_err("it never returns");
        assert!(matches!(error, Error::Stopped(_)), "{error}");
    }
    #[test]
    fn a_worker_that_eats_the_heap_is_stopped() {
        let error = run(
            "export default () => { const held = []; for (;;) { held.push(new Array(100000).fill('x')) } }",
            "[]",
            Limits {
                heap_bytes: 16 * 1024 * 1024,
                deadline: Duration::from_secs(20),
            },
        )
        .expect_err("it eats the heap");
        assert!(matches!(error, Error::Stopped(_)), "{error}");
    }
    #[test]
    fn one_worker_keeps_nothing_for_the_next() {
        let counter = "export default () => { globalThis.n = (globalThis.n ?? 0) + 1; \
                       return [{ n: globalThis.n }] }";
        assert_eq!(run_ok(counter, "[]"), r#"[{"n":1}]"#);
        assert_eq!(
            run_ok(counter, "[]"),
            r#"[{"n":1}]"#,
            "a second batch starts clean"
        );
    }
}
