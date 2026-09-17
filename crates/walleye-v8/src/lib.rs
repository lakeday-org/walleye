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

/// What one batch produced.
///
/// A worker may return rows, write them to named streams, or both. Returning
/// them is the short way to fill the view's own target; writing them is how a
/// worker fans out, sends a row somewhere else, or splits a batch between
/// streams. The host commits all of it together, so a worker chooses where
/// rows go without owning the transaction they go in.
#[derive(Debug, Default)]
pub struct Outcome {
    /// The array the worker returned, as JSON. Empty when it returned nothing.
    pub returned: String,
    /// Rows the worker wrote, in the order it wrote them, as (stream, JSON).
    pub writes: Vec<(String, String)>,
}

/// Rows a worker wrote during one turn, collected for the host.
#[derive(Default)]
struct Written(std::sync::Mutex<Vec<(String, String)>>);

/// The one way out of an isolate.
///
/// A worker cannot open a socket, read a file, or name a host. It hands the
/// host a request as text and gets an answer as text, and the host decides
/// what that request is allowed to be. Keeping the crossing this narrow is
/// what makes the capability auditable: there is one function to review.
pub trait Host: Send + Sync {
    /// Answer one request, or say why not. Both are JSON.
    fn call(&self, request: &str) -> Result<String, String>;
}
/// A host that refuses everything, for a deployment that grants no reach.
pub struct Sealed;
impl Host for Sealed {
    fn call(&self, _request: &str) -> Result<String, String> {
        Err("this worker has no host access".into())
    }
}
struct Reach(std::sync::Arc<dyn Host>);

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
pub fn run(worker: &str, rows: &str, limits: Limits) -> Result<Outcome, Error> {
    run_with_host(worker, rows, limits, std::sync::Arc::new(Sealed))
}

/// Run a worker that may reach the host it was given.
pub fn run_with_host(
    worker: &str,
    rows: &str,
    limits: Limits,
    host: std::sync::Arc<dyn Host>,
) -> Result<Outcome, Error> {
    run_entry(worker, rows, limits, host, Entry::Batch)
}

/// Answer one request with a worker's `fetch` handler. The request and the
/// response are JSON; anything the worker wrote is in the outcome, and the
/// host is expected to land it before it answers.
pub fn run_request(
    worker: &str,
    request: &str,
    limits: Limits,
    host: std::sync::Arc<dyn Host>,
) -> Result<Outcome, Error> {
    run_entry(worker, request, limits, host, Entry::Fetch)
}

fn run_entry(
    worker: &str,
    rows: &str,
    limits: Limits,
    host: std::sync::Arc<dyn Host>,
    entry: Entry,
) -> Result<Outcome, Error> {
    start();
    let heap = limits.heap_bytes.max(8 * 1024 * 1024);
    let mut isolate = v8::Isolate::new(v8::CreateParams::default().heap_limits(0, heap));
    isolate.set_slot(std::sync::Arc::new(Written::default()));
    isolate.set_slot(Reach(host));

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
    let outcome = evaluate(&mut isolate, worker, rows, entry);
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

/// `ctx.write(stream, rowOrRows)` from inside a worker. The rows are held
/// until the turn ends; nothing reaches storage while the worker is still
/// running, so a worker that later throws writes nothing at all.
fn write_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue,
) {
    let fail = |scope: &mut v8::PinScope<'_, '_>, message: &str| {
        if let Some(message) = v8::String::new(scope, message) {
            let exception = v8::Exception::error(scope, message);
            scope.throw_exception(exception);
        }
    };
    let stream = args.get(0);
    if !stream.is_string() {
        return fail(scope, "write needs the name of a stream first");
    }
    let stream = stream.to_rust_string_lossy(scope);
    if stream.is_empty() {
        return fail(scope, "write needs the name of a stream first");
    }
    let rows = args.get(1);
    if rows.is_null_or_undefined() {
        return fail(scope, "write needs a row, or an array of rows");
    }
    let Some(json) = v8::json::stringify(scope, rows) else {
        return fail(scope, "those rows cannot be written down");
    };
    let json = json.to_rust_string_lossy(scope);
    let Some(written) = scope.get_slot::<std::sync::Arc<Written>>().cloned() else {
        return fail(scope, "this worker cannot write");
    };
    if let Ok(mut held) = written.0.lock() {
        held.push((stream, json));
    }
}

/// `ctx.call(request)` from inside a worker: one request out, one answer
/// back, both JSON. A refusal becomes an exception the worker can catch.
fn call_callback(
    scope: &mut v8::PinScope<'_, '_>,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue,
) {
    let throw = |scope: &mut v8::PinScope<'_, '_>, message: &str| {
        if let Some(message) = v8::String::new(scope, message) {
            let exception = v8::Exception::error(scope, message);
            scope.throw_exception(exception);
        }
    };
    let request = args.get(0);
    let request = if request.is_string() {
        request.to_rust_string_lossy(scope)
    } else {
        match v8::json::stringify(scope, request) {
            Some(json) => json.to_rust_string_lossy(scope),
            None => return throw(scope, "that request cannot be sent"),
        }
    };
    let Some(reach) = scope.get_slot::<Reach>().map(|reach| reach.0.clone()) else {
        return throw(scope, "this worker has no host access");
    };
    match reach.call(&request) {
        Ok(answer) => match v8::String::new(scope, &answer) {
            Some(answer) => match v8::json::parse(scope, answer) {
                Some(value) => rv.set(value),
                None => throw(scope, "the host answered with something unreadable"),
            },
            None => throw(scope, "the host answered with more than fits"),
        },
        Err(refused) => throw(scope, &refused),
    }
}

/// The function to call for this kind of turn.
///
/// A worker may default-export a function, which is the whole of it, or an
/// object of named handlers in the shape the reference runtime uses:
/// `export default { fetch, batch }`. A plain function is `batch`, because a
/// worker that only transforms rows should not have to write a wrapper.
fn handler<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    default: v8::Local<'s, v8::Value>,
    entry: Entry,
) -> Result<v8::Local<'s, v8::Function>, Error> {
    if let Ok(function) = v8::Local::<v8::Function>::try_from(default) {
        return match entry {
            Entry::Batch => Ok(function),
            Entry::Fetch => Err(Error::Invalid(
                "it default-exports a plain function, so it has no fetch handler; \
                 export default { fetch } to answer requests"
                    .into(),
            )),
        };
    }
    let Some(object) = default.to_object(scope) else {
        return Err(Error::Invalid(
            "its default export is neither a function nor an object of handlers".into(),
        ));
    };
    let name = entry.name();
    let key = v8::String::new(scope, name).expect("a short name");
    let found = object
        .get(scope, key.into())
        .filter(|value| !value.is_null_or_undefined())
        .ok_or_else(|| Error::Invalid(format!("it has no {name} handler")))?;
    v8::Local::<v8::Function>::try_from(found)
        .map_err(|_| Error::Invalid(format!("its {name} handler is not a function")))
}

/// Which handler a turn is asking for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Entry {
    /// A batch of rows to transform. The default export may be the function
    /// itself.
    Batch,
    /// One request to answer.
    Fetch,
}
impl Entry {
    fn name(self) -> &'static str {
        match self {
            Self::Batch => "batch",
            Self::Fetch => "fetch",
        }
    }
}

fn evaluate(
    isolate: &mut v8::Isolate,
    worker: &str,
    rows: &str,
    entry: Entry,
) -> Result<Outcome, Error> {
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
    let transform = handler(scope, default, entry)?;

    let input =
        v8::String::new(scope, rows).ok_or_else(|| Error::Invalid("batch too large".into()))?;
    let input = v8::json::parse(scope, input)
        .ok_or_else(|| Error::Invalid("the batch could not be given to it".into()))?;
    // The second argument is how a worker reaches storage. There is nothing
    // else on it: no network, no clock, no host beyond this.
    let context_object = v8::Object::new(scope);
    let write_key = v8::String::new(scope, "write").expect("a short name");
    let write = v8::Function::new(scope, write_callback)
        .ok_or_else(|| Error::Invalid("the runtime could not be prepared".into()))?;
    if context_object.set(scope, write_key.into(), write.into()) != Some(true) {
        return Err(Error::Invalid("the runtime could not be prepared".into()));
    }
    let call_key = v8::String::new(scope, "call").expect("a short name");
    let call = v8::Function::new(scope, call_callback)
        .ok_or_else(|| Error::Invalid("the runtime could not be prepared".into()))?;
    if context_object.set(scope, call_key.into(), call.into()) != Some(true) {
        return Err(Error::Invalid("the runtime could not be prepared".into()));
    }
    let receiver = v8::undefined(scope);
    let Some(returned) = transform.call(scope, receiver.into(), &[input, context_object.into()])
    else {
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

    let writes = scope
        .get_slot::<std::sync::Arc<Written>>()
        .cloned()
        .and_then(|written| written.0.lock().ok().map(|held| held.clone()))
        .unwrap_or_default();

    if entry == Entry::Fetch {
        let response = v8::json::stringify(scope, returned)
            .ok_or_else(|| Error::Returned("a response that cannot be read".into()))?;
        return Ok(Outcome {
            returned: response.to_rust_string_lossy(scope),
            writes,
        });
    }
    // A worker that wrote its rows need not also return them. One that did
    // neither has done nothing, and saying so beats a silent empty tier.
    if returned.is_null_or_undefined() {
        if writes.is_empty() {
            return Err(Error::Returned(
                "nothing: a worker must return rows, write them, or both".into(),
            ));
        }
        return Ok(Outcome {
            returned: String::new(),
            writes,
        });
    }
    if !returned.is_array() {
        return Err(Error::Returned(format!(
            "{}, but a worker must return an array of rows",
            describe(scope, returned)
        )));
    }
    let output = v8::json::stringify(scope, returned)
        .ok_or_else(|| Error::Returned("rows that cannot be written down".into()))?;
    Ok(Outcome {
        returned: output.to_rust_string_lossy(scope),
        writes,
    })
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
        run(worker, rows, Limits::default())
            .expect("worker ran")
            .returned
    }
    fn wrote(worker: &str, rows: &str) -> Vec<(String, String)> {
        run(worker, rows, Limits::default())
            .expect("worker ran")
            .writes
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
    fn a_worker_writes_rows_to_the_streams_it_names() {
        let writes = wrote(
            "export default (rows, ctx) => { \
               for (const r of rows) ctx.write(r.ok ? 'kept' : 'quarantine', r); \
             }",
            r#"[{"ok":true,"n":1},{"ok":false,"n":2},{"ok":true,"n":3}]"#,
        );
        let streams: Vec<&str> = writes.iter().map(|(stream, _)| stream.as_str()).collect();
        assert_eq!(
            streams,
            vec!["kept", "quarantine", "kept"],
            "in the order written"
        );
        assert!(writes[1].1.contains("\"n\":2"), "{:?}", writes[1]);
    }
    #[test]
    fn a_worker_may_write_a_whole_array_at_once() {
        let writes = wrote(
            "export default (rows, ctx) => ctx.write('bulk', rows.map(r => ({ n: r.n * 10 })))",
            r#"[{"n":1},{"n":2}]"#,
        );
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, "bulk");
        assert_eq!(writes[0].1, r#"[{"n":10},{"n":20}]"#);
    }
    #[test]
    fn a_worker_may_both_return_and_write() {
        let outcome = run(
            "export default (rows, ctx) => { ctx.write('audit', { seen: rows.length }); \
             return rows; }",
            r#"[{"n":1}]"#,
            Limits::default(),
        )
        .expect("worker ran");
        assert_eq!(outcome.returned, r#"[{"n":1}]"#);
        assert_eq!(outcome.writes.len(), 1);
        assert_eq!(outcome.writes[0].0, "audit");
    }
    #[test]
    fn a_worker_that_neither_returns_nor_writes_is_refused() {
        let error =
            run("export default () => {}", "[]", Limits::default()).expect_err("it did nothing");
        assert!(
            matches!(error, Error::Returned(ref detail) if detail.contains("return rows, write them")),
            "{error}"
        );
    }
    #[test]
    fn a_write_without_a_stream_name_throws_inside_the_worker() {
        let error = run(
            "export default (rows, ctx) => { ctx.write('', {a:1}); return [] }",
            "[]",
            Limits::default(),
        )
        .expect_err("no stream named");
        assert!(
            matches!(error, Error::Threw(ref detail) if detail.contains("name of a stream")),
            "{error}"
        );
    }
    #[test]
    fn a_worker_that_throws_after_writing_writes_nothing() {
        let error = run(
            "export default (rows, ctx) => { ctx.write('kept', {a:1}); throw new Error('later') }",
            "[]",
            Limits::default(),
        )
        .expect_err("it threw");
        assert!(matches!(error, Error::Threw(_)), "{error}");
        // The rows never reach the host, so nothing was written.
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

#[cfg(test)]
mod request_tests {
    use super::*;

    fn answer(worker: &str, request: &str) -> Outcome {
        run_request(
            worker,
            request,
            Limits::default(),
            std::sync::Arc::new(Sealed),
        )
        .expect("worker answered")
    }

    #[test]
    fn a_worker_answers_a_request_with_its_fetch_handler() {
        let outcome = answer(
            "export default { fetch(request) { \
               return { status: 200, body: 'hello ' + JSON.parse(request.body).name }; \
             } }",
            r#"{"method":"POST","path":"/hook","headers":{},"body":"{\"name\":\"world\"}"}"#,
        );
        assert!(
            outcome.returned.contains("hello world"),
            "{}",
            outcome.returned
        );
        assert!(outcome.returned.contains("200"), "{}", outcome.returned);
    }

    #[test]
    fn a_request_handler_may_write_rows_as_well_as_answer() {
        let outcome = answer(
            "export default { fetch(request, ctx) { \
               const event = JSON.parse(request.body); \
               ctx.write('events', event); \
               return { status: 202, body: 'queued' }; \
             } }",
            r#"{"method":"POST","path":"/hook","headers":{},"body":"{\"kind\":\"ping\"}"}"#,
        );
        assert_eq!(outcome.writes.len(), 1);
        assert_eq!(outcome.writes[0].0, "events");
        assert!(
            outcome.writes[0].1.contains("ping"),
            "{:?}",
            outcome.writes[0]
        );
        assert!(outcome.returned.contains("202"), "{}", outcome.returned);
    }

    #[test]
    fn a_worker_with_only_a_batch_handler_cannot_answer_requests() {
        let error = run_request(
            "export default rows => rows",
            "{}",
            Limits::default(),
            std::sync::Arc::new(Sealed),
        )
        .expect_err("it transforms rows, it does not answer");
        assert!(
            matches!(error, Error::Invalid(ref detail) if detail.contains("fetch")),
            "{error}"
        );
    }

    #[test]
    fn a_named_batch_handler_works_as_well_as_a_bare_function() {
        let outcome = run(
            "export default { batch(rows) { return rows.map(r => ({ n: r.n + 1 })) } }",
            r#"[{"n":1}]"#,
            Limits::default(),
        )
        .expect("worker ran");
        assert_eq!(outcome.returned, r#"[{"n":2}]"#);
    }

    #[test]
    fn a_request_handler_is_held_to_the_same_deadline() {
        let error = run_request(
            "export default { fetch() { while (true) {} } }",
            "{}",
            Limits {
                heap_bytes: DEFAULT_HEAP_BYTES,
                deadline: Duration::from_millis(300),
            },
            std::sync::Arc::new(Sealed),
        )
        .expect_err("it never returns");
        assert!(matches!(error, Error::Stopped(_)), "{error}");
    }
}
