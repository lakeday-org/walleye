// The few web APIs json-render's composer uses, for an isolate that has none.
// Imported first, so they exist before anything else runs.
globalThis.structuredClone ??= (value) =>
  value === undefined ? value : JSON.parse(JSON.stringify(value));
globalThis.performance ??= { now: () => Date.now() };
if (!globalThis.AbortController) {
  class Signal {
    aborted = false;
    reason = undefined;
    throwIfAborted() {
      if (this.aborted) throw this.reason;
    }
    addEventListener() {}
    removeEventListener() {}
  }
  globalThis.AbortController = class {
    signal = new Signal();
    abort(reason) {
      this.signal.aborted = true;
      this.signal.reason = reason;
    }
  };
}
