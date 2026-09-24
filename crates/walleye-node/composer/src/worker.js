// A walleye V8 worker that composes a dashboard with json-render's composer.
//
// The engine sends the panels it can draw - each an element built from real
// query results, with a description of what it shows - and what the person
// asked for. The composer asks Jev which panels that needs and how to lay
// them out, and answers with a spec. Jev is reached through `ctx.call`, so
// its key stays in the engine; the rows never enter this isolate at all,
// since the composer only chooses among descriptions of them.
//
// A bare isolate has no web APIs. What the composer reaches for is shimmed
// here: specs are JSON, so a JSON round trip is a faithful clone, and nothing
// here is ever aborted, so the signal only has to exist.
import "./shims.js";
import { experimental_composeSpec } from "@json-render/core";
import { catalog, validate } from "./catalog.js";

// What every choice is made under. Offered one query several ways, "which
// does the request need" read literally is the table, since it holds the
// answer; a dashboard wants the drawing that shows the answer at a glance.
const GUIDANCE = [
  "This is an analytics dashboard: each query is offered drawn several ways, and exactly one",
  "way should be used for each query the request asks about.",
  "Choose the drawing a reader takes in at a glance: a line for a measure over time, bars to",
  "compare a measure across categories or rank them, a pie only for shares of a whole across",
  "a few categories, one number for a single value.",
  "Use a table only when the request asks for the rows or records themselves, or when no",
  "drawing fits the data.",
].join(" ");

async function compose(request, ctx) {
  const evaluate = async ({ state, questions }) => {
    const answer = ctx.call({ jev: { state, questions } });
    return { answers: answer.answers };
  };
  let complete = null;
  let steps = 0;
  for await (const event of experimental_composeSpec({
    catalog,
    candidates: request.candidates,
    prompt: request.prompt,
    evaluate,
    initialSpec: request.spec ?? undefined,
    elementDescriptions: request.descriptions ?? undefined,
    context: request.context ?? undefined,
    initialState: request.state ?? {},
    instructions: { next: GUIDANCE },
    maxElements: request.maxElements ?? 16,
  })) {
    if (event.type === "step") steps += 1;
    if (event.type === "complete") complete = event;
  }
  if (!complete) throw new Error("the composer ended without a dashboard");
  const spec = complete.spec;
  if (spec) delete spec.state;
  return {
    spec,
    valid: spec ? validate(spec).success : false,
    stopReason: complete.stopReason,
    steps: complete.steps.map((step) => ({
      choice: step.choice,
      description: step.description,
      confidence: step.confidence,
    })),
    rounds: steps,
  };
}

export default { fetch: compose };
