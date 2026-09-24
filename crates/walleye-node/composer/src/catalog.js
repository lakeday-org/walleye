// What a dashboard may be made of. The engine offers panels built from these
// and nothing else, and a renderer draws exactly these, so this file is the
// contract between the two: a component added here needs a renderer too.
//
// Every visual reads its rows from state, by a `{ "$state": "/q0" }` binding
// the engine writes, so a saved dashboard is redrawn by running its queries
// again, without composing it again.
import { z } from "zod";

// Rows, or where they are bound. The composer checks props with bindings
// resolved against its state, where each panel's rows are an empty list;
// a saved spec keeps the binding.
const rows = z.union([
  z.array(z.record(z.string(), z.unknown())),
  z.object({ $state: z.string().startsWith("/") }),
]);
const column = z.string().min(1);

export const components = {
  // Holds the other panels, in reading order.
  Grid: {
    props: z.object({ columns: z.number().int().min(1).max(4) }),
    slots: ["default"],
  },
  // One number: `value` of the first row.
  Metric: {
    props: z.object({ title: z.string(), data: rows, value: column }),
  },
  // How measures change along an ordered axis, usually time.
  LineChart: {
    props: z.object({ title: z.string(), data: rows, x: column, y: z.array(column).min(1) }),
  },
  // How measures compare across categories.
  BarChart: {
    props: z.object({ title: z.string(), data: rows, x: column, y: z.array(column).min(1) }),
  },
  // Shares of a whole, for a few categories.
  PieChart: {
    props: z.object({ title: z.string(), data: rows, label: column, value: column }),
  },
  // How two measures relate, one point per row.
  ScatterChart: {
    props: z.object({ title: z.string(), data: rows, x: column, y: column }),
  },
  // The rows themselves.
  Table: {
    props: z.object({ title: z.string(), data: rows, columns: z.array(column).min(1) }),
  },
};

/// Whether every element is a known component with props it accepts, and
/// the tree hangs together from its root.
export function validate(spec) {
  if (!spec || typeof spec.root !== "string" || typeof spec.elements !== "object") {
    return { success: false };
  }
  for (const [id, element] of Object.entries(spec.elements)) {
    const component = components[element?.type];
    if (!component || !component.props.safeParse(element.props ?? {}).success) {
      return { success: false, id };
    }
    for (const child of element.children ?? []) {
      if (!(child in spec.elements)) return { success: false, id };
    }
  }
  return { success: spec.root in spec.elements };
}

export const catalog = { data: { components }, validate };
