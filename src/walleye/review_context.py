"""Snapshot-backed, bounded source retrieval. Agents never search the repository."""

import hashlib
import math
from collections import defaultdict
from pathlib import Path

from .discovery import ScanOptions, discover, exclusion_reason
from .metrics import is_function, walk
from .scanner import parser_for

DECLARATIONS = {
    "type_alias_declaration",
    "interface_declaration",
    "struct_item",
    "enum_item",
    "class_definition",
    "class_declaration",
    "const_item",
    "static_item",
    "variable_declarator",
    "assignment",
    "field_declaration",
    "type_definition",
}
IMPORTS = {"import_statement", "import_from_statement", "use_declaration", "preproc_include"}


def encode(value) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def estimate_tokens(text: str) -> int:
    """Explicit estimate, not a claim to implement a model-specific tokenizer."""
    return math.ceil(len(text.encode("utf-8")) / 3)


def syntax(source: bytes, language: str):
    root = parser_for(language).parse(source).root_node
    if root.has_error and language in {"javascript", "typescript", "tsx"}:
        from .javascript import parse

        root, errors = parse(source, language, "<context>")
        if errors:
            return None
    return None if root is None or root.has_error else root


def span(node) -> tuple[int, int]:
    return node.start_point.row + 1, max(
        node.start_point.row + 1, node.end_point.row + bool(node.end_point.column)
    )


class SourceIndex:
    def __init__(self, report: dict, sources: dict[str, bytes]):
        self.report = report
        self.root = Path(report["root"])
        self.base = self.root.parent if self.root.is_file() else self.root
        self.sources = sources
        self.hashes = dict(report["source_hashes"])
        self.rows = {
            r.get("graph_id", f"function:{r['path']}:{r['line']}"): r
            for r in report["records"]
            if r["kind"] == "function"
        }
        self.by_file = defaultdict(list)
        for key, row in self.rows.items():
            self.by_file[row["path"]].append((key, row))
        self._lines = {}
        self._declarations = {}
        self.tests = defaultdict(list)
        self.test_coverage = {"parsed_files": 0, "failed_files": 0, "limited": False}

    def lines(self, path):
        if path not in self._lines:
            # Match parser line coordinates: only LF starts a new source line.
            self._lines[path] = self.sources[path].decode("utf-8").split("\n")
        return self._lines[path]

    def resource(self, path, *, key=None, line=1, end_line=None, name=None):
        return {
            "id": key or f"file:{path}",
            "path": path,
            "line": line,
            "end_line": end_line or len(self.lines(path)),
            "name": name,
            "sha256": self.hashes[path],
        }

    def excerpt(self, resource, line=None, end_line=None, role="source"):
        start = resource["line"] if line is None else line
        end = resource["end_line"] if end_line is None else end_line
        if not resource["line"] <= start <= end <= resource["end_line"]:
            raise ValueError("Requested lines are outside the indexed resource")
        return {
            "resource_id": resource["id"],
            "path": resource["path"],
            "line": start,
            "end_line": end,
            "sha256": resource["sha256"],
            "role": role,
            "code": "\n".join(
                f"{i}: {self.lines(resource['path'])[i - 1]}" for i in range(start, end + 1)
            ),
            "complete_resource": start == resource["line"] and end == resource["end_line"],
        }

    def declarations(self, path, language):
        if path in self._declarations:
            return self._declarations[path]
        declarations, imports = [], []
        root = syntax(self.sources[path], language)
        if root is not None:
            for node in walk(root):
                if node.type in IMPORTS:
                    imports.append(
                        self.resource(
                            path,
                            key=f"import:{path}:{node.start_byte}",
                            line=span(node)[0],
                            end_line=span(node)[1],
                        )
                    )
                if node.type not in DECLARATIONS:
                    continue
                name = node.child_by_field_name("name") or node.child_by_field_name("left")
                if name is None or not name.text.decode("utf-8").isidentifier():
                    continue
                scopes = []
                parent = node.parent
                while parent is not None:
                    if is_function(parent) or parent.type in {
                        "class_declaration",
                        "class_definition",
                        "impl_item",
                    }:
                        scopes.append(span(parent))
                    parent = parent.parent
                declarations.append(
                    {
                        **self.resource(
                            path,
                            key=f"declaration:{path}:{node.start_byte}",
                            name=name.text.decode(),
                            line=span(node)[0],
                            end_line=span(node)[1],
                        ),
                        "scopes": scopes,
                    }
                )
        self._declarations[path] = declarations, imports
        return declarations, imports

    def index_tests(self, names: set[str], *, max_bytes=None):
        """One AST identifier index; test matches are candidates, not resolved test coverage."""
        options = ScanOptions(include_tests=True)
        discovered = discover(self.root, options)
        self.test_coverage["discovery_issues"] = len(discovered.issues)
        remaining = max_bytes if max_bytes is not None else math.inf
        for path, relative, language in discovered.files:
            if exclusion_reason(relative, ScanOptions()) != "tests-or-fixtures":
                continue
            if language not in {
                "python",
                "javascript",
                "typescript",
                "tsx",
                "rust",
                "go",
                "java",
                "c",
                "cpp",
            }:
                continue
            if remaining <= 0:
                self.test_coverage["limited"] = True
                break
            try:
                with path.open("rb") as stream:
                    source = stream.read(min(options.max_bytes, remaining) + 1)
                if len(source) > min(options.max_bytes, remaining) or b"\0" in source:
                    self.test_coverage["limited"] = True
                    continue
                remaining -= len(source)
                source.decode("utf-8")
                root = syntax(source, language)
                if root is None:
                    self.test_coverage["failed_files"] += 1
                    continue
                self.test_coverage["parsed_files"] += 1
                matches = defaultdict(list)
                for node in walk(root):
                    if node.type not in {"identifier", "property_identifier", "field_identifier"}:
                        continue
                    name = node.text.decode("utf-8")
                    if name in names and len(matches[name]) < 3:
                        matches[name].append(node.start_point.row + 1)
                if matches:
                    self.sources[relative] = source
                    self.hashes[relative] = hashlib.sha256(source).hexdigest()
                    for name, lines in matches.items():
                        if len(self.tests[name]) < 12:
                            self.tests[name].extend((relative, line) for line in lines)
            except (OSError, UnicodeError, ValueError, RuntimeError, LookupError):
                self.test_coverage["failed_files"] += 1

    def verify(self, resources):
        """Reject stale or redirected source before dispatch and before accepting evidence."""
        for path in sorted({item["path"] for item in resources}):
            real = self.base / path
            if not real.resolve().is_relative_to(self.base.resolve()):
                raise ValueError(f"Source escaped the repository: {path}")
            if any(part.is_symlink() for part in [real, *real.parents] if part != self.base):
                raise ValueError(f"Source became a symlink: {path}")
            with real.open("rb") as stream:
                source = stream.read(len(self.sources[path]) + 1)
            if hashlib.sha256(source).hexdigest() != self.hashes[path]:
                raise ValueError(f"Source changed since scanning; rescan: {path}")


def build_packet(candidate, index: SourceIndex, graph, token_limit=200000):
    row = candidate["row"]
    target_id = candidate["id"]
    target = index.resource(
        row["path"],
        key=target_id,
        line=row["line"],
        end_line=row["end_line"],
        name=row["qualified_name"],
    )
    packet = {
        "schema_version": 1,
        "task_id": candidate["task_id"],
        "objective": candidate["objective"],
        "assignment": (
            "Identify at most one concrete behavioral defect in the target. Establish a trigger, "
            "expected behavior, actual behavior, source evidence, and a regression test."
            if candidate["objective"] == "bug"
            else "Identify at most one behavior-preserving refactor of the target. Explain "
            "the change, preserved contracts, expected benefit, and a validation test."
        ),
        "target": target,
        "selection": {
            k: v for k, v in candidate.items() if k not in {"row", "neighbors", "cycle_members"}
        },
        "metrics": {
            key: row.get(key)
            for key in (
                "cyclomatic_complexity",
                "max_nesting",
                "maintainability_index",
                "difficulty",
                "fan_in",
                "fan_out",
                "dependent_count",
                "cycle_size",
                "graph_status",
                "complexity_status",
                "parser",
            )
        },
        "branches": row.get("structure_hotspots", [])[:3],
        "graph": {"direction": "caller -> callee", "edges": [], "nodes": [], "unresolved": []},
        "resources": [target],
        "source": [],
        "gaps": [],
        "test_evidence": "AST identifier matches are test candidates; linkage is not proven.",
        "context_budget": {
            "estimated_tokens_limit": token_limit,
            "estimator": "ceil(UTF-8 bytes / 3); excludes Codex's own framing",
        },
    }
    # Leave room for instructions/schema and a useful response, never silently oversize.
    packet_limit = token_limit - 900

    def fits():
        return estimate_tokens(encode(packet)) <= packet_limit

    def add_resource(resource):
        if any(r["id"] == resource["id"] for r in packet["resources"]):
            return True
        packet["resources"].append(resource)
        if not fits():
            packet["resources"].pop()
            return False
        return True

    def add_excerpt(resource, start=None, end=None, role="source"):
        excerpt = index.excerpt(resource, start, end, role)
        if any(
            c["path"] == excerpt["path"]
            and c["line"] <= excerpt["line"] <= excerpt["end_line"] <= c["end_line"]
            for c in packet["source"]
        ):
            return True
        for position, existing in enumerate(packet["source"]):
            if (
                existing["resource_id"] == resource["id"]
                and existing["role"] == role
                and (
                    max(existing["line"], excerpt["line"])
                    <= min(existing["end_line"], excerpt["end_line"]) + 1
                )
            ):
                merged = index.excerpt(
                    resource,
                    min(existing["line"], excerpt["line"]),
                    max(existing["end_line"], excerpt["end_line"]),
                    role,
                )
                packet["source"][position] = merged
                if fits():
                    return True
                packet["source"][position] = existing
        packet["source"].append(excerpt)
        if not fits():
            packet["source"].pop()
            return False
        return True

    if not add_excerpt(target, role="target"):
        raise ValueError(
            f"Complete target exceeds the context window: {row['path']}:{row['line']}; "
            "it was not sent as a fragment"
        )

    # Keep only this target's edges. Neighbor metadata resolves every emitted endpoint.
    edges = row.get("callers", []) + row.get("callees", [])
    omitted_edges = 0
    for edge in edges:
        packet["graph"]["edges"].append(edge)
        if not fits():
            packet["graph"]["edges"].pop()
            omitted_edges += 1
    packet["graph"]["omitted_edges"] = omitted_edges
    for key in sorted({e[k] for e in packet["graph"]["edges"] for k in ("source", "target")}):
        node_row = index.rows.get(key)
        packet["graph"]["nodes"].append(
            {"id": key, **{k: node_row[k] for k in ("path", "qualified_name", "line", "end_line")}}
            if node_row
            else {"id": key, "name": "<module>"}
        )
    unresolved = graph.get("unresolved_by_source", {}).get(target_id, [])
    packet["graph"]["unresolved"] = unresolved[:8]
    packet["graph"]["omitted_unresolved"] = max(0, len(unresolved) - 8)
    packet["graph"]["cycle_members"] = candidate.get("cycle_members", [])[:12]
    packet["graph"]["omitted_cycle_members"] = max(0, len(candidate.get("cycle_members", [])) - 12)
    related = []
    for edge in edges:
        other = edge["source"] if edge["target"] == target_id else edge["target"]
        other_row = index.rows.get(other)
        if other_row:
            res = index.resource(
                other_row["path"],
                key=other,
                line=other_row["line"],
                end_line=other_row["end_line"],
                name=other_row["qualified_name"],
            )
        elif edge["path"] in index.sources:
            res = index.resource(edge["path"])
        else:
            continue
        role = "caller" if edge["target"] == target_id else "callee"
        if add_resource(res):
            related.append((res, edge, role))

    # Whole callees and caller functions supply contracts and surrounding control flow.
    for res, _edge, role in related:
        if role == "callee":
            if not add_excerpt(res, role="callee"):
                add_excerpt(
                    res,
                    res["line"],
                    min(res["end_line"], res["line"] + 5),
                    "callee signature; body available by indexed request",
                )
    for res, edge, role in related:
        if role == "caller":
            if add_excerpt(res, role="caller"):
                continue
            add_excerpt(
                res,
                max(res["line"], edge["line"] - 3),
                min(res["end_line"], edge["line"] + 4),
                "caller call site",
            )

    declarations, imports = index.declarations(row["path"], row["language"])
    target_text = "\n".join(index.lines(row["path"])[row["line"] - 1 : row["end_line"]])
    # Token membership selects local declarations, never claims semantic type resolution.
    import re

    identifiers = set(re.findall(r"\b[A-Za-z_$][\w$]*\b", target_text))
    for res in imports:
        source = "\n".join(index.lines(res["path"])[res["line"] - 1 : res["end_line"]])
        names = set(re.findall(r"\b[A-Za-z_$][\w$]*\b", source))
        names -= {"import", "from", "as", "type", "use", "pub", "const", "return"}
        if names & identifiers and add_resource(res):
            add_excerpt(res, role="import")
    for res in declarations:
        scopes = res["scopes"]
        resource = {k: v for k, v in res.items() if k != "scopes"}
        if (
            res["name"] in identifiers
            and not (row["line"] <= res["line"] <= res["end_line"] <= row["end_line"])
            and all(start <= row["line"] <= end for start, end in scopes)
            and add_resource(resource)
        ):
            add_excerpt(resource, role="local declaration candidate")
    parents = sorted(
        [
            (key, parent)
            for key, parent in index.by_file[row["path"]]
            if key != target_id
            and parent["line"] <= row["line"] <= row["end_line"] <= parent["end_line"]
        ],
        key=lambda entry: entry[1]["end_line"] - entry[1]["line"],
    )
    for key, parent in parents:
        res = index.resource(
            row["path"],
            key=key,
            name=parent["qualified_name"],
            line=parent["line"],
            end_line=parent["end_line"],
        )
        if add_resource(res):
            add_excerpt(res, role="enclosing function")
            for guard in parent.get("structure_hotspots", []):
                if guard["line"] <= row["line"] <= guard["end_line"]:
                    add_excerpt(
                        res,
                        guard["line"],
                        min(guard["end_line"], guard["line"] + 2),
                        "enclosing control flow",
                    )
    for path, line in index.tests.get(row["name"], [])[:3]:
        res = index.resource(path)
        if add_resource(res):
            if not add_excerpt(res, role="test candidate"):
                add_excerpt(
                    res, max(1, line - 10), min(res["end_line"], line + 16), "test candidate"
                )
    file_res = index.resource(row["path"])
    add_resource(file_res)
    for path in graph.get("modules_by_source", {}).get(row["path"], []):
        if path in index.sources:
            add_resource(index.resource(path, name="related module; binding not proven"))
    if not any(s["role"] == "test candidate" for s in packet["source"]):
        packet["gaps"].append(
            "No related test excerpt supplied; this is not a test coverage claim."
        )
    packet["gaps"].append(
        "Static calls, imports, and declaration matching are incomplete. Request indexed context "
        "when a contract or enclosing condition is missing; do not infer it from metrics."
    )
    if packet["graph"]["omitted_edges"]:
        packet["gaps"].append("Some callgraph edges exceed the context window.")
    # Metadata also counts. Shrink optional evidence first, never the only target excerpt.
    while not fits():
        removable = [
            i
            for i, s in enumerate(packet["source"])
            if s["role"] not in {"target", "signature", "target branch excerpt"}
        ]
        if not removable:
            break
        packet["source"].pop(removable[-1])
    if not fits():
        raise ValueError("Context metadata exceeds the packet budget")
    packet["context_budget"]["estimated_tokens"] = estimate_tokens(encode(packet)) + 16
    return packet


def expand_context(packet, requests, index: SourceIndex, token_limit: int | None = None):
    resources = {r["id"]: r for r in packet["resources"]}
    expanded = []
    for request in requests:
        resource = resources.get(request["resource_id"])
        if resource is None:
            raise ValueError("Context request is not in this task's resource catalog")
        start, end = request["line"], request["end_line"]
        expanded.append(index.excerpt(resource, start, end, "requested context"))
        if token_limit is not None and estimate_tokens(encode(expanded)) > token_limit:
            raise ValueError("Context expansion exceeds the task budget")
    return expanded
