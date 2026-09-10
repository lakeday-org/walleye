"""Frozen, data-only regression cases against isolated JavaScript function scopes.

QuickJS receives no host callbacks, filesystem, network, process, or credentials.
TypeScript is stripped by Node without evaluating project modules or configuration.
Unsupported runtimes/closures remain explicitly unverified.
"""

import hashlib
import json
import os
import re
import subprocess

import quickjs

from .metrics import walk
from .review_context import syntax

LANGUAGES = {"javascript", "typescript", "tsx"}
PROFILE = "declank-json-functions-v1"


def digest(source):
    return hashlib.sha256(source).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)


def function_node(source, language, name):
    root = syntax(source, language)
    if root is None:
        raise ValueError("Source does not parse")
    matches = []
    for node in root.named_children:
        if node.type == "export_statement":
            node = node.child_by_field_name("declaration")
        if node is not None and node.type == "function_declaration":
            named = node.child_by_field_name("name")
            if named and named.text.decode() == name:
                matches.append(node)
    if len(matches) != 1:
        raise ValueError("Verification requires one named top-level function")
    return matches[0]


def source_bundle(source, language, name):
    if language not in LANGUAGES:
        raise ValueError(f"No executable verification adapter for {language}")
    function_node(source, language, name)
    root = syntax(source, language)
    declarations = {}
    for node in root.named_children:
        if node.type == "export_statement":
            node = node.child_by_field_name("declaration")
        if node is None:
            continue
        if node.type == "function_declaration":
            declarations[node.child_by_field_name("name").text.decode()] = node
        elif node.type in {"lexical_declaration", "variable_declaration"}:
            for child in node.named_children:
                named = child.child_by_field_name("name")
                if named is not None and named.type == "identifier":
                    declarations[named.text.decode()] = node
    pending, selected = [name], {}
    while pending:
        key = pending.pop()
        node = declarations.get(key)
        if node is None or node.start_byte in selected:
            continue
        selected[node.start_byte] = node
        pending.extend(
            n.text.decode()
            for n in walk(node)
            if n.type == "identifier" and n.text.decode() in declarations
        )
    code = "\n".join(
        source[n.start_byte : n.end_byte].decode() for _, n in sorted(selected.items())
    )
    if language in {"typescript", "tsx"}:
        strip = (
            "const fs=require('node:fs');"
            "const {stripTypeScriptTypes}=require('node:module');"
            "process.stdout.write(stripTypeScriptTypes(fs.readFileSync(0,'utf8')));"
        )
        try:
            result = subprocess.run(
                ["node", "--no-warnings", "-e", strip],
                input=code,
                text=True,
                capture_output=True,
                timeout=15,
                env={"PATH": os.environ.get("PATH", "")},
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise ValueError(
                "TypeScript verification requires Node with stripTypeScriptTypes"
            ) from error
        if result.returncode:
            raise ValueError("The function bundle needs unsupported TypeScript/JSX or imports")
        code = result.stdout
    context = quickjs.Context()
    context.set_memory_limit(64 * 1024 * 1024)
    context.set_time_limit(1)
    try:
        context.eval(code)
    except quickjs.JSException as error:
        raise ValueError(
            f"Function dependencies cannot run in the isolated adapter: {error}"
        ) from error
    return code


def validate_cases(cases, objective="bug"):
    if not 2 <= len(cases) <= 30:
        raise ValueError("Provide 2–30 independent regression/control cases")
    ids = set()
    kinds = set()
    for case in cases:
        if case["id"] in ids or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", case["id"]):
            raise ValueError("Test IDs must be unique short identifiers")
        ids.add(case["id"])
        kinds.add(case["kind"])
        args = json.loads(case["arguments_json"])
        expected = json.loads(case["expected_json"])
        canonical(args)
        canonical(expected)
        if not isinstance(args, list):
            raise ValueError("Test arguments must be a JSON array")
        if case["outcome"] == "throw" and not isinstance(expected, str):
            raise ValueError("A thrown outcome needs the expected error name")
        if not case["reason"].strip():
            raise ValueError("Each test needs a contract rationale")
    if "control" not in kinds or (objective == "bug" and "regression" not in kinds):
        raise ValueError("A bug test plan needs regression and control cases")


def run_cases(bundle, name, cases):
    if not re.fullmatch(r"[A-Za-z_$][\w$]*", name):
        raise ValueError("Unsupported callable name")
    results = []
    for case in cases:
        context = quickjs.Context()
        context.set_memory_limit(64 * 1024 * 1024)
        context.set_max_stack_size(1024 * 1024)
        context.set_time_limit(1)
        args = canonical(json.loads(case["arguments_json"]))
        script = """JSON.stringify((() => {
          try {
            const value = CALL;
            if (value && typeof value.then === 'function')
              return {outcome:'unsupported', value:'Asynchronous function'};
            if (typeof value === 'number' && !Number.isFinite(value))
              return {outcome:'nonfinite', value:String(value)};
            return {outcome: value === undefined ? 'undefined' : 'value',
                    value: value === undefined ? null : value};
          } catch (error) { return {outcome:'throw', value:error.name}; }
        })())""".replace("CALL", f"{name}(...{args})")
        try:
            context.eval(bundle)
            actual = json.loads(context.eval(script))
            # Missing runtime dependencies are harness limitations, not reproduced defects.
            error = actual["outcome"] == "unsupported" or (
                actual["outcome"] == "throw" and actual["value"] == "ReferenceError"
            )
            expected = json.loads(case["expected_json"])
            passed = (
                not error
                and actual["outcome"] == case["outcome"]
                and (
                    case["outcome"] == "undefined"
                    or canonical(actual["value"]) == canonical(expected)
                )
            )
            results.append(
                {
                    "id": case["id"],
                    "kind": case["kind"],
                    "passed": passed,
                    "actual": actual,
                    "harness_error": error,
                }
            )
        except (quickjs.JSException, ValueError, TypeError) as error:
            results.append(
                {
                    "id": case["id"],
                    "kind": case["kind"],
                    "passed": False,
                    "harness_error": True,
                    "error": str(error)[:1000],
                }
            )
    return {
        "adapter": PROFILE,
        "total": len(results),
        "passed": sum(r["passed"] for r in results),
        "harness_errors": sum(r["harness_error"] for r in results),
        "cases": results,
    }


def baseline_gate(result, objective):
    if result["harness_errors"] or any(
        not r["passed"] for r in result["cases"] if r["kind"] == "control"
    ):
        return False
    regressions = [r for r in result["cases"] if r["kind"] == "regression"]
    if objective == "bug":
        return bool(regressions) and all(not r["passed"] for r in regressions)
    return result["passed"] == result["total"]


def replace_function(source, language, name, replacement):
    original = function_node(source, language, name)
    proposed = replacement.encode()
    updated = function_node(proposed, language, name)
    root = syntax(proposed, language)
    if (
        len(root.named_children) != 1
        or updated.start_byte != 0
        or updated.end_byte != len(proposed)
    ):
        raise ValueError("A patch must replace only the assigned function")
    original_header = source[original.start_byte : original.child_by_field_name("body").start_byte]
    updated_header = proposed[: updated.child_by_field_name("body").start_byte]
    if b"".join(original_header.split()) != b"".join(updated_header.split()):
        raise ValueError("A candidate must preserve the function name and signature")
    return source[: original.start_byte] + proposed + source[original.end_byte :]
