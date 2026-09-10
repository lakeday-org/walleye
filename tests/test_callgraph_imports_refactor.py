import walleye.callgraph as callgraph
from walleye.callgraph import _imports
from walleye.discovery import ScanOptions
from walleye.scanner import scan


def _imports_from_scan(tmp_path, monkeypatch, filename, source):
    captured = []
    original = callgraph._imports

    def capture(root, source_bytes, language):
        result = original(root, source_bytes, language)
        captured.append(result)
        return result

    monkeypatch.setattr(callgraph, "_imports", capture)
    (tmp_path / filename).write_text(source)
    scan(tmp_path, ScanOptions(functions=True))
    assert len(captured) == 1
    return captured[0]


def test_python_imports_keep_order_alias_defaults_and_relative_modules():
    source = (
        b"import os, package.sub as sub\n"
        b"from . import local, other as alias\n"
        b"from ..pkg import thing as renamed\n"
    )

    assert _imports(None, source, "python") == [
        {"module": "os", "name": None, "alias": "os", "line": 1},
        {"module": "package.sub", "name": None, "alias": "sub", "line": 1},
        {"module": ".", "name": "local", "alias": "local", "line": 2},
        {"module": ".", "name": "other", "alias": "alias", "line": 2},
        {"module": "..pkg", "name": "thing", "alias": "renamed", "line": 3},
    ]


def test_python_syntax_error_returns_no_imports():
    assert _imports(None, b"def broken(", "python") == []


def test_typescript_imports_and_reexports_keep_order(tmp_path, monkeypatch):
    imports = _imports_from_scan(
        tmp_path,
        monkeypatch,
        "main.ts",
        (
            'import defaultThing, { helper as h, value } from "./lib";\n'
            'import * as namespace from "./namespace";\n'
            'export { public_name } from "./public";\n'
            "export const local = 1;\n"
        ),
    )

    assert imports == [
        {"module": "./lib", "name": None, "alias": None, "line": 1},
        {"module": "./lib", "name": "default", "alias": "defaultThing", "line": 1},
        {"module": "./lib", "name": "helper", "alias": "h", "line": 1},
        {"module": "./lib", "name": "value", "alias": "value", "line": 1},
        {"module": "./namespace", "name": None, "alias": None, "line": 2},
        {"module": "./namespace", "name": None, "alias": "namespace", "line": 2},
        {"module": "./public", "name": None, "alias": None, "line": 3},
    ]


def test_rust_use_and_module_declarations_keep_boundaries(tmp_path, monkeypatch):
    imports = _imports_from_scan(
        tmp_path,
        monkeypatch,
        "main.rs",
        (
            "use crate::worker::helper;\n"
            "use crate::worker::helper as h;\n"
            "use crate::worker::*;\n"
            "mod worker;\n"
            "mod inline { fn helper() {} }\n"
        ),
    )

    assert imports == [
        {"module": "crate::worker", "name": "helper", "alias": "helper", "line": 1},
        {"module": "crate::worker", "name": "helper", "alias": "h", "line": 2},
        {"module": "self::worker", "name": None, "alias": None, "line": 4},
    ]
