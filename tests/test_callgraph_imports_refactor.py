from walleye.callgraph import _imports
from walleye.scanner import parser_for


def imports_for(source: bytes, language: str) -> list[dict]:
    root = parser_for(language).parse(source).root_node
    return _imports(root, source, language)


def test_python_imports_preserve_order_aliases_and_relative_modules():
    source = (
        b"import os\n"
        b"import package.module as module_alias, sibling\n"
        b"from .subpackage import item as local_item, other\n"
        b"from .. import parent\n"
    )

    assert imports_for(source, "python") == [
        {"module": "os", "name": None, "alias": "os", "line": 1},
        {
            "module": "package.module",
            "name": None,
            "alias": "module_alias",
            "line": 2,
        },
        {"module": "sibling", "name": None, "alias": "sibling", "line": 2},
        {
            "module": ".subpackage",
            "name": "item",
            "alias": "local_item",
            "line": 3,
        },
        {
            "module": ".subpackage",
            "name": "other",
            "alias": "other",
            "line": 3,
        },
        {"module": "..", "name": "parent", "alias": "parent", "line": 4},
    ]


def test_invalid_python_syntax_returns_no_imports():
    assert imports_for(b"def broken(", "python") == []


def test_typescript_imports_record_bindings_but_reexports_only_record_modules():
    source = (
        b"import default_value, { helper, renamed as local } from './lib';\n"
        b"import * as namespace from './namespace';\n"
        b"import './side-effect';\n"
        b"export { helper } from './lib';\n"
    )

    assert imports_for(source, "typescript") == [
        {"module": "./lib", "name": None, "alias": None, "line": 1},
        {
            "module": "./lib",
            "name": "default",
            "alias": "default_value",
            "line": 1,
        },
        {"module": "./lib", "name": "helper", "alias": "helper", "line": 1},
        {"module": "./lib", "name": "renamed", "alias": "local", "line": 1},
        {"module": "./namespace", "name": None, "alias": None, "line": 2},
        {"module": "./namespace", "name": None, "alias": "namespace", "line": 2},
        {"module": "./side-effect", "name": None, "alias": None, "line": 3},
        {"module": "./lib", "name": None, "alias": None, "line": 4},
    ]


def test_rust_imports_keep_aliases_and_external_paths_but_skip_wildcards_and_inline_modules():
    source = (
        b"use crate::items::{Thing, other as Other, *};\n"
        b"use external::Item;\n"
        b"mod worker;\n"
        b"mod inline { fn hidden() {} }\n"
    )

    assert imports_for(source, "rust") == [
        {"module": "crate::items", "name": "Thing", "alias": "Thing", "line": 1},
        {"module": "crate::items", "name": "other", "alias": "Other", "line": 1},
        {"module": "external", "name": "Item", "alias": "Item", "line": 2},
        {"module": "self::worker", "name": None, "alias": None, "line": 3},
    ]
