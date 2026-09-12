from collections import defaultdict

from walleye.callgraph import Facts, Resolver


class _ResolverFixture(Resolver):
    def __init__(self):
        self.symbols = defaultdict(list)
        self.parents = {}
        self.module_cache = {}
        self.symbols["src/module.rs", "name"] = ["module-symbol"]

    def _file(self, path, _language):
        if path == "src/module":
            return "src/module.rs"
        return None


def _resolve(reference: str) -> str | None:
    facts = Facts(path="src/main.rs", language="rust")
    return _ResolverFixture().call(
        facts,
        {"reference": reference, "scope": ""},
    )


def test_leading_absolute_rust_call_resolves_unique_symbol():
    assert _resolve("::module::name") == "module-symbol"


def test_rust_call_with_empty_final_component_stays_unresolved():
    assert _resolve("::module::") is None


def test_nonleading_rust_call_resolves_unique_symbol():
    assert _resolve("module::name") == "module-symbol"
