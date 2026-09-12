from types import SimpleNamespace

from walleye.callgraph import Resolver


def resolver_with_paths(paths: set[str]) -> Resolver:
    resolver = Resolver.__new__(Resolver)
    resolver.paths = paths
    resolver.module_cache = {}
    return resolver


def test_file_resolves_mts_package_index():
    resolver = resolver_with_paths({"pkg/index.mts"})

    assert resolver._file("pkg", "typescript") == "pkg/index.mts"


def test_file_resolves_cts_package_index():
    resolver = resolver_with_paths({"pkg/index.cts"})

    assert resolver._file("pkg", "typescript") == "pkg/index.cts"


def test_relative_module_resolves_mts_package_index():
    resolver = resolver_with_paths({"src/pkg/index.mts"})
    importing_file = SimpleNamespace(path="src/main.ts", language="typescript")

    assert resolver.module(importing_file, "./pkg") == "src/pkg/index.mts"


def test_relative_module_resolves_cts_package_index():
    resolver = resolver_with_paths({"src/pkg/index.cts"})
    importing_file = SimpleNamespace(path="src/main.ts", language="typescript")

    assert resolver.module(importing_file, "./pkg") == "src/pkg/index.cts"


def test_file_keeps_direct_mts_candidate():
    resolver = resolver_with_paths({"pkg.mts"})

    assert resolver._file("pkg", "typescript") == "pkg.mts"


def test_file_keeps_direct_cts_candidate():
    resolver = resolver_with_paths({"pkg.cts"})

    assert resolver._file("pkg", "typescript") == "pkg.cts"


def test_file_keeps_exact_path_precedence():
    resolver = resolver_with_paths({"pkg", "pkg/index.mts"})

    assert resolver._file("pkg", "typescript") == "pkg"


def test_file_returns_none_for_ambiguous_matches():
    resolver = resolver_with_paths({"pkg/index.mts", "pkg/index.cts"})

    assert resolver._file("pkg", "typescript") is None
