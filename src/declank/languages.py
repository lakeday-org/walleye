"""Extension detection is deliberately limited to code, not every bundled grammar."""

from pathlib import Path
from typing import get_args

from tree_sitter_language_pack import SupportedLanguage

LANGUAGES = frozenset(get_args(SupportedLanguage))

# Ambiguous extensions use conventional defaults; --map overrides them.
EXTENSIONS = {
    "actionscript": ".as",
    "ada": ".ada .adb .ads",
    "apex": ".cls .trigger",
    "bash": ".sh .bash",
    "c": ".c .h",
    "cairo": ".cairo",
    "clojure": ".clj .cljs .cljc .edn",
    "commonlisp": ".lisp .lsp",
    "cpp": ".cpp .cc .cxx .hpp .hh .hxx .C .H",
    "csharp": ".cs",
    "cuda": ".cu .cuh",
    "d": ".d",
    "dart": ".dart",
    "elixir": ".ex .exs",
    "elm": ".elm",
    "erlang": ".erl .hrl",
    "fish": ".fish",
    "fortran": ".f .f90 .f95 .f03 .f08 .F .F90",
    "fsharp": ".fs .fsx",
    "gdscript": ".gd",
    "gleam": ".gleam",
    "glsl": ".glsl .vert .frag",
    "go": ".go",
    "groovy": ".groovy .gradle",
    "haskell": ".hs",
    "haxe": ".hx",
    "java": ".java",
    "javascript": ".js .jsx .mjs .cjs",
    "julia": ".jl",
    "kotlin": ".kt .kts",
    "lua": ".lua",
    "luau": ".luau",
    "matlab": ".m",
    "nim": ".nim .nims",
    "nix": ".nix",
    "objc": ".mm",
    "ocaml": ".ml",
    "ocaml_interface": ".mli",
    "odin": ".odin",
    "pascal": ".pas .pp",
    "perl": ".pl .pm",
    "php": ".php .phtml",
    "powershell": ".ps1 .psm1 .psd1",
    "purescript": ".purs",
    "python": ".py .pyw .pyi",
    "r": ".r .R",
    "racket": ".rkt",
    "ruby": ".rb .rake .gemspec",
    "rust": ".rs",
    "scala": ".scala .sc",
    "scheme": ".scm .ss",
    "solidity": ".sol",
    "sql": ".sql",
    "starlark": ".bzl .star",
    "swift": ".swift",
    "tcl": ".tcl",
    "tsx": ".tsx",
    "typescript": ".ts .mts .cts",
    "v": ".v",
    "verilog": ".sv .svh .vh",
    "vhdl": ".vhd .vhdl",
    "vim": ".vim",
    "wgsl": ".wgsl",
    "zig": ".zig",
}
BY_EXTENSION = {ext: lang for lang, extensions in EXTENSIONS.items() for ext in extensions.split()}
FILENAMES = {
    "Rakefile": "ruby",
    "Gemfile": "ruby",
    "Vagrantfile": "ruby",
    "BUILD": "starlark",
    "BUILD.bazel": "starlark",
    "WORKSPACE": "starlark",
    ".bashrc": "bash",
    ".bash_profile": "bash",
}


def detect(path: Path, mappings: dict[str, str] | None = None) -> str | None:
    return (
        (mappings or {}).get(path.suffix)
        or BY_EXTENSION.get(path.suffix)
        or FILENAMES.get(path.name)
    )


def parse_mapping(value: str) -> tuple[str, str]:
    extension, separator, language = value.partition("=")
    if not separator or not extension.startswith(".") or language not in LANGUAGES:
        raise ValueError(
            f"Invalid mapping {value!r}; expected .ext=language (see declank languages)"
        )
    return extension, language
