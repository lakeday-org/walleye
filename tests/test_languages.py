import pytest

from declank.languages import EXTENSIONS, LANGUAGES
from declank.scanner import analyze, parser_for

# Real, minimal programs exercise the parser and metric walker together.
PROGRAMS = {
    "actionscript": "var x:int = 1 + 2;\n",
    "ada": "procedure Main is\n X : Integer := 1 + 2;\nbegin\n null;\nend Main;\n",
    "apex": "class A { Integer add(Integer a, Integer b) { return a + b; } }",
    "bash": 'x=1\necho "$x"\n',
    "c": "int add(int a, int b) { return a + b; }",
    "cairo": "fn add(a: felt252, b: felt252) -> felt252 { a + b }",
    "cpp": "int add(int a, int b) { return a + b; }",
    "cuda": "__device__ int add(int a, int b) { return a + b; }",
    "d": "int add(int a, int b) { return a + b; }",
    "csharp": "class A { int Add(int a, int b) { return a + b; } }",
    "clojure": "(defn add [a b] (+ a b))",
    "commonlisp": "(defun add (a b) (+ a b))",
    "dart": "int add(int a, int b) { return a + b; }",
    "elixir": "defmodule A do\n def add(a, b), do: a + b\nend",
    "elm": "module Main exposing (add)\nadd a b = a + b\n",
    "erlang": "-module(a).\n-export([add/2]).\nadd(A, B) -> A + B.\n",
    "fish": "set x 1\necho $x\n",
    "fortran": "program main\ninteger :: x\nx = 1 + 2\nend program main\n",
    "fsharp": "let add a b = a + b\n",
    "gdscript": "func add(a, b):\n\treturn a + b\n",
    "gleam": "pub fn add(a: Int, b: Int) -> Int { a + b }",
    "glsl": "float add(float a, float b) { return a + b; }",
    "go": "package main\nfunc add(a int, b int) int { return a + b }\n",
    "groovy": "def add(a, b) { return a + b; }\n",
    "haskell": "add a b = a + b\n",
    "haxe": "class Main { static function main() { var x = 1 + 2; } }\n",
    "java": "class A { int add(int a, int b) { return a + b; } }",
    "javascript": "function add(a, b) { return a + b; }",
    "julia": "function add(a, b)\n a + b\nend\n",
    "kotlin": "fun add(a: Int, b: Int): Int { return a + b }",
    "lua": "function add(a, b) return a + b end",
    "luau": "local function add(a: number, b: number): number return a + b end",
    "matlab": "function y = add(a, b)\ny = a + b;\nend\n",
    "nim": "proc add(a, b: int): int =\n  a + b\n",
    "nix": "{ add = a: b: a + b; }",
    "objc": "int add(int a, int b) { return a + b; }",
    "ocaml": "let add a b = a + b\n",
    "ocaml_interface": "val add : int -> int -> int\n",
    "odin": "package main\nadd :: proc(a, b: int) -> int { return a + b }",
    "pascal": "program Main;\nvar x: integer;\nbegin x := 1 + 2; end.\n",
    "perl": "sub add { my ($a, $b) = @_; return $a + $b; }",
    "php": "<?php function add($a, $b) { return $a + $b; }",
    "powershell": "function Add($a, $b) { return $a + $b }",
    "python": "def add(a, b):\n    return a + b\n",
    "purescript": "module Main where\nadd a b = a + b\n",
    "r": "add <- function(a, b) { a + b }",
    "racket": "#lang racket\n(define (add a b) (+ a b))\n",
    "ruby": "def add(a, b)\n a + b\nend",
    "rust": "fn add(a: i32, b: i32) -> i32 { a + b }",
    "scala": "object A { def add(a: Int, b: Int): Int = a + b }",
    "scheme": "(define (add a b) (+ a b))",
    "solidity": (
        "pragma solidity ^0.8.0; contract A { function add(uint a, uint b) "
        "public pure returns (uint) { return a + b; } }"
    ),
    "sql": "SELECT a + b FROM values_table;",
    "starlark": "def add(a, b):\n    return a + b\n",
    "swift": "func add(_ a: Int, _ b: Int) -> Int { return a + b }",
    "tcl": "proc add {a b} { return [expr {$a + $b}] }\n",
    "tsx": "const Add = ({a}: {a: number}) => <span>{a + 1}</span>;",
    "typescript": "function add(a: number, b: number): number { return a + b; }",
    "v": "fn add(a int, b int) int { return a + b }",
    "verilog": "module adder(input a, b, output c); assign c = a + b; endmodule",
    "vhdl": "entity adder is port(a, b: in bit; c: out bit); end adder;",
    "vim": "let x = 1 + 2\n",
    "wgsl": "fn add(a: i32, b: i32) -> i32 { return a + b; }",
    "zig": "fn add(a: i32, b: i32) i32 { return a + b; }",
}


def test_more_than_thirty_languages_have_executable_metric_fixtures():
    assert len(PROGRAMS) > 30
    assert set(PROGRAMS) == set(EXTENSIONS)


@pytest.mark.parametrize("language", sorted(PROGRAMS))
def test_real_program_parses_and_produces_metrics(language):
    records, issues = analyze(PROGRAMS[language].encode(), language, "sample")
    assert not issues, issues
    assert len(records) == 1
    assert records[0]["volume"] > 0
    assert records[0]["distinct_operators"] > 0
    assert records[0]["distinct_operands"] > 0


@pytest.mark.parametrize("language", sorted(LANGUAGES))
def test_every_advertised_grammar_loads(language):
    assert parser_for(language) is not None
