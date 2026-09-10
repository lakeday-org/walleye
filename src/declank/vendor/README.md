# Bundled parser

`babel-parser.cjs` is the unmodified `lib/index.js` from `@babel/parser` 7.28.4,
downloaded from its npm release tarball. The MIT license is in `BABEL-LICENSE`.
Source: https://github.com/babel/babel/tree/v7.28.4/packages/babel-parser

It runs inside the Python QuickJS binding with no filesystem or network APIs.
Only parsing is performed; repository code, Babel configs and plugins are never
loaded or executed. It provides a complete AST/token fallback for JS/TS syntax
that the bundled Tree-sitter grammars reject.
