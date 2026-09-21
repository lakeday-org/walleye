# Benchmarks

What walleye is measured against, kept beside the code so a number in a commit
message can be reproduced rather than taken on trust.

| | what it measures |
| --- | --- |
| [text-to-sql](text-to-sql/) | questions answered from a search box, scored by running the SQL |

Each one is a directory with its own README, its data, and a runner that takes
a node's address and a token. They are not part of `cargo test`: they need a
running node, they call paid services, and they measure quality rather than
assert it.
