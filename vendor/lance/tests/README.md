# Lance resource tests

Memory and IO tests for the vendored Lance crate. Upstream tooling; not a
Lakeday product surface.

```shell
TEST_BINARY=$(cargo test --test resource_tests --no-run 2>&1 | tail -n1 | sed -n 's/.*(\([^)]*\)).*/\1/p')
```
