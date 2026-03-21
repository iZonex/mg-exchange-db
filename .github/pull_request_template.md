## What

<!-- One-line summary of the change -->

## Why

<!-- Motivation: what problem does this solve? Link issue if applicable -->
Closes #

## Changes

-
-

## Risk Assessment

- [ ] Affects storage engine (WAL, partitions, mmap)
- [ ] Affects query execution (planner, cursors, optimizer)
- [ ] Affects network protocols (pgwire, HTTP, ILP)
- [ ] Affects security (auth, encryption, RLS)
- [ ] Affects replication
- [ ] Config format change (migration needed)
- [ ] None of the above

## Testing

- [ ] Unit tests added / updated
- [ ] Integration tests pass
- [ ] Benchmarks checked (no regression)

## Checklist

- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] `cargo test --workspace` passes
- [ ] No new `unwrap()` in production code paths
- [ ] Documentation updated (if behavior changed)
