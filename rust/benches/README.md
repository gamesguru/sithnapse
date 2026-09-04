# Rust benchmarks

Two harness styles live here, matching the two things Synapse's Rust code
does: hot algorithmic paths that should stay comparable release-to-release
(`evaluator.rs`, `glob.rs`), and one-off empirical validation of a specific
complexity claim (`state_hamt.rs`).

## `evaluator.rs`, `glob.rs` — `#![feature(test)]` (nightly only)

Push-rule evaluation and glob-to-regex compilation, using the standard
library's built-in (unstable) `test::Bencher` harness. Run with:

```sh
cargo +nightly bench
```

These use the default libtest harness (no `[[bench]]` entry needed — it's
auto-discovered), so they only build/run under a nightly toolchain.

## `state_hamt.rs` — hand-rolled, stable

Cumulative full-rebuild-vs-incremental-path-copying comparison for the flat
HAMT state store, validating the O(K log₃₂ S) claim in
`docs/development-gg/persistent-typed-hamt-architecture.md` against a real
measurement rather than leaving it asserted. See that file's module doc for
the full methodology (in particular: why it fetches nodes on demand from a
backing-store stand-in instead of handing the whole node history to every
call — an earlier draft that took the shortcut produced a *shrinking*
speedup curve, the wrong signature for the claim it was supposed to verify).

Uses `harness = false` with a hand-rolled `main()` (`std::time::Instant`,
printed table) rather than the `criterion` crate. This matches rezzy's
own bench style ([`benches/db/cumulative_rebuild.rs`][rezzy-bench]) —
despite having Criterion-shaped benchmark files, `rezzy` doesn't actually
depend on the `criterion` crate either, so this avoids adding a new
dependency for one bench file. If more
benches accumulate here later and start wanting Criterion's statistical
rigor (confidence intervals, regression detection against a saved baseline,
HTML reports), reconsider then — don't add it speculatively now.

Runs on stable:

```sh
cargo bench --bench state_hamt
```

Takes noticeably longer than the other two benches (tens of seconds): it
does real cumulative work up to a state size of 4096 entries, not a single
warmed-up hot loop.

[rezzy-bench]: https://github.com/gamesguru/rezzy/blob/3916d64b0c67cb4cc9aa273939db9e76afbc40f1/benches/db/cumulative_rebuild.rs
