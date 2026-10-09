# Contributing to BitDaemon

Thanks for your interest! BitDaemon is a large, long-running port —
contributions of any size are welcome.

## Ground rules

Everything in **[`AGENTS.md`](AGENTS.md)** applies to humans too.
In particular:

- 100% safe Rust (`#![forbid(unsafe_code)]`), no exceptions.
- Semantically faithful to libtorrent — when unsure, read the C++
  source (the *behavioral* spec), don't guess.
- `cargo fmt`, `clippy -D warnings`, `cargo test` must pass.
- Update `docs/PORTING.md` status and `CHANGELOG.md` with your change.

## How to pick work

1. Open [`docs/PORTING.md`](docs/PORTING.md) and pick a `not started`
   module inside the current [`ROADMAP.md`](ROADMAP.md) phase.
2. Respect dependency order — a phase unlocks the next.
3. Prefer finishing (`verified`) an open module over starting a new one.

## Commits & PRs

- Commit style: `port(<crate>): <module>` for ports,
  `fix(<crate>): …`, `docs: …`, `chore: …`.
- One module (or a coherent slice of it) per PR where practical.
- PR description: which C++ files were ported, which upstream tests
  were ported, and any intentional behavioral deviation.
- Keep PRs reviewable: giant "port everything" PRs will be asked to
  split by subsystem.

## Testing expectations

Every ported module should carry over the corresponding
`test/test_<module>.cpp` cases from the upstream checkout. Wire-format
code additionally needs golden vectors generated *from* C++ libtorrent
output — see `docs/adr/0006-testing-and-conformance.md`.

## Questions / discussion

Open a GitHub issue for design questions, and an ADR proposal
(`docs/adr/NNNN-<slug>.md`, see the template in `docs/adr/README.md`)
for architectural changes.
