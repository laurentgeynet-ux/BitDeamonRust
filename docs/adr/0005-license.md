# 0005. BSD-2-Clause with upstream attribution

- Status: accepted
- Date: 2026-10-10
- Deciders: project owner

## Context

BitDaemon is a derivative work of libtorrent, whose COPYING file is
BSD-style (3 clauses incl. non-endorsement). Permissive licenses allow
sublicensing under compatible terms provided the original copyright
notice is retained.

Options considered: BSD-2-Clause (upstream-spirit), BSD-3-Clause
(exact upstream wording), MIT OR Apache-2.0 (Rust ecosystem
convention).

## Decision

License the project **BSD-2-Clause**, with the upstream attribution
preserved in `LICENSE` ("Portions derived from libtorrent, Copyright
(c) 2003-2020, Arvid Norberg"). `license = "BSD-2-Clause"` in all
crate manifests.

Rationale:

- Maximally aligned with upstream intent (permissive, attribution).
- Single license keeps packaging trivial; `MIT OR Apache-2.0` would be
  the Rust-conventional choice but adds a patent clause and dual
  license complexity for no benefit a port needs.
- Because we re-implement rather than copy code, the attribution is a
  courtesy *and* a safeguard for the parts that are close ports.

## Consequences

- Every new file is covered by the project LICENSE — per-file headers
  are not required (no license boilerplate in source files; rustdoc
  `//!` headers describe the port provenance instead).
- If a file does reproduce non-trivial upstream text (e.g. protocol
  tables), keep the attribution comment in that file.
- `cargo deny` license check must allow BSD-2-Clause, MIT, Apache-2.0,
  ISC, Unicode-DFS-2016, Zlib (typical Rust dep licenses) — to be
  configured in Phase 0.
