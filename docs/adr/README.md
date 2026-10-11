# Architecture Decision Records

ADRs capture *why* the project is built the way it is. Each ADR is a
short, dated, numbered record of a significant decision, its context,
and its consequences.

## Rules

- One decision per file: `NNNN-title-in-kebab-case.md` (4-digit,
  zero-padded, monotonically increasing — never reuse a number).
- Statuses: `proposed` → `accepted` → `superseded by ADR-XXXX` |
  `deprecated`.
- ADRs are immutable once accepted: change a decision by writing a new
  ADR that supersedes it, not by editing the old one.
- Small decisions don't need ADRs — commit messages suffice. If it
  changes the architecture, the API contract, the dependency set, or
  how future porters must write code, it needs an ADR.

## Index

| # | Title | Status | Date |
|---|---|---|---|
| [0001](0001-port-strategy.md) | Semantic port, not transliteration nor FFI | accepted | 2026-10-10 |
| [0002](0002-async-runtime-tokio.md) | tokio as the async runtime | accepted | 2026-10-10 |
| [0003](0003-workspace-layout.md) | Multi-crate workspace layout | accepted | 2026-10-10 |
| [0004](0004-error-handling-and-api.md) | Error handling and API conventions | accepted | 2026-10-10 |
| [0005](0005-license.md) | BSD-2-Clause with upstream attribution | accepted | 2026-10-10 |
| [0006](0006-testing-and-conformance.md) | Testing and conformance strategy | accepted | 2026-10-10 |
| [0007](0007-external-dependencies.md) | External dependency policy | accepted | 2026-10-10 |
| [0008](0008-injectable-transports.md) | Injectable transports — no subsystem binds OS sockets | accepted | 2026-10-10 |
| [0009](0009-disk-io-fidelity.md) | Disk I/O — port the full upstream machinery | accepted | 2026-10-10 |
| [0010](0010-complete-port.md) | Complete-fidelity port — every upstream file ported or reasoned `won't port` | accepted | 2026-10-11 |

## Template

```markdown
# NNNN. Title

- Status: proposed | accepted | superseded by ADR-XXXX | deprecated
- Date: YYYY-MM-DD
- Deciders: <who>

## Context
Why a decision is needed. Constraints, alternatives.

## Decision
What was decided, unambiguously.

## Consequences
What becomes easier, harder, or required. Follow-ups.
```
- [0011 — WebRTC stack: injected PeerConnection seam](0011-webrtc-stack.md)
