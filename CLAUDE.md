# CLAUDE.md

Working instructions for this repository.

Sections 1–4 are the [Karpathy guidelines](https://github.com/multica-ai/karpathy-skills)
(MIT), derived from [Andrej Karpathy's observations](https://x.com/karpathy/status/2015883857489522876)
on LLM coding pitfalls. Section 5 is specific to SentinelFlow.

**Tradeoff:** sections 1–4 bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them — don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it — don't delete it.

When your changes create orphans:
- Remove imports, variables and functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: every changed line should trace directly to the request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

Prefer a measurement over an assertion. If a change is meant to be faster,
smaller or safer, produce the before-and-after number.

## 5. This repository

**Do not commit or push unless asked.** Leave changes in the working tree.

**Layout.** `cmd/<service>` is wiring only: config → telemetry → dependencies →
run. Logic lives in `internal/`. Deployable services are listed in the Makefile's
`SERVICES`; development-only binaries go in `TOOLS` and get no image or manifest.

**Quality bar.** Everything compiles, nothing is stubbed, no error is swallowed.
Dependencies are passed explicitly, never read from a package-level variable
after startup. Every service shuts down gracefully.

**Tests.** Deterministic: clocks, ids and randomness are injectable seams, not
wall-clock reads. Unit tests run under `-race`. Anything needing a real database,
broker or Temporal server lives in `test/integration` behind the `integration`
build tag — not in a unit test.

**Decisions get written down.** A significant choice belongs in an ADR under
`docs/adr/`, including the alternatives rejected and why. If a change narrows a
guarantee the README promises, update the README in the same change.
