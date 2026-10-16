---
paths: ["**/*.py"]
---

# Python Readability

Apply this guidance only when editing or reviewing Python (`.py`) files. In
mixed-language changes, apply it only to the Python portion. Write for a human
reviewer who should understand the purpose and flow without reconstructing it
from test setup or several layers of indirection.

Make production APIs describe real dependencies and operations. Keep test setup
in fixtures, and replace scattered test callbacks with a cohesive dependency
object when they represent one service. Dependency injection, including clocks
and transports, is useful when it makes a real boundary explicit.

## Names and Contracts

- Name functions, variables, and modules for the domain concepts they represent.
  Prefer specific names over generic containers or actions when meaning is
  otherwise ambiguous.
- Document non-obvious validations, side effects, and postconditions. Explain
  intent and constraints rather than restating syntax.

## Dependencies and Production Paths

- Group endpoints, settings, and behavior that belong to one dependency. Do not
  collect unrelated callbacks into a bag or add a class just to shorten a
  signature. A simple function parameter is enough for a simple dependency.
- Let tests supply fixtures through the same dependency boundaries production
  uses. Do not detect test implementations or branch on callable identity.
  Keep fixture construction and fault injection in test code.

For example, simplify scattered registry callbacks and an implementation check:

```python
# Before: a caller wires separate pieces of the same registry.
def verify_visible(identity, *, fetch=download, resolve=cargo_grid):
    ...
    data = (
        download(url, limit, timeout=30) if fetch is download else fetch(url, limit)
    )
    ...

verify_visible(identity, fetch=fixture.request, resolve=fixture.resolve)
```

```python
# After: the registry owns its endpoints, requests, and Cargo resolution.
def verify_visible(identity, *, registry):
    ...
    data = registry.download(identity)
    ...

verify_visible(identity, registry=fixture.registry)
```

This is an illustrative boundary, not a required class layout. A dataclass or a
small service object can fit; preserve request deadlines and other guarantees
when moving the implementation.

## Modules and Validation

- Keep imports one-way. Avoid CLI wrappers importing each other, and split
  modules by responsibility rather than file length. Keep command entry points
  easy to find and the high-level sequence easy to read.
- Give each invariant an authoritative check and reuse named validation steps.
  Preserve checks required at independent trust boundaries; deduplication must
  not turn untrusted input into trusted input without validation.
- Move substantial Python embedded in YAML or shell heredocs into importable,
  testable Python commands. Small glue expressions can stay inline. Apply this
  principle when working on the Python code; it does not activate this rule for
  unrelated YAML or shell changes.

## Tests and Behavior Preservation

- Keep test doubles focused on the contract being exercised. Separate unrelated
  external services rather than building one fake with many unrelated fault
  switches. Retain integration tests where real protocol behavior matters.
- Preserve validation, deadlines, error handling, and failure scenarios during
  readability refactors. Review intentional behavior changes separately; shorter
  code alone does not justify changing guarantees.

## Review Examples

- [Evan's review of release-state publication][review] motivates the dependency
  boundary and removal of test-driven production branches.
- [The release-state readability follow-up][follow-up] illustrates cohesive
  modules, shared validation, smaller fakes, and simpler workflow glue. Its exact
  module count, dataclass choice, and timeout changes are not general rules.

[review]: https://github.com/zakura-core/zakura/pull/1420#pullrequestreview-5542771096
[follow-up]: https://github.com/zakura-core/zakura/pull/1445
