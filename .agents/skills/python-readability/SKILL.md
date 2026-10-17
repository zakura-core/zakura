---
name: python-readability
description: >-
  Apply Python readability guidance when editing or reviewing .py files:
  semantic naming, cohesive dependencies, clear module boundaries, and focused
  test fixtures. Applies only to the Python portion of mixed-language changes;
  do not activate for changes without Python files.
---

# Python Readability

When editing or reviewing Python (`.py`) files, read and follow
[the Python readability rule](../../rules/python-readability.md).

Apply the guidance only to Python files. For mixed-language changes, leave the
other languages under their existing guidance. Do not load the rule for tasks
without Python files.

This skill uses description-based selection and an explicit applicability
instruction. It does not provide a native file-glob activation gate for Codex.
