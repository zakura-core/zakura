# Mutation loop generator prompt

This prompt writes small production breaks for one work-list entry (mutation
plan, mut-7.2). A generator, such as a coding agent, runs it once per entry
in `work-list.toml`. `cargo xtask planted loop` runs what it writes.

## Inputs

You get one entry of `qa/native-mutations/work-list.toml`. Read these before
you write anything:

1. **The checker.** For each property ID in the entry, read its check
   function in the entry's `check` files and its claim in `properties.md`.
   For a protocol table or the shared wire path, read the content labeller
   (`crates/zakura-sim/src/peer/content.rs`), the consequence table
   (`crates/zakura-sim/src/model/consequence.rs`) and the generated IDs
   (`crates/zakura-sim/src/table.rs`). You read the checker. You never change
   it.
2. **The guarded production code.** Read the entry's `shared` files, then
   each instance's `code`. A break in `shared` code runs against every
   instance. A break in one instance's `code` runs against that instance
   only.
3. **The reaching witness.** Each instance names one witness: a retained
   `qa/planted/` case's committed input (`planted`), a committed fuzz input
   (`input`), or one `zakura-sim` test (`test` and `test_target`). The
   witness holds on `main`. Read what it drives, so that your break lies on
   its path.
4. **The existing patches.** Read every patch for the entry's IDs under
   `qa/planted/` and `qa/native-mutations/`. Don't repeat one.
5. **The fault classes.** Use the mutation plan's catalogue: stale result,
   resource lifetime, ownership, wakeup, boundary accounting, request
   authorization, queue refusal and fault isolation.

## What you write

Write two to five patches for the entry. Write fewer, or none, when fewer
credible breaks reach its properties. Put each patch in its own directory,
`qa/native-mutations/<entry-id>/<name>/`, with three things:

- `<name>.patch`: a `git diff` against `main` that changes production code
  only.
- `README.md`, in the existing form: what the patch breaks, the witness,
  the commands that reproduce it, and one line per ID it must fail:
  `Required failure: the run ends at \`<id>\` …`. A shared-path patch has
  one such line for each protocol table it runs against, and a shared
  toolkit patch one for each instance.
- One `cases.json` entry for each required failure:

  ```json
  {
    "patch": "<entry-id>/<name>/<name>.patch",
    "entry": "<entry-id>",
    "property": "<id>",
    "witness": { "planted": "qa/planted/<id>/<case>.toml" },
    "fault_class": "<class>"
  }
  ```

  The witness is the instance's from the work list, or another listed for a
  protocol table. A shared-path entry also sets `"table"`. A patch whose bug
  breaks another property too lists it in `"also_violates"`.

## Rules

- Change one mechanism per patch.
- Keep honest traffic working. An honest run must still finish.
- Change production behaviour only:
  - No change to `zakura-sim`, to a test, to a check, to the content
    labeller or to the consequence table.
  - No change to an observation emitter alone.
  - No unconditional panic.
- Name every guard on the rule's path. When two guards stand in sequence,
  break both for the rule. Removing only the shared frame filter is an
  equivalent mutant, because discovery's codec still closes the connection.
  A patch that breaks one guard anyway names the remaining guard in its
  README.
- Write no patch per message type for a wire rule. A guard that several
  rows share gets one patch.
- When no credible production break reaches a property, write no patch.
  Record why in the run's output.

## Before you hand the patches over

Run `python3 scripts/mutation_inventory.py --loop --output <file>` from the
repository root. It checks that each patch applies, changes only production
files, and that its README and `cases.json` entries name the same
registered ID and one witness. Fix every error it reports.
