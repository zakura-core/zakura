# Release notes

The [Release notes workflow](../workflows/release-notes.yml) drafts the notes
of every published release from its `CHANGELOG.md` section, so each release
gets the same layout without anyone writing it from scratch. Edit the result
on the release page like any other release.

## What it writes

The notes always have these parts, in this order:

1. A bold one-sentence headline.
2. The support window: "Supported on Mainnet until block N, around DATE.
   Upgrade warnings start about three days earlier."
3. "No config changes are needed." when there are no upgrade notes.
4. `### Before you upgrade`, only when operators must act on something.
5. `### Highlights`.
6. `### Library users`, only when Rust crate interfaces changed.
7. The full changelog section in a collapsed `<details>` block, without its
   HTML comments.
8. A link to the section in `CHANGELOG.md` at the tag.

The summary leaves out work that operators can't use yet, listed by
[`config.toml`](config.toml): for now, the experimental Zakura P2P v2 stack,
which is off by default on Mainnet. Those PRs still appear in the full
changelog. When that work ships, delete its group from `config.toml`.

The model only writes the text of 1, 3, 4, 5, and 6, as JSON checked against
a schema. [`release_notes.py`](../scripts/release_notes.py) renders the rest
and adds every PR link, so the layout cannot drift. It rejects a draft that
cites a PR the section does not link, writes its own links, or skips the
highlights, and asks the model once to fix it. Overlong entries, more than
six highlights or four upgrade notes, entries about unreleased work, and any
released `Added`, `Security`, `Removed`, or `Deprecated` entry that the
summary leaves out also get that one retry; if they remain, the draft is kept
and the run summary lists them for review.

The support window is computed, not written by the model. The height is
`zakurad`'s own end-of-support height: `ESTIMATED_RELEASE_HEIGHT` plus
`EOS_PANIC_AFTER` days of target spacing, capped at `MAINNET_EOS_HEIGHT`. The
date assumes the chain is at `ESTIMATED_RELEASE_HEIGHT` when the release is
published. A tested parser reads these from the Rust sources at the tag, so a
change to them fails CI on that pull request rather than at release time.

## When it runs

`Create release` publishes each release with a placeholder body. The
workflow then drafts the notes and replaces that placeholder, usually within
a few minutes. It never replaces notes that someone has already edited,
including edits made while the draft was being written. If drafting fails,
the placeholder stays and the run shows why.

To see a draft without changing the release, or to redraft one:

```bash
gh workflow run release-notes.yml -R zakura-core/zakura --ref main \
  -f release_tag=v1.8.6 -f dry_run=true
```

The run summary shows the rendered notes and their Markdown source to copy.
Add `-f overwrite=true` instead of `dry_run` to replace notes that were
already edited.

## Model and cost

The workflow uses the `OPENAI_API_KEY` secret and the `openai` entry of the
`RELEASE_REVIEW_MODELS` variable, shared with the
[Release AI review](../release-review/README.md). One release takes one or
two requests; the run summary reports the tokens and, when prices are
configured, the estimated cost.

## Change the style

Edit [`prompt.md`](prompt.md): its rules and the two example summaries, which
the tests also check against the validator. Changes take effect on the next
release once merged. Section names, ordering, and link formatting live in
`render` in `release_notes.py`.

## Run it locally

Render only the computed parts, without a model call:

```bash
python3 .github/scripts/release_notes.py generate --tag v1.8.6 --skeleton
```

Draft with the model, for example to preview a release branch before it is
tagged:

```bash
OPENAI_API_KEY=... \
RELEASE_REVIEW_MODELS="$(gh variable get RELEASE_REVIEW_MODELS -R zakura-core/zakura)" \
python3 .github/scripts/release_notes.py generate --tag v1.9.0 --ref origin/release/v1.9.0
```

Run the tests with Python 3.11 or later:

```bash
python3 -m unittest discover -s .github/scripts/tests -p 'test_release_notes*.py'
```
