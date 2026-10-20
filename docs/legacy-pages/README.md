# Legacy GitHub Pages landing page

The old `https://zakura-core.github.io/zakura/` page advertised an installer
release asset that is no longer published. PR #311 moved installer ownership
to `zakura.com`; PR #338 removed the old `book/valarbook/` source and its
publishing workflow, leaving the last Pages deployment online.

`index.html` directs visitors to <https://zakura.com/download/>. Keep installation
commands, platform support, and release selection on that maintained website.
The visible link works when automatic redirects are disabled; no JavaScript is
required. This does not restore the retired book or internal API documentation.

## Validate

```sh
python3 scripts/check-legacy-pages.py docs/legacy-pages/index.html
```

The **Legacy Pages redirect** workflow validates and stages the page on pull
requests and relevant pushes. Its artifact contains only `index.html`.

## Publish after review

After merging, a maintainer must manually run **Legacy Pages redirect** from
`main` and approve the existing `github-pages` environment if required. PRs,
ordinary pushes, and manual runs from other branches cannot deploy. The workflow
uses the existing Pages site and does not enable Pages or change its settings.

Until that deployment completes, the old broken installer command remains live.
After publishing, check the legacy URL redirects to the maintained download page
and that disabling automatic redirects still exposes the visible link.
