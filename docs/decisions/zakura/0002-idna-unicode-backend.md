# IDNA Unicode backend

## Decision

Zakura constrains `idna_adapter` to `~1.1.0`, allowing 1.1.x patch releases while
excluding 1.2.x. Upstream deliberately uses these minor-version lines to select
different Unicode backends: 1.1.x selects unicode-rs; 1.2.x selects ICU4X.

The constraint is a normal dependency of the node package, inherited from the
workspace manifest. It exists to select the backend used transitively by URL
parsing and DNS, rather than to call the adapter directly. A lockfile-only pin
would be lost during a dependency update. Revisit this decision before widening
the requirement or removing the direct dependency.

## Motivation

The unicode-rs backend reduces the dependency graph and is documented upstream
as faster to compile. For the workspace graph when this decision was made,
switching `idna_adapter` 1.2.2 to 1.1.0 removed 20 packages and added four: a net
reduction of 16. The removed packages include seven ICU crates and their data
and derive infrastructure. The added packages are `idna_mapping`, `unicode-bidi`,
`unicode-joining-type`, and `unicode-normalization`.

`idna` remains at 1.1.0. This retains the current IDNA algorithm and its checks
for Punycode, bidirectional text, and contextual joiners. We are selecting a
different Unicode backend, not downgrading `idna` or disabling IDNA processing.

## Accepted tradeoffs

Upstream reports larger binaries and slower runtime processing with unicode-rs.
Hostname processing is outside block validation and established connection
traffic. No Zakura build-time, runtime, or binary-size benchmarks were run.
An older upstream Wasm URL-parser comparison reported approximately 94 KiB of
additional size with the previous unicode-rs implementation; that is not a
measurement of this change or a prediction for Zakura's native binary.

The important compatibility tradeoff is Unicode data coverage. The selected
`idna_mapping` 1.1.0 tables cover Unicode 16, while the previous ICU 2.2 backend
accepts Unicode 17 additions. Hostnames containing those newer characters can
therefore be rejected, including when provided as ASCII `xn--` Punycode labels.
The ASCII spelling does not bypass Unicode validation.

Observed examples include new CJK ideographs, newly encoded scripts, Arabic
letters and ligatures, specialized Latin letters, new emoji, and the Saudi
riyal sign. For example, `U+088F` (Arabic noon with ring above) and its Punycode
label `xn--7xb.example` are rejected. `U+A7F1` (modifier capital S), which ICU
maps to `s.example`, is also rejected.

This loss of Unicode 17 hostname support is accepted for Zakura. It affects URL
and hostname interpretation, rather than UTF-8 support in unrelated strings.
Supporting Unicode 17 again requires revisiting the backend and its data.

## Compatibility investigation

An isolated comparison used the same `idna` 1.1.0 and `url` 2.5.8 with
`idna_adapter` 1.2.2 / ICU 2.2 versus `idna_adapter` 1.1.0 / unicode-rs:

- Strict ToASCII matched all 6,389 Unicode 16 conformance expectations with
  unicode-rs. ICU differed on one expectation involving a character added in
  Unicode 17.
- Strict ToASCII matched all 6,391 Unicode 17 conformance expectations with ICU.
  unicode-rs differed on three expectations involving Unicode 17 additions.
- Results from all 819 upstream URL fixtures were identical between backends.
  This was a comparison of results, not a claim of complete URL conformance.
- Every backend difference in the IDNA fixtures involved a Unicode 17 character.
- Testing all 4,803 Unicode 17 additions as single-character hostname labels
  found 4,761 accepted by ICU and rejected by unicode-rs. Of those, 4,316 were
  CJK additions. Punycode forms were tested separately and also lost support.
- Focused cases matched for ordinary ASCII, accented Latin, fullwidth letters,
  Arabic, and established Punycode domains. Both backends rejected malformed
  joiner contexts and the ASCII-only Punycode labels from RUSTSEC-2024-0421.

These checks establish the observed differences, not equivalence for every
possible hostname. They were correctness tests, not performance benchmarks.

## References

- [Backend selection and tradeoffs](https://github.com/hsivonen/idna_adapter/blob/main/README.md)
- [Upstream backend-selection PR](https://github.com/servo/rust-url/pull/965)
- [Historical Wasm size comparison](https://github.com/servo/rust-url/pull/923#issuecomment-2074865005)
- [Unicode 16 conformance fixture](https://github.com/servo/rust-url/blob/main/idna/tests/IdnaTestV2-Unicode16.txt)
- [Unicode 17 conformance fixture](https://github.com/servo/rust-url/blob/main/idna/tests/IdnaTestV2.txt)
- [Unicode 17 additions](https://www.unicode.org/versions/Unicode17.0.0/)
- [Punycode advisory](https://rustsec.org/advisories/RUSTSEC-2024-0421.html)
