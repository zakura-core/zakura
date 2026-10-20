# zakura-derive

Derive macros for Zakura.

`#[derive(zakura_derive::OutlineDebug)]` produces the same `Debug` output as
`#[derive(Debug)]`, but the generated `fmt` is `#[inline(never)]`. The standard
derive marks `fmt` `#[inline]`, so every codegen unit that formats a type
compiles its own copy, and ThinLTO never merges those copies. Use it on types
whose `Debug` output only appears on cold paths, such as errors and logged
requests.

Like the standard derive, the impl requires `T: Debug` for each type parameter,
and for each field type projected from one, such as `T::Proof`.

```sh
cargo test -p zakura-derive
```
