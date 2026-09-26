# Funded response storage

`ResponseVec<T>` and `ResponseIndex<K>` separate allocation planning from
admission. Planning allocates nothing. The caller sums the plans and reserves
that sum together with its exchange metadata using a funded `WriterFence`.
Only then does it apply the plans and publish the request.

A vector owns its memory permit for as long as its backing allocation exists.
Clearing entries does not return the permit. Growth requires the old and new
buffers to fit at the same time. Exact growth is available when geometric growth
would exceed the budget. Heap allocations inside elements need separate funding.

The index accepts keys supplied by the message adapter and returns missing,
unique or ambiguous ownership. It does not parse block hashes or decide message
policy. Callers remove or move keys when their request storage changes. Lookups
and updates use an ordered tree rather than scanning all requests.

The standard library does not expose tree capacity. The index keeps a conservative
node allowance until drop, including after removal empties the tree. Allocation
tests measure splits, replacement and empty roots on the supported compiler.
Recheck those tests when updating Rust. This is a bound on requested allocations,
not on allocator overhead or total process memory.

Tests cover denied growth without allocation, old/new buffer overlap, retained
empty buffers, duplicate-key ambiguity and generated histories against independent
models. A combined test funds an exchange, vector and index in one admission,
then confirms that ending the exchange does not refund retained container storage.

These are shared tools for the subsequent message-adapter migration. They do not
yet replace the production GetBlocks request registry or hash matching path.
