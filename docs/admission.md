# Admission control

The admission gate refuses a Lance scan that would not fit the node's physical memory before the scan starts. Lance allocates native memory the plugin's breakers never see, so on a large enough table such a scan ends the node with a kernel OOM kill before any accounting sees it. Before each gated scan the executor estimates what it will make Lance allocate and answers 429 `circuit_breaking_exception` under the label `lance_admission` when the node's available memory minus a headroom, plus what earlier admitted scans of the same identity retained, cannot hold it.

[features.md](features.md#native-memory-bounds) lists the other memory bounds. `GET /_lance/stats` reports the decisions under `admission` ([Cache statistics](features.md#cache-statistics)).

- [Settings](#settings)
- [The decision](#the-decision)
- [Estimators](#estimators)
- [Retained memory](#retained-memory)
- [Observing the gate](#observing-the-gate)

## Settings

| setting | scope | default | dynamic | effect |
|---|---|---|---|---|
| `lance.admission.enabled` | node | `true` | yes | `false` turns the gate off |
| `lance.admission.headroom` | node | `8gb` | yes | memory kept free below the available reading; the retained credit may reach into it |
| `lance.admission.bounded_shapes_gated` | node | `true` | yes | `false` caps a bounded filter page at its limit and restores the pass-through for bounded full text pages |
| `lance.test.index_cache_shard_share` | node | `0` | yes | test only; overrides the index cache shard share the estimates are compared with |
| `lance.test.admission_available_memory` | node | empty | yes | test only; scripts the available memory readings |

The two `lance.test.*` settings are described under [Tables above the Lucene document bound](features.md#tables-above-the-lucene-document-bound); do not set them on a real node.

## The decision

- The executor judges each gated scan before it starts, per kind (see [Estimators](#estimators)). The request is refused when `estimate > available - headroom + retained`, where `available` is the node's available physical memory, `headroom` is `lance.admission.headroom` and `retained` is the credit of [Retained memory](#retained-memory) for a scan of the same identity, else zero.
- The 429 message reads `[lance_admission] <kind> estimate [X] exceeds available [Y] minus headroom [Z] plus [R] retained by earlier admitted scans: <what the scan is> <what to relax>`. A scan the retained pool does not cover shows `plus [0b] retained`.
- Available memory is the kernel's `MemAvailable` from `/proc/meminfo`, which counts the reclaimable page cache and slab on top of the free pages. `MemFree` alone reads close to zero on a node whose page cache is warm. Where the file or the line does not exist (macOS, Windows) the gate falls back to the free physical memory.
- Every estimate at or below the index cache shard share is zero: a cached load is not repeated, and scan buffers smaller than one shard of the cache the node dedicates to Lance are within its sizing.
- A request that runs several gated paths (a filter under a knn, a filtered aggregate) is judged per path in order and counts once in flight.
- A 429 on a table whose scan does not fit the node is the designed outcome. The coefficients are a model, fitted to measurements as they come in.

What Lance allocates that the breakers do not see: an inverted index document set or the matching pages of a BTree that do not fit one index cache shard, the IVF partitions a nearest scan probes, the row addresses a filtered scan materialises, and the read queue and decoded batches of every scan.

## Estimators

One estimator per kind, each a static method of `ScanAdmission` with its coefficients as named constants.

### `fts`

A full text scan. The estimate is the sum of three parts; it is zero when one document set fits the shard share.

- The document set rebuild, `rows × 52 bytes`, over whichever fragments the scan keeps, once per full text clause. Lance searches every `match`, `match_phrase` and `multi_match` column of a `lance_fts_bool`, a fused stock `bool` or a `lance_fts_boost` on its own and holds each result while it joins them, so `bool(must [match, match])` counts two document sets.
- The positions of each `match_phrase` clause's tokens, `rows × 48 bytes` per phrase clause (`PHRASE_POSITION_BYTES_PER_ROW`).
  - Why positions are counted: the phrase `w000000 w000001 size 10` over 1B rows peaked at 103 GB on one 128 GB node and killed every node of a 4 node cluster after being admitted at the document set alone.
- The hits scan buffers: one row in ten of the table for an unbounded shape, the top-k limit for a bounded page, at 12 bytes per row, doubled.

### `scalar_index`

The index Lance loads to answer a `term`, `range` or other scalar filter. Zero when the load fits the shard share.

- A BTree reads only the pages whose value range the predicate meets, so the load is the index's manifest size times the filter's selectivity.
- A bitmap or label list reads one bitmap per matching value and holds it twice (deserialised and cloned into the cache).
- A zone map and every other type is read whole.
- Selectivity is one over the column's distinct count for an equality on a column whose index reports one (a bitmap's `num_bitmaps`), else one row in five (`FILTER_MATCH_RATIO_UNKNOWN`).
- A missing manifest size falls back to 16 bytes per row for a BTree and 1 byte per row for a bitmap.

### `filter_scan`

The native scan of a scalar filter (`LanceScanFilterQuery`) and the sorted, limited scan of a pushed top-k page. The estimate is the sum of three parts.

- The rows expected to match on the node (its physical rows times the selectivity, every row for an unfiltered sorted page) at 256 bytes each (`FILTER_SCAN_BYTES_PER_MATCHING_ROW`).
  - Why 256: the row addresses Lance's `MaterializeIndexExec` collects into a `RowAddrTreeMap` and a `Vec<u64>` are 16 bytes per row by the code; the measured resident set of `term rating=5 size 0` over 10B rows was above 250 bytes per matching row and the constant is pinned to that.
- The decoded batches in flight: `batch_readahead`, the CPU count, times 8192 rows times the row width, doubled.
- The scan's read queue: at most 2 GiB, Lance's `io_buffer_size`, and at most the scanned bytes.
- The per fragment bit sets the filter keeps (one bit per row) are heap and are judged against the request breaker's room, not against physical memory.
- A bounded filter page is judged on every matching row, because the index result is materialised before the limit applies. `lance.admission.bounded_shapes_gated: false` caps it at its limit and restores the pass-through for bounded full text pages.

### `vector_index`

A `lance_knn` scan. Zero when the probed bytes fit the shard share; zero without statistics of the table.

- The vector index's manifest size scaled by `nprobes / partitions`, times two for the read then concatenated copy Lance makes of each partition it loads, plus the refine step's full vector reads (`k × refine_factor × dimension × 4`).
- The partition count comes from the table statistics the planner collects: `num_partitions` of the index statistics (`Dataset.getIndexStatistics`), the smallest over the index's deltas since a nearest scan probes `nprobes` partitions of each. When the statistics report no count the whole index is taken as probed.

### `aggregate_scan`

A pushed aggregate. Zero when the sum fits the shard share.

- Per parallel scan (`lance.aggregation.pushdown_parallelism` fragment groups) the read queue (at most 2 GiB and at most the group's rows times the projected row width) plus the batches in flight, summed over the scans, over the rows the filter keeps.
- For a filtered aggregate, the row addresses the filter's scalar indexes materialise at 256 bytes each (`FILTER_SCAN_BYTES_PER_MATCHING_ROW`, the filter scan's constant). The materialised rows are the table's rows times the sum over the filter's columns with a scalar index of each predicate's selectivity (one over the distinct count for an equality on a column whose index reports one, else one row in five).
  - Why the whole table and every predicate: Lance's `MaterializeIndexExec` evaluates the filter's index expression over the whole table before it keeps the scan's fragments, and evaluates every indexed predicate of an `AND` or `OR` on its own index and holds each result until it combines them.
  - Example: `range price [100,200) + terms(category)` over 1B rows is 200M row addresses, 51.2 GB, on top of the scans' buffers.
  - The unfiltered aggregate, and a filter no scalar index answers (Lance then evaluates it on the scanned batches), materialise nothing.
- The group state (the resolver's group estimate times 64 bytes plus 24 per metric, per scan) is heap and is reported in the message.

### `column_load`

The heap copy of a column the off-heap store had no room for. The request breaker judges it before the allocation (`lance_heap_column:<column>`, see [limitations.md](limitations.md)) and the gate records the charge and the refusal under this kind.

## Retained memory

The gate keeps a per node pool of retained memory under the identity of the scan that left it, and credits that pool to the next scan of the same identity. An admitted scan leaves memory in the process after it completes: the posting lists and per partition document row ids Lance admits to its index cache next to the refused entry, and the pages the native allocator keeps after the entry is dropped. `MemAvailable` therefore reads lower after the scan than before it, while the next scan that loads the same index needs that much less fresh memory.

Without the credit the second identical unbounded shape on a node that admitted the first is refused, and a 128 GB node serves one unbounded full text aggregation over a 1B row table per process lifetime.

How the pool is filled:

- When a non zero estimate is admitted with no other gated request in flight, the gate samples `MemAvailable` and the process resident set.
- When that request's scan completes with no other gated scan running, it adds `MemAvailable` before minus `MemAvailable` after, capped by the resident set growth over the same interval (memory another process took meanwhile is not this process's to reuse).
- Every gated scan (full text, filter, sorted page, nearest, aggregate) brackets itself so the pool samples its completion.
- The pool is not credited while a gated request is in flight or a gated scan runs (that memory is in use), nor when the process resident set exceeds `lance.native_memory.limit` plus the JVM heap by more than the pool (something the plugin does not account holds memory).

How the pool is bounded:

- By the largest estimate admitted since it started (one scan cannot leave behind more than it allocated).
- Reduced at decision time by whatever `MemAvailable` has recovered since the last completion sample.
- Never larger than the whole drop since the pool started.
- It starts over when an admission finds `MemAvailable` at or above its starting point, and when a scan of another identity is admitted (see below).

Who is credited:

- Only a scan of the same kind that reads the same index. The identity is the kind, the table and the columns the scan loads: the full text columns of an `fts` scan, the vector column of a `vector_index` load, the indexed columns the filter references for `scalar_index`, `filter_scan` and `aggregate_scan`. Without table statistics the filter's SQL stands for its columns, so only the very same filter matches.
- The identity has to match the pool's identity exactly, not overlap or contain it.
- A scan of the same identity is judged on `MemAvailable - headroom + pool`. A scan of any other identity is judged on `MemAvailable - headroom` alone, because it does not reuse what the earlier scan left, and once admitted it starts the pool over under its own identity.
  - Why: a filtered aggregate on another index admitted on the credit of an earlier one allocated its own row address set on top and killed the node.
- Because the credit depends on the identity and not on which node the scan lands on, the coordinator's gate and the shard nodes' gates lean the same way for a given scan: all of them credit a repeat of the shape that filled their pools, none of them credits a different shape.
- The credit lets the transient peak of the repeated scan reach into the headroom by up to the credited amount. That is why the pool is capped by what the process actually kept and why `lance.admission.headroom` keeps its default of 8 GB.

## Observing the gate

- The 429 message names the credited figure next to the available memory and the headroom.
- `GET /_lance/stats` reports the pool as `admission.retained_bytes` and its identity as `admission.retained_scope` (`kind:table:columns`), the last decision under `admission.last_estimate_bytes`, `admission.last_kind` and `admission.last_source`, and the refusals per kind under `admission.rejections`. The full shape of the `admission` object is under [Cache statistics](features.md#cache-statistics).
- The metadata warm up's full text probe is judged by the same gate and skipped, not refused, when it does not fit; see [Index warm-up](features.md#index-warm-up).
