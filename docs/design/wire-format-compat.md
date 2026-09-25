# Wire format compatibility: how two plugin versions read each other

This note records how the plugin's internal messages are laid out so that a cluster running two
plugin versions at once, as a rolling upgrade does, keeps answering, and the rules a change to
one of those messages follows. It is the reference for the version history of every message.

## Why the plugin needs its own rule

OpenSearch versions its own wire messages by the version of the node at the other end of the
connection: `StreamOutput.getVersion()` is the receiving node's OpenSearch version, and a writer
leaves out or spells differently what that version does not know. The plugin's messages do not
get that for free. The plugin is built against one OpenSearch version, so during a plugin only
upgrade both ends report the same OpenSearch version, and `writeTo(StreamOutput)` learns nothing
about the plugin version of the reader. Negotiating it would mean publishing the plugin version
as a node attribute, looking it up for every target of every fan out, handing it to every
`writeTo`, and echoing it in every request so the answer can be written for the node that asked.
The plugin instead writes every message in one shape that both the previous and the current
plugin version can read, and puts the knowledge of how to bridge the versions in the reader.

## The layout

Every message that crosses nodes carries a `WIRE_VERSION` constant and is written through the
`WireVersion` helper in three parts.

1. The marker: the writer's `WIRE_VERSION`, a variable length integer, written first. In a
   request or a per node response it follows the fields the OpenSearch base class writes (the
   parent task id, the cluster manager timeout, the node), which OpenSearch versions itself.
2. The base fields: the fields the message had at version 1, inline, in that order, forever.
3. One block per version above 1, in ascending order: a flag, then the fields that version added
   as a length prefixed byte array. The flag says whether the block is critical.

A reader opens with `WireVersion.read`, which returns the marker in a `Reader`, reads the base
fields itself, then calls `Reader.block(version, parser, fallback)` once for every version above
1 it knows, and `Reader.finish()` last. `finish` throws an `IllegalStateException` when the walk
skipped a version the reader knows, whatever the writer's marker, so a reader class that forgot to
register one of its own blocks fails the first message it reads instead of taking the fallback
silently against an older writer. The three cases a mixed cluster produces fall out of
that walk.

- Same version on both ends: every block is present and known; the reader decodes each.
- Older writer, newer reader: the marker is below the reader's version, so `block` returns the
  fallback for every version the writer did not reach. The pruning list falls back to empty, a
  counter to zero, an optional filter to absent.
- Newer writer, older reader: the marker is above the reader's version, so `finish` steps over
  every block of a version the reader does not know, using the length prefix. It refuses the
  message with an `IOException` when such a block is critical:
  `FragmentPlan wire version [3] adds fields in version [3] that this node's [2] cannot ignore:
  upgrade this node before sending it this message`.

The writer decides whether a block is critical, per message, when it writes it: a block is
critical when the fallback an older reader would substitute changes the answer, and optional when
the fallback is merely slower or less informative. `FragmentPlan` marks its Substrait filter block
critical only while a filter is set, because an older data node that ignored the filter would scan
without the predicate and answer wrongly, and marks its pruning block optional, because an older
node that scans the pruned fragments too still answers correctly. `LanceNodeStats` marks its
pruned fragment counter and its admission source optional, because an older coordinator merely
shows the stats without them.

A block is decoded from a stream of its own bytes, so a parser that leaves bytes of the block
unread fails the message with `<Message> wire version block [n] left k bytes unread` rather than
misreading what follows. A `Writeable` nested in a message without a marker of its own (the
records inside `LanceNodeStats`, `KindResult` inside the build node response,
`LanceBuildIndexesRequest` inside the build node request) is covered by the enclosing message's
number; a change to its fields is a new block of the enclosing message.

The cost is one byte for the marker, one for each flag, and the length prefix of each block, plus
one copy of the block's bytes on the writer. A version 1 message costs what it did before.

## The rules a change follows

- Adding a field adds a block: the message's `WIRE_VERSION` goes up by one, the field is written
  with `WireVersion.writeBlock` after the blocks before it and read with `Reader.block` with the
  fallback an older writer's message stands for. The base fields and the blocks that shipped are
  not touched.
- Nothing is removed from or retyped in the base fields or in a block that shipped. A field that
  falls out of use keeps being written with its default, because a reader of the previous version
  still expects it at that position. Its replacement, if any, is a new block.
- The writer marks the new block critical when an older reader that ignored it would return a
  wrong answer, and optional otherwise. A critical block on a request means the request fails on
  a data node of the previous version during the upgrade window, with the message above, which is
  the accepted trade off against a silently wrong answer; the change log entry of such a release
  says so.
- The marker is per message: a change to one message bumps that message's number and no other.
  `LanceFragmentQueryRequest` and `FragmentPlan` travel together but are versioned apart.
- A new message that crosses nodes starts at `WIRE_VERSION = 1`, writes its marker through
  `WireVersion.write` and reads it through `WireVersion.read`, and calls `finish` even when it
  has no block, so the version after it can add one.
- The version history table below gains a row for every bump, in the same pull request.

Under these rules a plugin release N reads every message of release N minus 1 and writes
messages release N minus 1 reads, so the cluster keeps answering while the nodes are upgraded
one at a time, in either order. The guarantee covers one release back; a cluster running three
plugin versions at once is not a supported state. Removing or retyping a field, and dropping the
default a retired field is still written with, are not covered by this note; a policy for them
comes with the first release that needs one.

The message classes that only execute on the node that received the REST call
(`LanceExplainRequest`, `LanceRefsRequest`, `LanceNamespaceListRequest`, `LanceBuildIndexesRequest`
at the top level, and their responses) carry no marker: a `HandledTransportAction` invoked through
the node client never serialises them. `LanceNamespaceMetadata` is cluster state, versioned and
published by OpenSearch's own mechanism, and keeps the policy written on the class.

## Version history

Every message that crosses nodes, its current `WIRE_VERSION`, and what each version carries.
"Base" is the version 1 layout; a later version is one block.

| Message | Version | Contents |
|---|---|---|
| `FragmentPlan` | 1 | Base: kind, filter SQL, Lance clause (named writeable query builder), pushed page (orderings, fetch, cursor SQL), pushed aggregate (Substrait bytes, group count, metric slots, two cost predictions, column names) |
| | 2 | Block, optional: excluded fragment ids (integer array; fallback empty) |
| | 3 | Block, critical while a filter is set: Substrait filter bytes (byte array; the block is empty when no filter is set, and the reader takes an empty block as absent) |
| `LanceExplainResponse` | 1 | Retired layout: index, route (`fragment` or `shard_path`), shard path reasons, logical and physical text, optional fragment plan, optional unplanned message, refinements, traits. Read by the current version and never written |
| | 2 | Base: index, route (`fragment` or `unsupported`), optional logical and physical text, optional fragment plan, optional unplanned message, refinements, optional traits |
| `LanceFragmentQueryRequest` | 1 | Base: table URI, index name, storage options, pinned version, the fragment plan, optional query and post filter, sorts, search after, size, aggregations, fragment ids, track scores, track total hits up to, min score, terminate after, hit projection, rescores, collapse |
| `LanceFragmentQueryResponse` | 1 | Base: matched, matched is lower bound, fragment count, hits, row addresses, aggregations, terminated early |
| `LanceNodeStats` | 1 | Base: every figure of the node stats but the pruned fragment counter and the admission source, then the freshness stats |
| | 2 | Block, optional: pruned fragment counter (fallback zero) |
| | 3 | Block, optional: source of the last admission decision (`request` or `warm_up`; fallback `none`) |
| `LanceStatsNodeRequest` | 1 | Base: nothing after the marker |
| `LanceBuildIndexesNodeRequest` | 1 | Base: the build request, the source version |
| `LanceBuildIndexesNodeResponse` | 1 | Base: the three kind results, the status, the optional mapping JSON |
| `LanceIndexSyncRequest` | 1 | Base: nothing after the marker (the index travels in the OpenSearch base class) |
| `LanceIndexSyncResponse` | 1 | Base: the freshness outcome (index, checked, reason, moved, served and target version, mapping changed, rebuilt) |
| `LanceNamespacePollRequest` | 1 | Base: optional namespace name |
| `LanceNamespacePollResponse` | 1 | Base: surfaced indexes, skipped tables, unavailable namespaces |
| `LanceNamespaceUpdateRequest` | 1 | Base: operation, name, optional type, optional root URI, storage options, config, overrides JSON |
| `LanceNamespaceUpdateResponse` | 1 | Base: changed |
| `LanceAttachRequest` | 1 | Base: table, optional index name, optional pinned version, optional tag, storage options, overrides JSON, optional index placement, async derive |
| `LanceAttachResponse` | 1 | Base: index, table, version, rows, fragments, derived key field, derived mapping JSON, notes, already attached, Lucene bound exceeded, optional backfill |

`LanceExplainResponse` version 1 predates the block layout and changed the base fields, which the
rules above no longer allow; it is decoded by a branch on the marker that maps a shard path answer
to an unsupported answer with the message `LanceExplainResponse.SHARD_PATH_RETIRED`, and a
fragment answer field by field. Its blocks, when it gains one, start at version 3.

## Compatibility matrix

What happens when a message written by one version of a class is read by another. "Reads" means
the reader decodes it and continues.

| Writer | Reader | Result |
|---|---|---|
| Same version | Same version | Reads every field |
| Older | Newer | Reads; the blocks the writer did not send take their fallbacks |
| Newer, only optional blocks beyond the reader | Older | Reads; the unknown blocks are stepped over |
| Newer, a critical block beyond the reader | Older | Refuses by name before any field of the block is decoded |

For `FragmentPlan` in particular, as the fragment path ships it from a coordinator to every data
node:

| Coordinator writes | Data node reads as | Result |
|---|---|---|
| 1 | 1, 2 or 3 | Reads; no pruning, no Substrait filter |
| 2 | 1 | Reads; the pruning list is stepped over, every fragment is scanned |
| 2 | 2 or 3 | Reads with the pruning list |
| 3, no Substrait filter | 1 or 2 | Reads; the empty Substrait block is stepped over |
| 3, Substrait filter set | 1 or 2 | Refused: `FragmentPlan wire version [3] adds fields in version [3] that this node's [k] cannot ignore` |
| 3 | 3 | Reads every field |

`FragmentPlanMixedVersionTests`, `LanceStatsSerializationTests` and `LanceExplainResponseTests`
pin these rows: they write the streams of the earlier versions by hand and read today's streams
with `FragmentPlan.read` and `LanceNodeStats.read` dialled down to the earlier versions, and
`WireVersionTests` pins the framing itself. No released plugin binary exists yet to run a two
binary cluster against; when one does, a mixed version integration test replaces the hand written
streams.
