# Namespace freshness: where the mapping is kept in step with the table

This note records the decision to take table freshness off the elected cluster manager and the
direction it points at. It is a record of the decision and its reasons, not a task list.

## The mapping is a derived cache

A Lance backed index has no documents of its own. Its mapping is derived from the Arrow schema
of the Lance table version the shard serves, plus the operator's overrides, and it exists
because the OpenSearch ecosystem reads it. `_field_caps` is how Dashboards builds an index
pattern; `GET /{index}/_mapping` is how clients discover fields; the security plugin's field
level and document level security are declared against mapped field names; the SQL plugin
resolves columns from the mapping; and on the data node the stock aggregators, sort and fetch
resolve a `MappedFieldType` for every field they touch. None of these read the Lance schema, so
the plugin keeps a mapping that reflects it.

The request path does not depend on the mapping being fresh. The fragment coordinator opens the
table's latest manifest per request, reads the schema and the version from it, and ships the
version to every executor as `pinnedVersion`, so a `_search` sees a new row on its next call. The
mapping lags the table by at most one freshness check, and that lag is visible only where the
ecosystem reads the mapping: a new column is searchable before it is in `_field_caps`, and
`GET /_doc/{id}` follows the shard's reader, which advances on refresh.

## What moved off the cluster manager, and why

The manager's poll ran two unrelated jobs each cycle. It listed every registered catalog and
created an index for every table that had none; that is cluster state work and stays on the
manager. It also opened the latest manifest of every Lance backed index, compared the version
with a served version it kept in memory, and when the version had moved it re-derived the
mapping, sent a `PutMapping`, sent a shard refresh, and retired the previous snapshot from the
warm cache.

That second job was moved to the node that holds the index's shard, for three reasons.

The manifest read is object store I/O per index per cycle, on the node whose only role should be
cluster state. With hundreds of tables the manager spent its poll cycle opening tables it does
not serve.

The `PutMapping` was sent on every version move whether or not the schema had changed. Every
append became a cluster state update task on the manager. The mapping update now happens only
when the derived mapping differs from the mapping the index has: the derived mapping is merged
into the shard's own `MapperService` as a preflight, and the merged mapping source is compared
with the current one, which is the comparison the manager itself makes before it publishes a
mapping update. Equal means no request is sent.

The shard's own node already detected the same move. `LanceReaderManager.refreshIfNeeded` opens
the latest manifest on every explicit refresh and compares it with the version the reader
serves. Having two nodes open the same manifest to answer the same question, one of them the
manager, was redundant.

The precedent is OpenSearch's own indexing path. When a document introduces a new field, the data
node that sees it derives the mapping change and submits it to the manager
(`MappingUpdatedAction`); the manager batches and publishes. The freshness service follows that
shape: the node holding the shard derives the mapping from the table and submits it only when it
changed. The manager receives fewer requests and none of them are no-ops.

The shard lifecycle is what ties a check to a node. A Lance backed index has one shard and no
replica, so exactly one node holds it. The service registers a started Lance backed shard through
`IndexEventListener.afterIndexShardStarted`, checks it at `lance.namespace.poll_cadence`, and
unregisters it in `beforeIndexShardClosed`. A node restart or a shard relocation therefore moves
the check with the shard, and the first check after a shard starts derives the mapping once even
when the version did not move, because the table may have changed while no node held the shard.
The manager's in memory served version table, and the adoption scan that rebuilt it after a
restart or a failover, are gone; the index settings and the shard's reader carry everything the
check needs.

Two manual triggers remove the wait on the cadence. `POST /_lance/namespace/_poll` runs one
catalog listing cycle on the manager and answers what it surfaced and what it skipped with the
reason. `POST /{index}/_lance/sync` runs the freshness check of one index on the node holding its
shard and answers whether the version moved, the served and the target version, and whether the
mapping changed.

## Direction

The next step follows from the mapping being a derived cache: the request path already has the
Arrow schema of the version it reads, so the coordinator can compare that schema with the schema
the mapping records (every derived field carries its Lance field id and Arrow type fingerprint in
its `meta`) and submit the mapping update through the indexing style path when they differ. The
freshness check then stops being a scheduled probe of the object store and becomes a side effect
of a request that noticed the schema changed; the scheduled check remains for the reader advance
of indexes nobody queries.

Catalog listing is the other background job left on the manager. It becomes on demand (the
`_poll` trigger is the first form of that) or moves to a data node elected for it, so the manager
runs no plugin background work at all.

An open question is whether a per snapshot `MapperService` held in `LanceWarmCache` next to the
snapshot's dataset could replace the cluster state mapping for the data node side. The executors
would then resolve field types from the schema of the exact version they read, with no lag and no
cluster state update per schema change. The cost is on the ecosystem side listed above: every
consumer of `GET /_mapping`, `_field_caps`, field level security and the SQL plugin reads the
cluster state mapping, so it would still have to be maintained, and two mappings for one index
(the cluster state one for the ecosystem, the snapshot one for execution) can disagree for the
length of the lag. The question is whether that disagreement is acceptable in exchange for the
executors never seeing a stale field type.
