/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.lance.engine.HidingReaderWrapper;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceIndexWarmer;
import org.opensearch.lance.namespace.AllowedCatalogEndpoints;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.ScanAdmission;

/**
 * The settings of the plugin. {@link LancePlugin#getSettings()} registers
 * {@link #all()}; the components read the constants declared here.
 *
 * <p>Every setting has two registered keys: the current one under
 * {@code plugins.lance} (node scope) or {@code index.plugins.lance} (index
 * scope), and the deprecated one under {@code lance} or {@code index.lance}
 * that the first preview releases used. The current setting names the
 * deprecated one as its fallback, which with OpenSearch's {@code Setting}
 * gives these three outcomes when a current setting is read.
 * <ul>
 * <li>Only the current key is present: its value is used and nothing is
 * logged.</li>
 * <li>Only the deprecated key is present: the current setting returns the
 * deprecated key's value, and a deprecation warning naming the deprecated
 * key is logged (the deprecation logger reports each key once per node).</li>
 * <li>Both keys are present: the current key's value is used and the
 * deprecated key's value is ignored, but the deprecation warning is still
 * logged, because {@code Setting} evaluates the fallback's raw value on
 * every read and the fallback finds its key in the settings.</li>
 * </ul>
 * The code reads only the current settings; attach and the namespace poll
 * write only the current keys. The deprecated settings are registered so an
 * {@code opensearch.yml} and an index whose cluster state carries the old
 * keys stay valid for one release.
 */
public final class LanceSettings {

    private LanceSettings() {}

    public static final Setting<String> TABLE_SETTING_DEPRECATED = Setting.simpleString(
        "index.lance.table",
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.Deprecated
    );
    public static final Setting<String> TABLE_SETTING = Setting.simpleString(
        LanceEngineFactory.TABLE_SETTING,
        TABLE_SETTING_DEPRECATED,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    public static final Setting<String> PRIMARY_KEY_FIELD_SETTING_DEPRECATED = Setting.simpleString(
        "index.lance.primary_key_field",
        "",
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.Deprecated
    );
    public static final Setting<String> PRIMARY_KEY_FIELD_SETTING = Setting.simpleString(
        LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING,
        PRIMARY_KEY_FIELD_SETTING_DEPRECATED,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    /**
     * String form of the declared primary key's Arrow type family, used by
     * {@link LanceEngineFactory} to pick the right lookup strategy. Only
     * {@code "long"} (signed integer PK, default) and {@code "keyword"}
     * (Utf8 PK) are recognised; unknown values fall back to {@code "long"}
     * so indices created before this setting existed stay readable. The
     * setting has no meaning when
     * {@link #PRIMARY_KEY_FIELD_SETTING} is empty (the table has no PK).
     */
    public static final Setting<String> PRIMARY_KEY_TYPE_SETTING_DEPRECATED = Setting.simpleString(
        "index.lance.primary_key_type",
        "long",
        LanceSettings::validatePrimaryKeyType,
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.Deprecated
    );
    public static final Setting<String> PRIMARY_KEY_TYPE_SETTING = Setting.simpleString(
        LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING,
        LanceSettings::validatePrimaryKeyType,
        PRIMARY_KEY_TYPE_SETTING_DEPRECATED,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    public static final Setting<Long> VERSION_SETTING_DEPRECATED = Setting.longSetting(
        "index.lance.version",
        -1L,
        -1L,
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.Deprecated
    );
    public static final Setting<Long> VERSION_SETTING = Setting.longSetting(
        LanceEngineFactory.VERSION_SETTING,
        VERSION_SETTING_DEPRECATED,
        -1L,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    /**
     * Lance tag the index follows, written by attach when the body carries
     * {@code "tag"}. Empty means the index follows the latest manifest (or
     * a pinned version when {@link #VERSION_SETTING} is set). Dynamic
     * because the tag is a moving pin the operator can rewrite with
     * {@code PUT /{index}/_settings} on a running index; the namespace
     * poll cycle re-resolves the tag from cluster state every cycle, so
     * the next poll after the update refreshes the reader onto the
     * version the new tag points at.
     */
    public static final Setting<String> TAG_SETTING_DEPRECATED = Setting.simpleString(
        "index.lance.tag",
        "",
        Setting.Property.IndexScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<String> TAG_SETTING = Setting.simpleString(
        LanceEngineFactory.TAG_SETTING,
        TAG_SETTING_DEPRECATED,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );
    /**
     * JSON stringified multi-fields spec, persisted by attach so the
     * engine can rehydrate keyword sub-fields on shard open. Empty
     * means no sub-fields declared.
     */
    public static final Setting<String> MULTI_FIELDS_SETTING_DEPRECATED = Setting.simpleString(
        "index.lance.multi_fields",
        "",
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.Deprecated
    );
    public static final Setting<String> MULTI_FIELDS_SETTING = Setting.simpleString(
        LanceEngineFactory.MULTI_FIELDS_SETTING,
        MULTI_FIELDS_SETTING_DEPRECATED,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    /**
     * JSON stringified per-column mapping overrides, persisted by attach
     * and namespace surface so derivation can re-apply them on every
     * manifest version advance. Empty means no overrides declared. New
     * attaches write this setting only; {@link #MULTI_FIELDS_SETTING}
     * stays registered so indexes created before it existed keep
     * opening. Dynamic rather than Final because the namespace poll
     * itself rewrites the value when the Lance table renames an
     * overridden column (the override follows the column to its new
     * name) or resets one to a type the override no longer fits.
     */
    public static final Setting<String> OVERRIDES_SETTING_DEPRECATED = Setting.simpleString(
        "index.lance.overrides",
        "",
        Setting.Property.IndexScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<String> OVERRIDES_SETTING = Setting.simpleString(
        LanceEngineFactory.OVERRIDES_SETTING,
        OVERRIDES_SETTING_DEPRECATED,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );
    public static final Setting<String> UNCOVERED_FRAGMENT_POLICY_SETTING_DEPRECATED = Setting.simpleString(
        "index.lance.uncovered_fragment_policy",
        "immediate",
        LanceSettings::validateUncoveredFragmentPolicy,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<String> UNCOVERED_FRAGMENT_POLICY_SETTING = Setting.simpleString(
        LanceEngineFactory.UNCOVERED_FRAGMENT_POLICY_SETTING,
        LanceSettings::validateUncoveredFragmentPolicy,
        UNCOVERED_FRAGMENT_POLICY_SETTING_DEPRECATED,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );
    public static final Setting<TimeValue> NAMESPACE_POLL_CADENCE_SETTING_DEPRECATED = Setting.timeSetting(
        "lance.namespace.poll_cadence",
        TimeValue.timeValueSeconds(10),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<TimeValue> NAMESPACE_POLL_CADENCE_SETTING = Setting.timeSetting(
        "plugins.lance.namespace.poll_cadence",
        NAMESPACE_POLL_CADENCE_SETTING_DEPRECATED,
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );
    /**
     * How long a Lance-backed index that got deleted from OpenSearch
     * (through {@code DELETE /{index}}) is held in the namespace poll's
     * tombstone list so a subsequent poll cycle does not immediately
     * recreate it. Zero disables the guard (poll re-surfaces
     * immediately). Applies only to
     * indexes that were surfaced or attached by this plugin; ordinary
     * OpenSearch indexes are never in the tombstone list.
     */
    public static final Setting<TimeValue> NAMESPACE_RESURFACE_GRACE_SETTING_DEPRECATED = Setting.timeSetting(
        "lance.namespace.resurface_guard_grace",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueMillis(0),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<TimeValue> NAMESPACE_RESURFACE_GRACE_SETTING = Setting.timeSetting(
        "plugins.lance.namespace.resurface_guard_grace",
        NAMESPACE_RESURFACE_GRACE_SETTING_DEPRECATED,
        TimeValue.timeValueMillis(0),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<List<String>> ALLOWED_TABLE_ROOTS_SETTING_DEPRECATED = Setting.listSetting(
        "lance.allowed_table_roots",
        List.of(),
        Function.identity(),
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<List<String>> ALLOWED_TABLE_ROOTS_SETTING = Setting.listSetting(
        "plugins.lance.allowed_table_roots",
        ALLOWED_TABLE_ROOTS_SETTING_DEPRECATED,
        Function.identity(),
        Setting.Property.NodeScope
    );
    /**
     * URI prefixes the catalog endpoint of a {@code rest}, {@code glue},
     * {@code iceberg}, {@code polaris} or {@code unity} namespace
     * registration may fall under; see {@link AllowedCatalogEndpoints}
     * for the match and for what an empty list refuses.
     */
    public static final Setting<List<String>> ALLOWED_CATALOG_ENDPOINTS_SETTING = Setting.listSetting(
        AllowedCatalogEndpoints.SETTING_KEY,
        List.of(),
        Function.identity(),
        Setting.Property.NodeScope
    );
    public static final Setting<Settings> STORAGE_OPTIONS_SETTING_DEPRECATED = Setting.groupSetting(
        StorageOptions.DEPRECATED_INDEX_SETTING_PREFIX,
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.Deprecated
    );
    public static final Setting<Settings> STORAGE_OPTIONS_SETTING = Setting.groupSetting(
        StorageOptions.INDEX_SETTING_PREFIX,
        STORAGE_OPTIONS_SETTING_DEPRECATED,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Node-scoped upper bound on the memory that Lance's shared
     * {@link org.lance.Session} may consume for its index and metadata
     * caches. Accepts either an absolute {@link org.opensearch.core.common.unit.ByteSizeValue}
     * (for example {@code "10gb"}) or a percentage of the memory left on
     * the host once the JVM heap is subtracted (for example {@code "40%"}).
     *
     * <p>The default of {@code "40%"} lets Lance scale with the node's
     * physical memory rather than a fixed byte count, and leaves room
     * for the k-NN plugin's own {@code knn.memory.circuit_breaker.limit}
     * (default {@code "50%"}) on nodes that host both plugins. Layer 2
     * of the native-memory design will make this dynamic; for now the
     * setting is node-scoped only, so a change requires a rolling
     * restart to take effect.
     */
    public static final Setting<String> NATIVE_MEMORY_LIMIT_SETTING_DEPRECATED = Setting.simpleString(
        "lance.native_memory.limit",
        "40%",
        LanceSettings::validateNativeMemoryLimit,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<String> NATIVE_MEMORY_LIMIT_SETTING = Setting.simpleString(
        "plugins.lance.native_memory.limit",
        LanceSettings::validateNativeMemoryLimit,
        NATIVE_MEMORY_LIMIT_SETTING_DEPRECATED,
        Setting.Property.NodeScope
    );

    /**
     * Toggles the circuit breaker that rejects FTS and knn queries when
     * Lance's shared {@link org.lance.Session} caches have caught up to
     * the limit configured by {@link #NATIVE_MEMORY_LIMIT_SETTING}. Left
     * on by default; operators may temporarily disable it if the check
     * itself gets in the way of an investigation. The breaker's byte
     * limit is not configurable through this setting; it always mirrors
     * the Session cache limit so operators have one number to reason
     * about.
     */
    public static final Setting<Boolean> NATIVE_MEMORY_CB_ENABLED_SETTING_DEPRECATED = Setting.boolSetting(
        "lance.native_memory.circuit_breaker.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Boolean> NATIVE_MEMORY_CB_ENABLED_SETTING = Setting.boolSetting(
        "plugins.lance.native_memory.circuit_breaker.enabled",
        NATIVE_MEMORY_CB_ENABLED_SETTING_DEPRECATED,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How often the plugin samples {@link org.lance.Session#sizeBytes()}
     * and forwards the reading to the {@code lance_native} circuit
     * breaker's accounting. Kept intentionally short (five seconds by
     * default) because a single 100M-row FTS query can grow the cache
     * by several GiB, and a slower cadence would let the breaker lag
     * far behind the real footprint. Node scoped and dynamic so
     * operators can tune it without a restart.
     */
    public static final Setting<TimeValue> NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING_DEPRECATED = Setting.timeSetting(
        "lance.native_memory.circuit_breaker.poll_interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<TimeValue> NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING = Setting.timeSetting(
        "plugins.lance.native_memory.circuit_breaker.poll_interval",
        NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING_DEPRECATED,
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cap on how many fragment path queries this node executes in
     * parallel. Fragment path processes every fragment of an index on
     * one node, so per-query heap (FTS score arrays sized by
     * {@code maxDoc}, aggregation buffers) scales with the number of
     * concurrent requests rather than with cluster fan-out. The
     * {@code lance_native} circuit breaker still catches individual
     * runaway queries, but at high concurrency allocation races the
     * breaker and the node can drop into {@code OutOfMemoryError}
     * before the breaker fires; a bounded semaphore backstops that
     * race by serialising the tail once the limit is reached.
     *
     * <p>Default {@code 4} is chosen so that fragment path stays
     * comfortably below the search threadpool size (which is
     * {@code (allocated_processors * 3) / 2 + 1}) on typical
     * hardware, and matches the concurrency level at which the
     * evaluation observed the parent circuit breaker successfully
     * rejecting overflow with 429 rather than the JVM dying. Node
     * scoped and static: changing the value requires a restart
     * because the underlying semaphore's permit count is fixed at
     * plugin init.
     */
    public static final Setting<Integer> FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING_DEPRECATED = Setting.intSetting(
        "lance.fragment_dispatch.max_concurrent",
        4,
        1,
        128,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING = Setting.intSetting(
        "plugins.lance.fragment_dispatch.max_concurrent",
        FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING_DEPRECATED,
        1,
        128,
        Setting.Property.NodeScope
    );

    /**
     * Whether the fragment path keeps a node scoped snapshot of each
     * Lance table version it has served (open dataset, fragment metadata,
     * schema) and an off-heap cache of the numeric and boolean columns it
     * has read, so a second request against the same version opens no
     * dataset and scans no column it already holds. Dynamic: turning it
     * off retires every snapshot at once and later requests open the
     * table per request as before.
     */
    public static final Setting<Boolean> CACHE_ENABLED_SETTING_DEPRECATED = Setting.boolSetting(
        "lance.cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Boolean> CACHE_ENABLED_SETTING = Setting.boolSetting(
        "plugins.lance.cache.enabled",
        CACHE_ENABLED_SETTING_DEPRECATED,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How many table snapshots {@link org.opensearch.lance.engine.LanceWarmCache}
     * keeps before it closes the least recently used one that no request
     * holds. Each snapshot is one open Lance dataset plus a few kilobytes
     * of metadata per fragment. Static, node scope.
     */
    public static final Setting<Integer> CACHE_MAX_SNAPSHOTS_SETTING_DEPRECATED = Setting.intSetting(
        "lance.cache.max_snapshots",
        64,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> CACHE_MAX_SNAPSHOTS_SETTING = Setting.intSetting(
        "plugins.lance.cache.max_snapshots",
        CACHE_MAX_SNAPSHOTS_SETTING_DEPRECATED,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Fraction of {@link #NATIVE_MEMORY_LIMIT_SETTING} reserved for the
     * off-heap column cache. The remainder goes to the Lance Session's
     * index and metadata caches in their 6:1 ratio. Static, node scope.
     */
    public static final Setting<Double> CACHE_COLUMN_SHARE_SETTING_DEPRECATED = Setting.doubleSetting(
        "lance.cache.column_share",
        0.4,
        0.0,
        0.95,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<Double> CACHE_COLUMN_SHARE_SETTING = Setting.doubleSetting(
        "plugins.lance.cache.column_share",
        CACHE_COLUMN_SHARE_SETTING_DEPRECATED,
        0.0,
        0.95,
        Setting.Property.NodeScope
    );

    /**
     * Whether the coordinator keeps the reduced answer of every
     * {@code size: 0} request against a single Lance backed index, keyed
     * on the table version it was computed from, and answers the same
     * request again from that entry while the table stays at that
     * version ({@link org.opensearch.lance.dispatch.LanceRequestCache}). Dynamic: turning it off drops
     * every entry.
     */
    public static final Setting<Boolean> REQUEST_CACHE_ENABLED_SETTING_DEPRECATED = Setting.boolSetting(
        "lance.request_cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Boolean> REQUEST_CACHE_ENABLED_SETTING = Setting.boolSetting(
        "plugins.lance.request_cache.enabled",
        REQUEST_CACHE_ENABLED_SETTING_DEPRECATED,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How much heap the coordinator result cache may hold, as a byte size
     * or a percentage of the heap; the same default as
     * {@code indices.requests.cache.size}. Static, node scope.
     */
    public static final Setting<ByteSizeValue> REQUEST_CACHE_SIZE_SETTING_DEPRECATED = Setting.memorySizeSetting(
        "lance.request_cache.size",
        "1%",
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<ByteSizeValue> REQUEST_CACHE_SIZE_SETTING = Setting.memorySizeSetting(
        "plugins.lance.request_cache.size",
        REQUEST_CACHE_SIZE_SETTING_DEPRECATED,
        Setting.Property.NodeScope
    );

    /**
     * The largest answer the coordinator result cache stores, measured as
     * the serialised size of the reduced aggregations. Static, node scope.
     */
    public static final Setting<ByteSizeValue> REQUEST_CACHE_MAX_ENTRY_SIZE_SETTING_DEPRECATED = Setting.byteSizeSetting(
        "lance.request_cache.max_entry_size",
        new ByteSizeValue(1, ByteSizeUnit.MB),
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<ByteSizeValue> REQUEST_CACHE_MAX_ENTRY_SIZE_SETTING = Setting.byteSizeSetting(
        "plugins.lance.request_cache.max_entry_size",
        REQUEST_CACHE_MAX_ENTRY_SIZE_SETTING_DEPRECATED,
        Setting.Property.NodeScope
    );

    /**
     * How long an entry of the coordinator result cache is served after
     * it was stored; zero (the default) keeps it until the table moves to
     * another version or the cache evicts it. Dynamic.
     */
    public static final Setting<TimeValue> REQUEST_CACHE_EXPIRE_SETTING_DEPRECATED = Setting.positiveTimeSetting(
        "lance.request_cache.expire",
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<TimeValue> REQUEST_CACHE_EXPIRE_SETTING = Setting.positiveTimeSetting(
        "plugins.lance.request_cache.expire",
        REQUEST_CACHE_EXPIRE_SETTING_DEPRECATED,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether every data node keeps the cells of the rows it took for the
     * hits of a page, keyed on the table version, the row address and
     * the column, and renders a row whose cells it holds without a take
     * ({@link org.opensearch.lance.engine.LanceFetchCache}). Dynamic: turning it off drops every
     * entry.
     */
    public static final Setting<Boolean> FETCH_CACHE_ENABLED_SETTING_DEPRECATED = Setting.boolSetting(
        "lance.fetch_cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Boolean> FETCH_CACHE_ENABLED_SETTING = Setting.boolSetting(
        "plugins.lance.fetch_cache.enabled",
        FETCH_CACHE_ENABLED_SETTING_DEPRECATED,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How much heap the fetch cache may hold, as a byte size or a
     * percentage of the heap; the same default as
     * {@code indices.requests.cache.size}. Static, node scope.
     */
    public static final Setting<ByteSizeValue> FETCH_CACHE_SIZE_SETTING_DEPRECATED = Setting.memorySizeSetting(
        "lance.fetch_cache.size",
        "1%",
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<ByteSizeValue> FETCH_CACHE_SIZE_SETTING = Setting.memorySizeSetting(
        "plugins.lance.fetch_cache.size",
        FETCH_CACHE_SIZE_SETTING_DEPRECATED,
        Setting.Property.NodeScope
    );

    /**
     * The heaviest cell the fetch cache stores, as the estimate of the
     * decoded value's heap; a larger cell (a long text, a wide struct)
     * is taken on every request. Static, node scope.
     */
    public static final Setting<ByteSizeValue> FETCH_CACHE_MAX_ENTRY_SIZE_SETTING_DEPRECATED = Setting.byteSizeSetting(
        "lance.fetch_cache.max_entry_size",
        new ByteSizeValue(256, ByteSizeUnit.KB),
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<ByteSizeValue> FETCH_CACHE_MAX_ENTRY_SIZE_SETTING = Setting.byteSizeSetting(
        "plugins.lance.fetch_cache.max_entry_size",
        FETCH_CACHE_MAX_ENTRY_SIZE_SETTING_DEPRECATED,
        Setting.Property.NodeScope
    );

    /**
     * How long a cell of the fetch cache is served after it was stored;
     * zero (the default) keeps it until its table version's snapshot
     * closes, its index is deleted or the cache evicts it. Dynamic.
     */
    public static final Setting<TimeValue> FETCH_CACHE_EXPIRE_SETTING_DEPRECATED = Setting.positiveTimeSetting(
        "lance.fetch_cache.expire",
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<TimeValue> FETCH_CACHE_EXPIRE_SETTING = Setting.positiveTimeSetting(
        "plugins.lance.fetch_cache.expire",
        FETCH_CACHE_EXPIRE_SETTING_DEPRECATED,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Row cap of the probe scan a full-text query runs when the
     * executing node holds a proper subset of the table's fragments
     * (several data nodes) and the shape needs every match
     * (aggregations, sort by a field, post_filter, {@code size 0},
     * {@code track_total_hits: true}). The probe scans the whole table
     * from the inverted index and keeps the node's rows; when it
     * returns this many rows the node repeats the scan restricted to
     * its fragments instead, which Lance answers through a
     * {@code _rowid} prefilter read. Dynamic: the next scan picks up
     * a new value. See {@link LanceFtsQuery}.
     */
    public static final Setting<Integer> FTS_SUBSET_PROBE_LIMIT_SETTING_DEPRECATED = Setting.intSetting(
        "lance.fts.subset_probe_limit",
        LanceFtsQuery.DEFAULT_SUBSET_PROBE_LIMIT,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> FTS_SUBSET_PROBE_LIMIT_SETTING = Setting.intSetting(
        "plugins.lance.fts.subset_probe_limit",
        FTS_SUBSET_PROBE_LIMIT_SETTING_DEPRECATED,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Share of the rows a subset node covers that the FTS probe may
     * return before the node repeats the scan restricted to its
     * fragments. The effective probe limit is
     * {@code min(subset_probe_limit, max(subset_probe_min_rows,
     * floor(covered rows * subset_probe_ratio)))}; the derivation of
     * the default is in {@link LanceFtsQuery#effectiveSubsetProbeLimit}.
     * Dynamic.
     */
    public static final Setting<Double> FTS_SUBSET_PROBE_RATIO_SETTING_DEPRECATED = Setting.doubleSetting(
        "lance.fts.subset_probe_ratio",
        LanceFtsQuery.DEFAULT_SUBSET_PROBE_RATIO,
        0d,
        1d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Double> FTS_SUBSET_PROBE_RATIO_SETTING = Setting.doubleSetting(
        "plugins.lance.fts.subset_probe_ratio",
        FTS_SUBSET_PROBE_RATIO_SETTING_DEPRECATED,
        0d,
        1d,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Floor of the effective FTS probe limit, so small tables and few
     * hit queries stay on the whole table lookup whatever the ratio
     * gives. Dynamic.
     */
    public static final Setting<Integer> FTS_SUBSET_PROBE_MIN_ROWS_SETTING_DEPRECATED = Setting.intSetting(
        "lance.fts.subset_probe_min_rows",
        LanceFtsQuery.DEFAULT_SUBSET_PROBE_MIN_ROWS,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> FTS_SUBSET_PROBE_MIN_ROWS_SETTING = Setting.intSetting(
        "plugins.lance.fts.subset_probe_min_rows",
        FTS_SUBSET_PROBE_MIN_ROWS_SETTING_DEPRECATED,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether a native scan or index load of the fragment path is
     * admitted only when the node's available physical memory can hold
     * the gate's estimate of what Lance will allocate for it (a full
     * text document set rebuild, a scalar or vector index load, the row
     * addresses and buffers of a filter scan, the parallel scans of a
     * pushed aggregate). {@code false} admits every shape, restoring
     * the behaviour that let a large enough table end the node with a
     * kernel OOM kill. Dynamic. See {@link ScanAdmission}.
     */
    public static final Setting<Boolean> ADMISSION_ENABLED_SETTING_DEPRECATED = Setting.boolSetting(
        "lance.admission.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Boolean> ADMISSION_ENABLED_SETTING = Setting.boolSetting(
        "plugins.lance.admission.enabled",
        ADMISSION_ENABLED_SETTING_DEPRECATED,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Available physical memory the admission gate keeps out of reach
     * of a gated scan: the scan is admitted when its estimate fits
     * {@code MemAvailable - headroom} plus the memory earlier admitted
     * scans retained. Dynamic.
     */
    public static final Setting<ByteSizeValue> ADMISSION_HEADROOM_SETTING_DEPRECATED = Setting.byteSizeSetting(
        "lance.admission.headroom",
        ScanAdmission.DEFAULT_HEADROOM,
        ByteSizeValue.ZERO,
        new ByteSizeValue(Long.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<ByteSizeValue> ADMISSION_HEADROOM_SETTING = new Setting<>(
        "plugins.lance.admission.headroom",
        ADMISSION_HEADROOM_SETTING_DEPRECATED,
        new Setting.ByteSizeValueParser(ByteSizeValue.ZERO, new ByteSizeValue(Long.MAX_VALUE), "plugins.lance.admission.headroom"),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether a bounded top-k page (a full text page, a filter page
     * with a scan limit) is judged by the same admission estimate as
     * the unbounded shapes. Lance rebuilds the inverted index document
     * set and materialises the scalar index result for a bounded page
     * just as it does for an unbounded scan, so a large enough table
     * can end the node with a kernel OOM kill even at {@code size: 10}.
     * {@code false} restores the pass-through for bounded pages (a
     * filter page is then judged on its limit). Dynamic. See
     * {@link ScanAdmission}.
     */
    public static final Setting<Boolean> ADMISSION_BOUNDED_SHAPES_GATED_SETTING_DEPRECATED = Setting.boolSetting(
        "lance.admission.bounded_shapes_gated",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Boolean> ADMISSION_BOUNDED_SHAPES_GATED_SETTING = Setting.boolSetting(
        "plugins.lance.admission.bounded_shapes_gated",
        ADMISSION_BOUNDED_SHAPES_GATED_SETTING_DEPRECATED,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Test override of the index cache shard share the admission gate
     * compares its estimates with (an estimate at or below the share is
     * zero). Zero (the default) reads the installed Session's sizing.
     * It exists so the integration tests can declare a small fixture
     * table's indexes and scans as not fitting the cache; do not change
     * it on a real node. Dynamic.
     */
    public static final Setting<ByteSizeValue> TEST_INDEX_CACHE_SHARD_SHARE_SETTING_DEPRECATED = Setting.byteSizeSetting(
        "lance.test.index_cache_shard_share",
        ByteSizeValue.ZERO,
        ByteSizeValue.ZERO,
        new ByteSizeValue(Long.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<ByteSizeValue> TEST_INDEX_CACHE_SHARD_SHARE_SETTING = new Setting<>(
        "plugins.lance.test.index_cache_shard_share",
        TEST_INDEX_CACHE_SHARD_SHARE_SETTING_DEPRECATED,
        new Setting.ByteSizeValueParser(
            ByteSizeValue.ZERO,
            new ByteSizeValue(Long.MAX_VALUE),
            "plugins.lance.test.index_cache_shard_share"
        ),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Test override of the available memory readings the admission gate
     * judges on: a list of byte sizes handed out one per reading, the
     * last one repeating. Empty (the default) reads the kernel's
     * {@code MemAvailable}. It exists so the integration tests can
     * script what an admitted scan leaves behind (the reading at the
     * admission and the reading at the scan's completion) and prove the
     * retained credit; do not set it on a real node. Dynamic.
     */
    public static final Setting<List<String>> TEST_ADMISSION_AVAILABLE_MEMORY_SETTING_DEPRECATED = Setting.listSetting(
        "lance.test.admission_available_memory",
        List.of(),
        raw -> ByteSizeValue.parseBytesSizeValue(raw, "lance.test.admission_available_memory").getStringRep(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<List<String>> TEST_ADMISSION_AVAILABLE_MEMORY_SETTING = Setting.listSetting(
        "plugins.lance.test.admission_available_memory",
        TEST_ADMISSION_AVAILABLE_MEMORY_SETTING_DEPRECATED,
        raw -> ByteSizeValue.parseBytesSizeValue(raw, "plugins.lance.test.admission_available_memory").getStringRep(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Test override that makes every background collection of the
     * planner's table statistics wait this long before it reads the
     * table. Zero (the default) collects at once. It exists so the
     * integration tests can observe, on a small table whose statistics
     * would otherwise be ready within milliseconds, the request that
     * plans without them and the {@code GET /_plugins/_lance/stats} counters that
     * record it; do not set it on a real node. Dynamic.
     */
    public static final Setting<TimeValue> TEST_STATISTICS_COLLECT_DELAY_SETTING_DEPRECATED = Setting.timeSetting(
        "lance.test.statistics_collect_delay",
        TimeValue.ZERO,
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<TimeValue> TEST_STATISTICS_COLLECT_DELAY_SETTING = Setting.timeSetting(
        "plugins.lance.test.statistics_collect_delay",
        TEST_STATISTICS_COLLECT_DELAY_SETTING_DEPRECATED,
        TimeValue.ZERO,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Test hook that installs a reader wrapper shaped like the security
     * plugin's document and field level security reader
     * ({@link HidingReaderWrapper}) on every Lance backed index created
     * while it is set whose name starts with the given prefix. The value
     * is {@code <index prefix>:<hidden column>:<filter column>:<minimum>}:
     * the wrapper drops the hidden column from the leaves' field infos
     * and shows only the rows whose filter column is at least the
     * minimum. Empty (the default) installs nothing. It exists so the
     * integration tests, whose cluster has no security plugin, can pin
     * what the plugin does under such a wrapper; do not set it on a real
     * node. Dynamic: {@link LancePlugin#onIndexModule} reads it when an index
     * service is built, so an index created after an update follows the
     * new value and an index created before keeps its wrapper.
     */
    public static final Setting<String> TEST_HIDING_WRAPPER_INDEX_PREFIX_SETTING = Setting.simpleString(
        "plugins.lance.test.hiding_wrapper_index_prefix",
        HidingReaderWrapper.Rule::validate,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether a {@code size: 0} aggregation request whose shape the
     * scan can compute (metrics including stats, cardinality and tdigest
     * percentiles; {@code terms} / {@code histogram} / {@code date_histogram}
     * / {@code range} / {@code date_range} / {@code filter} /
     * {@code filters} / {@code missing} nested up to three levels
     * with metric children; {@code composite} over terms and fixed
     * interval date_histogram sources; over a {@code match_all} or
     * scalar filter query; see
     * {@code LanceAggregateResults} in the execute package) runs
     * as a Substrait group by inside the Lance scan. Off, every
     * aggregation goes through the Lucene aggregators over the fragment
     * leaf readers. Dynamic so the two paths can be compared without a
     * restart.
     */
    public static final Setting<Boolean> AGGREGATION_PUSHDOWN_SETTING_DEPRECATED = Setting.boolSetting(
        "lance.aggregation.pushdown",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Boolean> AGGREGATION_PUSHDOWN_SETTING = Setting.boolSetting(
        "plugins.lance.aggregation.pushdown",
        AGGREGATION_PUSHDOWN_SETTING_DEPRECATED,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How many Lance scans a pushed down aggregation runs side by side
     * on one executor. The executor cuts its fragments into that many
     * contiguous groups (fewer when it holds fewer fragments), scans each
     * with its own Substrait aggregate and merges the group rows in Java.
     * Lance runs the aggregate of one scan in a single DataFusion
     * partition, so a node holding many fragments hashes every row on
     * one thread unless the plugin splits the scan. Default: half the
     * CPUs the JVM sees ({@link NativeMemoryLimit#availableCpus()}),
     * at least 1 and at most 32. Half because each scan already keeps
     * Lance's decode threads busy alongside the aggregating thread, so
     * the aggregates and the decoding share the cores instead of
     * oversubscribing them; the cap keeps the fan-out and the memory of
     * the concurrent hash tables bounded on large hosts. 1 restores the
     * single scan.
     */
    public static final Setting<Integer> AGGREGATION_PUSHDOWN_PARALLELISM_SETTING_DEPRECATED = Setting.intSetting(
        "lance.aggregation.pushdown_parallelism",
        Math.max(1, Math.min(32, NativeMemoryLimit.availableCpus() / 2)),
        1,
        32,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> AGGREGATION_PUSHDOWN_PARALLELISM_SETTING = Setting.intSetting(
        "plugins.lance.aggregation.pushdown_parallelism",
        AGGREGATION_PUSHDOWN_PARALLELISM_SETTING_DEPRECATED,
        1,
        32,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Largest number of groups a nested bucket tree may be expected to
     * produce and still take the aggregation pushdown. The estimate is
     * the product of the {@code shard_size} of every {@code terms}
     * level (a histogram level has no size and counts as one); above
     * it the request goes through the Lucene aggregators, because the
     * scan would return one row per key combination and the executor
     * would hold them all. Static: the executor reads it from the node
     * settings when it plans a request.
     */
    public static final Setting<Integer> AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING_DEPRECATED = Setting.intSetting(
        "lance.aggregation.pushdown_max_groups",
        1_000_000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING = Setting.intSetting(
        "plugins.lance.aggregation.pushdown_max_groups",
        AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING_DEPRECATED,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Number of equal width bins a pushed down tdigest {@code percentiles}
     * / {@code percentile_ranks} cuts the value range into. The executor
     * asks Lance for the minimum and maximum of the field, then for the
     * row count of every bin of width {@code (max - min) / bins}, and
     * feeds each bin's rows to the TDigest sketch the coordinator merges
     * as one value at each bin edge and the rest at the centre. The
     * histogram the sketch sees is therefore accurate to one bin width
     * (0.025 % of the range at the default); the sketch itself, built
     * from a few thousand weighted points instead of every document,
     * interpolates less accurately than the aggregators' digest, which
     * is the larger error on a long tailed field (a p95 measured 0.16 %
     * of the range from the exact value on a 20M row table where the
     * aggregators' digest was 0.01 % off). More bins mean a finer
     * histogram and more rows for the executor to read (one per non
     * empty bin, per bucket of the enclosing aggregation). Dynamic so the
     * trade-off can be tuned without a restart; the executor reads it
     * when it plans a request.
     */
    public static final Setting<Integer> AGGREGATION_PERCENTILES_BINS_SETTING_DEPRECATED = Setting.intSetting(
        "lance.aggregation.percentiles_bins",
        4096,
        16,
        1_000_000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> AGGREGATION_PERCENTILES_BINS_SETTING = Setting.intSetting(
        "plugins.lance.aggregation.percentiles_bins",
        AGGREGATION_PERCENTILES_BINS_SETTING_DEPRECATED,
        16,
        1_000_000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How many times {@code shard_size} groups each scan of a single
     * level {@code terms} ordered by {@code _count} or by one metric
     * keeps while it reads Lance's group rows. Groups outside the
     * selection only add their count to {@code sum_other_doc_count},
     * so the executor's memory and time stop growing with the number
     * of distinct keys; a key another scan kept has its counts summed
     * before the final {@code shard_size} cut, and the slack is what
     * keeps a key split over several scans from being dropped while it
     * is still a contender. The doc count error the coordinator
     * derives from the smallest returned bucket keeps its meaning, the
     * same way it covers the terms a shard did not return. Raise it
     * when high cardinality terms need tighter counts, at the cost of
     * proportionally more retained groups per scan. Dynamic; the
     * executor reads it when it plans a request.
     */
    public static final Setting<Integer> AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING_DEPRECATED = Setting.intSetting(
        "lance.aggregation.pushdown_topk_slack",
        4,
        1,
        64,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING = Setting.intSetting(
        "plugins.lance.aggregation.pushdown_topk_slack",
        AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING_DEPRECATED,
        1,
        64,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How many Lance scans a fragment path request runs side by side on
     * one executor when it materialises a column (the aggregator and
     * sort paths that read a column into the off-heap store or into
     * heap). The executor cuts its fragments into that many contiguous
     * groups (fewer when it holds fewer fragments) and scans each group
     * on the index_searcher pool. Lance decodes a scan on its own threads, but
     * the Java side that reads the batches into the column arrays is one
     * thread per scan, so a node holding many fragments loads a column
     * on one core unless the plugin splits the scan. Same default and
     * bounds as {@link #AGGREGATION_PUSHDOWN_PARALLELISM_SETTING}, for
     * the same reason: each scan already keeps Lance's decode threads
     * busy next to the consuming thread. 1 restores the single scan.
     */
    public static final Setting<Integer> FRAGMENT_PATH_PARALLELISM_SETTING_DEPRECATED = Setting.intSetting(
        "lance.fragment_path.parallelism",
        Math.max(1, Math.min(32, NativeMemoryLimit.availableCpus() / 2)),
        1,
        32,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> FRAGMENT_PATH_PARALLELISM_SETTING = Setting.intSetting(
        "plugins.lance.fragment_path.parallelism",
        FRAGMENT_PATH_PARALLELISM_SETTING_DEPRECATED,
        1,
        32,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How many slices a fragment path executor cuts its fragment leaves
     * into when it collects a page of hits or an aggregation, the way
     * concurrent segment search slices a shard's segments. Each slice
     * collects on its own thread of the index_searcher pool with its own
     * collector (its own aggregator tree), and the slice results are
     * reduced on the executor before the answer goes to the
     * coordinator. Without this an executor collected every fragment
     * of the node on the one search thread that carried the request,
     * so the aggregators, which the column loads and the pushdown do
     * not speed up, kept a large node at one busy core. Same default
     * and bounds as {@link #FRAGMENT_PATH_PARALLELISM_SETTING}: a slice
     * shares the cores with the column loads of the same request. 1
     * collects on the request's thread alone, in fragment order, and
     * reduces nothing, which is the behaviour before slicing existed.
     */
    public static final Setting<Integer> FRAGMENT_PATH_SLICES_SETTING_DEPRECATED = Setting.intSetting(
        "lance.fragment_path.slices",
        Math.max(1, Math.min(32, NativeMemoryLimit.availableCpus() / 2)),
        1,
        32,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Integer> FRAGMENT_PATH_SLICES_SETTING = Setting.intSetting(
        "plugins.lance.fragment_path.slices",
        FRAGMENT_PATH_SLICES_SETTING_DEPRECATED,
        1,
        32,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether a page answered by two or more executors is rendered in a
     * fetch round: the executors return the row address, score and sort
     * values of their top hits, the coordinator merges them and asks the
     * executors holding the {@code size} rows it keeps to render those
     * alone. Without it every executor renders its own top
     * {@code from + size} rows and the coordinator discards all but the
     * page, so a page takes {@code size} rows per executor from the
     * object store. {@code false} renders on the query round on every
     * request. A page served by one executor, a {@code collapse} or
     * {@code "explain": true} body and an index with a reader wrapper
     * (the security plugin's document and field level security) render
     * on the query round whatever the setting says. Dynamic; the
     * coordinator reads it per request.
     */
    public static final Setting<Boolean> FRAGMENT_PATH_DEFER_FETCH_SETTING = Setting.boolSetting(
        "plugins.lance.fragment_path.defer_fetch",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * What {@link LanceIndexWarmer} reads of a table's indexes into this
     * node's Lance Session cache when a Lance-backed index appears
     * (attach, namespace poll, node restart): {@code none} nothing,
     * {@code metadata} what Lance loads to open each index (BTree page
     * lookup, bitmap keys, full-text token dictionaries, IVF centroids
     * and one partition), {@code all} additionally every BTree page,
     * every bitmap and every IVF partition. Dynamic: warm-ups started
     * after the change use the new value.
     */
    public static final Setting<LanceIndexWarmer.Mode> ATTACH_WARM_INDEXES_SETTING_DEPRECATED = new Setting<>(
        "lance.attach.warm_indexes",
        LanceIndexWarmer.Mode.METADATA.settingValue(),
        LanceIndexWarmer.Mode::parse,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<LanceIndexWarmer.Mode> ATTACH_WARM_INDEXES_SETTING = new Setting<>(
        "plugins.lance.attach.warm_indexes",
        ATTACH_WARM_INDEXES_SETTING_DEPRECATED,
        LanceIndexWarmer.Mode::parse,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * The most physical rows one Lucene reader of a Lance table may
     * hold. Lucene refuses a composite reader whose leaves' {@code maxDoc}
     * sum exceeds {@link IndexWriter#MAX_DOCS} (2,147,483,519), and a
     * fragment leaf's {@code maxDoc} is the fragment's physical row
     * count, so a table with more rows than that is served in pieces:
     * the coordinator cuts each data node's fragments into groups within
     * the bound and sends one fragment request per group, and the shard
     * engine's whole table reader holds the leading fragments that fit.
     * The default is the Lucene bound itself. The setting exists so the
     * integration tests can exercise the split on a small table; it is
     * not meant to be changed on a real node. Dynamic: the coordinator
     * and the dispatch filter read it per request, the engine at every
     * reader open.
     */
    public static final Setting<Long> MAX_DOCS_PER_READER_SETTING_DEPRECATED = Setting.longSetting(
        "lance.test.max_docs_per_reader",
        IndexWriter.MAX_DOCS,
        1L,
        IndexWriter.MAX_DOCS,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic,
        Setting.Property.Deprecated
    );
    public static final Setting<Long> MAX_DOCS_PER_READER_SETTING = Setting.longSetting(
        "plugins.lance.test.max_docs_per_reader",
        MAX_DOCS_PER_READER_SETTING_DEPRECATED,
        1L,
        IndexWriter.MAX_DOCS,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static void validateUncoveredFragmentPolicy(String value) {
        if (!"wait".equals(value) && !"immediate".equals(value)) {
            throw new IllegalArgumentException(
                "index.plugins.lance.uncovered_fragment_policy must be 'wait' or 'immediate', got '" + value + "'"
            );
        }
    }

    private static void validatePrimaryKeyType(String value) {
        // Empty is accepted so the setting can be omitted on indices that
        // do not declare a primary key (the runtime path treats the PK
        // field name as the source of truth for "PK present"). Otherwise
        // restrict to the two enum-mapped forms so a typo like "keywords"
        // fails at CreateIndex time rather than silently falling back to
        // long.
        if (value == null || value.isEmpty()) {
            return;
        }
        if (!"long".equals(value) && !"keyword".equals(value) && !"unsigned_long".equals(value) && !"none".equals(value)) {
            throw new IllegalArgumentException(
                "index.plugins.lance.primary_key_type must be 'long', 'unsigned_long', or 'keyword', got '" + value + "'"
            );
        }
    }

    private static void validateNativeMemoryLimit(String value) {
        // Delegate to the parser so validation and resolution stay in
        // one place. The parser throws OpenSearchParseException /
        // IllegalArgumentException on malformed input, which
        // Setting.simpleString surfaces back to the operator as a
        // 400-style validation error.
        NativeMemoryLimit.parse(value, "plugins.lance.native_memory.limit");
    }

    /** Every setting of the plugin, the current keys first and then the deprecated ones. */
    public static List<Setting<?>> all() {
        List<Setting<?>> current = List.of(
            TABLE_SETTING,
            PRIMARY_KEY_FIELD_SETTING,
            PRIMARY_KEY_TYPE_SETTING,
            VERSION_SETTING,
            TAG_SETTING,
            MULTI_FIELDS_SETTING,
            OVERRIDES_SETTING,
            UNCOVERED_FRAGMENT_POLICY_SETTING,
            NAMESPACE_POLL_CADENCE_SETTING,
            NAMESPACE_RESURFACE_GRACE_SETTING,
            ALLOWED_TABLE_ROOTS_SETTING,
            ALLOWED_CATALOG_ENDPOINTS_SETTING,
            STORAGE_OPTIONS_SETTING,
            NATIVE_MEMORY_LIMIT_SETTING,
            NATIVE_MEMORY_CB_ENABLED_SETTING,
            NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING,
            FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING,
            CACHE_ENABLED_SETTING,
            CACHE_MAX_SNAPSHOTS_SETTING,
            CACHE_COLUMN_SHARE_SETTING,
            REQUEST_CACHE_ENABLED_SETTING,
            REQUEST_CACHE_SIZE_SETTING,
            REQUEST_CACHE_MAX_ENTRY_SIZE_SETTING,
            REQUEST_CACHE_EXPIRE_SETTING,
            FETCH_CACHE_ENABLED_SETTING,
            FETCH_CACHE_SIZE_SETTING,
            FETCH_CACHE_MAX_ENTRY_SIZE_SETTING,
            FETCH_CACHE_EXPIRE_SETTING,
            FTS_SUBSET_PROBE_LIMIT_SETTING,
            FTS_SUBSET_PROBE_RATIO_SETTING,
            FTS_SUBSET_PROBE_MIN_ROWS_SETTING,
            ADMISSION_ENABLED_SETTING,
            ADMISSION_HEADROOM_SETTING,
            ADMISSION_BOUNDED_SHAPES_GATED_SETTING,
            TEST_INDEX_CACHE_SHARD_SHARE_SETTING,
            TEST_ADMISSION_AVAILABLE_MEMORY_SETTING,
            TEST_STATISTICS_COLLECT_DELAY_SETTING,
            TEST_HIDING_WRAPPER_INDEX_PREFIX_SETTING,
            AGGREGATION_PUSHDOWN_SETTING,
            AGGREGATION_PUSHDOWN_PARALLELISM_SETTING,
            AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING,
            AGGREGATION_PERCENTILES_BINS_SETTING,
            AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING,
            FRAGMENT_PATH_PARALLELISM_SETTING,
            FRAGMENT_PATH_SLICES_SETTING,
            FRAGMENT_PATH_DEFER_FETCH_SETTING,
            ATTACH_WARM_INDEXES_SETTING,
            MAX_DOCS_PER_READER_SETTING
        );
        List<Setting<?>> deprecated = List.of(
            TABLE_SETTING_DEPRECATED,
            PRIMARY_KEY_FIELD_SETTING_DEPRECATED,
            PRIMARY_KEY_TYPE_SETTING_DEPRECATED,
            VERSION_SETTING_DEPRECATED,
            TAG_SETTING_DEPRECATED,
            MULTI_FIELDS_SETTING_DEPRECATED,
            OVERRIDES_SETTING_DEPRECATED,
            UNCOVERED_FRAGMENT_POLICY_SETTING_DEPRECATED,
            NAMESPACE_POLL_CADENCE_SETTING_DEPRECATED,
            NAMESPACE_RESURFACE_GRACE_SETTING_DEPRECATED,
            ALLOWED_TABLE_ROOTS_SETTING_DEPRECATED,
            STORAGE_OPTIONS_SETTING_DEPRECATED,
            NATIVE_MEMORY_LIMIT_SETTING_DEPRECATED,
            NATIVE_MEMORY_CB_ENABLED_SETTING_DEPRECATED,
            NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING_DEPRECATED,
            FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING_DEPRECATED,
            CACHE_ENABLED_SETTING_DEPRECATED,
            CACHE_MAX_SNAPSHOTS_SETTING_DEPRECATED,
            CACHE_COLUMN_SHARE_SETTING_DEPRECATED,
            REQUEST_CACHE_ENABLED_SETTING_DEPRECATED,
            REQUEST_CACHE_SIZE_SETTING_DEPRECATED,
            REQUEST_CACHE_MAX_ENTRY_SIZE_SETTING_DEPRECATED,
            REQUEST_CACHE_EXPIRE_SETTING_DEPRECATED,
            FETCH_CACHE_ENABLED_SETTING_DEPRECATED,
            FETCH_CACHE_SIZE_SETTING_DEPRECATED,
            FETCH_CACHE_MAX_ENTRY_SIZE_SETTING_DEPRECATED,
            FETCH_CACHE_EXPIRE_SETTING_DEPRECATED,
            FTS_SUBSET_PROBE_LIMIT_SETTING_DEPRECATED,
            FTS_SUBSET_PROBE_RATIO_SETTING_DEPRECATED,
            FTS_SUBSET_PROBE_MIN_ROWS_SETTING_DEPRECATED,
            ADMISSION_ENABLED_SETTING_DEPRECATED,
            ADMISSION_HEADROOM_SETTING_DEPRECATED,
            ADMISSION_BOUNDED_SHAPES_GATED_SETTING_DEPRECATED,
            TEST_INDEX_CACHE_SHARD_SHARE_SETTING_DEPRECATED,
            TEST_ADMISSION_AVAILABLE_MEMORY_SETTING_DEPRECATED,
            TEST_STATISTICS_COLLECT_DELAY_SETTING_DEPRECATED,
            AGGREGATION_PUSHDOWN_SETTING_DEPRECATED,
            AGGREGATION_PUSHDOWN_PARALLELISM_SETTING_DEPRECATED,
            AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING_DEPRECATED,
            AGGREGATION_PERCENTILES_BINS_SETTING_DEPRECATED,
            AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING_DEPRECATED,
            FRAGMENT_PATH_PARALLELISM_SETTING_DEPRECATED,
            FRAGMENT_PATH_SLICES_SETTING_DEPRECATED,
            ATTACH_WARM_INDEXES_SETTING_DEPRECATED,
            MAX_DOCS_PER_READER_SETTING_DEPRECATED
        );
        List<Setting<?>> all = new ArrayList<>(current.size() + deprecated.size());
        all.addAll(current);
        all.addAll(deprecated);
        return all;
    }
}
