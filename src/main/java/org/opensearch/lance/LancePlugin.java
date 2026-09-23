/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.lance.Session;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.indices.breaker.BreakerSettings;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.TransportLanceAttachAction;
import org.opensearch.lance.execute.LanceAggregateResults;
import org.opensearch.lance.dispatch.LanceDispatchActionFilter;
import org.opensearch.lance.dispatch.LanceCreateIndexActionFilter;
import org.opensearch.lance.engine.LanceEngineFactory;
import org.opensearch.lance.engine.LanceIndexWarmer;
import org.opensearch.lance.engine.LanceLocalClones;
import org.opensearch.lance.engine.LanceWarmCache;
import org.opensearch.lance.index.LanceBuildIndexesAction;
import org.opensearch.lance.index.LanceBuildIndexesNodesAction;
import org.opensearch.lance.index.TransportLanceBuildIndexesAction;
import org.opensearch.lance.index.TransportLanceBuildIndexesNodesAction;
import org.opensearch.lance.mapper.LanceTextFieldMapper;
import org.opensearch.lance.mapper.LanceVectorFieldMapper;
import org.opensearch.lance.namespace.AllowedTableRoots;
import org.opensearch.lance.namespace.LanceNamespaceListAction;
import org.opensearch.lance.namespace.LanceNamespaceService;
import org.opensearch.lance.namespace.TransportLanceNamespaceListAction;
import org.opensearch.lance.plan.explain.LanceExplainAction;
import org.opensearch.lance.plan.explain.TransportLanceExplainAction;
import org.opensearch.lance.query.FtsAdmission;
import org.opensearch.lance.query.LanceFtsBoolQueryBuilder;
import org.opensearch.lance.query.LanceFtsBoostQueryBuilder;
import org.opensearch.lance.query.LanceFtsQuery;
import org.opensearch.lance.query.LanceKnnQueryBuilder;
import org.opensearch.lance.query.LanceMatchPhraseQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.lance.query.LanceMultiMatchQueryBuilder;
import org.opensearch.lance.refs.LanceRefsAction;
import org.opensearch.lance.refs.TransportLanceRefsAction;
import org.opensearch.lance.rest.RestAttachAction;
import org.opensearch.lance.rest.RestBuildIndexesAction;
import org.opensearch.lance.rest.RestLanceExplainAction;
import org.opensearch.lance.rest.RestNamespaceAction;
import org.opensearch.lance.rest.RestLanceStatsAction;
import org.opensearch.lance.rest.RestRefsAction;
import org.opensearch.lance.stats.LanceStatsAction;
import org.opensearch.lance.stats.LanceStatsCollector;
import org.opensearch.lance.stats.TransportLanceStatsAction;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.plugins.CircuitBreakerPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

/**
 * Plugin entry point. Registers the reader-side surface for Lance tables:
 * the REST endpoints for namespace / attach / build_indexes, the
 * {@link LanceEngineFactory} that wraps each Lance-backed index in a
 * read-only engine, mapping type parsers, and the {@code lance_knn} query.
 */
public class LancePlugin extends Plugin implements ActionPlugin, EnginePlugin, MapperPlugin, SearchPlugin, CircuitBreakerPlugin {

    private static final Logger LOGGER = LogManager.getLogger(LancePlugin.class);

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(
            new QuerySpec<>(LanceKnnQueryBuilder.NAME, LanceKnnQueryBuilder::new, LanceKnnQueryBuilder::fromXContent),
            new QuerySpec<>(LanceMatchQueryBuilder.NAME, LanceMatchQueryBuilder::new, LanceMatchQueryBuilder::fromXContent),
            new QuerySpec<>(
                LanceMatchPhraseQueryBuilder.NAME,
                LanceMatchPhraseQueryBuilder::new,
                LanceMatchPhraseQueryBuilder::fromXContent
            ),
            new QuerySpec<>(LanceMultiMatchQueryBuilder.NAME, LanceMultiMatchQueryBuilder::new, LanceMultiMatchQueryBuilder::fromXContent),
            new QuerySpec<>(LanceFtsBoostQueryBuilder.NAME, LanceFtsBoostQueryBuilder::new, LanceFtsBoostQueryBuilder::fromXContent),
            new QuerySpec<>(LanceFtsBoolQueryBuilder.NAME, LanceFtsBoolQueryBuilder::new, LanceFtsBoolQueryBuilder::fromXContent)
        );
    }

    @Override
    public java.util.Map<String, Mapper.TypeParser> getMappers() {
        return java.util.Map.of(
            LanceTextFieldMapper.CONTENT_TYPE,
            LanceTextFieldMapper.PARSER,
            LanceVectorFieldMapper.CONTENT_TYPE,
            LanceVectorFieldMapper.PARSER
        );
    }

    public static final Setting<String> TABLE_SETTING = Setting.simpleString(
        LanceEngineFactory.TABLE_SETTING,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    public static final Setting<String> PRIMARY_KEY_FIELD_SETTING = Setting.simpleString(
        LanceEngineFactory.PRIMARY_KEY_FIELD_SETTING,
        "",
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
    public static final Setting<String> PRIMARY_KEY_TYPE_SETTING = Setting.simpleString(
        LanceEngineFactory.PRIMARY_KEY_TYPE_SETTING,
        "long",
        LancePlugin::validatePrimaryKeyType,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    public static final Setting<Long> VERSION_SETTING = Setting.longSetting(
        LanceEngineFactory.VERSION_SETTING,
        -1L,
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
    public static final Setting<String> TAG_SETTING = Setting.simpleString(
        LanceEngineFactory.TAG_SETTING,
        "",
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );
    /**
     * JSON stringified multi-fields spec, persisted by attach so the
     * engine can rehydrate keyword sub-fields on shard open. Empty
     * means no sub-fields declared.
     */
    public static final Setting<String> MULTI_FIELDS_SETTING = Setting.simpleString(
        LanceEngineFactory.MULTI_FIELDS_SETTING,
        "",
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
    public static final Setting<String> OVERRIDES_SETTING = Setting.simpleString(
        LanceEngineFactory.OVERRIDES_SETTING,
        "",
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );
    public static final Setting<String> UNCOVERED_FRAGMENT_POLICY_SETTING = Setting.simpleString(
        "index.lance.uncovered_fragment_policy",
        "immediate",
        LancePlugin::validateUncoveredFragmentPolicy,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );
    /**
     * Where index builds commit: {@code in_table} (default) writes into
     * the source table's manifest chain; {@code node_local} keeps the
     * source read-only and gives every data node a shallow clone under
     * its data path that receives the builds and serves the node's
     * reads. Written by attach when the body carries
     * {@code "index_placement"}. Final because moving an existing
     * index between placements would strand the structures already
     * built in the other location.
     */
    public static final Setting<String> INDEX_PLACEMENT_SETTING = Setting.simpleString(
        LanceEngineFactory.INDEX_PLACEMENT_SETTING,
        LanceLocalClones.PLACEMENT_IN_TABLE,
        LancePlugin::validateIndexPlacement,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );
    public static final Setting<TimeValue> NAMESPACE_POLL_CADENCE_SETTING = Setting.timeSetting(
        "lance.namespace.poll_cadence",
        TimeValue.timeValueSeconds(10),
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
    public static final Setting<TimeValue> NAMESPACE_RESURFACE_GRACE_SETTING = Setting.timeSetting(
        "lance.namespace.resurface_guard_grace",
        TimeValue.timeValueHours(1),
        TimeValue.timeValueMillis(0),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Long> BUILDER_MAX_ROWS_SETTING = Setting.longSetting(
        "lance.builder.max_rows",
        1_000_000L,
        1L,
        Setting.Property.NodeScope
    );
    public static final Setting<List<String>> ALLOWED_TABLE_ROOTS_SETTING = Setting.listSetting(
        "lance.allowed_table_roots",
        List.of(),
        java.util.function.Function.identity(),
        Setting.Property.NodeScope
    );
    public static final Setting<Settings> STORAGE_OPTIONS_SETTING = Setting.groupSetting(
        StorageOptions.INDEX_SETTING_PREFIX,
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
    public static final Setting<String> NATIVE_MEMORY_LIMIT_SETTING = Setting.simpleString(
        "lance.native_memory.limit",
        "40%",
        LancePlugin::validateNativeMemoryLimit,
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
    public static final Setting<Boolean> NATIVE_MEMORY_CB_ENABLED_SETTING = Setting.boolSetting(
        "lance.native_memory.circuit_breaker.enabled",
        true,
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
    public static final Setting<TimeValue> NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING = Setting.timeSetting(
        "lance.native_memory.circuit_breaker.poll_interval",
        TimeValue.timeValueSeconds(5),
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
    public static final Setting<Integer> FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING = Setting.intSetting(
        "lance.fragment_dispatch.max_concurrent",
        4,
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
    public static final Setting<Boolean> CACHE_ENABLED_SETTING = Setting.boolSetting(
        "lance.cache.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * How many table snapshots {@link org.opensearch.lance.engine.LanceWarmCache}
     * keeps before it closes the least recently used one that no request
     * holds. Each snapshot is one open Lance dataset plus a few kilobytes
     * of metadata per fragment. Static, node scope.
     */
    public static final Setting<Integer> CACHE_MAX_SNAPSHOTS_SETTING = Setting.intSetting(
        "lance.cache.max_snapshots",
        64,
        1,
        Setting.Property.NodeScope
    );

    /**
     * Fraction of {@link #NATIVE_MEMORY_LIMIT_SETTING} reserved for the
     * off-heap column cache. The remainder goes to the Lance Session's
     * index and metadata caches in their 6:1 ratio. Static, node scope.
     */
    public static final Setting<Double> CACHE_COLUMN_SHARE_SETTING = Setting.doubleSetting(
        "lance.cache.column_share",
        0.4,
        0.0,
        0.95,
        Setting.Property.NodeScope
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
    public static final Setting<Integer> FTS_SUBSET_PROBE_LIMIT_SETTING = Setting.intSetting(
        "lance.fts.subset_probe_limit",
        LanceFtsQuery.DEFAULT_SUBSET_PROBE_LIMIT,
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
    public static final Setting<Double> FTS_SUBSET_PROBE_RATIO_SETTING = Setting.doubleSetting(
        "lance.fts.subset_probe_ratio",
        LanceFtsQuery.DEFAULT_SUBSET_PROBE_RATIO,
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
    public static final Setting<Integer> FTS_SUBSET_PROBE_MIN_ROWS_SETTING = Setting.intSetting(
        "lance.fts.subset_probe_min_rows",
        LanceFtsQuery.DEFAULT_SUBSET_PROBE_MIN_ROWS,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether a full-text shape is admitted only when the node's free
     * physical memory can hold the estimated native rebuild of the
     * inverted index document set plus the scan buffers. {@code false}
     * admits every shape, restoring the behaviour that let a large
     * enough scan end the node with a kernel OOM kill. Dynamic. See
     * {@link FtsAdmission}.
     */
    public static final Setting<Boolean> FTS_ADMISSION_ENABLED_SETTING = Setting.boolSetting(
        "lance.fts.admission.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Free physical memory the admission gate keeps out of reach of an
     * unbounded full-text scan: the scan is admitted when its document
     * set estimate fits {@code MemAvailable - headroom}. Dynamic.
     */
    public static final Setting<ByteSizeValue> FTS_ADMISSION_HEADROOM_SETTING = Setting.byteSizeSetting(
        "lance.fts.admission.headroom",
        FtsAdmission.DEFAULT_HEADROOM,
        ByteSizeValue.ZERO,
        new ByteSizeValue(Long.MAX_VALUE),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Whether a bounded full-text top-k page is judged by the same
     * admission estimate as the unbounded shapes. Lance rebuilds the
     * inverted index document set for a bounded page just as it does
     * for an unbounded scan when the index does not fit the index
     * cache shard, so a large enough table can end the node with a
     * kernel OOM kill even at {@code size: 10}. {@code false} restores
     * the pass-through for bounded pages. Dynamic. See
     * {@link FtsAdmission}.
     */
    public static final Setting<Boolean> FTS_ADMISSION_BOUNDED_SHAPES_GATED_SETTING = Setting.boolSetting(
        "lance.fts.admission.bounded_shapes_gated",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Test override of the index cache shard share the full-text
     * admission gate compares its estimate with. Zero (the default)
     * reads the installed Session's sizing. It exists so the
     * integration tests can declare a small fixture table's inverted
     * index as not fitting the cache; do not change it on a real node.
     * Dynamic.
     */
    public static final Setting<ByteSizeValue> TEST_INDEX_CACHE_SHARD_SHARE_SETTING = Setting.byteSizeSetting(
        "lance.test.index_cache_shard_share",
        ByteSizeValue.ZERO,
        ByteSizeValue.ZERO,
        new ByteSizeValue(Long.MAX_VALUE),
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
    public static final Setting<Boolean> AGGREGATION_PUSHDOWN_SETTING = Setting.boolSetting(
        "lance.aggregation.pushdown",
        true,
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
    public static final Setting<Integer> AGGREGATION_PUSHDOWN_PARALLELISM_SETTING = Setting.intSetting(
        "lance.aggregation.pushdown_parallelism",
        Math.max(1, Math.min(32, NativeMemoryLimit.availableCpus() / 2)),
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
    public static final Setting<Integer> AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING = Setting.intSetting(
        "lance.aggregation.pushdown_max_groups",
        1_000_000,
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
    public static final Setting<Integer> AGGREGATION_PERCENTILES_BINS_SETTING = Setting.intSetting(
        "lance.aggregation.percentiles_bins",
        4096,
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
    public static final Setting<Integer> AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING = Setting.intSetting(
        "lance.aggregation.pushdown_topk_slack",
        4,
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
    public static final Setting<Integer> FRAGMENT_PATH_PARALLELISM_SETTING = Setting.intSetting(
        "lance.fragment_path.parallelism",
        Math.max(1, Math.min(32, NativeMemoryLimit.availableCpus() / 2)),
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
    public static final Setting<Integer> FRAGMENT_PATH_SLICES_SETTING = Setting.intSetting(
        "lance.fragment_path.slices",
        Math.max(1, Math.min(32, NativeMemoryLimit.availableCpus() / 2)),
        1,
        32,
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
    public static final Setting<LanceIndexWarmer.Mode> ATTACH_WARM_INDEXES_SETTING = new Setting<>(
        "lance.attach.warm_indexes",
        LanceIndexWarmer.Mode.METADATA.settingValue(),
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
    public static final Setting<Long> MAX_DOCS_PER_READER_SETTING = Setting.longSetting(
        "lance.test.max_docs_per_reader",
        IndexWriter.MAX_DOCS,
        1L,
        IndexWriter.MAX_DOCS,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            TABLE_SETTING,
            PRIMARY_KEY_FIELD_SETTING,
            PRIMARY_KEY_TYPE_SETTING,
            VERSION_SETTING,
            TAG_SETTING,
            MULTI_FIELDS_SETTING,
            OVERRIDES_SETTING,
            UNCOVERED_FRAGMENT_POLICY_SETTING,
            INDEX_PLACEMENT_SETTING,
            NAMESPACE_POLL_CADENCE_SETTING,
            NAMESPACE_RESURFACE_GRACE_SETTING,
            BUILDER_MAX_ROWS_SETTING,
            ALLOWED_TABLE_ROOTS_SETTING,
            STORAGE_OPTIONS_SETTING,
            NATIVE_MEMORY_LIMIT_SETTING,
            NATIVE_MEMORY_CB_ENABLED_SETTING,
            NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING,
            FRAGMENT_DISPATCH_MAX_CONCURRENT_SETTING,
            CACHE_ENABLED_SETTING,
            CACHE_MAX_SNAPSHOTS_SETTING,
            CACHE_COLUMN_SHARE_SETTING,
            FTS_SUBSET_PROBE_LIMIT_SETTING,
            FTS_SUBSET_PROBE_RATIO_SETTING,
            FTS_SUBSET_PROBE_MIN_ROWS_SETTING,
            FTS_ADMISSION_ENABLED_SETTING,
            FTS_ADMISSION_HEADROOM_SETTING,
            FTS_ADMISSION_BOUNDED_SHAPES_GATED_SETTING,
            TEST_INDEX_CACHE_SHARD_SHARE_SETTING,
            AGGREGATION_PUSHDOWN_SETTING,
            AGGREGATION_PUSHDOWN_PARALLELISM_SETTING,
            AGGREGATION_PUSHDOWN_MAX_GROUPS_SETTING,
            AGGREGATION_PERCENTILES_BINS_SETTING,
            AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING,
            FRAGMENT_PATH_PARALLELISM_SETTING,
            FRAGMENT_PATH_SLICES_SETTING,
            ATTACH_WARM_INDEXES_SETTING,
            MAX_DOCS_PER_READER_SETTING
        );
    }

    /**
     * Name of the thread pool the coordinator side of the fragment path
     * runs on: the entry of every {@code _search} against a Lance-backed
     * index (resolving the request, enumerating fragments, sending the
     * per-node requests) and the merge of the per-node responses (hit
     * sort merge, aggregation reduce). The per-node fragment executors
     * stay on the {@code search} pool. Keeping the two apart means a
     * burst of coordinator work cannot fill the {@code search} queue of
     * a data node, and a full {@code search} queue cannot make the
     * transport layer drop a fragment response.
     */
    public static final String LANCE_COORDINATOR_THREAD_POOL = "lance_coordinator";

    /**
     * Default queue length of {@link #LANCE_COORDINATOR_THREAD_POOL}.
     * Bounded, so a coordinator that cannot keep up rejects requests
     * with 429 instead of queueing them without limit; large enough that
     * the bound is only reached under a sustained overload.
     */
    static final int LANCE_COORDINATOR_QUEUE_SIZE = 10_000;

    /**
     * Register {@link #LANCE_COORDINATOR_THREAD_POOL} as a fixed pool of
     * {@code max(1, allocated processors / 2)} threads, and
     * {@link LanceIndexWarmer#THREAD_POOL} as a fixed pool of one thread
     * with a queue of {@link #LANCE_WARM_UP_QUEUE_SIZE}: one table warms
     * at a time so the warm-ups do not compete with each other or with
     * requests for the object store, and the queue holds the tables that
     * appeared while one was warming. The sizes and the queue lengths
     * are node settings under {@code thread_pool.<name>.*} like every
     * other pool; OpenSearch registers them from these builders, so they
     * are not part of {@link #getSettings()}.
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        int size = Math.max(1, OpenSearchExecutors.allocatedProcessors(settings) / 2);
        return List.of(
            new FixedExecutorBuilder(
                settings,
                LANCE_COORDINATOR_THREAD_POOL,
                size,
                LANCE_COORDINATOR_QUEUE_SIZE,
                "thread_pool." + LANCE_COORDINATOR_THREAD_POOL
            ),
            new FixedExecutorBuilder(
                settings,
                LanceIndexWarmer.THREAD_POOL,
                1,
                LANCE_WARM_UP_QUEUE_SIZE,
                "thread_pool." + LanceIndexWarmer.THREAD_POOL
            )
        );
    }

    /** Default queue length of {@link LanceIndexWarmer#THREAD_POOL}: tables waiting for their warm-up. */
    static final int LANCE_WARM_UP_QUEUE_SIZE = 1_000;

    private static void validateUncoveredFragmentPolicy(String value) {
        if (!"wait".equals(value) && !"immediate".equals(value)) {
            throw new IllegalArgumentException("index.lance.uncovered_fragment_policy must be 'wait' or 'immediate', got '" + value + "'");
        }
    }

    private static void validateIndexPlacement(String value) {
        if (!LanceLocalClones.PLACEMENT_IN_TABLE.equals(value) && !LanceLocalClones.PLACEMENT_NODE_LOCAL.equals(value)) {
            throw new IllegalArgumentException("index.lance.index_placement must be 'in_table' or 'node_local', got '" + value + "'");
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
                "index.lance.primary_key_type must be 'long', 'unsigned_long', or 'keyword', got '" + value + "'"
            );
        }
    }

    private static void validateNativeMemoryLimit(String value) {
        // Delegate to the parser so validation and resolution stay in
        // one place. The parser throws OpenSearchParseException /
        // IllegalArgumentException on malformed input, which
        // Setting.simpleString surfaces back to the operator as a
        // 400-style validation error.
        NativeMemoryLimit.parse(value, "lance.native_memory.limit");
    }

    /**
     * Lance-backed indexes get the read-only engine over the node's
     * {@link LanceWarmCache}, so the shard's reader and the fragment path
     * share one snapshot per table version. Index services are created
     * after {@link #createComponents} has run, so the cache is present;
     * a {@code null} here (the factory asked for before the components
     * exist, as a test harness may do) makes the engine open its own
     * dataset per reader instead.
     */
    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (indexSettings.getSettings().get(LanceEngineFactory.TABLE_SETTING) != null) {
            return Optional.of(new LanceEngineFactory(warmCache, () -> maxDocsPerReader));
        }
        return Optional.empty();
    }

    /**
     * No plugin-level index events are wired. Lance 12 keys its
     * index-metadata cache on the manifest ETag
     * (<a href="https://github.com/lancedb/lance/pull/8904">lance#8904</a>),
     * so a table recreated at the same path gets a fresh cache slot on
     * first access without a DELETE-time invalidation from the plugin;
     * {@code LanceAttachIT#testAttachRecreateAtSamePathServesNewContent}
     * covers that. The override is kept as the wiring point for any
     * future per-index hook (warm cache, per-index breaker).
     */
    @Override
    public void onIndexModule(org.opensearch.index.IndexModule indexModule) {}

    private LanceNamespaceService namespaceService;
    private org.opensearch.threadpool.ThreadPool threadPool;
    private AllowedTableRoots allowedTableRoots;
    private LanceDispatchActionFilter dispatchActionFilter;
    private LanceCreateIndexActionFilter createIndexActionFilter;
    private volatile LanceWarmCache warmCache;
    private volatile LanceLocalClones localClones;
    private volatile LanceIndexWarmer indexWarmer;
    /**
     * Current {@link #MAX_DOCS_PER_READER_SETTING}, handed to the engine
     * factories as a supplier so a reader opened after a settings update
     * sees the new bound. The Lucene bound until the components are
     * created.
     */
    private volatile long maxDocsPerReader = IndexWriter.MAX_DOCS;

    /**
     * Cancellable handle for the scheduled task that samples the shared
     * Lance Session and updates the {@code lance_native} circuit breaker's
     * accounting. Held so {@link #close()} can stop the task, and so the
     * settings-change listener can restart it with a new poll interval.
     */
    private volatile Cancellable circuitBreakerPollTask;

    /**
     * Current poll interval used by the scheduled task above. Kept
     * separately from the setting so the listener can compare and
     * avoid restarting the task when an unrelated cluster setting
     * update fires.
     */
    private volatile TimeValue circuitBreakerPollInterval;

    @Override
    public BreakerSettings getCircuitBreaker(Settings settings) {
        // Register a plugin-owned breaker keyed on {@link
        // LanceCircuitBreaker#NAME}. The byte limit mirrors
        // lance.native_memory.limit so operators have one number to
        // configure, and the overhead is 1.0 because the accounting we
        // push in from the polling loop is already actual usage, not
        // an estimate that needs scaling. TRANSIENT durability tells
        // OpenSearch that the condition is expected to resolve without
        // operator intervention (LRU eviction or another polling
        // cycle), which surfaces as a 429 response category rather
        // than a stuck cluster-level error.
        String rawLimit = NATIVE_MEMORY_LIMIT_SETTING.get(settings);
        long limitBytes = NativeMemoryLimit.parse(rawLimit, NATIVE_MEMORY_LIMIT_SETTING.getKey());
        return new BreakerSettings(
            LanceCircuitBreaker.NAME,
            limitBytes,
            1.0,
            CircuitBreaker.Type.MEMORY,
            CircuitBreaker.Durability.TRANSIENT
        );
    }

    @Override
    public void setCircuitBreaker(CircuitBreaker circuitBreaker) {
        // OpenSearch calls this once at node startup with the breaker
        // it built from getCircuitBreaker's BreakerSettings. Hand the
        // reference to the static helper so the FTS / knn scorers can
        // reach it from query paths that only see a QueryShardContext.
        LanceCircuitBreaker.setBreaker(circuitBreaker);
    }

    @Override
    public java.util.Collection<Object> createComponents(
        org.opensearch.transport.client.Client client,
        org.opensearch.cluster.service.ClusterService clusterService,
        org.opensearch.threadpool.ThreadPool threadPool,
        org.opensearch.watcher.ResourceWatcherService resourceWatcherService,
        org.opensearch.script.ScriptService scriptService,
        org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry,
        org.opensearch.env.Environment environment,
        org.opensearch.env.NodeEnvironment nodeEnvironment,
        org.opensearch.core.common.io.stream.NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<org.opensearch.repositories.RepositoriesService> repositoriesServiceSupplier
    ) {
        this.threadPool = threadPool;
        TimeValue cadence = NAMESPACE_POLL_CADENCE_SETTING.get(environment.settings());
        long builderMaxRows = BUILDER_MAX_ROWS_SETTING.get(environment.settings());
        this.allowedTableRoots = new AllowedTableRoots(ALLOWED_TABLE_ROOTS_SETTING.get(environment.settings()));

        // Install the node-scoped Lance Session before any Dataset is
        // opened. LanceEngineFactory and the REST attach / namespace
        // handlers all route their Dataset.open calls through
        // LanceRegistry.openDataset, so once the Session is set here
        // every shard on the node will share its index and metadata
        // caches instead of each shard allocating its own 6 GiB / 1 GiB
        // budget out of native memory. The column cache takes its share
        // of the same limit first; the Session gets the rest. The index
        // cache is not handed its whole budget: Lance shards it and
        // refuses any entry heavier than one shard's share, so the
        // capacity within the budget with the largest share is chosen and
        // the difference stays unused. It is not added to the column
        // cache, whose share is what shrank the index cache in the first
        // place.
        String rawLimit = NATIVE_MEMORY_LIMIT_SETTING.get(environment.settings());
        long totalBytes = NativeMemoryLimit.parse(rawLimit, NATIVE_MEMORY_LIMIT_SETTING.getKey());
        double columnShare = CACHE_COLUMN_SHARE_SETTING.get(environment.settings());
        long columnCacheBytes = NativeMemoryLimit.columnCacheBytes(totalBytes, columnShare);
        long sessionBytes = NativeMemoryLimit.sessionCacheBytes(totalBytes, columnShare);
        long metadataCacheBytes = NativeMemoryLimit.metadataCacheBytes(sessionBytes);
        int cpus = NativeMemoryLimit.availableCpus();
        NativeMemoryLimit.IndexCacheSizing indexCache = NativeMemoryLimit.sizeIndexCache(
            NativeMemoryLimit.indexCacheBudgetBytes(sessionBytes),
            cpus
        );
        LanceRegistry.initSession(indexCache, metadataCacheBytes);
        LOGGER.info(
            "installed shared Lance Session: limit [{}] -> index cache [{}] (shards {}, share {} per shard), metadata cache [{}], "
                + "column cache [{}], unused [{}] (from lance.native_memory.limit [{}], lance.cache.column_share [{}], {} cpus)",
            NativeMemoryLimit.humanReadable(totalBytes),
            NativeMemoryLimit.humanReadable(indexCache.capacityBytes()),
            indexCache.shards(),
            NativeMemoryLimit.humanReadable(indexCache.shardShareBytes()),
            NativeMemoryLimit.humanReadable(metadataCacheBytes),
            NativeMemoryLimit.humanReadable(columnCacheBytes),
            NativeMemoryLimit.humanReadable(indexCache.unusedBytes()),
            rawLimit,
            columnShare,
            cpus
        );

        // Node scoped snapshot and column cache for the fragment path.
        // Created before the transport actions so Guice can inject it
        // into TransportLanceFragmentQueryAction.
        this.warmCache = new LanceWarmCache(
            LanceRegistry.allocator(),
            columnCacheBytes,
            CACHE_MAX_SNAPSHOTS_SETTING.get(environment.settings()),
            CACHE_ENABLED_SETTING.get(environment.settings())
        );
        clusterService.getClusterSettings().addSettingsUpdateConsumer(CACHE_ENABLED_SETTING, warmCache::setEnabled);
        // Node-local shallow clones for indexes attached with
        // index.lance.index_placement = node_local. The service owns the
        // clone directories under the node's first data path, resolves
        // reads onto them (the warm cache and the engine consult the
        // node-wide instance installed here), and deletes them when the
        // index leaves the cluster state; the cluster listener also sweeps
        // orphan directories a previous process left behind.
        this.localClones = new LanceLocalClones(nodeEnvironment.nodeDataPaths()[0], clusterService, threadPool);
        LanceLocalClones.setInstance(localClones);
        clusterService.addListener(localClones);
        // Index warm-up: every Lance-backed index that appears in the
        // cluster state gets its indexes read into the Session cache on
        // this node, on the single threaded lance_warm_up pool.
        this.indexWarmer = new LanceIndexWarmer(
            warmCache,
            threadPool.executor(LanceIndexWarmer.THREAD_POOL),
            ATTACH_WARM_INDEXES_SETTING.get(environment.settings())
        );
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ATTACH_WARM_INDEXES_SETTING, indexWarmer::setMode);
        clusterService.addListener(indexWarmer);
        this.maxDocsPerReader = MAX_DOCS_PER_READER_SETTING.get(environment.settings());
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_DOCS_PER_READER_SETTING, value -> maxDocsPerReader = value);
        // Read side of GET /_lance/stats. The session size is read through
        // the registry here because the stats package cannot see the
        // registry's package-private session accessor.
        LanceStatsCollector statsCollector = new LanceStatsCollector(warmCache, () -> {
            Session session = LanceRegistry.currentSession();
            return session == null || session.isClosed() ? 0L : session.sizeBytes();
        }, LanceRegistry::indexCacheSizing, indexWarmer, localClones::cloneStats);

        // Prime the circuit-breaker helper with the current cluster
        // settings and start the polling loop that keeps its accounting
        // aligned with Session.sizeBytes() plus the column cache. The
        // listener below picks up dynamic changes to both the enabled
        // flag and the poll cadence; the breaker itself has already been
        // handed to LanceCircuitBreaker by setCircuitBreaker earlier in
        // the node lifecycle.
        LanceCircuitBreaker.setEnabled(NATIVE_MEMORY_CB_ENABLED_SETTING.get(environment.settings()));
        this.circuitBreakerPollInterval = NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING.get(environment.settings());
        this.circuitBreakerPollTask = scheduleCircuitBreakerPoll(threadPool, circuitBreakerPollInterval);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(NATIVE_MEMORY_CB_ENABLED_SETTING, LanceCircuitBreaker::setEnabled);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(NATIVE_MEMORY_CB_POLL_INTERVAL_SETTING, this::updatePollInterval);

        // The FTS probe parameters live in static holders read by every
        // scan, so the consumers only have to store the new values.
        LanceFtsQuery.setSubsetProbeLimit(FTS_SUBSET_PROBE_LIMIT_SETTING.get(environment.settings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FTS_SUBSET_PROBE_LIMIT_SETTING, LanceFtsQuery::setSubsetProbeLimit);
        LanceFtsQuery.setSubsetProbeRatio(FTS_SUBSET_PROBE_RATIO_SETTING.get(environment.settings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FTS_SUBSET_PROBE_RATIO_SETTING, LanceFtsQuery::setSubsetProbeRatio);
        LanceFtsQuery.setSubsetProbeMinRows(FTS_SUBSET_PROBE_MIN_ROWS_SETTING.get(environment.settings()));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(FTS_SUBSET_PROBE_MIN_ROWS_SETTING, LanceFtsQuery::setSubsetProbeMinRows);

        // The full-text admission gate reads its parameters from the
        // same kind of static holder.
        FtsAdmission.setEnabled(FTS_ADMISSION_ENABLED_SETTING.get(environment.settings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FTS_ADMISSION_ENABLED_SETTING, FtsAdmission::setEnabled);
        FtsAdmission.setHeadroom(FTS_ADMISSION_HEADROOM_SETTING.get(environment.settings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FTS_ADMISSION_HEADROOM_SETTING, FtsAdmission::setHeadroom);
        FtsAdmission.setBoundedShapesGated(FTS_ADMISSION_BOUNDED_SHAPES_GATED_SETTING.get(environment.settings()));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(FTS_ADMISSION_BOUNDED_SHAPES_GATED_SETTING, FtsAdmission::setBoundedShapesGated);
        FtsAdmission.setIndexCacheShardShareOverride(TEST_INDEX_CACHE_SHARD_SHARE_SETTING.get(environment.settings()));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(TEST_INDEX_CACHE_SHARD_SHARE_SETTING, FtsAdmission::setIndexCacheShardShareOverride);

        // The percentiles bin count and the terms top-k slack are read
        // by the aggregation pushdown when it plans a request, from the
        // same kind of static holder.
        LanceAggregateResults.setPercentilesBins(AGGREGATION_PERCENTILES_BINS_SETTING.get(environment.settings()));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AGGREGATION_PERCENTILES_BINS_SETTING, LanceAggregateResults::setPercentilesBins);
        LanceAggregateResults.setTopkSlack(AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING.get(environment.settings()));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(AGGREGATION_PUSHDOWN_TOPK_SLACK_SETTING, LanceAggregateResults::setTopkSlack);

        // Register the shard-free dispatch ActionFilter. It
        // intercepts every _search request against Lance-backed
        // indices, forks onto the lance_coordinator pool and delegates
        // to the plugin's own coordinator; the shard fan-out via
        // ReadOnlyEngine only runs when the fragment executor cannot
        // answer a shape yet (highlighter, suggest, collapse, ...).
        this.dispatchActionFilter = new LanceDispatchActionFilter(clusterService, indexNameExpressionResolver, client, threadPool);
        this.createIndexActionFilter = new LanceCreateIndexActionFilter(threadPool);

        namespaceService = new LanceNamespaceService(
            client,
            clusterService,
            threadPool,
            cadence,
            builderMaxRows,
            NAMESPACE_RESURFACE_GRACE_SETTING.get(environment.settings()),
            warmCache,
            allowedTableRoots
        );
        // Register a reactive consumer so an operator can adjust the grace
        // period at runtime without a rolling restart.
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(NAMESPACE_RESURFACE_GRACE_SETTING, namespaceService::setResurfaceGrace);
        // The components are injected into the plugin's transport
        // actions (attach, build_indexes, namespace list / update,
        // fragment query).
        return List.of(namespaceService, allowedTableRoots, warmCache, statsCollector, localClones);
    }

    /**
     * Schedule the periodic sampler that reads the current
     * {@code Session.sizeBytes()} and the column cache's allocated bytes
     * and pushes their sum into the circuit breaker via
     * {@link LanceCircuitBreaker#updateUsage(long, long)}. Runs on the
     * generic thread pool so it does not steal capacity from the search
     * or write executors.
     */
    private Cancellable scheduleCircuitBreakerPoll(ThreadPool pool, TimeValue interval) {
        Runnable sampler = () -> {
            try {
                org.lance.Session session = LanceRegistry.currentSession();
                if (session == null || session.isClosed()) {
                    return;
                }
                long sessionBytes = session.sizeBytes();
                LanceWarmCache cache = warmCache;
                long columnBytes = cache == null ? 0L : cache.columnCacheBytes();
                LanceCircuitBreaker.updateUsage(sessionBytes, columnBytes);
            } catch (Throwable t) {
                // Never let a poll iteration throw out of the
                // scheduler; a failed reading just means the breaker's
                // accounting stays as it was for one more cycle.
                LOGGER.warn("lance_native circuit breaker poll iteration failed", t);
            }
        };
        return pool.scheduleWithFixedDelay(sampler, interval, ThreadPool.Names.GENERIC);
    }

    private synchronized void updatePollInterval(TimeValue newInterval) {
        if (newInterval.equals(circuitBreakerPollInterval)) {
            return;
        }
        if (circuitBreakerPollTask != null) {
            circuitBreakerPollTask.cancel();
        }
        circuitBreakerPollInterval = newInterval;
        circuitBreakerPollTask = scheduleCircuitBreakerPoll(threadPool, newInterval);
        LOGGER.info("lance_native circuit breaker poll interval updated to [{}]", newInterval);
    }

    @Override
    public void close() throws IOException {
        // Cancel the polling loop before releasing the Session so the
        // sampler can never observe a half-closed Session on its way
        // out.
        Cancellable task = circuitBreakerPollTask;
        if (task != null) {
            task.cancel();
            circuitBreakerPollTask = null;
        }
        // Stop the warm-ups before their snapshots close under them.
        LanceIndexWarmer warmer = indexWarmer;
        if (warmer != null) {
            warmer.close();
            indexWarmer = null;
        }
        // Drop the node-wide clone resolution point so a test-framework
        // restart within the same JVM does not resolve reads onto a
        // previous node's clone directories.
        if (localClones != null) {
            LanceLocalClones.setInstance(null);
            localClones = null;
        }
        // Close every cached snapshot (their datasets) and the column
        // cache allocator before the Session goes away.
        LanceWarmCache cache = warmCache;
        if (cache != null) {
            cache.close();
            warmCache = null;
        }
        // Release the shared native Session so a test-framework restart
        // within the same JVM doesn't accumulate stale Session handles.
        // Existing Dataset handles keep their own Arc reference to the
        // underlying native session, so this call is safe even if some
        // shards are still open at the moment of shutdown.
        LanceRegistry.closeSession();
        super.close();
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        // The filter is created lazily in createComponents, so return
        // an empty list until then. In practice OpenSearch calls
        // createComponents before it consults getActionFilters, so the
        // filter is always present when the search machinery starts
        // routing through it; the null guard exists purely for the
        // test framework's out-of-order invocations.
        LanceDispatchActionFilter dispatch = dispatchActionFilter;
        LanceCreateIndexActionFilter guard = createIndexActionFilter;
        if (dispatch == null && guard == null) {
            return List.of();
        }
        if (guard == null) {
            return List.of(dispatch);
        }
        if (dispatch == null) {
            return List.of(guard);
        }
        return List.of(dispatch, guard);
    }

    @Override
    public
        List<
            org.opensearch.plugins.ActionPlugin.ActionHandler<
                ? extends org.opensearch.action.ActionRequest,
                ? extends org.opensearch.core.action.ActionResponse>>
        getActions() {
        return List.of(
            new org.opensearch.plugins.ActionPlugin.ActionHandler<>(
                org.opensearch.lance.dispatch.LanceFragmentQueryAction.INSTANCE,
                org.opensearch.lance.dispatch.TransportLanceFragmentQueryAction.class
            ),
            new org.opensearch.plugins.ActionPlugin.ActionHandler<>(
                org.opensearch.lance.dispatch.LanceCoordinatorAction.INSTANCE,
                org.opensearch.lance.dispatch.TransportLanceCoordinatorAction.class
            ),
            new org.opensearch.plugins.ActionPlugin.ActionHandler<>(
                org.opensearch.lance.namespace.LanceNamespaceUpdateAction.INSTANCE,
                org.opensearch.lance.namespace.TransportLanceNamespaceUpdateAction.class
            ),
            new ActionHandler<>(LanceNamespaceListAction.INSTANCE, TransportLanceNamespaceListAction.class),
            new ActionHandler<>(LanceAttachAction.INSTANCE, TransportLanceAttachAction.class),
            new ActionHandler<>(LanceBuildIndexesAction.INSTANCE, TransportLanceBuildIndexesAction.class),
            new ActionHandler<>(LanceBuildIndexesNodesAction.INSTANCE, TransportLanceBuildIndexesNodesAction.class),
            new ActionHandler<>(LanceRefsAction.INSTANCE, TransportLanceRefsAction.class),
            new ActionHandler<>(LanceStatsAction.INSTANCE, TransportLanceStatsAction.class),
            new ActionHandler<>(LanceExplainAction.INSTANCE, TransportLanceExplainAction.class)
        );
    }

    @Override
    public List<org.opensearch.core.common.io.stream.NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new org.opensearch.core.common.io.stream.NamedWriteableRegistry.Entry(
                org.opensearch.cluster.metadata.Metadata.Custom.class,
                org.opensearch.lance.namespace.LanceNamespaceMetadata.TYPE,
                org.opensearch.lance.namespace.LanceNamespaceMetadata::new
            ),
            new org.opensearch.core.common.io.stream.NamedWriteableRegistry.Entry(
                org.opensearch.cluster.NamedDiff.class,
                org.opensearch.lance.namespace.LanceNamespaceMetadata.TYPE,
                org.opensearch.lance.namespace.LanceNamespaceMetadata::readDiffFrom
            )
        );
    }

    @Override
    public List<org.opensearch.core.xcontent.NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new org.opensearch.core.xcontent.NamedXContentRegistry.Entry(
                org.opensearch.cluster.metadata.Metadata.Custom.class,
                new org.opensearch.core.ParseField(org.opensearch.lance.namespace.LanceNamespaceMetadata.TYPE),
                org.opensearch.lance.namespace.LanceNamespaceMetadata::fromXContent
            )
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(
            new RestAttachAction(),
            new RestNamespaceAction(),
            new RestBuildIndexesAction(),
            new RestRefsAction(),
            new RestLanceStatsAction(),
            new RestLanceExplainAction()
        );
    }
}
