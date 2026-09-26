/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.cache.RemovalReason;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indices.IndicesService;
import org.opensearch.lance.plan.explain.ReaderWrapperProbe;
import org.opensearch.lance.stats.LanceNodeStats;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * The coordinator's result cache: one per node, holding the reduced
 * answer of every {@code size: 0} request against a single Lance backed
 * index, keyed on the table version the answer was computed from. The
 * fragment path never reaches OpenSearch's shard request cache
 * ({@code indices.requests.cache.*}), so a dashboard that refreshes the
 * same aggregation body every few seconds fanned out and scanned the
 * table every time; this cache answers the repeat from the coordinator.
 *
 * <p>The key is {@code (index uuid, manifest version, data node ids,
 * canonical request)}. A Lance table names its version, so an answer is
 * valid for exactly as long as the table stays at the version it was
 * computed from: an append or a compaction advances the version and the
 * next request forms a different key, with no invalidation to run and
 * none of the refresh interval consistency questions the shard cache
 * has. A pinned or tag following index caches under the version the
 * coordinator observed, the same way. The data node ids are part of the
 * key because a {@code terms} aggregation with a {@code shard_size}
 * below the number of distinct values reports what the executors kept,
 * and the executors are assigned by a round robin over the data node
 * list. The canonical request is the search body as
 * {@link SearchSourceBuilder#toXContent} writes it, with every object's
 * keys sorted and the whitespace removed, so two bodies that spell the
 * same request in another key order share an entry. Request level
 * options that do not shape the answer are left out of the key:
 * {@code preference}, {@code request_cache} and
 * {@code allow_partial_search_results} (they live on the
 * {@link SearchRequest}, not in the body). Everything the body carries
 * stays in, including {@code timeout}, {@code terminate_after},
 * {@code profile}, {@code explain} and {@code track_total_hits}, which do
 * shape it.
 *
 * <p>A request is cached when the cache is enabled, the request does
 * not carry {@code request_cache=false}, its target is one Lance backed
 * index, its body asks for no hits ({@code size: 0} and {@code from: 0})
 * and the index has no reader wrapper (the security plugin's document
 * and field level security, whose answers differ per user). An answer
 * that timed out is not stored (it is partial), nor is one whose
 * aggregations serialise above {@code lance.request_cache.max_entry_size},
 * nor one the store threw on. Every other request counts as skipped.
 *
 * <p>The store is OpenSearch's {@link Cache}, weighed by the serialised
 * size of the reduced aggregations plus the key, evicted least recently
 * used at {@code lance.request_cache.size}. An entry also leaves the
 * cache when {@code lance.request_cache.expire} has passed since it was
 * stored (checked on the read, so the setting is dynamic), when its
 * index is deleted (the cache listens to cluster state), when
 * {@code POST /<index>/_cache/clear} names the index, and when the cache
 * is disabled.
 */
public final class LanceRequestCache implements ClusterStateListener {

    private static final Logger LOGGER = LogManager.getLogger(LanceRequestCache.class);

    /** Why a request was not served from or stored in the cache, as {@code GET /<index>/_lance/explain} names it. */
    public enum Skip {
        /** {@code lance.request_cache.enabled} is false on this node. */
        DISABLED("disabled"),
        /** The request carried {@code request_cache=false}. */
        OPTED_OUT("request_cache=false"),
        /** The target resolves to more than one index. */
        MIXED_TARGET("mixed target"),
        /** The body asks for hits. */
        SIZE("size > 0"),
        /** The body skips hits, which only a page does. */
        FROM("from > 0"),
        /** A reader wrapper (document or field level security) is installed on the index. */
        DLS("dls"),
        /** The answer was partial (a node did not answer in time). */
        TIMED_OUT("timed_out"),
        /** The answer's aggregations serialise above {@code lance.request_cache.max_entry_size}. */
        TOO_LARGE("entry above max_entry_size"),
        /** Storing the answer threw (its aggregations could not be measured, or the store rejected the entry). */
        STORE_FAILED("store_failed");

        private final String reason;

        Skip(String reason) {
            this.reason = reason;
        }

        /** The reason as the explain endpoint and the log spell it. */
        public String reason() {
            return reason;
        }
    }

    /**
     * What one request found in the cache and what it does with its
     * answer: {@link #find} looks the entry up once the coordinator knows
     * the table version, and {@link #complete} either renders the entry
     * it found or stores the answer the fan out produced. One instance
     * per request; the coordinator uses it from the thread that plans
     * the target and then from the thread that merges, in that order.
     */
    public final class Lookup {
        private final String indexUuid;
        private final String nodes;
        private final String request;
        private final long startMillis;
        private volatile Key key;
        private volatile Entry hit;

        private Lookup(String indexUuid, String nodes, String request, long startMillis) {
            this.indexUuid = indexUuid;
            this.nodes = nodes;
            this.request = request;
            this.startMillis = startMillis;
        }

        /**
         * The entry of this request at {@code version}, or null when the
         * cache holds none (a miss, counted); the key is kept for
         * {@link #complete}.
         */
        public Entry find(long version) {
            Key formed = new Key(indexUuid, version, nodes, request);
            this.key = formed;
            Entry found = get(formed);
            this.hit = found;
            return found;
        }

        /** Whether {@link #find} returned an entry. */
        public boolean isHit() {
            return hit != null;
        }

        /**
         * The response of this request: the entry {@link #find} returned,
         * rendered with the time this request took, or the response
         * {@code computed} builds, stored under the key {@link #find}
         * formed when the answer qualifies. Storing is a side effect of
         * a request that has already succeeded, so a store that throws
         * is counted as a skip and logged, and the computed response is
         * returned all the same.
         */
        public SearchResponse complete(LongFunction<SearchResponse> computed) {
            Entry found = hit;
            if (found != null) {
                return found.toResponse(System.currentTimeMillis() - startMillis);
            }
            SearchResponse response = computed.apply(startMillis);
            Key formed = key;
            if (formed != null) {
                try {
                    store(formed, response);
                } catch (RuntimeException e) {
                    skipped.increment();
                    LOGGER.debug(
                        "lance.request_cache: answer of index [{}] not stored ({}): {}",
                        indexUuid,
                        Skip.STORE_FAILED.reason(),
                        e.toString()
                    );
                }
            }
            return response;
        }
    }

    /**
     * The cache key. {@code nodes} is the sorted, comma separated ids of
     * the data nodes the request fans out over; {@code request} the
     * canonical body.
     */
    public record Key(String indexUuid, long version, String nodes, String request) {
        public Key {
            Objects.requireNonNull(indexUuid, "indexUuid");
            Objects.requireNonNull(nodes, "nodes");
            Objects.requireNonNull(request, "request");
        }

        /** The heap this key roughly costs: the strings, two bytes a character, plus the record. */
        long weight() {
            return 64L + 2L * (indexUuid.length() + nodes.length() + request.length());
        }
    }

    /**
     * A cached answer: the reduced aggregations, the total hits, the max
     * score (NaN for a size 0 request) and the {@code terminated_early}
     * flag of the response that was stored, with its serialised size and
     * the time it was stored at. {@link #toResponse} renders it with a
     * fresh {@code took}, the same envelope the merge builds otherwise
     * (one logical shard, no failures).
     */
    public static final class Entry {
        private final InternalAggregations aggregations;
        private final TotalHits totalHits;
        private final float maxScore;
        private final Boolean terminatedEarly;
        private final long bytes;
        private final long storedAtMillis;

        Entry(
            InternalAggregations aggregations,
            TotalHits totalHits,
            float maxScore,
            Boolean terminatedEarly,
            long bytes,
            long storedAtMillis
        ) {
            this.aggregations = aggregations;
            this.totalHits = totalHits;
            this.maxScore = maxScore;
            this.terminatedEarly = terminatedEarly;
            this.bytes = bytes;
            this.storedAtMillis = storedAtMillis;
        }

        /** The serialised size of the aggregations, what the entry weighs in the cache before its key. */
        public long bytes() {
            return bytes;
        }

        public InternalAggregations aggregations() {
            return aggregations;
        }

        public TotalHits totalHits() {
            return totalHits;
        }

        /** The response this entry stands for, taking {@code tookMillis}. */
        public SearchResponse toResponse(long tookMillis) {
            SearchHits hits = new SearchHits(new SearchHit[0], totalHits, maxScore);
            SearchResponseSections sections = new SearchResponseSections(hits, aggregations, null, false, terminatedEarly, null, 1);
            return new SearchResponse(sections, null, 1, 1, 0, tookMillis, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
        }
    }

    private final Cache<Key, Entry> cache;
    private final long limitBytes;
    private final long maxEntryBytes;
    private volatile boolean enabled;
    private volatile long expireMillis;
    private final LongAdder hits = new LongAdder();
    private final LongAdder misses = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final LongAdder invalidations = new LongAdder();
    private final LongAdder skipped = new LongAdder();
    /**
     * Whether a reader wrapper is installed on an index, by index uuid.
     * Read once per index on this node (the answer needs a temporary
     * index service on a node that holds no shard of the index) and
     * dropped when the index is deleted.
     */
    private final Map<String, Boolean> readerWrapperByIndex = new ConcurrentHashMap<>();

    /**
     * @param limitBytes the most the entries may weigh together
     * @param maxEntryBytes the largest serialised aggregations block stored
     * @param enabled the initial {@code lance.request_cache.enabled}
     * @param expire the initial {@code lance.request_cache.expire}; zero for none
     */
    public LanceRequestCache(long limitBytes, long maxEntryBytes, boolean enabled, TimeValue expire) {
        this.limitBytes = limitBytes;
        this.maxEntryBytes = maxEntryBytes;
        this.enabled = enabled;
        this.expireMillis = expire == null ? 0L : expire.millis();
        this.cache = CacheBuilder.<Key, Entry>builder()
            .setMaximumWeight(limitBytes)
            .weigher((key, entry) -> key.weight() + entry.bytes())
            .removalListener(notification -> {
                if (notification.getRemovalReason() == RemovalReason.EVICTED) {
                    evictions.increment();
                }
            })
            .build();
    }

    /** Current {@code lance.request_cache.enabled}. */
    public boolean isEnabled() {
        return enabled;
    }

    /** Applies {@code lance.request_cache.enabled}; turning the cache off drops every entry. */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (!enabled) {
            cache.invalidateAll();
        }
    }

    /** Applies {@code lance.request_cache.expire}; zero keeps entries until their version moves on. */
    public void setExpire(TimeValue expire) {
        this.expireMillis = expire == null ? 0L : expire.millis();
    }

    /**
     * The reason {@code source} alone cannot be cached, whatever the
     * request around it: a body asking for hits. Null when its shape
     * qualifies. Shared with the explain endpoint, which has a body and
     * no request.
     */
    public static Skip shapeSkip(SearchSourceBuilder source) {
        if (source == null || source.size() != 0) {
            return Skip.SIZE;
        }
        if (source.from() > 0) {
            return Skip.FROM;
        }
        return null;
    }

    /**
     * The reason the explain endpoint reports for a body against one
     * index, or null when a search with that body would be cached:
     * {@link Skip#DISABLED}, the shape, or {@link Skip#DLS} when
     * {@code readerWrapper} says the index has one.
     */
    public Skip explainSkip(SearchSourceBuilder source, boolean readerWrapper) {
        if (!enabled) {
            return Skip.DISABLED;
        }
        Skip shape = shapeSkip(source);
        if (shape != null) {
            return shape;
        }
        return readerWrapper ? Skip.DLS : null;
    }

    /**
     * Opens the lookup of one coordinator request, or returns null (and
     * counts the request as skipped when the cache is enabled) when the
     * request is not cached: the cache is off, the request opted out, the
     * target is several indexes, the body asks for hits, or the index has
     * a reader wrapper. The version is not known yet; {@link Lookup#find}
     * takes it once the coordinator has opened the table.
     *
     * @param request the search request as the coordinator received it
     * @param concrete the indexes the target resolved to
     * @param metadata the metadata of the one index when there is one
     * @param dataNodes the data nodes the request fans out over, in the order the coordinator assigns fragments
     * @param indicesService this node's index services, for the reader wrapper check
     * @param startMillis when the coordinator started on the request
     */
    public Lookup begin(
        SearchRequest request,
        Index[] concrete,
        IndexMetadata metadata,
        List<DiscoveryNode> dataNodes,
        IndicesService indicesService,
        long startMillis
    ) {
        if (!enabled) {
            return null;
        }
        Skip skip = null;
        if (Boolean.FALSE.equals(request.requestCache())) {
            skip = Skip.OPTED_OUT;
        } else if (concrete.length != 1 || metadata == null) {
            skip = Skip.MIXED_TARGET;
        } else {
            skip = shapeSkip(request.source());
        }
        if (skip == null && readerWrapperInstalled(metadata, indicesService)) {
            skip = Skip.DLS;
        }
        if (skip != null) {
            skipped.increment();
            LOGGER.debug("lance.request_cache: request over {} not cached ({})", request.indices(), skip.reason());
            return null;
        }
        String canonical;
        try {
            canonical = canonicalRequest(request.source());
        } catch (IOException | RuntimeException e) {
            // A body the builder cannot write back is not cached; the
            // request itself runs as before.
            skipped.increment();
            LOGGER.debug("lance.request_cache: request over {} not cached, body not canonical: {}", request.indices(), e.toString());
            return null;
        }
        return new Lookup(metadata.getIndexUUID(), nodeIds(dataNodes), canonical, startMillis);
    }

    private boolean readerWrapperInstalled(IndexMetadata metadata, IndicesService indicesService) {
        if (indicesService == null) {
            // Without the node's index services the probe cannot run;
            // like a probe that fails, the answer is the side that never
            // caches a wrapped answer.
            return true;
        }
        Boolean known = readerWrapperByIndex.get(metadata.getIndexUUID());
        if (known != null) {
            return known;
        }
        boolean installed;
        try {
            installed = ReaderWrapperProbe.installedOn(indicesService, metadata);
        } catch (IOException | RuntimeException e) {
            // When the probe cannot tell, the answer is not cached; the
            // next request asks again.
            LOGGER.debug("lance.request_cache: reader wrapper probe failed for [{}]: {}", metadata.getIndex().getName(), e.toString());
            return true;
        }
        readerWrapperByIndex.put(metadata.getIndexUUID(), installed);
        return installed;
    }

    /** The data node ids in id order, comma separated: the part of the key that fixes the fragment assignment. */
    static String nodeIds(Collection<DiscoveryNode> dataNodes) {
        List<String> ids = new ArrayList<>(dataNodes.size());
        for (DiscoveryNode node : dataNodes) {
            ids.add(node.getId());
        }
        Collections.sort(ids);
        return String.join(",", ids);
    }

    /**
     * The canonical form of a search body: its JSON with every object's
     * keys in sorted order and no whitespace. Array order is kept (the
     * order of sort clauses and of {@code should} clauses is part of the
     * request). Nothing is removed: the request level options the key
     * leaves out ({@code preference}, {@code request_cache},
     * {@code allow_partial_search_results}) are not in the body.
     */
    static String canonicalRequest(SearchSourceBuilder source) throws IOException {
        try (XContentBuilder written = XContentBuilder.builder(XContentType.JSON.xContent())) {
            source.toXContent(written, ToXContent.EMPTY_PARAMS);
            Map<String, Object> parsed = XContentHelper.convertToMap(BytesReference.bytes(written), true, XContentType.JSON).v2();
            try (XContentBuilder canonical = XContentBuilder.builder(XContentType.JSON.xContent())) {
                canonical.map(sortedKeys(parsed));
                return canonical.toString();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> sortedKeys(Map<String, Object> map) {
        Map<String, Object> sorted = new TreeMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            sorted.put(entry.getKey(), sortedValue(entry.getValue()));
        }
        return sorted;
    }

    @SuppressWarnings("unchecked")
    private static Object sortedValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            return sortedKeys((Map<String, Object>) map);
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object element : list) {
                out.add(sortedValue(element));
            }
            return out;
        }
        return value;
    }

    /** The entry under {@code key}, or null; an expired entry is dropped and counts as a miss. */
    Entry get(Key key) {
        Entry entry = cache.get(key);
        if (entry != null && expireMillis > 0L && System.currentTimeMillis() - entry.storedAtMillis > expireMillis) {
            cache.invalidate(key, entry);
            evictions.increment();
            entry = null;
        }
        if (entry == null) {
            misses.increment();
        } else {
            hits.increment();
        }
        return entry;
    }

    /**
     * Stores {@code response} under {@code key} when it qualifies: not
     * timed out, and its aggregations within the entry bound. Returns
     * the skip reason when it did not, null when it did.
     */
    Skip store(Key key, SearchResponse response) {
        if (!enabled) {
            return Skip.DISABLED;
        }
        if (response.isTimedOut()) {
            skipped.increment();
            return Skip.TIMED_OUT;
        }
        InternalAggregations aggregations = (InternalAggregations) response.getAggregations();
        long bytes = aggregationBytes(aggregations);
        if (bytes > maxEntryBytes) {
            skipped.increment();
            LOGGER.debug("lance.request_cache: answer of {} bytes above the entry bound of {} bytes, not cached", bytes, maxEntryBytes);
            return Skip.TOO_LARGE;
        }
        SearchHits hits = response.getHits();
        Entry entry = new Entry(
            aggregations,
            hits.getTotalHits(),
            hits.getMaxScore(),
            response.isTerminatedEarly(),
            bytes,
            System.currentTimeMillis()
        );
        cache.put(key, entry);
        return null;
    }

    /** The serialised size of {@code aggregations}, zero for none. */
    static long aggregationBytes(InternalAggregations aggregations) {
        if (aggregations == null) {
            return 0L;
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            aggregations.writeTo(out);
            return out.size();
        } catch (IOException e) {
            throw new IllegalStateException("cannot measure the reduced aggregations", e);
        }
    }

    /**
     * Drops every entry of the indexes with {@code indexUuids} and
     * returns how many were dropped; each counts as an invalidation.
     */
    public int invalidateIndexes(Collection<String> indexUuids) {
        if (indexUuids.isEmpty()) {
            return 0;
        }
        Set<String> uuids = Set.copyOf(indexUuids);
        // The keys are collected before anything is invalidated: the
        // cache's key iteration is undefined under a concurrent mutation.
        List<Key> matching = new ArrayList<>();
        for (Key key : cache.keys()) {
            if (uuids.contains(key.indexUuid())) {
                matching.add(key);
            }
        }
        int dropped = 0;
        for (Key key : matching) {
            cache.invalidate(key);
            dropped++;
        }
        for (String uuid : uuids) {
            readerWrapperByIndex.remove(uuid);
        }
        invalidations.add(dropped);
        return dropped;
    }

    /** Drops the entries of every index that left the cluster state. */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        List<Index> deleted = event.indicesDeleted();
        if (deleted.isEmpty()) {
            return;
        }
        List<String> uuids = new ArrayList<>(deleted.size());
        for (Index index : deleted) {
            uuids.add(index.getUUID());
        }
        int dropped = invalidateIndexes(uuids);
        if (dropped > 0) {
            LOGGER.debug("lance.request_cache: dropped {} entries of deleted indexes {}", dropped, deleted);
        }
    }

    /** Entries held right now. */
    public int count() {
        return cache.count();
    }

    /** Bytes the entries weigh right now. */
    public long weight() {
        return cache.weight();
    }

    /**
     * The cache's figures for {@code GET /_lance/stats}: the setting in
     * force, the bytes held against the limit, the entries, and the
     * cumulative hits, misses, evictions (least recently used and
     * expired), invalidations (index deletions and clears) and skipped
     * requests since the node started.
     */
    public LanceNodeStats.RequestCacheStats stats() {
        return new LanceNodeStats.RequestCacheStats(
            enabled,
            cache.weight(),
            limitBytes,
            cache.count(),
            hits.sum(),
            misses.sum(),
            evictions.sum(),
            invalidations.sum(),
            skipped.sum()
        );
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "LanceRequestCache[entries=%d, bytes=%d, limit=%d]", cache.count(), cache.weight(), limitBytes);
    }
}
