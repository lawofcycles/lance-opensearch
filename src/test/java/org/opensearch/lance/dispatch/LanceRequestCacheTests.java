/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The coordinator result cache on its own: the canonical key, which
 * requests qualify, what is stored and weighed, the expiry, and the
 * invalidation of an index's entries.
 */
public class LanceRequestCacheTests extends OpenSearchTestCase {

    private static final NamedXContentRegistry REGISTRY = new NamedXContentRegistry(
        new SearchModule(Settings.EMPTY, List.of()).getNamedXContents()
    );

    private static SearchSourceBuilder body(String json) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(REGISTRY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json)
        ) {
            return SearchSourceBuilder.fromXContent(parser);
        }
    }

    private static IndexMetadata indexMetadata(String name, String uuid) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();
    }

    private static DiscoveryNode node(String id) {
        return new DiscoveryNode(
            id,
            id,
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
    }

    private static SearchResponse response(double sum, long total, boolean timedOut) {
        InternalAggregations aggregations = InternalAggregations.from(List.of(new InternalSum("s", sum, DocValueFormat.RAW, Map.of())));
        SearchHits hits = new SearchHits(new SearchHit[0], new TotalHits(total, TotalHits.Relation.EQUAL_TO), Float.NaN);
        SearchResponseSections sections = new SearchResponseSections(hits, aggregations, null, timedOut, null, null, 1);
        return new SearchResponse(sections, null, 1, 1, 0, 5L, ShardSearchFailure.EMPTY_ARRAY, SearchResponse.Clusters.EMPTY);
    }

    private static LanceRequestCache cache() {
        return new LanceRequestCache(1L << 20, 1L << 16, true, TimeValue.ZERO);
    }

    private static LanceRequestCache.Lookup begin(
        LanceRequestCache cache,
        SearchRequest request,
        IndexMetadata metadata,
        DiscoveryNode... nodes
    ) {
        Index[] concrete = metadata == null ? new Index[0] : new Index[] { metadata.getIndex() };
        return cache.begin(request, concrete, metadata, List.of(nodes), null, 0L);
    }

    public void testCanonicalRequestSortsKeysAndKeepsArrayOrder() throws IOException {
        String a = LanceRequestCache.canonicalRequest(
            body("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}},\"query\":{\"term\":{\"category\":\"a\"}}}")
        );
        String b = LanceRequestCache.canonicalRequest(
            body("{\"query\":{\"term\":{\"category\":\"a\"}}, \"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}, \"size\":0}")
        );
        assertEquals("the same body in another key order forms the same key", a, b);
        assertFalse("no whitespace: " + a, a.contains(" "));
        assertTrue(a, a.startsWith("{\"aggregations\":"));

        String sortAsc = LanceRequestCache.canonicalRequest(body("{\"size\":0,\"sort\":[{\"a\":\"asc\"},{\"b\":\"asc\"}]}"));
        String sortSwapped = LanceRequestCache.canonicalRequest(body("{\"size\":0,\"sort\":[{\"b\":\"asc\"},{\"a\":\"asc\"}]}"));
        assertNotEquals("array order is part of the request", sortAsc, sortSwapped);
    }

    public void testCanonicalRequestKeepsTheBodyOptionsThatShapeTheAnswer() throws IOException {
        String plain = LanceRequestCache.canonicalRequest(body("{\"size\":0,\"query\":{\"match_all\":{}}}"));
        String timeout = LanceRequestCache.canonicalRequest(body("{\"size\":0,\"query\":{\"match_all\":{}},\"timeout\":\"1s\"}"));
        String terminate = LanceRequestCache.canonicalRequest(body("{\"size\":0,\"query\":{\"match_all\":{}},\"terminate_after\":5}"));
        String tracked = LanceRequestCache.canonicalRequest(body("{\"size\":0,\"query\":{\"match_all\":{}},\"track_total_hits\":true}"));
        assertNotEquals(plain, timeout);
        assertNotEquals(plain, terminate);
        assertNotEquals(plain, tracked);
    }

    public void testRequestLevelOptionsAreNotPartOfTheKey() throws IOException {
        LanceRequestCache cache = cache();
        IndexMetadata metadata = indexMetadata("demo", "uuid-1");
        SearchSourceBuilder source = body("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}");
        SearchRequest plain = new SearchRequest("demo").source(source);
        SearchRequest withPreference = new SearchRequest("demo").source(source).preference("_local").allowPartialSearchResults(false);
        LanceRequestCache.Lookup first = begin(cache, plain, metadata, node("n1"));
        assertNull(first.find(3L));
        first.complete(took -> response(1.5d, 10L, false));
        LanceRequestCache.Lookup second = begin(cache, withPreference, metadata, node("n1"));
        assertNotNull("preference and allow_partial_search_results do not change the key", second.find(3L));
        assertEquals(1L, cache.stats().hits());
        assertEquals(1L, cache.stats().misses());
    }

    public void testShapeDecidesEligibility() throws IOException {
        assertEquals(LanceRequestCache.Skip.SIZE, LanceRequestCache.shapeSkip(null));
        assertEquals(LanceRequestCache.Skip.SIZE, LanceRequestCache.shapeSkip(body("{\"query\":{\"match_all\":{}}}")));
        assertEquals(LanceRequestCache.Skip.SIZE, LanceRequestCache.shapeSkip(body("{\"size\":10}")));
        assertEquals(LanceRequestCache.Skip.FROM, LanceRequestCache.shapeSkip(body("{\"size\":0,\"from\":5}")));
        assertNull(LanceRequestCache.shapeSkip(body("{\"size\":0}")));
        assertNull(LanceRequestCache.shapeSkip(body("{\"size\":0,\"track_total_hits\":true,\"query\":{\"term\":{\"a\":1}}}")));
        assertNull(LanceRequestCache.shapeSkip(body("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}")));
    }

    public void testBeginSkipsAndCountsTheRequestsItDoesNotCache() throws IOException {
        LanceRequestCache cache = cache();
        IndexMetadata metadata = indexMetadata("demo", "uuid-1");
        SearchSourceBuilder eligible = body("{\"size\":0}");
        assertNull("size > 0", begin(cache, new SearchRequest("demo").source(body("{\"size\":1}")), metadata, node("n1")));
        assertNull("opted out", begin(cache, new SearchRequest("demo").source(eligible).requestCache(false), metadata, node("n1")));
        assertNull(
            "mixed target",
            cache.begin(
                new SearchRequest("a", "b").source(eligible),
                new Index[] { new Index("a", "u-a"), new Index("b", "u-b") },
                null,
                List.of(node("n1")),
                null,
                0L
            )
        );
        assertEquals(3L, cache.stats().skipped());
        assertNotNull(begin(cache, new SearchRequest("demo").source(eligible), metadata, node("n1")));
        assertEquals(3L, cache.stats().skipped());

        cache.setEnabled(false);
        assertNull("disabled", begin(cache, new SearchRequest("demo").source(eligible), metadata, node("n1")));
        assertEquals("a disabled cache counts nothing", 3L, cache.stats().skipped());
        assertFalse(cache.stats().enabled());
        assertEquals(LanceRequestCache.Skip.DISABLED, cache.explainSkip(eligible, false));
        cache.setEnabled(true);
        assertNull(cache.explainSkip(eligible, false));
        assertEquals(LanceRequestCache.Skip.DLS, cache.explainSkip(eligible, true));
        assertEquals(LanceRequestCache.Skip.SIZE, cache.explainSkip(body("{\"size\":3}"), false));
    }

    public void testHitRendersTheStoredAnswerWithAFreshTook() throws IOException {
        LanceRequestCache cache = cache();
        IndexMetadata metadata = indexMetadata("demo", "uuid-1");
        SearchRequest request = new SearchRequest("demo").source(body("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"));
        LanceRequestCache.Lookup miss = begin(cache, request, metadata, node("n1"), node("n2"));
        assertNull(miss.find(7L));
        assertFalse(miss.isHit());
        SearchResponse computed = miss.complete(took -> response(42.0d, 200L, false));
        assertEquals(1, cache.count());
        assertTrue("the entry weighs its aggregations and its key: " + cache.weight(), cache.weight() > 0L);

        LanceRequestCache.Lookup hit = begin(cache, request, metadata, node("n2"), node("n1"));
        LanceRequestCache.Entry entry = hit.find(7L);
        assertNotNull("the node list is read in id order", entry);
        assertTrue(hit.isHit());
        SearchResponse served = hit.complete(took -> { throw new AssertionError("a hit computes nothing"); });
        assertEquals(200L, served.getHits().getTotalHits().value());
        assertEquals(computed.getAggregations().asMap().keySet(), served.getAggregations().asMap().keySet());
        assertEquals(42.0d, ((InternalSum) served.getAggregations().get("s")).getValue(), 0.0d);
        assertFalse(served.isTimedOut());
        assertEquals(1, served.getTotalShards());

        assertNull("another version is another key", begin(cache, request, metadata, node("n1"), node("n2")).find(8L));
        assertNull("another node list is another key", begin(cache, request, metadata, node("n1")).find(7L));
        assertEquals(1L, cache.stats().hits());
        assertEquals(3L, cache.stats().misses());
        assertEquals(1, cache.stats().entries());
    }

    public void testTimedOutAndOversizedAnswersAreNotStored() throws IOException {
        LanceRequestCache small = new LanceRequestCache(1L << 20, 8L, true, TimeValue.ZERO);
        IndexMetadata metadata = indexMetadata("demo", "uuid-1");
        SearchRequest request = new SearchRequest("demo").source(body("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"rating\"}}}}"));
        LanceRequestCache.Lookup lookup = begin(small, request, metadata, node("n1"));
        assertNull(lookup.find(1L));
        lookup.complete(took -> response(1.0d, 1L, false));
        assertEquals("above max_entry_size", 0, small.count());
        assertEquals(1L, small.stats().skipped());

        LanceRequestCache cache = cache();
        LanceRequestCache.Lookup partial = begin(cache, request, metadata, node("n1"));
        assertNull(partial.find(1L));
        partial.complete(took -> response(1.0d, 1L, true));
        assertEquals("a timed out answer is partial", 0, cache.count());
        assertEquals(1L, cache.stats().skipped());
    }

    public void testExpiredEntriesAreMissesAndCountAsEvictions() throws Exception {
        LanceRequestCache cache = new LanceRequestCache(1L << 20, 1L << 16, true, TimeValue.timeValueMillis(50));
        IndexMetadata metadata = indexMetadata("demo", "uuid-1");
        SearchRequest request = new SearchRequest("demo").source(body("{\"size\":0}"));
        LanceRequestCache.Lookup first = begin(cache, request, metadata, node("n1"));
        assertNull(first.find(1L));
        first.complete(took -> response(1.0d, 1L, false));
        assertNotNull(begin(cache, request, metadata, node("n1")).find(1L));
        Thread.sleep(80L);
        assertNull("expired", begin(cache, request, metadata, node("n1")).find(1L));
        assertEquals(0, cache.count());
        assertEquals(1L, cache.stats().evictions());
        cache.setExpire(TimeValue.ZERO);
        LanceRequestCache.Lookup again = begin(cache, request, metadata, node("n1"));
        assertNull(again.find(1L));
        again.complete(took -> response(1.0d, 1L, false));
        Thread.sleep(80L);
        assertNotNull("no expiry keeps the entry", begin(cache, request, metadata, node("n1")).find(1L));
    }

    public void testEvictionUnderTheWeightLimitCounts() throws IOException {
        // Room for about one entry: the second put evicts the first.
        LanceRequestCache cache = new LanceRequestCache(300L, 1L << 16, true, TimeValue.ZERO);
        IndexMetadata metadata = indexMetadata("demo", "uuid-1");
        SearchRequest a = new SearchRequest("demo").source(body("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"a\"}}}}"));
        SearchRequest b = new SearchRequest("demo").source(body("{\"size\":0,\"aggs\":{\"s\":{\"sum\":{\"field\":\"b\"}}}}"));
        LanceRequestCache.Lookup first = begin(cache, a, metadata, node("n1"));
        assertNull(first.find(1L));
        first.complete(took -> response(1.0d, 1L, false));
        assertEquals(1, cache.count());
        LanceRequestCache.Lookup second = begin(cache, b, metadata, node("n1"));
        assertNull(second.find(1L));
        second.complete(took -> response(2.0d, 1L, false));
        assertEquals("the limit holds one entry", 1, cache.count());
        assertEquals(1L, cache.stats().evictions());
        assertEquals(300L, cache.stats().limitBytes());
    }

    public void testInvalidateIndexesDropsTheIndexEntriesOnly() throws IOException {
        LanceRequestCache cache = cache();
        IndexMetadata one = indexMetadata("one", "uuid-1");
        IndexMetadata two = indexMetadata("two", "uuid-2");
        SearchSourceBuilder source = body("{\"size\":0}");
        for (IndexMetadata metadata : List.of(one, two)) {
            LanceRequestCache.Lookup lookup = begin(
                cache,
                new SearchRequest(metadata.getIndex().getName()).source(source),
                metadata,
                node("n1")
            );
            assertNull(lookup.find(1L));
            lookup.complete(took -> response(1.0d, 1L, false));
        }
        LanceRequestCache.Lookup other = begin(
            cache,
            new SearchRequest("one").source(body("{\"size\":0,\"track_total_hits\":true}")),
            one,
            node("n1")
        );
        assertNull(other.find(1L));
        other.complete(took -> response(1.0d, 1L, false));
        assertEquals(3, cache.count());

        assertEquals(2, cache.invalidateIndexes(List.of("uuid-1")));
        assertEquals(1, cache.count());
        assertNotNull(begin(cache, new SearchRequest("two").source(source), two, node("n1")).find(1L));
        assertEquals(2L, cache.stats().invalidations());
        assertEquals(0, cache.invalidateIndexes(List.of()));
        assertEquals(0, cache.invalidateIndexes(List.of("no-such")));
    }

    public void testAnIndexDeletionInClusterStateInvalidatesItsEntries() throws IOException {
        LanceRequestCache cache = cache();
        IndexMetadata one = indexMetadata("one", "uuid-1");
        IndexMetadata two = indexMetadata("two", "uuid-2");
        SearchSourceBuilder source = body("{\"size\":0}");
        for (IndexMetadata metadata : List.of(one, two)) {
            LanceRequestCache.Lookup lookup = begin(
                cache,
                new SearchRequest(metadata.getIndex().getName()).source(source),
                metadata,
                node("n1")
            );
            assertNull(lookup.find(1L));
            lookup.complete(took -> response(1.0d, 1L, false));
        }
        ClusterState before = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().clusterUUID("cluster-1").put(one, false).put(two, false))
            .build();
        ClusterState after = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().clusterUUID("cluster-1").put(two, false))
            .build();
        cache.clusterChanged(new ClusterChangedEvent("delete one", after, before));
        assertEquals(1, cache.count());
        assertNull(begin(cache, new SearchRequest("one").source(source), one, node("n1")).find(1L));
        assertNotNull(begin(cache, new SearchRequest("two").source(source), two, node("n1")).find(1L));
        assertEquals(1L, cache.stats().invalidations());

        cache.clusterChanged(new ClusterChangedEvent("nothing", after, after));
        assertEquals("no deletion, nothing dropped", 1, cache.count());
    }

    public void testDisablingDropsEveryEntry() throws IOException {
        LanceRequestCache cache = cache();
        IndexMetadata metadata = indexMetadata("demo", "uuid-1");
        SearchRequest request = new SearchRequest("demo").source(body("{\"size\":0}"));
        LanceRequestCache.Lookup lookup = begin(cache, request, metadata, node("n1"));
        assertNull(lookup.find(1L));
        lookup.complete(took -> response(1.0d, 1L, false));
        assertEquals(1, cache.count());
        cache.setEnabled(false);
        assertEquals(0, cache.count());
        assertEquals(0L, cache.stats().sizeBytes());
    }

    public void testAggregationBytesIsTheSerialisedSize() {
        assertEquals(0L, LanceRequestCache.aggregationBytes(null));
        InternalAggregations one = InternalAggregations.from(List.of(new InternalSum("s", 1.0d, DocValueFormat.RAW, Map.of())));
        InternalAggregations two = InternalAggregations.from(
            List.of(new InternalSum("s", 1.0d, DocValueFormat.RAW, Map.of()), new InternalSum("t", 2.0d, DocValueFormat.RAW, Map.of()))
        );
        assertTrue(LanceRequestCache.aggregationBytes(one) > 0L);
        assertTrue(LanceRequestCache.aggregationBytes(two) > LanceRequestCache.aggregationBytes(one));
    }

    public void testNodeIdsAreSortedAndJoined() {
        assertEquals("a,b,c", LanceRequestCache.nodeIds(List.of(node("c"), node("a"), node("b"))));
        assertEquals("", LanceRequestCache.nodeIds(List.of()));
    }
}
