/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.io.IOException;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import static org.hamcrest.Matchers.greaterThan;

/**
 * The fragment executor's intra request work must land on the
 * {@code index_searcher} pool: a multi fragment search with a slice
 * count above one submits its collection slices there, so the pool's
 * {@code completed} counter moves. Submitting them to the SEARCH pool
 * instead fills the bounded queue that admits fragment requests and
 * turns concurrency the node could serve into 429s.
 */
public class LanceIntraRequestPoolIT extends LanceRestTestCase {

    public void testCollectionSlicesRunOnTheIndexSearcherPool() throws Exception {
        try (LanceTestCluster fixture = LanceTestCluster.setUpMultiFragment(12, 4, "slicepool")) {
            Request slices = new Request("PUT", "/_cluster/settings");
            slices.setJsonEntity("{\"transient\":{\"lance.fragment_path.slices\":3}}");
            client().performRequest(slices);
            try {
                long before = completedOnIndexSearcherPool();
                String body = readAll(postJson("/" + fixture.indexName() + "/_search", "{\"size\":5,\"query\":{\"match_all\":{}}}"));
                assertEquals(12, extractIntPath(body, "hits", "total", "value"));
                // Every slice task submitted to the pool is dequeued and
                // completed there, whether a pool thread collected the
                // slice or the request's thread had already run it, so
                // the counter moving is exactly "the slice tasks went to
                // this pool". Polled because the dequeue can trail the
                // response.
                assertBusy(() -> assertThat(completedOnIndexSearcherPool(), greaterThan(before)));
            } finally {
                Request reset = new Request("PUT", "/_cluster/settings");
                reset.setJsonEntity("{\"transient\":{\"lance.fragment_path.slices\":null}}");
                client().performRequest(reset);
            }
        }
    }

    /** Sum of the {@code completed} column of {@code _cat/thread_pool/index_searcher} over the cluster's nodes. */
    private long completedOnIndexSearcherPool() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_cat/thread_pool/index_searcher?h=completed"));
        long sum = 0L;
        for (String line : readAll(response).split("\n")) {
            if (line.isBlank() == false) {
                sum += Long.parseLong(line.trim());
            }
        }
        return sum;
    }
}
