/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

/**
 * Pins the pool split of the fragment executor: the transport handler
 * admits requests on the SEARCH pool, and the work a request runs
 * beyond its own thread (collection slices, column load group scans,
 * aggregation pushdown scans) goes to the {@code index_searcher} pool.
 * The two must not be the same pool. A slice or scan task submitted to
 * the SEARCH pool stays in its bounded queue as a spent entry after the
 * calling thread has run it, and with every SEARCH thread parked at the
 * fragment path semaphore those entries drain only when a request
 * completes; each waiting fragment request then traps the tasks of the
 * requests executing behind it, the queue population multiplies with
 * the client fan-in, and the queue rejects new fragment requests as 429
 * well below the load the node can serve.
 */
public class FragmentQueryIntraRequestPoolTests extends OpenSearchTestCase {

    public void testIntraRequestWorkGoesToTheIndexSearcherPool() {
        assertEquals(ThreadPool.Names.INDEX_SEARCHER, TransportLanceFragmentQueryAction.INTRA_REQUEST_POOL);
    }

    public void testIntraRequestPoolIsNotTheHandlerPool() {
        assertNotEquals(ThreadPool.Names.SEARCH, TransportLanceFragmentQueryAction.INTRA_REQUEST_POOL);
    }
}
