/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.lance.Dataset;
import org.opensearch.ExceptionsHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.attach.LanceAttachAction;
import org.opensearch.lance.attach.LanceAttachRequest;
import org.opensearch.lance.attach.LanceAttachResponse;
import org.opensearch.lance.engine.LanceIndexBuilder;
import org.opensearch.lance.query.LanceInvalidInput;
import org.opensearch.lance.query.LanceMatchPhraseQueryBuilder;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

/**
 * A request Lance refuses as invalid input fails the fragment executor
 * with the status of a client error. The fixture's {@code category}
 * column gets an inverted index without positions, which is what
 * {@code build_indexes} creates by default, so a phrase query on it
 * makes Lance throw {@code IllegalArgumentException} inside the FTS
 * scan; the executor must report that exception rather than the
 * {@code IOException} the Lucene Weight contract wrapped it in.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class FragmentExecutorInvalidInputTests extends OpenSearchSingleNodeTestCase {

    private static final int FRAGMENTS = 3;
    private static final int ROWS_PER_FRAGMENT = 4;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LancePlugin.class);
    }

    /**
     * Write the interleaved fixture, build a position-less FTS index on
     * {@code category} and attach the table, so {@code category} maps as
     * {@code lance_text} and accepts the plugin's FTS queries.
     */
    private String attachWithPositionlessIndex(String indexName) throws Exception {
        Path dir = createTempDir();
        String tableUri = LanceTableFactory.writeInterleavedTable(dir, indexName, FRAGMENTS, ROWS_PER_FRAGMENT);
        try (Dataset dataset = LanceRegistry.openDataset(tableUri, StorageOptions.empty())) {
            LanceIndexBuilder.BuildResult built = LanceIndexBuilder.ensureFtsIndexes(
                dataset,
                Set.of("category"),
                Long.MAX_VALUE,
                Optional.empty(),
                LanceIndexBuilder.DEFAULT_FTS_TOKENIZER,
                /* withPosition */ false
            );
            assertEquals("fts build failures: " + built.failed(), 0, built.failed().size());
            assertEquals(List.of("category"), built.built());
        }
        LanceAttachResponse attached = client().execute(
            LanceAttachAction.INSTANCE,
            new LanceAttachRequest(tableUri, indexName, null, null, StorageOptions.empty(), null)
        ).actionGet();
        assertEquals(indexName, attached.index());
        ensureGreen(indexName);
        return tableUri;
    }

    private static LanceFragmentQueryRequest request(String tableUri, String indexName, QueryBuilder query) {
        return new LanceFragmentQueryRequest(
            tableUri,
            indexName,
            StorageOptions.empty(),
            /* pinnedVersion */ -1L,
            /* filterSql */ null,
            query,
            /* postFilter */ null,
            List.of(),
            /* searchAfter */ null,
            10,
            /* aggregations */ null,
            /* fragmentIds */ List.of(),
            /* trackScores */ false,
            SearchContext.TRACK_TOTAL_HITS_ACCURATE
        );
    }

    /** Run {@code request} through {@code doExecute} and return what the listener received. */
    private Exception failureOf(LanceFragmentQueryRequest request) {
        TransportLanceFragmentQueryAction executor = getInstanceFromNode(TransportLanceFragmentQueryAction.class);
        AtomicReference<LanceFragmentQueryResponse> response = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        executor.doExecute(null, request, ActionListener.wrap(response::set, failure::set));
        assertNull("expected the request to fail, got " + response.get(), response.get());
        return failure.get();
    }

    public void testPhraseQueryWithoutPositionsFailsAsIllegalArgument() throws Exception {
        String indexName = "invalid-input-phrase";
        String tableUri = attachWithPositionlessIndex(indexName);

        Exception failure = failureOf(request(tableUri, indexName, new LanceMatchPhraseQueryBuilder("category", "c0 c1")));
        assertNotNull("expected a failure from the executor", failure);
        assertTrue("expected IllegalArgumentException, saw " + failure, failure instanceof IllegalArgumentException);
        assertTrue("expected the exception Lance's JNI raised, saw " + failure, LanceInvalidInput.isInvalidInput(failure));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(failure));
        assertTrue(
            "expected Lance's message about positions, saw " + failure.getMessage(),
            failure.getMessage().contains("position is not found but required for phrase queries")
        );
    }

    public void testTermQueryWithoutPositionsStillAnswers() throws Exception {
        String indexName = "invalid-input-term";
        String tableUri = attachWithPositionlessIndex(indexName);

        TransportLanceFragmentQueryAction executor = getInstanceFromNode(TransportLanceFragmentQueryAction.class);
        LanceFragmentQueryResponse response = executor.execute(request(tableUri, indexName, new LanceMatchQueryBuilder("category", "c0")));
        // category is "c" + (i % 3) over 12 rows, so four rows are c0.
        assertEquals(4, response.matched());
        assertEquals(4, response.hits().size());
    }
}
