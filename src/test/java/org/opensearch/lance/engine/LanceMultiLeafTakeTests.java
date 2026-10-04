/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.mapper.Uid;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The one take over the leaves of a fetch round
 * ({@link LanceMultiLeafTake#prefetchRows}) observes the round's
 * cancellation between the batches it reads, and without a
 * cancellation hands every leaf every row it asked for.
 *
 * <p>Fixture: {@link LanceTableFactory#writeHintFixtureTable} with two
 * fragments of 3000 rows read with {@code id} as the primary key. The
 * rows of both leaves together are more than one
 * {@link LanceStoredFields#TAKE_CHUNK}, so the take opens two scans and
 * the loop sees at least two batches whatever Lance's batch size.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceMultiLeafTakeTests extends OpenSearchTestCase {

    private static final int FRAGMENTS = 2;
    private static final int ROWS_PER_FRAGMENT = 3_000;

    private Dataset dataset;
    private List<Integer> fragmentIds;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(
            scratchDir,
            "take-" + getTestName().toLowerCase(Locale.ROOT),
            FRAGMENTS,
            ROWS_PER_FRAGMENT
        );
        dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        fragmentIds = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            fragmentIds.add(fragment.getId());
        }
        assertEquals(FRAGMENTS, fragmentIds.size());
        assertTrue("the page spans more than one chunk", FRAGMENTS * ROWS_PER_FRAGMENT > LanceStoredFields.TAKE_CHUNK);
    }

    @Override
    public void tearDown() throws Exception {
        if (dataset != null) {
            dataset.close();
        }
        super.tearDown();
    }

    private LanceDirectoryReader open() throws Exception {
        return LanceDirectoryReader.openForFragments(
            new ByteBuffersDirectory(),
            null,
            dataset,
            "id",
            LancePrimaryKeyType.LONG,
            LanceOverrides.EMPTY,
            fragmentIds
        );
    }

    /**
     * Every row of every leaf, keyed by leaf in leaf order, with one
     * accumulator shared by the leaves as the fetch round shares it.
     */
    private static Map<LanceFragmentLeafReader, int[]> wholeTable(LanceDirectoryReader reader, FetchTakeStats.Accumulator takes) {
        Map<LanceFragmentLeafReader, int[]> docIdsByLeaf = new LinkedHashMap<>();
        for (LeafReaderContext ctx : reader.leaves()) {
            LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(ctx.reader());
            assertNotNull(leaf);
            leaf.setTakeAccumulator(takes);
            int[] docIds = new int[ROWS_PER_FRAGMENT];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = i;
            }
            docIdsByLeaf.put(leaf, docIds);
        }
        assertEquals(FRAGMENTS, docIdsByLeaf.size());
        return docIdsByLeaf;
    }

    public void testCancelledTaskStopsTheTakeAtTheNextBatch() throws Exception {
        try (LanceDirectoryReader reader = open()) {
            FetchTakeStats.Accumulator takes = new FetchTakeStats.Accumulator();
            Map<LanceFragmentLeafReader, int[]> docIdsByLeaf = wholeTable(reader, takes);
            CancelOnSecondCheck task = new CancelOnSecondCheck(true);
            expectThrows(TaskCancelledException.class, () -> LanceMultiLeafTake.prefetchRows(docIdsByLeaf, LanceCancellation.of(task)));
            assertEquals("the loop stopped at the check that saw the cancellation", 2, task.checks);
        }
    }

    public void testUncancelledTakeReturnsEveryRow() throws Exception {
        try (LanceDirectoryReader reader = open()) {
            FetchTakeStats.Accumulator takes = new FetchTakeStats.Accumulator();
            Map<LanceFragmentLeafReader, int[]> docIdsByLeaf = wholeTable(reader, takes);
            CancelOnSecondCheck task = new CancelOnSecondCheck(false);
            LanceMultiLeafTake.prefetchRows(docIdsByLeaf, LanceCancellation.of(task));
            assertTrue("the take read at least two batches: " + task.checks, task.checks >= 2);
            assertEquals("two chunks, two takes", 2L, takes.takeCount());
            assertEquals((long) FRAGMENTS * ROWS_PER_FRAGMENT, takes.takeRows());
            // Rendering every row of every leaf reads the taken rows:
            // no leaf issues a take of its own.
            for (Map.Entry<LanceFragmentLeafReader, int[]> entry : docIdsByLeaf.entrySet()) {
                LanceFragmentLeafReader leaf = entry.getKey();
                int base = leaf.fragmentId() * ROWS_PER_FRAGMENT;
                for (int docId : entry.getValue()) {
                    IdOnly hit = new IdOnly();
                    leaf.materialiseStoredFields(docId, hit);
                    assertEquals(Integer.toString(base + docId), hit.id);
                }
            }
            assertEquals("every row came from the one take", 2L, takes.takeCount());
        }
    }

    /**
     * A task that, while {@code armed}, cancels itself the second time
     * its state is read: the first batch of a take passes its check and
     * the second does not. Counts the reads either way.
     */
    private static final class CancelOnSecondCheck extends CancellableTask {
        private final boolean armed;
        int checks;

        CancelOnSecondCheck(boolean armed) {
            super(1L, "transport", "test", "cancel on the second check", TaskId.EMPTY_TASK_ID, Map.of());
            this.armed = armed;
        }

        @Override
        public boolean isCancelled() {
            checks++;
            if (armed && checks == 2) {
                cancel("second batch");
            }
            return super.isCancelled();
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            return false;
        }
    }

    /** A visitor that keeps the {@code _id} a leaf renders. */
    private static final class IdOnly extends StoredFieldVisitor {
        String id;

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            return "_id".equals(fieldInfo.name) ? Status.YES : Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            if ("_id".equals(fieldInfo.name)) {
                id = Uid.decodeId(value);
            }
        }
    }
}
