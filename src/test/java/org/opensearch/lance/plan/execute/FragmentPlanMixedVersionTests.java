/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.execute;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.WireVersion;
import org.opensearch.lance.query.LanceMatchQueryBuilder;
import org.opensearch.search.SearchModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * The plan across plugin versions, as a rolling upgrade sends it: a
 * coordinator of one version writes it, a data node of another reads
 * it. A version 1 or 2 stream is what a node of the previous plugin
 * versions would write; a node of those versions reading today's stream
 * is played by {@link FragmentPlan#read(StreamInput, int)} with that
 * version.
 */
public class FragmentPlanMixedVersionTests extends OpenSearchTestCase {

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, List.of(new LancePlugin())).getNamedWriteables()
    );

    /** The stream a version 1 node writes: the marker and the base fields, no block. */
    private static void writeVersion1(
        StreamOutput out,
        FragmentPlan.Kind kind,
        String filterSql,
        QueryBuilder lanceClause,
        FragmentPlan.TopK topK,
        FragmentPlan.Aggregate aggregate
    ) throws IOException {
        WireVersion.write(out, 1);
        out.writeEnum(kind);
        out.writeOptionalString(filterSql);
        out.writeOptionalNamedWriteable(lanceClause);
        out.writeOptionalWriteable(topK);
        out.writeOptionalWriteable(aggregate);
    }

    /** The stream a version 2 node writes: version 1 plus the pruning block. */
    private static void writeVersion2(StreamOutput out, FragmentPlan.Kind kind, String filterSql, int[] excludedFragmentIds)
        throws IOException {
        WireVersion.write(out, 2);
        out.writeEnum(kind);
        out.writeOptionalString(filterSql);
        out.writeOptionalNamedWriteable(null);
        out.writeOptionalWriteable(null);
        out.writeOptionalWriteable(null);
        WireVersion.writeBlock(out, false, o -> o.writeVIntArray(excludedFragmentIds));
    }

    private static FragmentPlan readAs(BytesStreamOutput out, int asVersion) throws IOException {
        try (
            StreamInput raw = out.bytes().streamInput();
            NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(raw, REGISTRY)
        ) {
            FragmentPlan plan = FragmentPlan.read(in, asVersion);
            assertEquals("the reader consumed the whole plan", -1, in.read());
            return plan;
        }
    }

    public void testMixedPluginVersionNewerNodeReadsAVersion1PlanWithNoPruningAndNoSubstrait() throws IOException {
        LanceMatchQueryBuilder clause = new LanceMatchQueryBuilder("title", "lance");
        FragmentPlan.TopK topK = new FragmentPlan.TopK(List.of(new FragmentPlan.ScanOrdering("rating", false, false)), 10, "rating < 3");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writeVersion1(out, FragmentPlan.Kind.PUSHED_SCAN, "rating > 1", clause, topK, null);
            FragmentPlan plan = readAs(out, FragmentPlan.WIRE_VERSION);
            assertEquals(FragmentPlan.Kind.PUSHED_SCAN, plan.kind());
            assertEquals("rating > 1", plan.filterSql());
            assertEquals(clause, plan.lanceClause());
            assertEquals(topK, plan.topK());
            assertNull(plan.aggregate());
            assertArrayEquals("nothing pruned", new int[0], plan.excludedFragmentIds());
            assertNull("no Substrait filter", plan.filterSubstrait());
        }
    }

    public void testMixedPluginVersionNewerNodeReadsAVersion2PlanWithItsPruningAndNoSubstrait() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writeVersion2(out, FragmentPlan.Kind.LUCENE_COUNT, "rating = 5", new int[] { 1, 4 });
            FragmentPlan plan = readAs(out, FragmentPlan.WIRE_VERSION);
            assertEquals(FragmentPlan.Kind.LUCENE_COUNT, plan.kind());
            assertEquals("rating = 5", plan.filterSql());
            assertArrayEquals(new int[] { 1, 4 }, plan.excludedFragmentIds());
            assertNull(plan.filterSubstrait());
        }
    }

    public void testMixedPluginVersionOlderNodeStepsOverThePruningListOfANewerPlan() throws IOException {
        FragmentPlan pruned = new FragmentPlan(FragmentPlan.Kind.LUCENE_COUNT, "rating = 5", null, null, null, new int[] { 2, 3 });
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            pruned.writeTo(out);
            FragmentPlan asVersion1 = readAs(out, 1);
            assertEquals(
                "the older node scans every fragment, which still answers correctly",
                FragmentPlan.lucene(FragmentPlan.Kind.LUCENE_COUNT, "rating = 5"),
                asVersion1
            );
            FragmentPlan asVersion2 = readAs(out, 2);
            assertArrayEquals("a version 2 node keeps the pruning", new int[] { 2, 3 }, asVersion2.excludedFragmentIds());
            assertNull(asVersion2.filterSubstrait());
            assertEquals(pruned, readAs(out, FragmentPlan.WIRE_VERSION));
        }
    }

    public void testMixedPluginVersionOlderNodeRefusesAPlanWhoseSubstraitFilterItCannotEvaluate() throws IOException {
        FragmentPlan substrait = new FragmentPlan(FragmentPlan.Kind.LUCENE_COUNT, null, new byte[] { 1, 2, 3 }, null, null, null);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            substrait.writeTo(out);
            for (int older : new int[] { 1, 2 }) {
                IOException refused = expectThrows(IOException.class, () -> readAs(out, older));
                assertEquals(WireVersion.criticalBlockMessage("FragmentPlan", 3, 3, older), refused.getMessage());
            }
            assertEquals(substrait, readAs(out, FragmentPlan.WIRE_VERSION));
        }
    }

    public void testMixedPluginVersionOlderNodeReadsAPlanWithoutASubstraitFilterAsItsOwn() throws IOException {
        FragmentPlan sqlOnly = new FragmentPlan(FragmentPlan.Kind.LUCENE_TOPK, "rating = 5", null, null, null);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            sqlOnly.writeTo(out);
            assertEquals("the Substrait block is optional when empty", sqlOnly, readAs(out, 1));
            assertEquals(sqlOnly, readAs(out, 2));
        }
    }
}
