/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.mapper.Uid;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.dispatch.HitProjection;
import org.opensearch.lance.engine.LanceEngineFactory.LancePrimaryKeyType;
import org.opensearch.lance.engine.LanceFragmentSchema.TakeProjection;
import org.opensearch.lance.stats.LanceNodeStats;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The projection of the row take behind a page of hits: which columns
 * {@link LanceFragmentSchema#takeProjection(boolean, List, List, List)}
 * keeps for a body's {@code _source} filter, {@code fields} and
 * {@code stored_fields}, as {@link HitProjection#takeProjection} derives
 * them from the body, and that {@link LanceStoredFields} takes and
 * renders exactly those columns once a leaf carries the projection.
 *
 * <p>Fixture: one fragment of {@link LanceTableFactory#writeHintFixtureTable}
 * read with {@code id} as the primary key, so the surfaced columns are
 * {@code id, body, rating, category, tags, flag} in schema order (the
 * vector column is not surfaced); and the struct table of
 * {@link LanceTableFactory#writeStructTable} ({@code id} primary key,
 * {@code meta} struct) for the parent and child rules.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceStoredFieldsTests extends OpenSearchTestCase {

    private static final List<String> ALL = List.of("id", "body", "rating", "category", "tags", "flag");

    private static HitProjection projection(FetchSourceContext source, StoredFieldsContext stored, String... fields) {
        List<FieldAndFormat> fetchFields = new ArrayList<>();
        for (String field : fields) {
            fetchFields.add(new FieldAndFormat(field, null));
        }
        return new HitProjection(source, stored, List.of(), fetchFields, false);
    }

    private LanceFragmentSchema hintSchema(Dataset dataset) throws Exception {
        return LanceFragmentSchema.derive(dataset, "id", LancePrimaryKeyType.LONG, LanceOverrides.EMPTY, Collections.emptySet());
    }

    public void testSourceFilterAndFieldsNarrowTheTake() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "take-" + getTestName().toLowerCase(Locale.ROOT), 1, 10_000);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceFragmentSchema schema = hintSchema(dataset);
            assertEquals(ALL, schema.takeColumns());

            // No projection: every surfaced column, the key among them.
            assertEquals(new TakeProjection(ALL, 6, 0), HitProjection.NONE.takeProjection(schema));
            assertEquals(new TakeProjection(ALL, 6, 0), schema.takeProjection());

            // _source: false renders _id alone, so only the key is taken.
            assertEquals(
                new TakeProjection(List.of("id"), 0, 0),
                projection(FetchSourceContext.DO_NOT_FETCH_SOURCE, null).takeProjection(schema)
            );
            // docvalue_fields read doc values, not the source: no column joins.
            HitProjection docValues = new HitProjection(
                FetchSourceContext.DO_NOT_FETCH_SOURCE,
                null,
                List.of(new FieldAndFormat("rating", null)),
                List.of(),
                false
            );
            assertEquals(new TakeProjection(List.of("id"), 0, 0), docValues.takeProjection(schema));

            // includes keep the matching columns in schema order; the key
            // is appended when the filter drops it.
            assertEquals(
                new TakeProjection(List.of("rating", "category", "id"), 2, 2),
                projection(new FetchSourceContext(true, new String[] { "rating", "cat*" }, null), null).takeProjection(schema)
            );
            assertEquals(
                new TakeProjection(List.of("rating", "id"), 1, 1),
                projection(new FetchSourceContext(true, new String[] { "rating" }, null), null).takeProjection(schema)
            );

            // excludes drop the named columns and keep the rest.
            assertEquals(
                new TakeProjection(List.of("id", "rating", "category", "flag"), 4, 0),
                projection(new FetchSourceContext(true, null, new String[] { "body", "tags" }), null).takeProjection(schema)
            );
            assertEquals(
                new TakeProjection(List.of("rating", "id"), 1, 1),
                projection(new FetchSourceContext(true, new String[] { "rating", "body" }, new String[] { "b*" }), null).takeProjection(
                    schema
                )
            );

            // fields read the unfiltered source: their columns are taken
            // whether or not the _source filter keeps them.
            assertEquals(
                new TakeProjection(List.of("body", "id"), 1, 1),
                projection(FetchSourceContext.DO_NOT_FETCH_SOURCE, null, "body").takeProjection(schema)
            );
            assertEquals(
                new TakeProjection(List.of("body", "rating", "id"), 2, 2),
                projection(new FetchSourceContext(true, new String[] { "rating" }, null), null, "body").takeProjection(schema)
            );
            assertEquals(
                new TakeProjection(List.of("id", "body", "rating", "category", "flag"), 5, 0),
                projection(new FetchSourceContext(true, null, new String[] { "body", "tags" }), null, "bo*").takeProjection(schema)
            );

            // stored_fields: a named list without _source loads no source,
            // _none_ loads nothing at all, and naming _source loads it.
            assertEquals(
                new TakeProjection(List.of("id"), 0, 0),
                projection(null, StoredFieldsContext.fromList(List.of("rating"))).takeProjection(schema)
            );
            assertEquals(
                new TakeProjection(List.of(), 0, -1),
                projection(null, StoredFieldsContext.fromList(List.of(StoredFieldsContext._NONE_))).takeProjection(schema)
            );
            assertEquals(
                new TakeProjection(ALL, 6, 0),
                projection(null, StoredFieldsContext.fromList(List.of("_source"))).takeProjection(schema)
            );
            assertEquals(
                new TakeProjection(List.of("rating", "id"), 1, 1),
                projection(new FetchSourceContext(false, new String[] { "rating" }, null), StoredFieldsContext.fromList(List.of("_source")))
                    .takeProjection(schema)
            );
        }
    }

    public void testStructColumnsAreTakenByParentName() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeStructTable(scratchDir, "take-" + getTestName().toLowerCase(Locale.ROOT), 0);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceFragmentSchema schema = LanceFragmentSchema.derive(
                dataset,
                "id",
                LancePrimaryKeyType.LONG,
                LanceOverrides.EMPTY,
                Collections.emptySet()
            );
            assertEquals(List.of("id", "meta"), schema.takeColumns());

            // A child include or a wildcard over the children takes the
            // parent; the fetch phase filters the children.
            assertEquals(
                new TakeProjection(List.of("meta", "id"), 1, 1),
                projection(new FetchSourceContext(true, new String[] { "meta.region" }, null), null).takeProjection(schema)
            );
            assertEquals(
                new TakeProjection(List.of("meta", "id"), 1, 1),
                projection(new FetchSourceContext(true, new String[] { "*.score" }, null), null).takeProjection(schema)
            );
            assertEquals(
                new TakeProjection(List.of("id", "meta"), 2, 0),
                projection(new FetchSourceContext(true, new String[] { "id", "meta.flags.*" }, null), null).takeProjection(schema)
            );
            // A child exclude keeps the parent; excluding the parent
            // drops it.
            assertEquals(
                new TakeProjection(List.of("id", "meta"), 2, 0),
                projection(new FetchSourceContext(true, null, new String[] { "meta.region" }), null).takeProjection(schema)
            );
            assertEquals(
                new TakeProjection(List.of("id"), 1, 0),
                projection(new FetchSourceContext(true, null, new String[] { "meta" }), null).takeProjection(schema)
            );
            // fields on a child take the parent.
            assertEquals(
                new TakeProjection(List.of("meta", "id"), 1, 1),
                projection(FetchSourceContext.DO_NOT_FETCH_SOURCE, null, "meta.sc*").takeProjection(schema)
            );
            // A pattern that names nothing takes only the key.
            assertEquals(
                new TakeProjection(List.of("id"), 0, 0),
                projection(new FetchSourceContext(true, new String[] { "nothing.*" }, null), null).takeProjection(schema)
            );
        }
    }

    public void testLeafTakesAndRendersTheProjectedColumnsOnly() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeHintFixtureTable(scratchDir, "take-" + getTestName().toLowerCase(Locale.ROOT), 1, 10_000);
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            List<Integer> fragmentIds = new ArrayList<>();
            for (Fragment fragment : dataset.getFragments()) {
                fragmentIds.add(fragment.getId());
            }
            try (
                LanceDirectoryReader reader = LanceDirectoryReader.openForFragments(
                    new ByteBuffersDirectory(),
                    null,
                    dataset,
                    "id",
                    LancePrimaryKeyType.LONG,
                    LanceOverrides.EMPTY,
                    fragmentIds
                )
            ) {
                LeafReaderContext ctx = reader.leaves().get(0);
                LanceFragmentLeafReader leaf = LanceFragmentLeafReader.unwrap(ctx.reader());
                assertNotNull(leaf);
                LanceFragmentSchema schema = leaf.schema();
                assertEquals("a leaf starts with the full take", schema.takeProjection(), leaf.takeProjection());

                // _source: false: the take projects the key alone, and
                // _id still renders from it.
                FetchTakeStats.Accumulator takes = new FetchTakeStats.Accumulator();
                leaf.setTakeAccumulator(takes);
                leaf.setTakeProjection(projection(FetchSourceContext.DO_NOT_FETCH_SOURCE, null).takeProjection(schema));
                LanceNodeStats.FetchStats before = FetchTakeStats.snapshot();
                leaf.prefetchRows(new int[] { 1, 7 });
                LanceNodeStats.FetchStats after = FetchTakeStats.snapshot();
                assertEquals("one take", before.takeCount() + 1, after.takeCount());
                assertEquals("the take projects the key only", before.takeColumns() + 1, after.takeColumns());
                assertEquals(1L, takes.takeCount());
                Collected id = new Collected(true, false);
                leaf.materialiseStoredFields(7, id);
                assertEquals("7", id.id());
                assertNull(id.source());

                // An includes filter: the take projects the kept columns and
                // the key, and _source carries the kept columns only, in
                // schema order.
                leaf.setTakeProjection(
                    projection(new FetchSourceContext(true, new String[] { "category", "rating" }, null), null).takeProjection(schema)
                );
                before = FetchTakeStats.snapshot();
                leaf.prefetchRows(new int[] { 1 });
                after = FetchTakeStats.snapshot();
                assertEquals(before.takeColumns() + 3, after.takeColumns());
                Collected both = new Collected(true, true);
                leaf.materialiseStoredFields(1, both);
                assertEquals("1", both.id());
                Map<String, Object> source = both.source();
                assertEquals(List.of("rating", "category"), new ArrayList<>(source.keySet()));
                assertEquals(37, source.get("rating"));
                assertEquals("c1", source.get("category"));

                // A filter that drops the key: the key is taken after the
                // source columns and _id still renders, while _source
                // carries only the kept column.
                leaf.setTakeProjection(
                    projection(new FetchSourceContext(true, new String[] { "flag" }, null), null).takeProjection(schema)
                );
                Collected flagOnly = new Collected(true, true);
                leaf.materialiseStoredFields(2, flagOnly);
                assertEquals("2", flagOnly.id());
                assertEquals(Map.of("flag", true), flagOnly.source());
            }
        }
    }

    /** A visitor that keeps the {@code _id} and {@code _source} a leaf renders. */
    private static final class Collected extends StoredFieldVisitor {
        private final boolean needsId;
        private final boolean needsSource;
        private String id;
        private byte[] source;

        Collected(boolean needsId, boolean needsSource) {
            this.needsId = needsId;
            this.needsSource = needsSource;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) {
            if ("_id".equals(fieldInfo.name)) {
                return needsId ? Status.YES : Status.NO;
            }
            if ("_source".equals(fieldInfo.name)) {
                return needsSource ? Status.YES : Status.NO;
            }
            return Status.NO;
        }

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] value) {
            if ("_id".equals(fieldInfo.name)) {
                id = Uid.decodeId(value);
            } else if ("_source".equals(fieldInfo.name)) {
                source = value;
            }
        }

        String id() {
            return id;
        }

        Map<String, Object> source() {
            if (source == null) {
                return null;
            }
            assertTrue(new String(source, StandardCharsets.UTF_8), source.length > 0);
            return XContentHelper.convertToMap(new BytesArray(source), true, XContentType.JSON).v2();
        }
    }
}
