/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.lance.Dataset;
import org.lance.Fragment;
import org.opensearch.lance.LanceOverrides;
import org.opensearch.lance.LanceRegistry;
import org.opensearch.lance.LanceTableFactory;
import org.opensearch.lance.StorageOptions;
import org.opensearch.lance.engine.LanceFragmentSchema.ColumnKind;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Schema classification and doc values of a geo_point-overridden
 * column: the column classifies as {@link ColumnKind#GEO_POINT} with no
 * dotted child paths, carries a SORTED_NUMERIC {@code FieldInfo}, joins
 * the row take under its own name, and the leaf serves one encoded
 * {@code lat|lon} long per present row in the exact
 * {@code LatLonDocValuesField} layout.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceFragmentSchemaGeoTests extends OpenSearchTestCase {

    private static LanceOverrides geoOverride(String order) {
        Map<String, Object> spec = order == null ? Map.of("type", "geo_point") : Map.of("type", "geo_point", "order", order);
        return LanceOverrides.parseAttachClauses(Map.of("location", spec), null);
    }

    private static long encode(double lat, double lon) {
        return (((long) GeoEncodingUtils.encodeLatitude(lat)) << 32) | (GeoEncodingUtils.encodeLongitude(lon) & 0xFFFFFFFFL);
    }

    public void testStructGeoColumnClassifiesAsGeoPoint() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeGeoStructTable(scratchDir, "schema-" + getTestName().toLowerCase(Locale.ROOT));
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceFragmentSchema schema = LanceFragmentSchema.derive(
                dataset,
                "id",
                LanceEngineFactory.LancePrimaryKeyType.LONG,
                geoOverride(null),
                Collections.emptySet()
            );

            assertEquals(ColumnKind.GEO_POINT, schema.columnKind().get("location"));
            // The geo mapping replaces the object classification: no
            // dotted child paths, and the column is not a struct column.
            assertNull(schema.columnKind().get("location.lat"));
            assertNull(schema.columnKind().get("location.lon"));
            assertFalse(schema.structColumns().contains("location"));

            LanceFragmentSchema.GeoPointColumn spec = schema.geoPointColumns().get("location");
            assertNotNull(spec);
            assertTrue(spec.isStruct());
            assertEquals("lat", spec.latChild());
            assertEquals("lon", spec.lonChild());

            assertEquals(DocValuesType.SORTED_NUMERIC, schema.fieldInfos().fieldInfo("location").getDocValuesType());
            assertEquals(List.of("id", "location"), schema.takeColumns());
        }
    }

    public void testStructGeoColumnWithoutOverrideStaysObject() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeGeoStructTable(scratchDir, "schema-" + getTestName().toLowerCase(Locale.ROOT));
        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            LanceFragmentSchema schema = LanceFragmentSchema.derive(
                dataset,
                "id",
                LanceEngineFactory.LancePrimaryKeyType.LONG,
                LanceOverrides.EMPTY,
                Collections.emptySet()
            );
            assertNull(schema.columnKind().get("location"));
            assertEquals(ColumnKind.NUMERIC, schema.columnKind().get("location.lat"));
            assertEquals(ColumnKind.NUMERIC, schema.columnKind().get("location.lon"));
            assertTrue(schema.structColumns().contains("location"));
            assertTrue(schema.geoPointColumns().isEmpty());
        }
    }

    public void testFslGeoColumnOrders() throws Exception {
        Path scratchDir = createTempDir();
        String latLonUri = LanceTableFactory.writeGeoFslTable(scratchDir, "schema-latlon-" + getTestName().toLowerCase(Locale.ROOT), true);
        try (Dataset dataset = LanceRegistry.openDataset(latLonUri, StorageOptions.empty())) {
            LanceFragmentSchema schema = LanceFragmentSchema.derive(
                dataset,
                "id",
                LanceEngineFactory.LancePrimaryKeyType.LONG,
                geoOverride(null),
                Collections.emptySet()
            );
            LanceFragmentSchema.GeoPointColumn spec = schema.geoPointColumns().get("location");
            assertNotNull(spec);
            assertFalse(spec.isStruct());
            assertTrue("default order is lat_lon", spec.latFirst());
        }
        String lonLatUri = LanceTableFactory.writeGeoFslTable(scratchDir, "schema-lonlat-" + getTestName().toLowerCase(Locale.ROOT), false);
        try (Dataset dataset = LanceRegistry.openDataset(lonLatUri, StorageOptions.empty())) {
            LanceFragmentSchema schema = LanceFragmentSchema.derive(
                dataset,
                "id",
                LanceEngineFactory.LancePrimaryKeyType.LONG,
                geoOverride("lon_lat"),
                Collections.emptySet()
            );
            assertFalse(schema.geoPointColumns().get("location").latFirst());
        }
    }

    public void testStructGeoDocValuesServeEncodedLongs() throws Exception {
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeGeoStructTable(scratchDir, "leaf-" + getTestName().toLowerCase(Locale.ROOT));
        assertGeoDocValues(uri, geoOverride(null));
    }

    public void testFslLonLatGeoDocValuesServeEncodedLongs() throws Exception {
        // The FSL fixture stores (lon, lat); the declared order flips the
        // components back, so the doc values must match the same source
        // coordinates as the struct fixture.
        Path scratchDir = createTempDir();
        String uri = LanceTableFactory.writeGeoFslTable(scratchDir, "leaf-" + getTestName().toLowerCase(Locale.ROOT), false);
        assertGeoDocValues(uri, geoOverride("lon_lat"));
    }

    private void assertGeoDocValues(String uri, LanceOverrides overrides) throws Exception {
        Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty());
        List<Integer> fragmentIds = new ArrayList<>();
        for (Fragment fragment : dataset.getFragments()) {
            fragmentIds.add(fragment.getId());
        }
        double[][] expected = LanceTableFactory.geoStructFixtureValues();
        try (
            LanceDirectoryReader reader = LanceDirectoryReader.openForFragments(
                new ByteBuffersDirectory(),
                null,
                dataset,
                "id",
                LanceEngineFactory.LancePrimaryKeyType.LONG,
                overrides,
                fragmentIds
            )
        ) {
            assertEquals(1, reader.leaves().size());
            LeafReaderContext ctx = reader.leaves().get(0);
            SortedNumericDocValues values = ctx.reader().getSortedNumericDocValues("location");
            assertNotNull(values);
            for (int doc = 0; doc < expected.length; doc++) {
                boolean present = values.advanceExact(doc);
                if (expected[doc] == null) {
                    assertFalse("row " + doc + " is Arrow null and must be missing", present);
                    continue;
                }
                assertTrue("row " + doc + " must be present", present);
                assertEquals(1, values.docValueCount());
                assertEquals("row " + doc, encode(expected[doc][0], expected[doc][1]), values.nextValue());
            }
        }
    }
}
