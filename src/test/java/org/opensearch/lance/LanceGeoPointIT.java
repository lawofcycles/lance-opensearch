/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;

/**
 * The attach body's {@code type: geo_point} override end to end, over
 * both accepted Arrow shapes. Mapping shape, {@code geo_distance}
 * around a Tokyo landmark, {@code geo_bounding_box} over Kanto,
 * {@code geo_polygon} over the same region, sort by
 * {@code _geo_distance} with sort values, the geo aggregations
 * ({@code geohash_grid}, {@code geotile_grid}, {@code geo_distance},
 * {@code geo_centroid}, {@code geo_bounds}, run by the fragment
 * executors' aggregators over the encoded doc values and compared with
 * the shard path), {@code _source} rendering the point as a
 * {@code {lat, lon}} object, and {@code exists} skipping the Arrow
 * null row. The FixedSizeList fixture is stored {@code (lon, lat)}
 * and attached with {@code order: lon_lat}, so it must answer the same
 * result set as the Struct fixture.
 *
 * <p>Fixture rows (see {@link LanceTableFactory#geoStructFixtureValues}):
 * row 0 is exactly (35.6812, 139.7671); rows 6, 2, 1 are ~1.8 / ~3.2 /
 * ~6.5 km away; row 3 (Yokohama) ~29 km; row 4 (Nikko) ~98 km; row 5
 * (Osaka) ~400 km; row 7 is Arrow null. Distances keep several km of
 * margin from every query bound, so encoding precision cannot flip a
 * hit.
 */
public class LanceGeoPointIT extends LanceRestTestCase {

    private static final String TOKYO = "{\"lat\":35.6812,\"lon\":139.7671}";

    public void testStructGeoPointEndToEnd() throws Exception {
        String suffix = "geostruct-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String indexName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeGeoStructTable(scratchDir, indexName);
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"location\":{\"type\":\"geo_point\"}}}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("location must map as geo_point: " + mapping, mapping.contains("\"location\":{\"type\":\"geo_point\""));
            assertTrue("meta must record the Arrow shape: " + mapping, mapping.contains("\"lance_arrow_type\":\"struct\""));
            assertTrue("meta must record the order: " + mapping, mapping.contains("\"lance_geo_order\":\"lat_lon\""));

            assertGeoQueries(indexName);
            assertGeoAggregations(indexName);

            // _source renders the point as a {lat, lon} object with the
            // original double values.
            String pinned = readAll(postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"term\":{\"id\":0}}}"));
            assertEquals(35.6812, extractDoublePath(pinned, "hits", "hits", "0", "_source", "location", "lat"), 0.0);
            assertEquals(139.7671, extractDoublePath(pinned, "hits", "hits", "0", "_source", "location", "lon"), 0.0);

            // The Arrow-null row keeps its key out of _source and out of
            // exists.
            String nullRow = readAll(postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"term\":{\"id\":7}}}"));
            assertEquals(1, extractIntPath(nullRow, "hits", "total", "value"));
            assertFalse("null location must not render: " + nullRow, nullRow.contains("\"location\""));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testFslLonLatGeoPointAnswersTheSameResults() throws Exception {
        String suffix = "geofsl-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String indexName = "demo-" + suffix;
        // Stored (lon, lat); the declared order flips the components back.
        String tableUri = LanceTableFactory.writeGeoFslTable(scratchDir, indexName, false);
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"location\":{\"type\":\"geo_point\",\"order\":\"lon_lat\"}}}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("location must map as geo_point: " + mapping, mapping.contains("\"location\":{\"type\":\"geo_point\""));
            assertTrue("meta must record the Arrow shape: " + mapping, mapping.contains("\"lance_arrow_type\":\"fsl2f64\""));
            assertTrue("meta must record the declared order: " + mapping, mapping.contains("\"lance_geo_order\":\"lon_lat\""));

            assertGeoQueries(indexName);

            // _source renders the same canonical object as the struct
            // shape: the declared order was applied on read.
            String pinned = readAll(postJson("/" + indexName + "/_search", "{\"size\":1,\"query\":{\"term\":{\"id\":0}}}"));
            assertEquals(35.6812, extractDoublePath(pinned, "hits", "hits", "0", "_source", "location", "lat"), 0.0);
            assertEquals(139.7671, extractDoublePath(pinned, "hits", "hits", "0", "_source", "location", "lon"), 0.0);
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /** The query shapes both fixtures must answer identically. */
    private void assertGeoQueries(String indexName) throws Exception {
        // geo_distance 10km around the landmark: rows 0 (0 km), 6
        // (~1.8 km), 2 (~3.2 km), 1 (~6.5 km).
        String within10 = readAll(
            postJson(
                "/" + indexName + "/_search",
                "{\"size\":10,\"query\":{\"geo_distance\":{\"distance\":\"10km\",\"location\":" + TOKYO + "}}}"
            )
        );
        assertEquals("10km around Tokyo must hold 4 rows: " + within10, 4, extractIntPath(within10, "hits", "total", "value"));

        String within5 = readAll(
            postJson(
                "/" + indexName + "/_search",
                "{\"size\":10,\"query\":{\"geo_distance\":{\"distance\":\"5km\",\"location\":" + TOKYO + "}}}"
            )
        );
        assertEquals("5km must drop the ~6.5km row: " + within5, 3, extractIntPath(within5, "hits", "total", "value"));

        // geo_bounding_box over Kanto: lat 35..36, lon 139..140.5 holds
        // rows 0, 1, 2, 3, 6; Nikko (36.56) and Osaka (135.5) stay out.
        String box = readAll(
            postJson(
                "/" + indexName + "/_search",
                "{\"size\":10,\"query\":{\"geo_bounding_box\":{\"location\":{"
                    + "\"top_left\":{\"lat\":36.0,\"lon\":139.0},\"bottom_right\":{\"lat\":35.0,\"lon\":140.5}}}}}"
            )
        );
        assertEquals("Kanto box must hold 5 rows: " + box, 5, extractIntPath(box, "hits", "total", "value"));

        // geo_polygon over the same rectangle answers the same subset.
        String polygon = readAll(
            postJson(
                "/" + indexName + "/_search",
                "{\"size\":10,\"query\":{\"geo_polygon\":{\"location\":{\"points\":["
                    + "{\"lat\":36.0,\"lon\":139.0},{\"lat\":36.0,\"lon\":140.5},"
                    + "{\"lat\":35.0,\"lon\":140.5},{\"lat\":35.0,\"lon\":139.0}]}}}}"
            )
        );
        assertEquals("polygon must equal the box subset: " + polygon, 5, extractIntPath(polygon, "hits", "total", "value"));

        // Sort by _geo_distance ascending over the present rows: 0, 6,
        // 2, 1, 3, 4, 5, with sort values populated (the landmark row's
        // distance from itself is bounded by encoding precision, well
        // under a metre).
        String sorted = readAll(
            postJson(
                "/" + indexName + "/_search",
                "{\"size\":7,\"query\":{\"exists\":{\"field\":\"location\"}},"
                    + "\"sort\":[{\"_geo_distance\":{\"location\":"
                    + TOKYO
                    + ",\"order\":\"asc\",\"unit\":\"m\"}}]}"
            )
        );
        assertEquals(7, extractIntPath(sorted, "hits", "total", "value"));
        int[] expectedOrder = { 0, 6, 2, 1, 3, 4, 5 };
        for (int rank = 0; rank < expectedOrder.length; rank++) {
            assertEquals(
                "rank " + rank + " of the distance sort: " + sorted,
                expectedOrder[rank],
                extractIntPath(sorted, "hits", "hits", Integer.toString(rank), "_source", "id")
            );
        }
        assertTrue("row 0 sorts at (near) zero distance: " + sorted, extractDoublePath(sorted, "hits", "hits", "0", "sort", "0") < 1.0);
        assertTrue("row 5 sorts hundreds of km out: " + sorted, extractDoublePath(sorted, "hits", "hits", "6", "sort", "0") > 100_000.0);

        // exists: 7 of 8 rows carry a location.
        String exists = readAll(postJson("/" + indexName + "/_search", "{\"size\":0,\"query\":{\"exists\":{\"field\":\"location\"}}}"));
        assertEquals("the Arrow-null row must be missing: " + exists, 7, extractIntPath(exists, "hits", "total", "value"));
    }

    /**
     * Geo aggregations run on the fragment path through the stock
     * aggregators over the encoded doc values, and answer what the shard
     * path answers.
     */
    private void assertGeoAggregations(String indexName) throws Exception {
        // geohash_grid at precision 1: every fixture point falls in the
        // "x" cell that covers Japan, and the null row contributes
        // nothing.
        String grid = readAll(
            postJson(
                "/" + indexName + "/_search",
                "{\"size\":0,\"aggs\":{\"cells\":{\"geohash_grid\":{\"field\":\"location\",\"precision\":1}}}}"
            )
        );
        assertEquals("one precision-1 cell: " + grid, "x", stringPath(grid, "aggregations", "cells", "buckets", "0", "key"));
        assertEquals(7, extractIntPath(grid, "aggregations", "cells", "buckets", "0", "doc_count"));

        // geo_distance bucketing around the landmark: [0,10km) holds 4
        // rows, [10km,50km) one (Yokohama), [50km,∞) two (Nikko, Osaka).
        String rings = readAll(
            postJson(
                "/" + indexName + "/_search",
                "{\"size\":0,\"aggs\":{\"rings\":{\"geo_distance\":{\"field\":\"location\",\"origin\":"
                    + TOKYO
                    + ","
                    + "\"unit\":\"km\",\"ranges\":[{\"to\":10},{\"from\":10,\"to\":50},{\"from\":50}]}}}}"
            )
        );
        assertEquals(4, extractIntPath(rings, "aggregations", "rings", "buckets", "0", "doc_count"));
        assertEquals(1, extractIntPath(rings, "aggregations", "rings", "buckets", "1", "doc_count"));
        assertEquals(2, extractIntPath(rings, "aggregations", "rings", "buckets", "2", "doc_count"));

        // The fragment executors served both (one executor on this
        // cluster), and every geo aggregation type answers what the
        // shard path answers over the same encoded doc values.
        long before = fragmentRequestsExecuted();
        for (String shape : new String[] {
            "{\"size\":0,\"aggs\":{\"c\":{\"geo_centroid\":{\"field\":\"location\"}}}}",
            "{\"size\":0,\"aggs\":{\"b\":{\"geo_bounds\":{\"field\":\"location\"}}}}",
            "{\"size\":0,\"aggs\":{\"g\":{\"geohash_grid\":{\"field\":\"location\",\"precision\":3}}}}",
            "{\"size\":0,\"aggs\":{\"g\":{\"geotile_grid\":{\"field\":\"location\",\"precision\":8}}}}",
            "{\"size\":0,\"query\":{\"range\":{\"id\":{\"gte\":1}}},\"aggs\":{\"rings\":{\"geo_distance\":{\"field\":\"location\",\"origin\":"
                + TOKYO
                + ",\"unit\":\"km\",\"ranges\":[{\"to\":10},{\"from\":10}]},\"aggs\":{\"c\":{\"geo_centroid\":{\"field\":\"location\"}}}}}}" }) {
            Map<String, Object> fragmentPath = parseJson(readAll(postJson("/" + indexName + "/_search", shape)));
            Map<String, Object> shardPath = parseJson(
                readAll(postJson("/" + indexName + "/_search?request_cache=false", onShardPath(shape)))
            );
            assertEquals(shape, withoutShardPathOracle(shardPath.get("aggregations")), fragmentPath.get("aggregations"));
        }
        assertEquals("every geo aggregation ran on the fragment path", before + 5, fragmentRequestsExecuted());
    }
}
