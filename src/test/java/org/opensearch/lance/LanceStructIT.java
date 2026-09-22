/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;

/**
 * Arrow Struct columns surfaced as OpenSearch {@code object} fields:
 * the derived mapping carries {@code properties} per child (recursing
 * into nested structs), the fragment reader serves the children as doc
 * values under their dotted path, {@code _source} renders the struct
 * as a JSON object, and the scalar query shapes (term / terms / range
 * / exists / sort / aggregations) resolve on {@code parent.child}
 * field names.
 *
 * <p>Fixture ({@link LanceTableFactory#writeStructTable}): six rows,
 * {@code id} int32 PK, {@code meta} struct of {@code region} Utf8,
 * {@code score} Float64, {@code raw} UInt32 (unsupported inside a
 * struct, so it must stay out of the mapping and {@code _source}),
 * {@code flags} struct of {@code active} Bool, and {@code audit}, a
 * struct with no supported children (skipped whole); on row 3
 * {@code meta.flags} is Arrow null and {@code meta.score} is a null
 * scalar leaf inside the present struct.
 */
public class LanceStructIT extends LanceRestTestCase {

    public void testStructColumnMapsToObjectAndAnswersQueries() throws Exception {
        String suffix = "struct-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeStructTable(scratchDir, tableName, 0);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(
                "attach on struct table failed: " + readAll(attach),
                RestStatus.OK.getStatus(),
                attach.getStatusLine().getStatusCode()
            );

            // Mapping: meta is an object whose properties carry the
            // supported children; the unsupported uint32 child stays out.
            String mappingBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("meta must map as object: " + mappingBody, mappingBody.contains("\"meta\":{\"properties\":{"));
            assertTrue("meta.region must map as keyword: " + mappingBody, mappingBody.contains("\"region\":{\"type\":\"keyword\""));
            assertTrue("meta.score must map as double: " + mappingBody, mappingBody.contains("\"score\":{\"type\":\"double\""));
            assertTrue("meta.flags must nest an object: " + mappingBody, mappingBody.contains("\"flags\":{\"properties\":{"));
            assertTrue("meta.flags.active must map as boolean: " + mappingBody, mappingBody.contains("\"active\":{\"type\":\"boolean\""));
            assertFalse("uint32 child must stay unmapped: " + mappingBody, mappingBody.contains("\"raw\""));
            assertFalse("all-unsupported nested struct must stay unmapped: " + mappingBody, mappingBody.contains("\"audit\""));

            // term on a struct child: region == east on rows 0, 2, 3.
            String termBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"meta.region\":\"east\"}}}"));
            assertEquals("term meta.region east: " + termBody, 3, extractIntPath(termBody, "hits", "total", "value"));

            // terms: east or south covers rows 0, 2, 3, 4.
            String termsBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"terms\":{\"meta.region\":[\"east\",\"south\"]}}}")
            );
            assertEquals("terms meta.region east,south: " + termsBody, 4, extractIntPath(termsBody, "hits", "total", "value"));

            // range on the double child: scores 3.0, 6.0, 7.5 (rows 2, 4,
            // 5) pass; row 3's null score never matches.
            String rangeBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"meta.score\":{\"gte\":3.0}}}}"));
            assertEquals("range meta.score gte 3.0: " + rangeBody, 3, extractIntPath(rangeBody, "hits", "total", "value"));

            // exists on the nested boolean: flags is null on row 3, so
            // active exists on five rows.
            String existsBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"exists\":{\"field\":\"meta.flags.active\"}}}")
            );
            assertEquals("exists meta.flags.active: " + existsBody, 5, extractIntPath(existsBody, "hits", "total", "value"));

            // Plain sorted page (sort without aggregations) by the double
            // child: routes through resolvePushdownOrderings, whose column
            // resolution must return null for a dotted name (instead of
            // throwing, as Arrow's Schema.findField does) so the request
            // falls back to the Lucene comparator over the child's doc
            // values. Descending: 7.5, 6.0, 3.0, 1.5, 0.0, then row 3
            // whose null score sorts last; the sort values echo the
            // decoded doubles.
            String sortBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}},\"sort\":[{\"meta.score\":\"desc\"}],\"size\":6}")
            );
            assertEquals("sort meta.score total: " + sortBody, 6, extractIntPath(sortBody, "hits", "total", "value"));
            assertEquals(
                "sort meta.score desc first hit must be id=5: " + sortBody,
                5,
                extractIntPath(sortBody, "hits", "hits", "0", "_source", "id")
            );
            assertEquals(
                "sort meta.score desc fifth hit must be id=0: " + sortBody,
                0,
                extractIntPath(sortBody, "hits", "hits", "4", "_source", "id")
            );
            assertEquals("null score must sort last: " + sortBody, 3, extractIntPath(sortBody, "hits", "hits", "5", "_source", "id"));
            assertEquals(
                "first sort value must be 7.5: " + sortBody,
                7.5d,
                extractDoublePath(sortBody, "hits", "hits", "0", "sort", "0"),
                1e-9
            );
            assertEquals(
                "fifth sort value must be 0.0: " + sortBody,
                0.0d,
                extractDoublePath(sortBody, "hits", "hits", "4", "sort", "0"),
                1e-9
            );

            // The same sort combined with an aggregation takes the shape
            // that skips the sort pushdown probe entirely; order must not
            // change.
            String sortAggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match_all\":{}},\"sort\":[{\"meta.score\":\"desc\"}],\"size\":6,"
                        + "\"aggs\":{\"ids\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("sort+agg total: " + sortAggBody, 6, extractIntPath(sortAggBody, "hits", "total", "value"));
            assertEquals(
                "sort+agg first hit must be id=5: " + sortAggBody,
                5,
                extractIntPath(sortAggBody, "hits", "hits", "0", "_source", "id")
            );

            // terms aggregation on the keyword child: east 3, west 2, south 1.
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"aggs\":{\"regions\":{\"terms\":{\"field\":\"meta.region\",\"order\":{\"_key\":\"asc\"}}},"
                        + "\"avg_score\":{\"avg\":{\"field\":\"meta.score\"}},"
                        + "\"sum_score\":{\"sum\":{\"field\":\"meta.score\"}}}}"
                )
            );
            assertEquals(
                "first bucket key asc is east: " + aggBody,
                3,
                extractIntPath(aggBody, "aggregations", "regions", "buckets", "0", "doc_count")
            );
            assertTrue("bucket keys must include east/south/west: " + aggBody, aggBody.contains("\"key\":\"east\""));
            assertTrue("bucket keys must include south: " + aggBody, aggBody.contains("\"key\":\"south\""));
            assertTrue("bucket keys must include west: " + aggBody, aggBody.contains("\"key\":\"west\""));
            // avg(score) = (0 + 1.5 + 3 + 6 + 7.5) / 5 = 3.6; row 3's
            // null score contributes to neither the sum nor the count.
            assertEquals("avg meta.score", 3.6d, extractDoublePath(aggBody, "aggregations", "avg_score", "value"), 1e-9);
            assertEquals("sum meta.score", 18.0d, extractDoublePath(aggBody, "aggregations", "sum_score", "value"), 1e-9);

            // A struct-child filter combined with a struct-child metric:
            // exercises the Lance SQL path expression for dotted fields
            // (meta.region = 'east' as a nested field access) end to end.
            String filteredAgg = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"term\":{\"meta.region\":\"east\"}},\"size\":0,"
                        + "\"aggs\":{\"s\":{\"sum\":{\"field\":\"meta.score\"}}}}"
                )
            );
            assertEquals("filtered total: " + filteredAgg, 3, extractIntPath(filteredAgg, "hits", "total", "value"));
            // rows 0, 2, 3: 0 + 3 + null = 3.0.
            assertEquals("sum over east rows", 3.0d, extractDoublePath(filteredAgg, "aggregations", "s", "value"), 1e-9);

            // GET by id renders the struct as a nested JSON object; the
            // all-unsupported audit struct stays out.
            String getBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_doc/0")));
            assertTrue(
                "GET _source must nest the struct: " + getBody,
                getBody.contains("\"meta\":{\"region\":\"east\",\"score\":0.0,\"flags\":{\"active\":true}}")
            );
            assertFalse("uint32 child must stay out of _source: " + getBody, getBody.contains("\"raw\""));
            assertFalse("all-unsupported nested struct must stay out of _source: " + getBody, getBody.contains("\"audit\""));

            // Row 3 carries the nested nulls: the null flags struct and
            // the null score leaf inside the present struct both render
            // as JSON null.
            String nullBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"id\":3}},\"size\":1}"));
            assertEquals("term id=3 total: " + nullBody, 1, extractIntPath(nullBody, "hits", "total", "value"));
            assertTrue(
                "null score leaf and null nested struct must render as JSON null: " + nullBody,
                nullBody.contains("\"meta\":{\"region\":\"east\",\"score\":null,\"flags\":null}")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }
}
