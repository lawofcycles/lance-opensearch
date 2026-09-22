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
 * struct, so it must stay out of the mapping and {@code _source}) and
 * {@code flags} struct of {@code active} Bool; {@code meta.flags} is
 * Arrow null on row 3.
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

            // term on a struct child: region == east on rows 0, 2, 3.
            String termBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"meta.region\":\"east\"}}}"));
            assertEquals("term meta.region east: " + termBody, 3, extractIntPath(termBody, "hits", "total", "value"));

            // terms: east or south covers rows 0, 2, 3, 4.
            String termsBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"terms\":{\"meta.region\":[\"east\",\"south\"]}}}")
            );
            assertEquals("terms meta.region east,south: " + termsBody, 4, extractIntPath(termsBody, "hits", "total", "value"));

            // range on the double child: score >= 3.0 keeps rows 2..5.
            String rangeBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"range\":{\"meta.score\":{\"gte\":3.0}}}}"));
            assertEquals("range meta.score gte 3.0: " + rangeBody, 4, extractIntPath(rangeBody, "hits", "total", "value"));

            // exists on the nested boolean: flags is null on row 3, so
            // active exists on five rows.
            String existsBody = readAll(
                postJson("/" + indexName + "/_search", "{\"query\":{\"exists\":{\"field\":\"meta.flags.active\"}}}")
            );
            assertEquals("exists meta.flags.active: " + existsBody, 5, extractIntPath(existsBody, "hits", "total", "value"));

            // sort by the double child; descending puts row 5 first.
            // The aggregation clause routes the request through the
            // Lucene comparator over the child's doc values: the plain
            // sorted-page shape (sort without aggregations) currently
            // answers 400 because the sort pushdown's column resolution
            // in TransportLanceFragmentQueryAction (a file owned by
            // another in-flight change) resolves names with Arrow's
            // Schema.findField, which throws for any non-top-level
            // name instead of returning null. See the PR description.
            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"match_all\":{}},\"sort\":[{\"meta.score\":\"desc\"}],\"size\":6,"
                        + "\"aggs\":{\"ids\":{\"sum\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals("sort meta.score total: " + sortBody, 6, extractIntPath(sortBody, "hits", "total", "value"));
            assertEquals(
                "sort meta.score desc first hit must be id=5: " + sortBody,
                5,
                extractIntPath(sortBody, "hits", "hits", "0", "_source", "id")
            );
            assertEquals(
                "sort meta.score desc last hit must be id=0: " + sortBody,
                0,
                extractIntPath(sortBody, "hits", "hits", "5", "_source", "id")
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
            // avg(score) = (0 + 1.5 + 3 + 4.5 + 6 + 7.5) / 6 = 3.75.
            assertEquals("avg meta.score", 3.75d, extractDoublePath(aggBody, "aggregations", "avg_score", "value"), 1e-9);
            assertEquals("sum meta.score", 22.5d, extractDoublePath(aggBody, "aggregations", "sum_score", "value"), 1e-9);

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
            // rows 0, 2, 3: 0 + 3 + 4.5 = 7.5.
            assertEquals("sum over east rows", 7.5d, extractDoublePath(filteredAgg, "aggregations", "s", "value"), 1e-9);

            // GET by id renders the struct as a nested JSON object.
            String getBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_doc/0")));
            assertTrue(
                "GET _source must nest the struct: " + getBody,
                getBody.contains("\"meta\":{\"region\":\"east\",\"score\":0.0,\"flags\":{\"active\":true}}")
            );
            assertFalse("uint32 child must stay out of _source: " + getBody, getBody.contains("\"raw\""));

            // Row 3 carries the nested null: flags renders as JSON null.
            String nullBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"term\":{\"id\":3}},\"size\":1}"));
            assertEquals("term id=3 total: " + nullBody, 1, extractIntPath(nullBody, "hits", "total", "value"));
            assertTrue(
                "null nested struct must render as JSON null: " + nullBody,
                nullBody.contains("\"meta\":{\"region\":\"east\",\"score\":4.5,\"flags\":null}")
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }
}
