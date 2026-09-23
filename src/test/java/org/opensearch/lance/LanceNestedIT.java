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
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * Arrow {@code List<Struct>} columns surfaced as OpenSearch
 * {@code nested} fields: the derived mapping carries
 * {@code "type": "nested"} with the element children as properties,
 * every list element becomes a hidden child doc before its row's
 * parent doc, a {@code nested} query matches several attributes of the
 * same element (and not the same attributes spread across elements),
 * {@code _source} and GET render the array, the {@code nested} and
 * {@code reverse_nested} aggregations answer the shard path's buckets
 * from the fragment executors, and counts stay on the parents.
 *
 * <p>Fixture ({@link LanceTableFactory#writeNestedTable}): six rows,
 * {@code id} int32 PK, {@code title} Utf8 keyword, {@code items}
 * list of {@code struct<color, size, qty>}. The element sets per row
 * are documented on the factory; row 5 is deleted through a deletion
 * file before attach, and row 3 carries {@code red} and {@code large}
 * in different elements, the cross element case a nested query must
 * not match.
 */
public class LanceNestedIT extends LanceRestTestCase {

    public void testNestedColumnMapsAndAnswersQueries() throws Exception {
        String suffix = "nested-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        LanceTableFactory.writeNestedTable(scratchDir, tableName, 0);
        String tableUri = scratchDir.resolve(tableName + ".lance").toString();
        LanceTableFactory.deleteRows(tableUri, "id = 5");
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(
                "attach on nested table failed: " + readAll(attach),
                RestStatus.OK.getStatus(),
                attach.getStatusLine().getStatusCode()
            );

            // Mapping: items is nested with the element children as
            // properties.
            String mappingBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("items must map as nested: " + mappingBody, mappingBody.contains("\"items\":{\"type\":\"nested\""));
            assertTrue("items.color must map as keyword: " + mappingBody, mappingBody.contains("\"color\":{\"type\":\"keyword\""));
            assertTrue("items.qty must map as integer: " + mappingBody, mappingBody.contains("\"qty\":{\"type\":\"integer\""));

            // The motivating shape: color=red AND size=large inside the
            // same element. Row 1 has the element (red, large); row 3 has
            // red and large in different elements and must not match; row
            // 5 would match but is deleted.
            String sameElement = "{\"query\":{\"nested\":{\"path\":\"items\",\"query\":{\"bool\":{\"must\":["
                + "{\"term\":{\"items.color\":\"red\"}},{\"term\":{\"items.size\":\"large\"}}]}}}}}";
            String sameElementBody = readAll(postJson("/" + indexName + "/_search", sameElement));
            assertEquals("nested red+large total: " + sameElementBody, 1, extractIntPath(sameElementBody, "hits", "total", "value"));
            assertEquals(
                "nested red+large must hit row 1: " + sameElementBody,
                1,
                extractIntPath(sameElementBody, "hits", "hits", "0", "_source", "id")
            );

            // A single-attribute nested term: red appears in rows 0, 1, 3
            // (row 5 deleted).
            String redBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"nested\":{\"path\":\"items\",\"query\":{\"term\":{\"items.color\":\"red\"}}}},\"size\":0}"
                )
            );
            assertEquals("nested red total: " + redBody, 3, extractIntPath(redBody, "hits", "total", "value"));

            // range on the numeric child: qty >= 5 lives in rows 3 (5, 6)
            // and 4 (7); row 5's 8 and 9 are deleted.
            String rangeBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"nested\":{\"path\":\"items\",\"query\":{\"range\":{\"items.qty\":{\"gte\":5}}}}},\"size\":0}"
                )
            );
            assertEquals("nested qty>=5 total: " + rangeBody, 2, extractIntPath(rangeBody, "hits", "total", "value"));

            // exists on a child path: rows with at least one element
            // whose qty is set are 0, 1, 3, 4 (row 2 has zero elements,
            // row 5 is deleted).
            String existsBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"nested\":{\"path\":\"items\",\"query\":{\"exists\":{\"field\":\"items.qty\"}}}},\"size\":0}"
                )
            );
            assertEquals("nested exists qty total: " + existsBody, 4, extractIntPath(existsBody, "hits", "total", "value"));

            // Top-level term combined with a nested clause in one bool:
            // title=beta rows are 1 and 3; blue appears in both.
            String boolBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"title\":\"beta\"}},"
                        + "{\"nested\":{\"path\":\"items\",\"query\":{\"term\":{\"items.color\":\"blue\"}}}}]}},\"size\":0}"
                )
            );
            assertEquals("beta+nested blue total: " + boolBody, 2, extractIntPath(boolBody, "hits", "total", "value"));

            // match_all counts parents, not child docs, and the deleted
            // row stays out.
            String matchAllBody = readAll(postJson("/" + indexName + "/_search", "{\"query\":{\"match_all\":{}},\"size\":10}"));
            assertEquals("match_all total counts parents: " + matchAllBody, 5, extractIntPath(matchAllBody, "hits", "total", "value"));

            // _count with a nested query agrees with hits.total.
            String countBody = readAll(
                postJson(
                    "/" + indexName + "/_count",
                    "{\"query\":{\"nested\":{\"path\":\"items\",\"query\":{\"term\":{\"items.color\":\"red\"}}}}}"
                )
            );
            assertEquals("_count nested red: " + countBody, 3, extractIntPath(countBody, "count"));

            // terms aggregation on a top-level column under a nested
            // filter query: red rows are 0 (alpha), 1 (beta), 3 (beta).
            String aggBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":0,\"query\":{\"nested\":{\"path\":\"items\",\"query\":{\"term\":{\"items.color\":\"red\"}}}},"
                        + "\"aggs\":{\"titles\":{\"terms\":{\"field\":\"title\",\"order\":{\"_key\":\"asc\"}}}}}"
                )
            );
            assertEquals(
                "alpha bucket under nested filter: " + aggBody,
                1,
                extractIntPath(aggBody, "aggregations", "titles", "buckets", "0", "doc_count")
            );
            assertEquals(
                "beta bucket under nested filter: " + aggBody,
                2,
                extractIntPath(aggBody, "aggregations", "titles", "buckets", "1", "doc_count")
            );

            // nested and reverse_nested aggregations run on the fragment
            // path (the leaf reader carries the parent join the stock
            // aggregators read) and answer what the shard path answers:
            // seven elements over the five surviving rows, red on rows 0,
            // 1 and 3 (titles alpha, beta), qty summing to 28.
            String nestedAggs = "{\"size\":0,\"aggs\":{\"n\":{\"nested\":{\"path\":\"items\"},\"aggs\":{"
                + "\"colors\":{\"terms\":{\"field\":\"items.color\",\"order\":{\"_key\":\"asc\"}},"
                + "\"aggs\":{\"back\":{\"reverse_nested\":{},\"aggs\":{\"titles\":{\"terms\":{\"field\":\"title\",\"order\":{\"_key\":\"asc\"}}}}}}},"
                + "\"qty\":{\"sum\":{\"field\":\"items.qty\"}}}}}}";
            long before = fragmentRequestsExecuted();
            String nestedAggBody = readAll(postJson("/" + indexName + "/_search", nestedAggs));
            assertEquals("the fragment path served the nested aggregation", before + 1, fragmentRequestsExecuted());
            assertEquals(7, extractIntPath(nestedAggBody, "aggregations", "n", "doc_count"));
            assertEquals(28.0d, extractDoublePath(nestedAggBody, "aggregations", "n", "qty", "value"), 0d);
            Map<String, Object> shardPath = parseJson(
                readAll(postJson("/" + indexName + "/_search?request_cache=false", onShardPath(nestedAggs)))
            );
            assertEquals(withoutShardPathOracle(shardPath.get("aggregations")), parseJson(nestedAggBody).get("aggregations"));
            String filteredNested =
                "{\"size\":0,\"query\":{\"term\":{\"title\":\"alpha\"}},\"aggs\":{\"n\":{\"nested\":{\"path\":\"items\"},"
                    + "\"aggs\":{\"colors\":{\"terms\":{\"field\":\"items.color\",\"order\":{\"_key\":\"asc\"}}}}}}}";
            Map<String, Object> filteredShardPath = parseJson(
                readAll(postJson("/" + indexName + "/_search?request_cache=false", onShardPath(filteredNested)))
            );
            assertEquals(
                withoutShardPathOracle(filteredShardPath.get("aggregations")),
                parseJson(readAll(postJson("/" + indexName + "/_search", filteredNested))).get("aggregations")
            );

            // sort + nested query: the sort stays on the Lucene comparator
            // (Lance cannot evaluate the nested predicate); ids desc are
            // 3, 1, 0.
            String sortBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"nested\":{\"path\":\"items\",\"query\":{\"term\":{\"items.color\":\"red\"}}}},"
                        + "\"sort\":[{\"id\":\"desc\"}],\"size\":3}"
                )
            );
            assertEquals("sorted nested first id: " + sortBody, 3, extractIntPath(sortBody, "hits", "hits", "0", "_source", "id"));
            assertEquals("sorted nested last id: " + sortBody, 0, extractIntPath(sortBody, "hits", "hits", "2", "_source", "id"));

            // GET by id renders the array of element objects in _source.
            String getBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_doc/1")));
            assertTrue("GET _source must carry the items array: " + getBody, getBody.contains("\"items\":[{"));
            assertTrue("GET _source must carry the first element: " + getBody, getBody.contains("\"color\":\"red\""));
            assertEquals("GET _source qty of first element: " + getBody, 2, extractIntPath(getBody, "_source", "items", "0", "qty"));
            assertEquals("GET _source qty of third element: " + getBody, 4, extractIntPath(getBody, "_source", "items", "2", "qty"));

            // The zero-element row renders an empty array, and the
            // deleted row is gone.
            String zeroBody = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_doc/2")));
            assertTrue("zero-element row must render an empty array: " + zeroBody, zeroBody.contains("\"items\":[]"));
            ResponseException deleted = expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("GET", "/" + indexName + "/_doc/5"))
            );
            assertEquals(RestStatus.NOT_FOUND.getStatus(), deleted.getResponse().getStatusLine().getStatusCode());

            // inner_hits is refused loudly rather than silently dropped.
            ResponseException innerHits = expectThrows(
                ResponseException.class,
                () -> postJson(
                    "/" + indexName + "/_search",
                    "{\"query\":{\"nested\":{\"path\":\"items\",\"query\":{\"term\":{\"items.color\":\"red\"}},\"inner_hits\":{}}}}"
                )
            );
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), innerHits.getResponse().getStatusLine().getStatusCode());
            String innerHitsBody = readAll(innerHits.getResponse());
            assertTrue("inner_hits refusal must name the feature: " + innerHitsBody, innerHitsBody.contains("inner_hits"));

            // Stats: shard_reader_rows counts visible parents (5 live
            // rows), nested_docs the hidden child docs (7 elements of
            // the live rows), and the two sum to the reader's numDocs,
            // which is what {index}/_stats reports as docs.count.
            String docStats = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_stats/docs")));
            int numDocs = extractIntPath(docStats, "indices", indexName, "primaries", "docs", "count");
            assertEquals("docs.count counts parents plus child docs: " + docStats, 12, numDocs);
            String lanceStats = readAll(client().performRequest(new Request("GET", "/_lance/stats")));
            Map<String, Object> nodes = castMap(parseJson(lanceStats).get("nodes"));
            Map<String, Object> indices = castMap(castMap(nodes.values().iterator().next()).get("indices"));
            Map<String, Object> indexStats = castMap(indices.get(indexName));
            int shardReaderRows = ((Number) indexStats.get("shard_reader_rows")).intValue();
            int nestedDocs = ((Number) indexStats.get("nested_docs")).intValue();
            assertEquals("shard_reader_rows counts live parents: " + lanceStats, 5, shardReaderRows);
            assertEquals("nested_docs counts live child docs: " + lanceStats, 7, nestedDocs);
            assertEquals(
                "nested_docs plus shard_reader_rows is the reader's numDocs: " + lanceStats,
                numDocs,
                shardReaderRows + nestedDocs
            );
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Object value) {
        assertTrue("expected a JSON object, got: " + value, value instanceof Map);
        return (Map<String, Object>) value;
    }
}
