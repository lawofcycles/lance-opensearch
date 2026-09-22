/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.rest.RestStatus;

/**
 * The attach body's {@code type: ip} and {@code type: wildcard}
 * overrides end to end. For ip: mapping shape
 * on a Utf8 and a {@code List<Utf8>} column, term (exact and CIDR),
 * terms, range, exists, sort and terms aggregation through the stock
 * {@code IpFieldType} over the encoded doc values, {@code _source}
 * rendering the original strings, the invalid-string row served as
 * missing, the raw-string keyword sub-field on the ip column,
 * {@code _count} with an ip range (the Lucene filter path, since ip
 * predicates never push to Lance SQL), and the namespace register's
 * lenient application of an ip override. For wildcard: the keyword
 * mapping with the declared type in the field meta, and wildcard /
 * prefix / regexp / term answers equal to the same column mapped
 * {@code keyword} under a second index name.
 *
 * <p>Fixture rows (see {@code LanceTableFactory.writeIpTable}): the ip
 * column holds 10.0.0.4, 10.0.0.30, 192.168.1.7, 2001:db8::1,
 * ::ffff:10.0.0.2 and the invalid string "not-an-ip". The 10.0.0.4 /
 * 10.0.0.30 pair orders one way as strings and the other way as
 * addresses, so the range and sort assertions prove the encoded form
 * is what compares. The path column holds one file-path-like string
 * per row.
 */
public class LanceIpWildcardIT extends LanceRestTestCase {

    private static final String OVERRIDES_CLAUSE = "\"overrides\":{"
        + "\"ip\":{\"type\":\"ip\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}},"
        + "\"addrs\":{\"type\":\"ip\"}}";

    public void testIpOverrideServesQueriesThroughEncodedDocValues() throws Exception {
        String suffix = "ip-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeIpTable(scratchDir, tableName);
        String indexName = tableName;
        try {
            Response attach = postJson("/_lance/attach", "{\"table\":\"" + tableUri + "\"," + OVERRIDES_CLAUSE + "}");
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("ip must map as ip: " + mapping, mapping.contains("\"ip\":{\"type\":\"ip\""));
            assertTrue("ip meta must keep the Arrow type: " + mapping, mapping.contains("\"lance_arrow_type\":\"Utf8\""));
            assertTrue("addrs must map as ip: " + mapping, mapping.contains("\"addrs\":{\"type\":\"ip\""));
            assertTrue("ip.raw sub-field must persist: " + mapping, mapping.contains("\"fields\":{\"raw\":{\"type\":\"keyword\""));

            // Exact term through the encoded doc values: the stored
            // IPv4-mapped form ::ffff:10.0.0.2 equals 10.0.0.2.
            String termBody = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"ip\":\"10.0.0.2\"}}}"));
            assertEquals("canonical term must match the mapped form: " + termBody, 1, extractIntPath(termBody, "hits", "total", "value"));
            assertEquals(4, extractIntPath(termBody, "hits", "hits", "0", "_source", "id"));
            // _source renders the original string, not the canonical form.
            assertEquals("::ffff:10.0.0.2", stringPath(termBody, "hits", "hits", "0", "_source", "ip"));

            // CIDR notation on a term query: 10.0.0.0/8 holds rows 0, 1
            // and the mapped row 4.
            String cidrBody = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"ip\":\"10.0.0.0/8\"}}}"));
            assertEquals("CIDR term must match three rows: " + cidrBody, 3, extractIntPath(cidrBody, "hits", "total", "value"));

            // terms: one IPv4, one IPv6.
            String termsBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"terms\":{\"ip\":[\"10.0.0.4\",\"2001:db8::1\"]}}}")
            );
            assertEquals(2, extractIntPath(termsBody, "hits", "total", "value"));

            // Range with explicit bounds compares addresses, not strings:
            // 10.0.0.30 >= 10.0.0.5 numerically although "10.0.0.30" <
            // "10.0.0.5" lexically, so the two hits are rows 1 and 2.
            String rangeBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":10,\"query\":{\"range\":{\"ip\":{\"gte\":\"10.0.0.5\",\"lte\":\"192.168.255.255\"}}}}"
                )
            );
            assertEquals(
                "address-order range must return rows 1 and 2: " + rangeBody,
                2,
                extractIntPath(rangeBody, "hits", "total", "value")
            );

            // exists: the invalid string row is missing.
            String existsBody = readAll(postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"exists\":{\"field\":\"ip\"}}}"));
            assertEquals("invalid row must be absent from exists: " + existsBody, 5, extractIntPath(existsBody, "hits", "total", "value"));

            // Ascending sort follows address order (10.0.0.2 mapped row
            // first, 10.0.0.4 before 10.0.0.30). The exists query keeps
            // the invalid row out of the page.
            String sortBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":6,\"query\":{\"exists\":{\"field\":\"ip\"}},\"sort\":[{\"ip\":\"asc\"}]}")
            );
            assertEquals(5, extractIntPath(sortBody, "hits", "total", "value"));
            assertEquals(4, extractIntPath(sortBody, "hits", "hits", "0", "_source", "id"));
            assertEquals(0, extractIntPath(sortBody, "hits", "hits", "1", "_source", "id"));
            assertEquals(1, extractIntPath(sortBody, "hits", "hits", "2", "_source", "id"));
            assertEquals(2, extractIntPath(sortBody, "hits", "hits", "3", "_source", "id"));
            assertEquals(3, extractIntPath(sortBody, "hits", "hits", "4", "_source", "id"));

            // The invalid row sorts last as a missing value; the metric
            // aggregation makes this a different request shape than the
            // plain page below, and its _source keeps the original
            // string.
            String sortAllBody = readAll(
                postJson(
                    "/" + indexName + "/_search",
                    "{\"size\":6,\"query\":{\"match_all\":{}},\"sort\":[{\"ip\":\"asc\"}],"
                        + "\"aggs\":{\"rows\":{\"value_count\":{\"field\":\"id\"}}}}"
                )
            );
            assertEquals(6, extractIntPath(sortAllBody, "hits", "total", "value"));
            assertEquals(
                "missing (invalid) row sorts last: " + sortAllBody,
                5,
                extractIntPath(sortAllBody, "hits", "hits", "5", "_source", "id")
            );
            // The invalid row is present in _source with its original string.
            assertEquals("not-an-ip", stringPath(sortAllBody, "hits", "hits", "5", "_source", "ip"));

            // A plain sorted page (match_all, size above zero, no
            // aggregation) is the shape the Lance sorted-scan pushdown
            // takes; the pushdown declines ip clauses so this page also
            // comes from the Lucene collector, in address order, with
            // sort values formatted as addresses.
            String plainSortBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":6,\"query\":{\"match_all\":{}},\"sort\":[{\"ip\":\"asc\"}]}")
            );
            assertEquals(6, extractIntPath(plainSortBody, "hits", "total", "value"));
            assertEquals(4, extractIntPath(plainSortBody, "hits", "hits", "0", "_source", "id"));
            assertEquals(0, extractIntPath(plainSortBody, "hits", "hits", "1", "_source", "id"));
            assertEquals(1, extractIntPath(plainSortBody, "hits", "hits", "2", "_source", "id"));
            assertEquals(2, extractIntPath(plainSortBody, "hits", "hits", "3", "_source", "id"));
            assertEquals(3, extractIntPath(plainSortBody, "hits", "hits", "4", "_source", "id"));
            assertEquals(5, extractIntPath(plainSortBody, "hits", "hits", "5", "_source", "id"));
            assertEquals("10.0.0.2", stringPath(plainSortBody, "hits", "hits", "0", "sort", "0"));
            assertEquals("10.0.0.4", stringPath(plainSortBody, "hits", "hits", "1", "sort", "0"));
            assertEquals("10.0.0.30", stringPath(plainSortBody, "hits", "hits", "2", "sort", "0"));

            // terms aggregation formats keys as canonical addresses; five
            // distinct values, the invalid row in no bucket.
            String aggBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":0,\"aggs\":{\"per_ip\":{\"terms\":{\"field\":\"ip\",\"size\":10}}}}")
            );
            List<String> buckets = bucketsOf(aggBody, "per_ip");
            assertEquals("five distinct addresses: " + aggBody, 5, buckets.size());
            assertTrue("mapped form must bucket under its canonical key: " + buckets, buckets.contains("10.0.0.2=1"));
            assertTrue(buckets.toString(), buckets.contains("10.0.0.30=1"));
            assertTrue(buckets.toString(), buckets.contains("2001:db8::1=1"));

            // The raw keyword sub-field matches the stored string exactly:
            // no canonicalisation, and the invalid row is reachable.
            String rawMapped = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"ip.raw\":\"::ffff:10.0.0.2\"}}}")
            );
            assertEquals(1, extractIntPath(rawMapped, "hits", "total", "value"));
            String rawCanonical = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"ip.raw\":\"10.0.0.2\"}}}")
            );
            assertEquals("raw view must not canonicalise: " + rawCanonical, 0, extractIntPath(rawCanonical, "hits", "total", "value"));
            String rawInvalid = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"ip.raw\":\"not-an-ip\"}}}")
            );
            assertEquals(1, extractIntPath(rawInvalid, "hits", "total", "value"));
            assertEquals(5, extractIntPath(rawInvalid, "hits", "hits", "0", "_source", "id"));

            // The multi-valued ip column: both rows carrying 2001:db8::1
            // match, the CIDR term reaches the element inside each list,
            // and the row whose only extra element is invalid ("bogus")
            // still exists through its valid element.
            String addrsTerm = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"addrs\":\"2001:db8::1\"}}}")
            );
            assertEquals(2, extractIntPath(addrsTerm, "hits", "total", "value"));
            String addrsCidr = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"term\":{\"addrs\":\"10.0.0.0/8\"}}}")
            );
            assertEquals("rows 0, 1 and 5 carry a 10.x element: " + addrsCidr, 3, extractIntPath(addrsCidr, "hits", "total", "value"));
            String addrsExists = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"exists\":{\"field\":\"addrs\"}}}")
            );
            assertEquals(
                "empty list row 4 is missing, bogus-element row 2 exists: " + addrsExists,
                5,
                extractIntPath(addrsExists, "hits", "total", "value")
            );

            // _count with an ip range exercises the Lucene filter path:
            // ip predicates never push to Lance SQL, so the scan runs
            // unfiltered and IndexSearcher.count applies the doc-value
            // query.
            String countBody = readAll(
                postJson("/" + indexName + "/_count", "{\"query\":{\"range\":{\"ip\":{\"gte\":\"10.0.0.0\",\"lte\":\"10.255.255.255\"}}}}")
            );
            assertEquals("CIDR-shaped range must count three rows: " + countBody, 3, extractIntPath(countBody, "count"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    /** The sorted list of {@code _source.id} values of every hit, for cross-index response comparison. */
    private static List<Integer> sortedSourceIds(String searchBody) throws Exception {
        List<Integer> ids = new java.util.ArrayList<>();
        for (java.util.Map<String, Object> hit : hitsOf(searchBody)) {
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> source = (java.util.Map<String, Object>) hit.get("_source");
            ids.add(((Number) source.get("id")).intValue());
        }
        java.util.Collections.sort(ids);
        return ids;
    }

    public void testWildcardOverrideServesKeywordSemantics() throws Exception {
        // `type: wildcard` is served by the keyword doc values path: the
        // mapping is keyword with the declared type recorded in the
        // field meta, and every pattern query answers exactly what the
        // same column mapped `keyword` answers (the oracle index below).
        String suffix = "wc-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeIpTable(scratchDir, tableName);
        String wildcardIndex = tableName;
        String keywordIndex = "oracle-" + suffix;
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"path\":{\"type\":\"wildcard\"}}}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            Response attachOracle = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"name\":\"" + keywordIndex + "\",\"overrides\":{\"path\":{\"type\":\"keyword\"}}}"
            );
            assertEquals(RestStatus.OK.getStatus(), attachOracle.getStatusLine().getStatusCode());

            String mapping = readAll(client().performRequest(new Request("GET", "/" + wildcardIndex + "/_mapping")));
            assertTrue("path must map as keyword: " + mapping, mapping.contains("\"path\":{\"type\":\"keyword\""));
            assertTrue("meta must record the declared type: " + mapping, mapping.contains("\"lance_override_type\":\"wildcard\""));

            String wildcardQuery = "{\"size\":10,\"query\":{\"wildcard\":{\"path\":{\"value\":\"/var/log/*\"}}}}";
            String prefixQuery = "{\"size\":10,\"query\":{\"prefix\":{\"path\":{\"value\":\"/var\"}}}}";
            String regexpQuery = "{\"size\":10,\"query\":{\"regexp\":{\"path\":{\"value\":\"/usr/.*\"}}}}";
            String termQuery = "{\"size\":10,\"query\":{\"term\":{\"path\":\"/etc/config.yaml\"}}}";

            String wildcardBody = readAll(postJson("/" + wildcardIndex + "/_search", wildcardQuery));
            assertEquals("the two /var/log rows: " + wildcardBody, List.of(0, 1), sortedSourceIds(wildcardBody));
            String prefixBody = readAll(postJson("/" + wildcardIndex + "/_search", prefixQuery));
            assertEquals("the three /var rows: " + prefixBody, List.of(0, 1, 4), sortedSourceIds(prefixBody));
            String regexpBody = readAll(postJson("/" + wildcardIndex + "/_search", regexpQuery));
            assertEquals("the one /usr row: " + regexpBody, List.of(2), sortedSourceIds(regexpBody));
            String termBody = readAll(postJson("/" + wildcardIndex + "/_search", termQuery));
            assertEquals("the one /etc row: " + termBody, List.of(5), sortedSourceIds(termBody));

            // Oracle: the same queries on the same column mapped keyword
            // return the same rows.
            for (String query : List.of(wildcardQuery, prefixQuery, regexpQuery, termQuery)) {
                String overridden = readAll(postJson("/" + wildcardIndex + "/_search", query));
                String oracle = readAll(postJson("/" + keywordIndex + "/_search", query));
                assertEquals("wildcard and keyword answers must agree for " + query, sortedSourceIds(oracle), sortedSourceIds(overridden));
            }
        } finally {
            for (String index : List.of(wildcardIndex, keywordIndex)) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {}
            }
        }
    }

    public void testWildcardOverrideOnInvertedIndexColumnAnswersPatternQueries() throws Exception {
        // A wildcard override on a Utf8 column that carries a Lance
        // inverted index: the column leaves the FTS path (keyword
        // mapping, not lance_text) and pattern queries answer through
        // the doc values, untouched by the index the table still has.
        // The epoch-millis fixture's label column carries the inverted
        // index; even rows hold "hello lance i", odd rows "quick brown
        // fox i".
        String suffix = "wcfts-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String tableName = "demo-" + suffix;
        String tableUri = LanceTableFactory.writeEpochMillisTable(scratchDir, tableName);
        String indexName = tableName;
        try {
            Response attach = postJson(
                "/_lance/attach",
                "{\"table\":\"" + tableUri + "\",\"overrides\":{\"label\":{\"type\":\"wildcard\"}}}"
            );
            assertEquals("attach failed: " + readAll(attach), RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());

            String mapping = readAll(client().performRequest(new Request("GET", "/" + indexName + "/_mapping")));
            assertTrue("label must map as keyword: " + mapping, mapping.contains("\"label\":{\"type\":\"keyword\""));
            assertTrue("meta must record the declared type: " + mapping, mapping.contains("\"lance_override_type\":\"wildcard\""));
            assertFalse("label must not map as lance_text: " + mapping, mapping.contains("lance_text"));

            String wildcardBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"wildcard\":{\"label\":{\"value\":\"hello lance*\"}}}}")
            );
            assertEquals("the three even rows: " + wildcardBody, List.of(0, 2, 4), sortedSourceIds(wildcardBody));

            String prefixBody = readAll(
                postJson("/" + indexName + "/_search", "{\"size\":10,\"query\":{\"prefix\":{\"label\":{\"value\":\"quick\"}}}}")
            );
            assertEquals("the three odd rows: " + prefixBody, List.of(1, 3, 5), sortedSourceIds(prefixBody));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + indexName));
            } catch (Exception ignored) {}
        }
    }

    public void testNamespaceIpOverrideAppliesToTablesThatCarryTheColumn() throws Exception {
        // One override list for the whole namespace: the table with the
        // ip column gets the ip mapping and answers a CIDR term, the
        // table without it surfaces untouched.
        String suffix = "nsip-" + randomAlphaOfLength(8).toLowerCase(java.util.Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String withIp = "addressed-" + suffix;
        String withoutIp = "plain-" + suffix;
        LanceTableFactory.writeIpTable(scratchDir, withIp);
        LanceTableFactory.writeTable(scratchDir, withoutIp, 4);
        try {
            Response register = postJson(
                "/_lance/namespace",
                "{\"path\":\"" + scratchDir.toString() + "\",\"overrides\":{\"ip\":{\"type\":\"ip\"}}}"
            );
            assertEquals("register failed: " + readAll(register), RestStatus.OK.getStatus(), register.getStatusLine().getStatusCode());

            assertBusy(() -> {
                String cat = readAll(client().performRequest(new Request("GET", "/_cat/indices?format=json")));
                assertTrue("waiting for " + withIp + ": " + cat, cat.contains("\"" + withIp + "\""));
                assertTrue("waiting for " + withoutIp + ": " + cat, cat.contains("\"" + withoutIp + "\""));
            });

            String ipMapping = readAll(client().performRequest(new Request("GET", "/" + withIp + "/_mapping")));
            assertTrue("ip must map as ip: " + ipMapping, ipMapping.contains("\"ip\":{\"type\":\"ip\""));

            String cidrBody = readAll(postJson("/" + withIp + "/_search", "{\"size\":10,\"query\":{\"term\":{\"ip\":\"10.0.0.0/8\"}}}"));
            assertEquals(3, extractIntPath(cidrBody, "hits", "total", "value"));

            String plainMapping = readAll(client().performRequest(new Request("GET", "/" + withoutIp + "/_mapping")));
            assertTrue("body must stay lance_text: " + plainMapping, plainMapping.contains("\"body\":{\"type\":\"lance_text\""));
            assertFalse("no ip field on the plain table: " + plainMapping, plainMapping.contains("\"type\":\"ip\""));
        } finally {
            for (String index : List.of(withIp, withoutIp)) {
                try {
                    client().performRequest(new Request("DELETE", "/" + index));
                } catch (Exception ignored) {}
            }
            try {
                deleteJson("/_lance/namespace", "{\"path\":\"" + scratchDir.toString() + "\"}");
            } catch (Exception ignored) {}
        }
    }
}
