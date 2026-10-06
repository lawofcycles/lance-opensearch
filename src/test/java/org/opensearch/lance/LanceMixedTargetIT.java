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
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/**
 * A {@code _search} whose target puts a Lance backed index next to an
 * index that is not Lance backed is refused with 400 naming the Lance
 * backed index, whether the two are listed, matched by one pattern or
 * reached through one alias; in {@code _msearch} the line that mixes
 * them carries the refusal and the other lines answer. A target of Lance
 * backed indexes alone, or of ordinary indexes alone, answers 200 as
 * before.
 */
public class LanceMixedTargetIT extends LanceRestTestCase {

    public void testALanceBackedIndexNextToAnOrdinaryOneIsRefused() throws Exception {
        String suffix = "mixed-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        Path scratchDir = Files.createDirectories(sharedRoot().resolve("lance-it-" + suffix));
        String lance = "lance-" + suffix;
        String plain = "plain-" + suffix;
        String alias = "alias-" + suffix;
        LanceTableFactory.writeTable(scratchDir, lance, 6);
        String tableUri = scratchDir.resolve(lance + ".lance").toString();
        String expected = "cannot search Lance backed index ["
            + lance
            + "] together with a target that is not Lance backed; run the request against the Lance backed target alone, or split the targets";
        try {
            Response attach = postJson("/_plugins/_lance/attach", "{\"table\":\"" + tableUri + "\"}");
            assertEquals(RestStatus.OK.getStatus(), attach.getStatusLine().getStatusCode());
            ensureGreen(lance);
            Request create = new Request("PUT", "/" + plain);
            create.setJsonEntity(
                "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},"
                    + "\"mappings\":{\"properties\":{\"id\":{\"type\":\"long\"},\"body\":{\"type\":\"text\"}}}}"
            );
            client().performRequest(create);
            Request doc = new Request("PUT", "/" + plain + "/_doc/100?refresh=true");
            doc.setJsonEntity("{\"id\":100,\"body\":\"plain row\"}");
            client().performRequest(doc);
            Request aliases = new Request("POST", "/_aliases");
            aliases.setJsonEntity(
                "{\"actions\":[{\"add\":{\"index\":\""
                    + lance
                    + "\",\"alias\":\""
                    + alias
                    + "\"}},{\"add\":{\"index\":\""
                    + plain
                    + "\",\"alias\":\""
                    + alias
                    + "\"}}]}"
            );
            client().performRequest(aliases);

            // Each kind alone answers as before.
            String body = "{\"size\":0,\"track_total_hits\":true}";
            assertEquals(6, extractIntPath(readAll(postJson("/" + lance + "/_search", body)), "hits", "total", "value"));
            assertEquals(1, extractIntPath(readAll(postJson("/" + plain + "/_search", body)), "hits", "total", "value"));
            assertEquals(6, extractIntPath(readAll(postJson("/" + lance + "*/_search", body)), "hits", "total", "value"));

            // Listed, matched by one pattern, or reached through one
            // alias that spans both kinds: the same 400.
            for (String target : new String[] { lance + "," + plain, plain + "," + lance, "*-" + suffix, alias }) {
                ResponseException refused = expectThrows(ResponseException.class, () -> postJson("/" + target + "/_search", body));
                String response = readAll(refused.getResponse());
                assertEquals(target + ": " + response, 400, refused.getResponse().getStatusLine().getStatusCode());
                assertEquals(target + ": " + response, "illegal_argument_exception", stringPath(response, "error", "type"));
                assertEquals(target + ": " + response, expected, stringPath(response, "error", "reason"));
            }

            // _msearch: the mixed line is refused in its own slot and the
            // lines over one kind answer.
            Request msearch = new Request("POST", "/_msearch");
            msearch.setJsonEntity(
                "{\"index\":\""
                    + lance
                    + ","
                    + plain
                    + "\"}\n"
                    + body
                    + "\n{\"index\":\""
                    + lance
                    + "\"}\n"
                    + body
                    + "\n{\"index\":\""
                    + plain
                    + "\"}\n"
                    + body
                    + "\n"
            );
            Response multi = client().performRequest(msearch);
            String multiBody = readAll(multi);
            assertEquals(multiBody, 200, multi.getStatusLine().getStatusCode());
            assertEquals(multiBody, 400, extractIntPath(multiBody, "responses", "0", "status"));
            assertEquals(multiBody, "illegal_argument_exception", stringPath(multiBody, "responses", "0", "error", "type"));
            assertEquals(multiBody, expected, stringPath(multiBody, "responses", "0", "error", "reason"));
            assertEquals(multiBody, 6, extractIntPath(multiBody, "responses", "1", "hits", "total", "value"));
            assertEquals(multiBody, 1, extractIntPath(multiBody, "responses", "2", "hits", "total", "value"));
        } finally {
            try {
                client().performRequest(new Request("DELETE", "/" + lance + "," + plain));
            } catch (Exception ignored) {}
        }
    }
}
