/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

import org.opensearch.common.SuppressForbidden;

/**
 * An HTTP listener standing in for an S3 endpoint that does not know
 * the access key id it is signed with: every request is answered with
 * 403 and the {@code InvalidAccessKeyId} error body, which echoes the
 * key id the way S3 does. A Lance open against it fails with that body
 * in the message, so a test can check what the plugin reports and
 * logs for such a failure without an object store.
 *
 * <p>The suppression covers {@code com.sun.net.httpserver}: the test
 * needs a listener inside the test JVM with no extra dependency, and
 * the JDK's built-in server is the standard choice for that in
 * OpenSearch test code.
 */
@SuppressForbidden(reason = "in-test HTTP server standing in for an S3 endpoint that refuses the access key id")
public final class InvalidAccessKeyIdS3Fixture implements AutoCloseable {

    private final HttpServer server;
    private final String accessKeyId;

    /**
     * Start the listener on a free loopback port. {@code accessKeyId}
     * is what the error body echoes; a test passes the id it also puts
     * in the storage options, so a leak of either is caught.
     */
    public InvalidAccessKeyIdS3Fixture(String accessKeyId) throws IOException {
        this.accessKeyId = accessKeyId;
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = errorBody().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/xml");
            exchange.getResponseHeaders().add("x-amz-request-id", "7A9E3F0C2B1D4E5F");
            if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(403, -1);
                exchange.close();
                return;
            }
            exchange.sendResponseHeaders(403, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
    }

    /** The error body S3 answers with, quoting the key id. */
    public String errorBody() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>InvalidAccessKeyId</Code>"
            + "<Message>The AWS Access Key Id you provided does not exist in our records.</Message>"
            + "<AWSAccessKeyId>"
            + accessKeyId
            + "</AWSAccessKeyId><RequestId>7A9E3F0C2B1D4E5F</RequestId><HostId>fixture</HostId></Error>";
    }

    /** The endpoint to put in {@code aws_endpoint}; plain HTTP, so {@code allow_http} must be {@code true}. */
    public String endpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    /** The storage options that send a Lance open to this listener signed with the refused key id. */
    public String storageOptionsJson() {
        return "{\"aws_access_key_id\":\""
            + accessKeyId
            + "\",\"aws_secret_access_key\":\"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\",\"aws_region\":\"us-east-1\","
            + "\"aws_endpoint\":\""
            + endpoint()
            + "\",\"allow_http\":\"true\"}";
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
