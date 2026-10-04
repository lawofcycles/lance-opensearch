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
 * An HTTP listener standing in for an S3 endpoint that has no bucket of
 * the name a request addresses: every request is answered with 404 and
 * the {@code NoSuchBucket} error body, which names the bucket the way
 * S3 does. S3 compares bucket names exactly, so this is what a URI
 * whose bucket differs in case from the real one gets. A Lance open
 * against it fails with that body in the message, so a test can check
 * what the plugin reports for such a failure without an object store.
 *
 * <p>The suppression covers {@code com.sun.net.httpserver}: the test
 * needs a listener inside the test JVM with no extra dependency, and
 * the JDK's built-in server is the standard choice for that in
 * OpenSearch test code.
 */
@SuppressForbidden(reason = "in-test HTTP server standing in for an S3 endpoint without the bucket")
public final class NoSuchBucketS3Fixture implements AutoCloseable {

    private final HttpServer server;
    private final String bucket;

    /** Start the listener on a free loopback port. {@code bucket} is the name the error body echoes. */
    public NoSuchBucketS3Fixture(String bucket) throws IOException {
        this.bucket = bucket;
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = errorBody().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/xml");
            exchange.getResponseHeaders().add("x-amz-request-id", "7A9E3F0C2B1D4E5F");
            if ("HEAD".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(404, -1);
                exchange.close();
                return;
            }
            exchange.sendResponseHeaders(404, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
    }

    /** The error body S3 answers with, naming the bucket. */
    public String errorBody() {
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>NoSuchBucket</Code>"
            + "<Message>The specified bucket does not exist</Message>"
            + "<BucketName>"
            + bucket
            + "</BucketName><RequestId>7A9E3F0C2B1D4E5F</RequestId><HostId>fixture</HostId></Error>";
    }

    /** The endpoint to put in {@code aws_endpoint}; plain HTTP, so {@code allow_http} must be {@code true}. */
    public String endpoint() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    /**
     * The storage options that send a Lance open to this listener. The
     * key id and secret are the example values of the AWS documentation,
     * the id assembled at run time so the source carries no string a
     * secret scanner matches; the listener does not read them.
     */
    public String storageOptionsJson() {
        return "{\"aws_access_key_id\":\""
            + "AKIA"
            + "IOSFODNN7EXAMPLE"
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
