/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

/**
 * Internal ThreadContext header names that the plugin uses to
 * distinguish its own calls from user requests going through the
 * same transport action.
 */
public final class LanceInternalHeaders {

    /**
     * Header stamped by {@code RestAttachAction} and {@code
     * LanceNamespaceService} before they issue their own {@code
     * indices:admin/create} calls. {@link
     * org.opensearch.lance.dispatch.LanceCreateIndexActionFilter}
     * lets the request through only when this header is present;
     * user {@code PUT /{index}} requests that try to set {@code
     * index.lance.table} directly are rejected with 400 because
     * they skip the {@code POST /_lance/attach} derive step and
     * produce a half-broken index (empty mapping, sort queries
     * failing with "No mapping found").
     */
    public static final String LANCE_INTERNAL_CREATE_INDEX = "X-Lance-Internal-Create-Index";

    private LanceInternalHeaders() {}
}
