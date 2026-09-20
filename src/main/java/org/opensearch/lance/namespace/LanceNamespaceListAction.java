/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import org.opensearch.action.ActionType;

/**
 * Read-only namespace action behind {@code GET /_lance/namespace} and
 * {@code POST /_lance/namespace/tables}. Both endpoints answer from
 * the registered namespaces, so they share one action name and one
 * privilege: a request without a path lists the registered roots, a
 * request with a path lists the tables under that root.
 *
 * <p>The name sits under {@code cluster:monitor/} so a security plugin
 * grants it to the same roles that may read cluster state, without
 * also granting the mutating {@link LanceNamespaceUpdateAction}.
 */
public final class LanceNamespaceListAction extends ActionType<LanceNamespaceListResponse> {

    public static final String NAME = "cluster:monitor/lance/namespace";
    public static final LanceNamespaceListAction INSTANCE = new LanceNamespaceListAction();

    private LanceNamespaceListAction() {
        super(NAME, LanceNamespaceListResponse::new);
    }
}
