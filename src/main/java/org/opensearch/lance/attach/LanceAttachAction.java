/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.attach;

import org.opensearch.action.ActionType;

/**
 * Action behind {@code POST /_lance/attach}. Attaching opens the Lance
 * table, derives a mapping from its schema, and creates an index, so
 * the name sits under {@code cluster:admin/} and a security plugin
 * evaluates it before any of that work starts. The transport action is
 * cluster-manager routed: the node that receives the REST call forwards
 * the request to the elected cluster manager, where the index is created.
 */
public final class LanceAttachAction extends ActionType<LanceAttachResponse> {

    public static final String NAME = "cluster:admin/lance/attach";
    public static final LanceAttachAction INSTANCE = new LanceAttachAction();

    private LanceAttachAction() {
        super(NAME, LanceAttachResponse::new);
    }
}
