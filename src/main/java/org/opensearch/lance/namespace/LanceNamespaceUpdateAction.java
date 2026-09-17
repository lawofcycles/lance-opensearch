/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import org.opensearch.action.ActionType;

/**
 * Internal action name for cluster-state-scoped namespace mutations.
 * The plugin only mutates cluster state through the cluster manager,
 * so both register and unregister are funnelled through a single
 * {@link org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction}
 * behind this action. The request carries a
 * {@link LanceNamespaceUpdateRequest.Operation} discriminator so one
 * handler can serve both mutations without duplicating the manager
 * routing plumbing.
 */
public final class LanceNamespaceUpdateAction extends ActionType<LanceNamespaceUpdateResponse> {

    public static final String NAME = "cluster:admin/lance/namespace/update";
    public static final LanceNamespaceUpdateAction INSTANCE = new LanceNamespaceUpdateAction();

    private LanceNamespaceUpdateAction() {
        super(NAME, LanceNamespaceUpdateResponse::new);
    }
}
