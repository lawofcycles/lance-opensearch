/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import org.opensearch.action.ActionType;

/**
 * Runs one catalog listing cycle now on the elected cluster manager,
 * behind {@code POST /_lance/namespace/_poll}. The cycle is the one the
 * manager runs at {@code lance.namespace.poll_cadence}: list every
 * registration (or the one named), create an index for each table that
 * has none, and report what was surfaced and what was skipped.
 *
 * <p>The name sits under {@code cluster:admin/} next to
 * {@link LanceNamespaceUpdateAction}: the cycle creates indexes, so the
 * roles that may register a namespace are the ones that may trigger it.
 */
public final class LanceNamespacePollAction extends ActionType<LanceNamespacePollResponse> {

    public static final String NAME = "cluster:admin/lance/namespace/poll";
    public static final LanceNamespacePollAction INSTANCE = new LanceNamespacePollAction();

    private LanceNamespacePollAction() {
        super(NAME, LanceNamespacePollResponse::new);
    }
}
