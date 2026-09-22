/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.explain;

import org.opensearch.action.ActionType;
import org.opensearch.action.IndicesRequest;

/**
 * Action behind {@code GET /<index>/_lance/explain}. Explaining a
 * request only reads the index's schema and row count, so the name sits
 * under {@code indices:monitor/} and the request implements
 * {@link IndicesRequest}: a security plugin evaluates the privilege per
 * target index before the plugin reads anything.
 */
public final class LanceExplainAction extends ActionType<LanceExplainResponse> {

    public static final String NAME = "indices:monitor/lance/explain";
    public static final LanceExplainAction INSTANCE = new LanceExplainAction();

    private LanceExplainAction() {
        super(NAME, LanceExplainResponse::new);
    }
}
