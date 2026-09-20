/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.index;

import org.opensearch.action.ActionType;

/**
 * Action behind {@code POST /_lance/build_indexes/{index}}. Building
 * writes Lance indexes into the table behind an OpenSearch index and
 * can hold a CPU for a long time, so the name sits under
 * {@code indices:admin/} and the request implements
 * {@link org.opensearch.action.IndicesRequest}: a security plugin
 * evaluates the privilege per target index before the build starts.
 */
public final class LanceBuildIndexesAction extends ActionType<LanceBuildIndexesResponse> {

    public static final String NAME = "indices:admin/lance/build_indexes";
    public static final LanceBuildIndexesAction INSTANCE = new LanceBuildIndexesAction();

    private LanceBuildIndexesAction() {
        super(NAME, LanceBuildIndexesResponse::new);
    }
}
