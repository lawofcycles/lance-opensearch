/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.List;

/**
 * Node-level allowlist for the paths / URIs {@code attach} and
 * {@code namespace} accept as Lance table roots.
 *
 * <p>Configured through the {@code lance.allowed_table_roots} setting, which
 * takes a list of prefixes. A candidate path is accepted when its normalised
 * form (guaranteed to end with {@code /}) starts with the normalised form of
 * any configured root. An empty list disables the check so operators can
 * upgrade an existing deployment without immediately having to enumerate
 * every table location — the recommendation in the README is to set the
 * list explicitly in production.
 *
 * <p>The class works uniformly on local paths ({@code /data/lance/}) and
 * object-store URIs ({@code s3://bucket/prefix/}); the caller passes in
 * whatever string arrives on the REST payload.
 */
public final class AllowedTableRoots {

    private final List<String> normalisedRoots;

    public AllowedTableRoots(List<String> configuredRoots) {
        List<String> source = configuredRoots == null ? List.of() : configuredRoots;
        List<String> normalised = new java.util.ArrayList<>(source.size());
        for (String root : source) {
            if (root == null || root.isEmpty()) {
                continue;
            }
            normalised.add(normalise(root));
        }
        this.normalisedRoots = List.copyOf(normalised);
    }

    public boolean isEmpty() {
        return normalisedRoots.isEmpty();
    }

    /**
     * Returns true when the candidate is inside one of the configured roots,
     * or when the allowlist is empty (initial-PR default). The comparison is
     * prefix-based and normalises both sides to end in {@code /} so
     * {@code /data/lance} does not accidentally match {@code /data/lance-old}.
     */
    public boolean allows(String candidate) {
        if (normalisedRoots.isEmpty()) {
            return true;
        }
        if (candidate == null || candidate.isEmpty()) {
            return false;
        }
        String normalisedCandidate = normalise(candidate);
        for (String root : normalisedRoots) {
            if (normalisedCandidate.startsWith(root)) {
                return true;
            }
        }
        return false;
    }

    public List<String> configuredRoots() {
        return normalisedRoots;
    }

    private static String normalise(String value) {
        return value.endsWith("/") ? value : value + "/";
    }
}
