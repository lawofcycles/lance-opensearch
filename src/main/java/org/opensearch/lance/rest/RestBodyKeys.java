/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.rest;

import java.util.List;
import java.util.Map;

/**
 * Top level key check for the JSON bodies the plugin's REST handlers read
 * with {@code body.get(key)}. Such a parser never looks at the keys it does
 * not read, so a body carrying a misspelt or no longer supported key would
 * answer 200 as if the key were absent. The handlers call
 * {@link #rejectUnknown} before reading anything so the first key outside
 * the accepted list is a 400 that names it and lists what is accepted,
 * which is how OpenSearch core answers an unknown field in a request body.
 */
final class RestBodyKeys {

    private RestBodyKeys() {}

    /**
     * Throws {@link IllegalArgumentException} for the first key of
     * {@code body} that is not in {@code accepted}. {@code handler} is the
     * handler name that prefixes the message, {@code accepted} is listed in
     * the message in the order given.
     */
    static void rejectUnknown(String handler, Map<String, Object> body, List<String> accepted) {
        for (String key : body.keySet()) {
            if (!accepted.contains(key)) {
                throw new IllegalArgumentException(
                    "[" + handler + "] unknown key [" + key + "]; accepted keys are " + String.join(", ", accepted)
                );
            }
        }
    }
}
