/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.network.InetAddresses;

/**
 * {@link KeywordDictionaryBuilder.TermEncoder} for {@code ip}-overridden
 * Utf8 columns. Each stored string is parsed with
 * {@link InetAddresses#forString} and encoded through
 * {@link InetAddressPoint#encode} into the 16 byte form (IPv4 mapped
 * into IPv6), which is the term shape OpenSearch's
 * {@code IpFieldMapper.IpFieldType} builds its doc-value queries,
 * sorts and aggregation keys from. A string that does not parse as an
 * IP address encodes to {@code null}, so the row is served as missing;
 * the loaders report the per-fragment count through
 * {@link #logInvalid} once per load rather than once per row.
 */
final class IpTermEncoder implements KeywordDictionaryBuilder.TermEncoder {

    private static final Logger LOGGER = LogManager.getLogger(IpTermEncoder.class);

    static final IpTermEncoder INSTANCE = new IpTermEncoder();

    private IpTermEncoder() {}

    @Override
    public BytesRef encode(byte[] utf8, int length) {
        String value = new String(utf8, 0, length, StandardCharsets.UTF_8);
        InetAddress address;
        try {
            address = InetAddresses.forString(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
        return new BytesRef(InetAddressPoint.encode(address));
    }

    /**
     * One log line per (column, fragment) load naming how many values
     * failed to parse as IP addresses and are served as missing. The
     * original strings stay readable through {@code _source}.
     */
    static void logInvalid(String column, int fragmentId, int count) {
        if (count > 0) {
            LOGGER.warn(
                "column [{}] fragment [{}]: [{}] value(s) did not parse as IP addresses and are treated as missing",
                column,
                fragmentId,
                count
            );
        }
    }
}
