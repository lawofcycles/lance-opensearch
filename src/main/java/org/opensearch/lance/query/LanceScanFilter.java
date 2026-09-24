/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import org.lance.ipc.ScanOptions;

/**
 * The scalar predicate a Lance scan evaluates, in the encoding the
 * planner chose for it: the Lance SQL string for
 * {@link ScanOptions.Builder#filter(String)}, or the Substrait
 * {@code ExtendedExpression} bytes for
 * {@link ScanOptions.Builder#substraitFilter(ByteBuffer)}. The two are
 * exclusive on the scan (Lance's scanner keeps one expression filter,
 * and its JNI layer applies the SQL last, so a scan given both would
 * evaluate the SQL alone), which is why {@link #apply} sets exactly
 * one. A Substrait filter may still carry the SQL of the same predicate
 * for the readers that take SQL only ({@link #sql()}); it is never
 * handed to the scan this object configures.
 *
 * <p>Immutable; the bytes are copied in and out.
 */
public final class LanceScanFilter {

    private final String sql;
    private final byte[] substrait;

    private LanceScanFilter(String sql, byte[] substrait) {
        this.sql = sql;
        this.substrait = substrait;
    }

    /** A filter the scan evaluates as Lance SQL. */
    public static LanceScanFilter sql(String sql) {
        Objects.requireNonNull(sql, "sql");
        if (sql.isEmpty()) {
            throw new IllegalArgumentException("the filter SQL must not be empty");
        }
        return new LanceScanFilter(sql, null);
    }

    /**
     * A filter the scan evaluates as Substrait bytes, with the SQL of the
     * same predicate when the planner had one (null otherwise).
     */
    public static LanceScanFilter substrait(byte[] substrait, String sql) {
        Objects.requireNonNull(substrait, "substrait");
        if (substrait.length == 0) {
            throw new IllegalArgumentException("the Substrait filter must not be empty");
        }
        if (sql != null && sql.isEmpty()) {
            throw new IllegalArgumentException("the filter SQL must not be empty");
        }
        return new LanceScanFilter(sql, substrait.clone());
    }

    /** Whether the scan evaluates the Substrait bytes rather than the SQL. */
    public boolean usesSubstrait() {
        return substrait != null;
    }

    /**
     * The Lance SQL of the predicate: what the scan evaluates for a SQL
     * filter, the companion spelling of a Substrait filter, or null for
     * a Substrait filter the SQL printer could not spell.
     */
    public String sql() {
        return sql;
    }

    /** The Substrait bytes, or null for a SQL filter. A copy. */
    public byte[] substraitBytes() {
        return substrait == null ? null : substrait.clone();
    }

    /**
     * Configures {@code builder} with this filter: the Substrait bytes in
     * a direct buffer (the JNI side reads it through
     * {@code GetDirectBufferAddress}, which returns null for a heap
     * buffer) when the filter is Substrait, the SQL otherwise.
     */
    public ScanOptions.Builder apply(ScanOptions.Builder builder) {
        if (substrait != null) {
            ByteBuffer direct = ByteBuffer.allocateDirect(substrait.length);
            direct.put(substrait);
            direct.flip();
            return builder.substraitFilter(direct);
        }
        return builder.filter(sql);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LanceScanFilter other)) {
            return false;
        }
        return Objects.equals(sql, other.sql) && Arrays.equals(substrait, other.substrait);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, Arrays.hashCode(substrait));
    }

    /** The SQL of a SQL filter; the byte count and, when present, the SQL of a Substrait filter. */
    @Override
    public String toString() {
        if (substrait == null) {
            return sql;
        }
        return "substrait[" + substrait.length + " bytes]" + (sql == null ? "" : " " + sql);
    }
}
