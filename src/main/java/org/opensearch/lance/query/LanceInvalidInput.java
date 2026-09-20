/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

import java.util.Objects;

/**
 * Recognises the exception Lance raises for a request the caller got
 * wrong, so the failure can be answered as a client error.
 *
 * <p>Lance's JNI layer maps its {@code Error::InvalidInput} (a phrase
 * query against an inverted index built without positions, an unknown
 * tokenizer, a malformed SQL predicate) to
 * {@link IllegalArgumentException}, which OpenSearch's
 * {@code ExceptionsHelper.status} answers with HTTP 400 and renders as
 * {@code illegal_argument_exception}. The Lucene contract of
 * {@code Weight} and {@code ScorerSupplier} allows only
 * {@link java.io.IOException}, so the Lance scans of the query classes
 * keep the original exception as the cause of an {@code IOException},
 * and a plain {@code IOException} is a 500 at the REST layer. The
 * fragment executor calls {@link #unwrap} on the failure it is about to
 * report so the client sees Lance's message with the status the
 * criterion below assigns.
 *
 * <p>{@code LanceIndexBuilder} classifies a failed index build with the
 * same idea (an {@code IllegalArgumentException} is Lance refusing the
 * input), but it looks at the exception it caught directly; a scan
 * failure arrives wrapped, so {@link #find} additionally walks the
 * cause chain, and only accepts an {@code IllegalArgumentException} that
 * Lance produced.
 */
public final class LanceInvalidInput {

    /**
     * Display prefix of Lance's {@code Error::InvalidInput}; the JNI
     * layer forwards it as the exception message.
     */
    private static final String INVALID_INPUT_PREFIX = "Invalid user input";

    /** Package of the Lance Java SDK, whose native methods throw the JNI exception. */
    private static final String LANCE_PACKAGE = "org.lance.";

    /** Bound on the cause chain walk, in case a chain is cyclic. */
    private static final int MAX_DEPTH = 10;

    private LanceInvalidInput() {}

    /**
     * Whether {@code t} is an {@link IllegalArgumentException} Lance
     * raised: its message carries the display form of
     * {@code Error::InvalidInput}, or the frame that threw it is a
     * method of the Lance SDK. An {@code IllegalArgumentException} from
     * Lucene or OpenSearch code inside a scan does not qualify, so a
     * plugin bug is not reported as the client's mistake.
     */
    public static boolean isInvalidInput(Throwable t) {
        if (!(t instanceof IllegalArgumentException)) {
            return false;
        }
        String message = t.getMessage();
        if (message != null && message.startsWith(INVALID_INPUT_PREFIX)) {
            return true;
        }
        StackTraceElement[] frames = t.getStackTrace();
        return frames.length > 0 && frames[0].getClassName().startsWith(LANCE_PACKAGE);
    }

    /**
     * The first exception in the cause chain of {@code t} (starting
     * with {@code t} itself) that {@link #isInvalidInput} accepts, or
     * {@code null} when the chain holds none. {@code null} in gives
     * {@code null} out.
     */
    public static IllegalArgumentException find(Throwable t) {
        Throwable current = t;
        for (int depth = 0; current != null && depth < MAX_DEPTH; depth++) {
            if (isInvalidInput(current)) {
                return (IllegalArgumentException) current;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                return null;
            }
            current = cause;
        }
        return null;
    }

    /**
     * The exception to report for {@code failure}: the Lance
     * {@link IllegalArgumentException} found in its cause chain when
     * there is one, so the status becomes 400 and the reason is Lance's
     * own message, otherwise {@code failure} unchanged. {@code failure}
     * must not be null.
     */
    public static Exception unwrap(Exception failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        IllegalArgumentException invalid = find(failure);
        return invalid == null ? failure : invalid;
    }
}
