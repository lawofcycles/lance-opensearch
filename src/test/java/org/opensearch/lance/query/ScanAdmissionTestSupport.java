/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.query;

/**
 * Access to {@link ScanAdmission#resetForTests} for test classes outside
 * this package. The admission gate's retained pool, counters and probe
 * overrides are static, so a test that admits a scan leaves them for the
 * next test class of the same JVM unless it resets them.
 */
public final class ScanAdmissionTestSupport {

    private ScanAdmissionTestSupport() {}

    /** Reset the gate's static state to its defaults; see {@link ScanAdmission#resetForTests}. */
    public static void reset() {
        ScanAdmission.resetForTests();
    }
}
