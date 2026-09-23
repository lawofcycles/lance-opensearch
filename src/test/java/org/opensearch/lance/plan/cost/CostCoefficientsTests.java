/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The coefficients are positive, finite, and exactly the rounded values
 * the fit report records, so a refit that changes a value cannot land
 * without the constant following it.
 */
public class CostCoefficientsTests extends OpenSearchTestCase {

    private static final Pattern COEFFICIENT_ROW = Pattern.compile("^\\| ([A-Z0-9_]+) \\| ([0-9.eE+-]+) \\| ([0-9.eE+-]+) \\| .* \\|$");

    public void testEveryCoefficientIsPositiveAndFinite() {
        Map<String, Double> fitted = CostCoefficients.fitted();
        assertFalse(fitted.isEmpty());
        for (Map.Entry<String, Double> coefficient : fitted.entrySet()) {
            assertTrue(coefficient.getKey() + " is finite", Double.isFinite(coefficient.getValue()));
            assertTrue(coefficient.getKey() + " is positive", coefficient.getValue() > 0.0);
        }
    }

    public void testEveryCoefficientNamesItsUnit() {
        for (String name : CostCoefficients.fitted().keySet()) {
            assertTrue(name + " ends in a unit", name.contains("_MS"));
        }
    }

    public void testCoefficientsMatchTheFitReport() throws IOException {
        Map<String, Double> reported = new LinkedHashMap<>();
        for (String line : CostMeasurements.readText("/cost/fit-report.md").split("\n")) {
            Matcher m = COEFFICIENT_ROW.matcher(line);
            if (m.matches()) {
                reported.put(m.group(1), Double.parseDouble(m.group(3)));
            }
        }
        Map<String, Double> fitted = CostCoefficients.fitted();
        assertEquals("the report and the class name the same coefficients", reported.keySet(), fitted.keySet());
        for (Map.Entry<String, Double> coefficient : fitted.entrySet()) {
            assertEquals(coefficient.getKey(), reported.get(coefficient.getKey()), coefficient.getValue(), 0.0);
        }
    }

    public void testStructuralConstants() {
        assertEquals(1_000_000L, CostCoefficients.FITTED_MODEL_MIN_ROWS);
        assertEquals(1_000_000L, CostCoefficients.LARGE_GROUPS);
        assertTrue(CostCoefficients.DICTIONARY_STRING_BYTES_PER_ROW < CostCoefficients.STRING_BYTES_PER_ROW);
        assertTrue(CostCoefficients.TERMS_TOP_K_RETENTION_FACTOR >= 1);
    }
}
