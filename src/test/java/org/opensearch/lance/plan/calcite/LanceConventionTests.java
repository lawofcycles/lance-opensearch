/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.opensearch.test.OpenSearchTestCase;

/**
 * The two conventions must stay distinct singletons whose {@code satisfies}
 * is reflexive only, so the planner never treats an operator of one
 * convention as satisfying the other.
 */
public class LanceConventionTests extends OpenSearchTestCase {

    public void testNames() {
        assertEquals("LANCE", LanceConvention.INSTANCE.getName());
        assertEquals("LUCENE", LuceneConvention.INSTANCE.getName());
    }

    public void testInterfaces() {
        assertEquals(LanceRel.class, LanceConvention.INSTANCE.getInterface());
        assertEquals(LuceneRel.class, LuceneConvention.INSTANCE.getInterface());
    }

    public void testDistinct() {
        assertNotSame(LanceConvention.INSTANCE, LuceneConvention.INSTANCE);
    }

    public void testSatisfiesReflexiveOnly() {
        assertTrue(LanceConvention.INSTANCE.satisfies(LanceConvention.INSTANCE));
        assertTrue(LuceneConvention.INSTANCE.satisfies(LuceneConvention.INSTANCE));

        assertFalse(LanceConvention.INSTANCE.satisfies(LuceneConvention.INSTANCE));
        assertFalse(LuceneConvention.INSTANCE.satisfies(LanceConvention.INSTANCE));
    }
}
