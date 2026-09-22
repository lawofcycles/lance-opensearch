/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.calcite;

import org.opensearch.test.OpenSearchTestCase;

/**
 * The three conventions must stay distinct singletons whose {@code satisfies}
 * is reflexive only, so the planner never treats an operator of one
 * convention as satisfying another.
 */
public class LanceConventionTests extends OpenSearchTestCase {

    public void testNames() {
        assertEquals("LANCE", LanceConvention.INSTANCE.getName());
        assertEquals("LUCENE", LuceneConvention.INSTANCE.getName());
        assertEquals("SHARD_PATH", ShardPathConvention.INSTANCE.getName());
    }

    public void testInterfaces() {
        assertEquals(LanceRel.class, LanceConvention.INSTANCE.getInterface());
        assertEquals(LuceneRel.class, LuceneConvention.INSTANCE.getInterface());
        assertEquals(ShardPathRel.class, ShardPathConvention.INSTANCE.getInterface());
    }

    public void testDistinct() {
        assertNotSame(LanceConvention.INSTANCE, LuceneConvention.INSTANCE);
        assertNotSame(LanceConvention.INSTANCE, ShardPathConvention.INSTANCE);
        assertNotSame(LuceneConvention.INSTANCE, ShardPathConvention.INSTANCE);
    }

    public void testSatisfiesReflexiveOnly() {
        assertTrue(LanceConvention.INSTANCE.satisfies(LanceConvention.INSTANCE));
        assertTrue(LuceneConvention.INSTANCE.satisfies(LuceneConvention.INSTANCE));
        assertTrue(ShardPathConvention.INSTANCE.satisfies(ShardPathConvention.INSTANCE));

        assertFalse(LanceConvention.INSTANCE.satisfies(LuceneConvention.INSTANCE));
        assertFalse(LanceConvention.INSTANCE.satisfies(ShardPathConvention.INSTANCE));
        assertFalse(LuceneConvention.INSTANCE.satisfies(LanceConvention.INSTANCE));
        assertFalse(LuceneConvention.INSTANCE.satisfies(ShardPathConvention.INSTANCE));
        assertFalse(ShardPathConvention.INSTANCE.satisfies(LanceConvention.INSTANCE));
        assertFalse(ShardPathConvention.INSTANCE.satisfies(LuceneConvention.INSTANCE));
    }
}
