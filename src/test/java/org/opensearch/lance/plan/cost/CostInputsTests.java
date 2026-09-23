/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.plan.cost;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.opensearch.lance.plan.calcite.LancePlannerFactory;
import org.opensearch.test.OpenSearchTestCase;

/** {@link CostInputs}: the storage kind a table URI names, the defaults, the validation, and how the holder answers. */
public class CostInputsTests extends OpenSearchTestCase {

    public void testObjectStoreSchemes() {
        for (String uri : new String[] {
            "s3://bucket/table.lance",
            "S3://bucket/table.lance",
            "s3a://bucket/table.lance",
            "gs://bucket/table.lance",
            "gcs://bucket/table.lance",
            "az://container/table.lance",
            "abfs://container@account.dfs.core.windows.net/table.lance",
            "abfss://container@account.dfs.core.windows.net/table.lance",
            "wasbs://container@account.blob.core.windows.net/table.lance",
            "oss://bucket/table.lance" }) {
            assertEquals(uri, StorageKind.OBJECT_STORE, StorageKind.fromUri(uri));
        }
    }

    public void testLocalPathsAndUnknownSchemes() {
        assertEquals(StorageKind.LOCAL, StorageKind.fromUri("/data/tables/perf20m.lance"));
        assertEquals(StorageKind.LOCAL, StorageKind.fromUri("file:///data/tables/perf20m.lance"));
        assertEquals(StorageKind.LOCAL, StorageKind.fromUri("relative/table.lance"));
        assertEquals(StorageKind.LOCAL, StorageKind.fromUri("hdfs://namenode/table.lance"));
        assertEquals(StorageKind.LOCAL, StorageKind.fromUri(""));
        assertEquals(StorageKind.LOCAL, StorageKind.fromUri(null));
        assertEquals("a Windows drive letter is a path, not a scheme", StorageKind.LOCAL, StorageKind.fromUri("C:\\tables\\t.lance"));
    }

    public void testDefaultParallelismIsHalfTheCpusCappedAt32() {
        assertEquals(1, CostInputs.defaultParallelism(1));
        assertEquals(1, CostInputs.defaultParallelism(2));
        assertEquals(8, CostInputs.defaultParallelism(16));
        assertEquals(32, CostInputs.defaultParallelism(64));
        assertEquals(32, CostInputs.defaultParallelism(192));
    }

    public void testLocalDefaults() {
        CostInputs local = CostInputs.local(16);
        assertEquals(1, local.nodes());
        assertEquals(StorageKind.LOCAL, local.storage());
        assertEquals(16, local.cpusPerNode());
        assertEquals(8, local.pushdownParallelism());
        assertEquals(8, local.slices());
        assertEquals(CostInputs.defaultParallelism(Runtime.getRuntime().availableProcessors()), CostInputs.local().slices());
    }

    public void testForClusterReadsTheStorageKindFromTheUri() {
        CostInputs inputs = CostInputs.forCluster(4, "s3://bucket/perf1b.lance", 16, 8, 8);
        assertEquals(4, inputs.nodes());
        assertEquals(StorageKind.OBJECT_STORE, inputs.storage());
        assertEquals(StorageKind.LOCAL, CostInputs.forCluster(4, "/mnt/nvme/perf1b.lance", 16, 8, 8).storage());
    }

    public void testValidation() {
        expectThrows(IllegalArgumentException.class, () -> new CostInputs(0, StorageKind.LOCAL, 16, 8, 8));
        expectThrows(IllegalArgumentException.class, () -> new CostInputs(1, StorageKind.LOCAL, 0, 8, 8));
        expectThrows(IllegalArgumentException.class, () -> new CostInputs(1, StorageKind.LOCAL, 16, 0, 8));
        expectThrows(IllegalArgumentException.class, () -> new CostInputs(1, StorageKind.LOCAL, 16, 8, 0));
        expectThrows(NullPointerException.class, () -> new CostInputs(1, null, 16, 8, 8));
    }

    public void testHolderAnswersTheLocalDefaultsUntilSet() {
        CostInputsHolder holder = new CostInputsHolder();
        assertEquals(CostInputs.local(), holder.get());
        CostInputs cluster = CostInputs.forCluster(6, "s3://bucket/t.lance", 16, 8, 8);
        holder.set(cluster);
        assertSame(cluster, holder.get());
    }

    public void testPlannerWithoutAHolderAnswersTheLocalDefaults() {
        HepPlanner bare = new HepPlanner(new HepProgramBuilder().build(), Contexts.empty());
        assertEquals(CostInputs.local(), CostInputsHolder.inputsOf(bare));
    }

    public void testFactoryClusterCarriesAHolder() {
        LancePlannerFactory factory = new LancePlannerFactory(1L << 30, 1L << 30);
        CostInputsHolder holder = factory.newCluster().getPlanner().getContext().unwrap(CostInputsHolder.class);
        assertNotNull(holder);
        assertEquals(CostInputs.local(), holder.get());
    }
}
