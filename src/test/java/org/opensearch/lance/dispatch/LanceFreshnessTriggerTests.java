/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.lance.LanceSettings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link LanceFreshnessTrigger}: one request per index
 * and observed version, none for a pinned index, and the entry of a
 * deleted index is dropped.
 */
public class LanceFreshnessTriggerTests extends OpenSearchTestCase {

    private final List<String> sent = new ArrayList<>();
    private final ClusterService clusterService = mock(ClusterService.class);
    private final LanceFreshnessTrigger trigger = new LanceFreshnessTrigger(
        (indexName, observedVersion) -> sent.add(indexName + "@" + observedVersion),
        clusterService
    );

    public void testRegistersAsAClusterStateListener() {
        verify(clusterService).addListener(trigger);
    }

    public void testOneRequestPerIndexAndVersion() {
        IndexMetadata demo = indexMetadata("demo", "uuid-1", Settings.EMPTY);
        assertTrue(trigger.observed(demo, 3L));
        assertFalse("the same version again sends nothing", trigger.observed(demo, 3L));
        assertFalse(trigger.observed(demo, 3L));
        assertEquals(List.of("demo@3"), sent);

        assertTrue("a new version is asked for once", trigger.observed(demo, 4L));
        assertFalse(trigger.observed(demo, 4L));
        assertEquals(List.of("demo@3", "demo@4"), sent);
        assertEquals(Long.valueOf(4L), trigger.lastRequested("uuid-1"));

        // Another index with the same version is its own entry.
        IndexMetadata other = indexMetadata("other", "uuid-2", Settings.EMPTY);
        assertTrue(trigger.observed(other, 4L));
        assertEquals(List.of("demo@3", "demo@4", "other@4"), sent);
        assertEquals(2, trigger.size());
    }

    public void testAPinnedIndexAndANegativeVersionSendNothing() {
        IndexMetadata pinned = indexMetadata(
            "pinned",
            "uuid-p",
            Settings.builder().put(LanceSettings.VERSION_SETTING.getKey(), 7L).build()
        );
        assertFalse(trigger.observed(pinned, 7L));
        IndexMetadata demo = indexMetadata("demo", "uuid-1", Settings.EMPTY);
        assertFalse(trigger.observed(demo, -1L));
        assertTrue(sent.isEmpty());
        assertEquals(0, trigger.size());
    }

    public void testAnIndexDeletionDropsItsEntry() {
        IndexMetadata one = indexMetadata("one", "uuid-1", Settings.EMPTY);
        IndexMetadata two = indexMetadata("two", "uuid-2", Settings.EMPTY);
        assertTrue(trigger.observed(one, 2L));
        assertTrue(trigger.observed(two, 2L));
        assertEquals(2, trigger.size());

        ClusterState before = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().clusterUUID("cluster-1").put(one, false).put(two, false))
            .build();
        ClusterState after = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().clusterUUID("cluster-1").put(two, false))
            .build();
        trigger.clusterChanged(new ClusterChangedEvent("delete one", after, before));
        assertNull(trigger.lastRequested("uuid-1"));
        assertEquals(Long.valueOf(2L), trigger.lastRequested("uuid-2"));
        assertEquals(1, trigger.size());

        trigger.clusterChanged(new ClusterChangedEvent("nothing", after, after));
        assertEquals(1, trigger.size());

        // An index created again under the same name is a new uuid and
        // its first observation is asked for.
        IndexMetadata oneAgain = indexMetadata("one", "uuid-3", Settings.EMPTY);
        assertTrue(trigger.observed(oneAgain, 2L));
        assertEquals(List.of("one@2", "two@2", "one@2"), sent);
    }

    private static IndexMetadata indexMetadata(String name, String uuid, Settings extra) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(extra)
            )
            .build();
    }
}
