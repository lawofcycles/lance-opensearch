/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance;

import java.nio.file.Path;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.lance.Dataset;
import org.lance.Session;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit-level tests for {@link LanceRegistry}.
 *
 * <p>Covers three pieces: the shared Arrow allocator (unchanged from the
 * previous version of this class), the {@link Session} lifecycle (install
 * and release via {@code initSession} / {@code closeSession}), and the
 * observation that every {@link Dataset} opened through the registry
 * ends up sharing the installed native Session. The last two exercises
 * touch the Lance JNI, so this test class doubles as a smoke check that
 * the plugin's Session API bindings are wired correctly against the
 * lance-core version we are compiled against.
 *
 * <p>Lance keeps a native thread pool alive across Session creations
 * (Tokio runtime shared per JVM), which trips the default
 * randomizedtesting thread-leak scanner. The same suppression is applied
 * in {@code LancePluginIT} and {@code LanceNamespaceServiceTests}, both
 * of which touch Lance from tests.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class LanceRegistryTests extends OpenSearchTestCase {

    @Override
    public void tearDown() throws Exception {
        // Each test is self-contained: leaving a Session installed from
        // one test into the next would let cache state leak between
        // cases and confuse the isSameAs check below.
        LanceRegistry.closeSession();
        super.tearDown();
    }

    public void testAllocatorIsSingletonAndNonNull() {
        assertNotNull(LanceRegistry.allocator());
        // The allocator is a static RootAllocator; two lookups must return the same instance.
        assertSame(LanceRegistry.allocator(), LanceRegistry.allocator());
    }

    public void testSessionLifecycleInstallsAndReleases() {
        assertNull("before initSession there is no shared Session", LanceRegistry.currentSession());

        LanceRegistry.initSession(64L * 1024 * 1024, 8L * 1024 * 1024);

        Session session = LanceRegistry.currentSession();
        assertNotNull("initSession must install a Session", session);
        assertFalse("newly installed Session should be open", session.isClosed());

        LanceRegistry.closeSession();

        assertNull("closeSession must clear the field", LanceRegistry.currentSession());
        assertTrue("released Session should be closed", session.isClosed());
    }

    public void testInitSessionReplacesExistingSession() {
        LanceRegistry.initSession(64L * 1024 * 1024, 8L * 1024 * 1024);
        Session first = LanceRegistry.currentSession();
        assertNotNull(first);

        LanceRegistry.initSession(32L * 1024 * 1024, 4L * 1024 * 1024);
        Session second = LanceRegistry.currentSession();

        assertNotNull(second);
        assertNotSame("replacing a Session must produce a new handle", first, second);
        assertTrue("previous Session must be closed after replacement", first.isClosed());
        assertFalse(second.isClosed());
    }

    public void testTwoDatasetsShareTheInstalledSession() throws Exception {
        LanceRegistry.initSession(64L * 1024 * 1024, 8L * 1024 * 1024);
        Session installed = LanceRegistry.currentSession();
        assertNotNull(installed);

        Path scratch = createTempDir();
        // Two independent tables so we exercise the "different URI, same
        // Session" path that is the whole point of installing the
        // Session in the first place.
        String uriOne = LanceTableFactory.writeTable(scratch, "one", 1);
        String uriTwo = LanceTableFactory.writeTable(scratch, "two", 1);

        try (
            Dataset first = LanceRegistry.openDataset(uriOne, StorageOptions.empty());
            Dataset second = LanceRegistry.openDataset(uriTwo, StorageOptions.empty())
        ) {
            assertTrue("both Datasets should be backed by the installed Session", first.session().isSameAs(installed));
            assertTrue("the two Datasets should observe the same native Session", first.session().isSameAs(second.session()));
        }
    }

    public void testOpenDatasetFallsBackToPerDatasetSessionWhenUninstalled() throws Exception {
        // Some unit tests (and any code path executed before the plugin's
        // createComponents runs) may open a Dataset without a shared
        // Session installed. The registry must fall back to Lance's
        // per-Dataset default Session so those callers keep working.
        assertNull(LanceRegistry.currentSession());
        Path scratch = createTempDir();
        String uri = LanceTableFactory.writeTable(scratch, "solo", 1);

        try (Dataset dataset = LanceRegistry.openDataset(uri, StorageOptions.empty())) {
            assertNotNull("Lance always attaches some Session to a Dataset", dataset.session());
        }
    }
}
