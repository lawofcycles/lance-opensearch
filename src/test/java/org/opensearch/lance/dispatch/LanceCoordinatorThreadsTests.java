/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.dispatch;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.opensearch.lance.plan.metadata.TableStatisticsCollector;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@link LanceCoordinatorThreads}: the check reads the pool name out of
 * the thread name, the test JVM runs with assertions enabled, and a
 * guarded entry point ({@link TableStatisticsCollector#collect}) refuses
 * a {@code lance_coordinator} thread with an {@link AssertionError} that
 * names the work and the thread.
 */
public class LanceCoordinatorThreadsTests extends OpenSearchTestCase {

    private static final String COORDINATOR_THREAD = "opensearch[node-0][lance_coordinator][T#1]";
    private static final String GENERIC_THREAD = "opensearch[node-0][generic][T#3]";

    /** Run {@code task} to completion on a new thread named {@code threadName} and answer its result. */
    private static <T> T onThreadNamed(String threadName, Callable<T> task) throws Throwable {
        FutureTask<T> future = new FutureTask<>(task);
        Thread thread = new Thread(future, threadName);
        thread.start();
        thread.join();
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    public void testAssertionsAreEnabledInTheTestJvm() {
        boolean enabled = false;
        assert enabled = true;
        assertTrue("the guard relies on -ea in the test JVM", enabled);
    }

    public void testCoordinatorThreadIsRecognisedByName() throws Throwable {
        assertFalse(onThreadNamed(COORDINATOR_THREAD, LanceCoordinatorThreads::notOnCoordinator));
        assertTrue(onThreadNamed(GENERIC_THREAD, LanceCoordinatorThreads::notOnCoordinator));
        assertTrue(onThreadNamed("opensearch[node-0][lance_warm_up][T#1]", LanceCoordinatorThreads::notOnCoordinator));
        assertTrue("the test's own thread is not a coordinator thread", LanceCoordinatorThreads.notOnCoordinator());
    }

    public void testMessageNamesTheWorkAndTheThread() throws Throwable {
        String message = onThreadNamed(COORDINATOR_THREAD, () -> LanceCoordinatorThreads.message("table statistics collection"));
        assertEquals(
            "table statistics collection must not run on the lance_coordinator pool but ran on thread [" + COORDINATOR_THREAD + "]",
            message
        );
    }

    public void testStatisticsCollectionRefusesTheCoordinatorThread() throws Throwable {
        // The assertion is the first statement of collect, so no dataset
        // is touched before it fires.
        AssertionError error = expectThrows(
            AssertionError.class,
            () -> onThreadNamed(COORDINATOR_THREAD, () -> TableStatisticsCollector.collect(null))
        );
        assertTrue(error.getMessage(), error.getMessage().contains("table statistics collection"));
        assertTrue(error.getMessage(), error.getMessage().contains(COORDINATOR_THREAD));
    }
}
