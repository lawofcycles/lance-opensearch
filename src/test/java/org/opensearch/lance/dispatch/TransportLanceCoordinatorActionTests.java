/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.lance.dispatch;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.lance.LancePlugin;
import org.opensearch.lance.dispatch.TransportLanceCoordinatorAction.FragmentFanOut;
import org.opensearch.lance.dispatch.TransportLanceCoordinatorAction.FragmentFanOut.Outcome;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * {@link FragmentFanOut}: the per-node response handlers run on the
 * transport thread and only store and count, the merge runs on the
 * {@code lance_coordinator} pool once every node has answered, and
 * every failure path (rejected merge submit, throwing merge, a node
 * failing, the send itself throwing) completes the coordinator's
 * listener exactly once with {@code onFailure}. A node whose request
 * timed out or whose executor was cancelled leaves its slot empty and
 * marks the outcome, or fails the fan-out with a 504 when partial
 * results are not allowed; a cancelled coordinator task ends the
 * fan-out with {@code TaskCancelledException} instead of a merge.
 */
public class TransportLanceCoordinatorActionTests extends OpenSearchTestCase {

    private static final DiscoveryNode NODE_A = new DiscoveryNode("a", buildNewFakeTransportAddress(), Version.CURRENT);
    private static final DiscoveryNode NODE_B = new DiscoveryNode("b", buildNewFakeTransportAddress(), Version.CURRENT);

    private TestThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // One thread and a queue of one, the same shape as
        // thread_pool.lance_coordinator.size / queue_size, so a test can
        // fill the pool with two runnables and have the third rejected.
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                LancePlugin.LANCE_COORDINATOR_THREAD_POOL,
                1,
                1,
                "thread_pool." + LancePlugin.LANCE_COORDINATOR_THREAD_POOL
            )
        );
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testResponseHandlerRunsOnTheTransportThread() throws Exception {
        // The executor name tells the transport layer to call the
        // handler inline; the recording executor below then shows that
        // the handler does its work (store, count, submit) on the
        // thread that called handleResponse, and that the merge runs
        // on the pool, not on that thread.
        CountingListener done = new CountingListener(coordinatorPool());
        Thread caller = Thread.currentThread();
        AtomicReference<Thread> submitThread = new AtomicReference<>();
        AtomicReference<Thread> mergeThread = new AtomicReference<>();
        Executor recordingSubmit = command -> {
            submitThread.set(Thread.currentThread());
            coordinatorPool().execute(command);
        };
        FragmentFanOut fanOut = new FragmentFanOut(1, recordingSubmit, responses -> mergeThread.set(Thread.currentThread()), done);

        assertEquals(ThreadPool.Names.SAME, fanOut.handler(0).executor());
        fanOut.handler(0).handleResponse(response(1L));
        assertSame("the merge submit must happen inside handleResponse, on the calling thread", caller, submitThread.get());

        done.await();
        assertEquals(1, done.responses.get());
        assertNotSame("the merge must not run on the calling thread", caller, mergeThread.get());
        assertThat(mergeThread.get().getName(), containsString(LancePlugin.LANCE_COORDINATOR_THREAD_POOL));
    }

    public void testMergeRunsOnCoordinatorPoolInSlotOrderAfterLastResponse() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicReference<String> mergeThread = new AtomicReference<>();
        AtomicReference<List<LanceFragmentQueryResponse>> mergedResponses = new AtomicReference<>();
        FragmentFanOut fanOut = new FragmentFanOut(2, coordinatorPool(), outcome -> {
            mergeThread.set(Thread.currentThread().getName());
            mergedResponses.set(outcome.responses());
        }, done);
        LanceFragmentQueryResponse first = response(1L);
        LanceFragmentQueryResponse second = response(2L);

        // Slot 1 answers first; the merge must still see slot order.
        fanOut.handler(1).handleResponse(second);
        assertNull("merge must wait for every node", mergedResponses.get());
        fanOut.handler(0).handleResponse(first);

        done.await();
        assertEquals(1, done.responses.get());
        assertEquals(0, done.failures.get());
        assertThat(mergeThread.get(), containsString(LancePlugin.LANCE_COORDINATOR_THREAD_POOL));
        assertSame(first, mergedResponses.get().get(0));
        assertSame(second, mergedResponses.get().get(1));
    }

    public void testRejectedMergeSubmitFailsListenerOnceWithRejection() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicInteger merges = new AtomicInteger();
        FragmentFanOut fanOut = new FragmentFanOut(1, coordinatorPool(), responses -> merges.incrementAndGet(), done);

        // Occupy the single thread and the single queue slot, so the
        // merge submit is refused on the calling thread.
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch queuedRan = new CountDownLatch(1);
        coordinatorPool().execute(() -> awaitQuietly(release));
        coordinatorPool().execute(queuedRan::countDown);

        fanOut.handler(0).handleResponse(response(1L));

        // The rejection has been delivered by now. Release the pool and
        // let it drain, so a merge that had been queued despite the
        // rejection would have run and completed the listener a second
        // time before the counts are read.
        release.countDown();
        assertTrue("the queued runnable did not run", queuedRan.await(10, TimeUnit.SECONDS));
        done.await();
        assertEquals(0, done.responses.get());
        assertEquals(1, done.failures.get());
        assertThat(done.failure.get(), instanceOf(OpenSearchRejectedExecutionException.class));
        assertThat(done.failure.get().getMessage(), containsString("rejected execution"));
        assertThat(done.failure.get().getMessage(), containsString(LancePlugin.LANCE_COORDINATOR_THREAD_POOL));
        assertEquals("a rejected merge must not run", 0, merges.get());
    }

    public void testThrowingMergeFailsListenerOnce() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        IllegalStateException boom = new IllegalStateException("cannot merge sort values");
        FragmentFanOut fanOut = new FragmentFanOut(2, coordinatorPool(), responses -> { throw boom; }, done);

        fanOut.handler(0).handleResponse(response(1L));
        fanOut.handler(1).handleResponse(response(2L));

        done.await();
        assertEquals(0, done.responses.get());
        assertEquals(1, done.failures.get());
        assertSame(boom, done.failure.get());
    }

    public void testNodeFailureFailsListenerOnceAndSkipsMerge() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicInteger merges = new AtomicInteger();
        FragmentFanOut fanOut = new FragmentFanOut(2, coordinatorPool(), responses -> merges.incrementAndGet(), done);
        RemoteTransportException nodeGone = new RemoteTransportException("node b left", null);

        fanOut.handler(1).handleException(nodeGone);
        assertEquals("the fan-out waits for the other node before failing", 0, done.failures.get());
        fanOut.handler(0).handleResponse(response(1L));

        done.await();
        assertEquals(0, done.responses.get());
        assertEquals(1, done.failures.get());
        assertSame(nodeGone, done.failure.get());
        assertEquals(0, merges.get());
    }

    public void testEveryNodeFailingFailsListenerOnce() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        FragmentFanOut fanOut = new FragmentFanOut(2, coordinatorPool(), responses -> {}, done);

        fanOut.handler(0).handleException(new RemoteTransportException("a", null));
        fanOut.handler(1).handleException(new RemoteTransportException("b", null));

        done.await();
        assertEquals(0, done.responses.get());
        assertEquals(1, done.failures.get());
    }

    public void testSendThrowingCountsAsThatNodeFailing() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicInteger merges = new AtomicInteger();
        FragmentFanOut fanOut = new FragmentFanOut(2, coordinatorPool(), responses -> merges.incrementAndGet(), done);
        IllegalStateException notConnected = new IllegalStateException("node b is not connected");
        AtomicReference<TransportResponseHandler<LanceFragmentQueryResponse>> handlerA = new AtomicReference<>();
        FragmentFanOut.Sender sender = (node, request, handler) -> {
            if (node == NODE_B) {
                throw notConnected;
            }
            handlerA.set(handler);
        };

        fanOut.send(sender, 0, NODE_A, null);
        fanOut.send(sender, 1, NODE_B, null);
        assertEquals("the fan-out waits for node a before failing", 0, done.failures.get());
        handlerA.get().handleResponse(response(1L));

        done.await();
        assertEquals(0, done.responses.get());
        assertEquals(1, done.failures.get());
        assertSame(notConnected, done.failure.get());
        assertEquals(0, merges.get());
    }

    public void testSendThrowingForEveryNodeFailsListenerOnceWithoutAnyResponse() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        FragmentFanOut fanOut = new FragmentFanOut(2, coordinatorPool(), responses -> {}, done);
        FragmentFanOut.Sender sender = (node, request, handler) -> { throw new IllegalStateException(node.getId()); };

        fanOut.send(sender, 0, NODE_A, null);
        fanOut.send(sender, 1, NODE_B, null);

        done.await();
        assertEquals(0, done.responses.get());
        assertEquals(1, done.failures.get());
    }

    public void testTimedOutNodeLeavesItsSlotEmptyAndMarksTheOutcomeWhenPartialResultsAreAllowed() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicReference<Outcome> merged = new AtomicReference<>();
        List<String> incomplete = new CopyOnWriteArrayList<>();
        FragmentFanOut fanOut = new FragmentFanOut(
            2,
            coordinatorPool(),
            merged::set,
            done,
            /* allowPartialResults */ true,
            /* task */ null,
            coordinatorPool(),
            (node, request, cause) -> incomplete.add(node.getId() + ":" + cause.getClass().getSimpleName())
        );
        FragmentFanOut.Sender keep = (node, request, handler) -> {};
        fanOut.send(keep, 0, NODE_A, null);
        fanOut.send(keep, 1, NODE_B, null);

        fanOut.handler(1).handleException(timeout(NODE_B));
        assertNull("the fan-out waits for the other node before merging", merged.get());
        LanceFragmentQueryResponse fromA = response(3L);
        fanOut.handler(0).handleResponse(fromA);

        done.await();
        assertEquals(1, done.responses.get());
        assertEquals(0, done.failures.get());
        assertEquals("only the response that arrived is merged", List.of(fromA), merged.get().responses());
        assertEquals(1, merged.get().incompleteNodes());
        assertEquals(List.of("b:ReceiveTimeoutTransportException"), incomplete);
    }

    public void testTimedOutNodeFailsTheFanOutWithGatewayTimeoutWhenPartialResultsAreNotAllowed() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicInteger merges = new AtomicInteger();
        List<String> incomplete = new CopyOnWriteArrayList<>();
        FragmentFanOut fanOut = new FragmentFanOut(
            2,
            coordinatorPool(),
            outcome -> merges.incrementAndGet(),
            done,
            /* allowPartialResults */ false,
            /* task */ null,
            coordinatorPool(),
            (node, request, cause) -> incomplete.add(node.getId())
        );
        FragmentFanOut.Sender keep = (node, request, handler) -> {};
        fanOut.send(keep, 0, NODE_A, null);
        fanOut.send(keep, 1, NODE_B, null);

        ReceiveTimeoutTransportException timeout = timeout(NODE_B);
        fanOut.handler(1).handleException(timeout);
        assertEquals("the fan-out waits for the other node before failing", 0, done.failures.get());
        fanOut.handler(0).handleResponse(response(1L));

        done.await();
        assertEquals(0, done.responses.get());
        assertEquals(1, done.failures.get());
        assertThat(done.failure.get(), instanceOf(OpenSearchTimeoutException.class));
        assertEquals(RestStatus.GATEWAY_TIMEOUT, ((OpenSearchTimeoutException) done.failure.get()).status());
        assertSame(timeout, done.failure.get().getCause());
        assertEquals("the node is reported even when the request fails", List.of("b"), incomplete);
        assertEquals(0, merges.get());
    }

    public void testCancelledExecutorCountsAsIncompleteNode() throws Exception {
        // A node whose executor task was cancelled answers a
        // TaskCancelledException wrapped by the transport layer; it is
        // treated like a node that timed out, not as a failure of the
        // request.
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicReference<Outcome> merged = new AtomicReference<>();
        FragmentFanOut fanOut = new FragmentFanOut(2, coordinatorPool(), merged::set, done);

        fanOut.handler(0)
            .handleException(new RemoteTransportException("a", new TaskCancelledException("cancelled task with reason: test")));
        LanceFragmentQueryResponse fromB = response(2L);
        fanOut.handler(1).handleResponse(fromB);

        done.await();
        assertEquals(1, done.responses.get());
        assertEquals(List.of(fromB), merged.get().responses());
        assertEquals(1, merged.get().incompleteNodes());
    }

    public void testEveryNodeTimingOutMergesNothingAndMarksTheOutcome() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicReference<Outcome> merged = new AtomicReference<>();
        FragmentFanOut fanOut = new FragmentFanOut(2, coordinatorPool(), merged::set, done);

        fanOut.handler(0).handleException(timeout(NODE_A));
        fanOut.handler(1).handleException(timeout(NODE_B));

        done.await();
        assertEquals(1, done.responses.get());
        assertEquals(List.of(), merged.get().responses());
        assertEquals(2, merged.get().incompleteNodes());
    }

    public void testCancelledCoordinatorTaskSkipsTheMergeAndFailsWithTaskCancelled() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicInteger merges = new AtomicInteger();
        TestTask task = new TestTask();
        FragmentFanOut fanOut = new FragmentFanOut(
            2,
            coordinatorPool(),
            outcome -> merges.incrementAndGet(),
            done,
            true,
            task,
            coordinatorPool(),
            (node, request, cause) -> {}
        );

        fanOut.handler(0).handleResponse(response(1L));
        task.cancel("client closed the connection");
        fanOut.handler(1).handleResponse(response(2L));

        done.await();
        assertEquals(0, done.responses.get());
        assertEquals(1, done.failures.get());
        assertThat(done.failure.get(), instanceOf(TaskCancelledException.class));
        assertThat(done.failure.get().getMessage(), containsString("client closed the connection"));
        assertEquals("a cancelled task must not merge", 0, merges.get());
    }

    public void testCancellationAfterTheLastResponseStillEndsCancelled() throws Exception {
        // The task is cancelled after every node has answered but before
        // the queued merge runs: the merge is skipped and the request
        // ends cancelled, because the task's state is read when the
        // fan-out completes, not when the responses arrived.
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicInteger merges = new AtomicInteger();
        TestTask task = new TestTask();
        FragmentFanOut fanOut = new FragmentFanOut(
            1,
            coordinatorPool(),
            outcome -> merges.incrementAndGet(),
            done,
            true,
            task,
            coordinatorPool(),
            (node, request, cause) -> {}
        );
        CountDownLatch release = new CountDownLatch(1);
        coordinatorPool().execute(() -> awaitQuietly(release));

        fanOut.handler(0).handleResponse(response(1L));
        task.cancel("cancelled while the merge waited for the pool");
        release.countDown();

        done.await();
        assertEquals(0, done.responses.get());
        assertEquals(1, done.failures.get());
        assertThat(done.failure.get(), instanceOf(TaskCancelledException.class));
        assertEquals(0, merges.get());
    }

    public void testIncompleteListenerRunsOnTheNotifyPoolNotOnTheTransportThread() throws Exception {
        CountingListener done = new CountingListener(coordinatorPool());
        AtomicReference<Thread> listenerThread = new AtomicReference<>();
        Thread caller = Thread.currentThread();
        FragmentFanOut fanOut = new FragmentFanOut(
            1,
            coordinatorPool(),
            outcome -> {},
            done,
            true,
            null,
            coordinatorPool(),
            (node, request, cause) -> listenerThread.set(Thread.currentThread())
        );

        fanOut.handler(0).handleException(timeout(NODE_A));

        done.await();
        assertNotNull("the listener must have run", listenerThread.get());
        assertNotSame("the listener must not run on the transport thread", caller, listenerThread.get());
        assertThat(listenerThread.get().getName(), containsString(LancePlugin.LANCE_COORDINATOR_THREAD_POOL));
    }

    public void testCancelledCoordinatorTaskReportsTaskCancelledInPlaceOfANodeFailure() throws Exception {
        // Once the coordinator task is cancelled its executors answer
        // TaskCancelledException too; whatever the last exception was,
        // the request ends as cancelled and the original is kept as a
        // suppressed exception.
        CountingListener done = new CountingListener(coordinatorPool());
        TestTask task = new TestTask();
        FragmentFanOut fanOut = new FragmentFanOut(
            2,
            coordinatorPool(),
            outcome -> {},
            done,
            false,
            task,
            coordinatorPool(),
            (node, request, cause) -> {}
        );

        task.cancel("cancelled through _tasks/_cancel");
        RemoteTransportException nodeGone = new RemoteTransportException("node b left", null);
        fanOut.handler(1).handleException(nodeGone);
        fanOut.handler(0).handleResponse(response(1L));

        done.await();
        assertEquals(1, done.failures.get());
        assertThat(done.failure.get(), instanceOf(TaskCancelledException.class));
        assertEquals(List.of(nodeGone), List.of(done.failure.get().getSuppressed()));
    }

    public void testSendThrowingAfterCancellationCountsAsThatNodeFailingAndEndsCancelled() throws Exception {
        // TransportService.sendChildRequest refuses a child of a
        // cancelled task by throwing from the send; the fan-out treats
        // that like any other synchronous send failure and the request
        // ends cancelled.
        CountingListener done = new CountingListener(coordinatorPool());
        TestTask task = new TestTask();
        task.cancel("cancelled before the fan-out");
        FragmentFanOut fanOut = new FragmentFanOut(
            1,
            coordinatorPool(),
            outcome -> {},
            done,
            true,
            task,
            coordinatorPool(),
            (node, request, cause) -> {}
        );
        FragmentFanOut.Sender refusing = (node, request, handler) -> {
            throw new TaskCancelledException("The parent task was cancelled, shouldn't start any child tasks");
        };

        fanOut.send(refusing, 0, NODE_A, null);

        done.await();
        assertEquals(0, done.responses.get());
        assertEquals(1, done.failures.get());
        assertThat(done.failure.get(), instanceOf(TaskCancelledException.class));
    }

    public void testIsIncompleteRecognisesTimeoutsAndCancelledExecutors() {
        assertTrue(FragmentFanOut.isIncomplete(timeout(NODE_A)));
        assertTrue(FragmentFanOut.isIncomplete(new RemoteTransportException("a", new TaskCancelledException("cancelled"))));
        assertFalse(FragmentFanOut.isIncomplete(new RemoteTransportException("a", new IllegalStateException("boom"))));
        assertFalse(FragmentFanOut.isIncomplete(new TransportException("closed")));
    }

    private static ReceiveTimeoutTransportException timeout(DiscoveryNode node) {
        return new ReceiveTimeoutTransportException(node, LanceFragmentQueryAction.NAME, "request_id [1] timed out after [10ms]");
    }

    /** A coordinator task that can be cancelled from the test. */
    private static final class TestTask extends CancellableTask {
        TestTask() {
            super(1L, "transport", LanceCoordinatorAction.NAME, "test", TaskId.EMPTY_TASK_ID, Map.of());
        }

        @Override
        public boolean shouldCancelChildrenOnCancellation() {
            return true;
        }
    }

    private ExecutorService coordinatorPool() {
        return threadPool.executor(LancePlugin.LANCE_COORDINATOR_THREAD_POOL);
    }

    private static LanceFragmentQueryResponse response(long matched) {
        return new LanceFragmentQueryResponse(matched, false, 1, List.of(), new long[0], null);
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Counts completions and lets a test wait for the first one and for
     * anything a second completion could ride on to have run.
     */
    private static final class CountingListener implements ActionListener<Void> {
        final AtomicInteger responses = new AtomicInteger();
        final AtomicInteger failures = new AtomicInteger();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        private final CountDownLatch completed = new CountDownLatch(1);
        private final ExecutorService pool;

        CountingListener(ExecutorService pool) {
            this.pool = pool;
        }

        @Override
        public void onResponse(Void unused) {
            responses.incrementAndGet();
            completed.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            failures.incrementAndGet();
            failure.set(e);
            completed.countDown();
        }

        /**
         * Waits for the first completion, then for the pool to have run
         * everything queued on it so far. A second completion can only
         * come from the calling thread (already returned when a test
         * gets here) or from the merge runnable on the pool, and the
         * pool is one thread with a FIFO queue, so once a marker
         * submitted after the first completion has run, every merge
         * that could still complete the listener has run too.
         */
        void await() throws Exception {
            assertTrue("listener was not completed", completed.await(10, TimeUnit.SECONDS));
            CountDownLatch drained = new CountDownLatch(1);
            pool.execute(drained::countDown);
            assertTrue("pool did not drain", drained.await(10, TimeUnit.SECONDS));
        }
    }
}
