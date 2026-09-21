/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Group cutting and scheduling of {@link FragmentGroupScan}: contiguous
 * groups of near equal size, results in group order whatever thread ran
 * them, the caller taking over what a full or rejecting pool leaves, and
 * the first failure ending the run.
 */
public class FragmentGroupScanTests extends OpenSearchTestCase {

    public void testGroupSplitIsContiguous() {
        assertEquals(List.of(List.of(0, 1, 2, 3, 4, 5, 6, 7)), FragmentGroupScan.splitContiguous(List.of(0, 1, 2, 3, 4, 5, 6, 7), 1));
        assertEquals(
            List.of(List.of(0, 1, 2, 3), List.of(4, 5, 6, 7)),
            FragmentGroupScan.splitContiguous(List.of(0, 1, 2, 3, 4, 5, 6, 7), 2)
        );
        assertEquals(
            List.of(List.of(0, 1), List.of(2, 3, 4), List.of(5, 6, 7)),
            FragmentGroupScan.splitContiguous(List.of(0, 1, 2, 3, 4, 5, 6, 7), 3)
        );
        assertEquals(
            "more parallelism than fragments: one fragment per group",
            List.of(List.of(3), List.of(9), List.of(12)),
            FragmentGroupScan.splitContiguous(List.of(3, 9, 12), 8)
        );
        assertEquals(List.of(List.of(7)), FragmentGroupScan.splitContiguous(List.of(7), 4));
        assertEquals(Collections.singletonList(null), FragmentGroupScan.splitContiguous(null, 4));
        assertEquals(Collections.singletonList(null), FragmentGroupScan.splitContiguous(List.of(), 4));
    }

    public void testResultsComeBackInGroupOrderFromSeveralThreads() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            FragmentGroupScan scan = new FragmentGroupScan(pool, 4);
            Set<String> threads = ConcurrentHashMap.newKeySet();
            List<Integer> fragments = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                fragments.add(i);
            }
            List<Integer> firstOfGroup = scan.run(fragments, group -> {
                threads.add(Thread.currentThread().getName());
                Thread.sleep(20);
                return group.get(0);
            });
            assertEquals(List.of(0, 4, 8, 12), firstOfGroup);
            assertTrue("the pool ran some of the groups: " + threads, threads.size() > 1);
        } finally {
            pool.shutdownNow();
        }
    }

    public void testSequentialRunsEveryGroupOnTheCaller() throws Exception {
        Set<String> threads = ConcurrentHashMap.newKeySet();
        List<Integer> sizes = FragmentGroupScan.SEQUENTIAL.run(List.of(1, 2, 3, 4, 5), group -> {
            threads.add(Thread.currentThread().getName());
            return group.size();
        });
        assertEquals(List.of(5), sizes);
        assertEquals(Set.of(Thread.currentThread().getName()), threads);
    }

    public void testCallerDrainsWhatARejectingPoolDoesNotTake() throws Exception {
        Executor rejecting = task -> { throw new RejectedExecutionException("full"); };
        AtomicInteger scans = new AtomicInteger();
        String caller = Thread.currentThread().getName();
        List<Integer> sizes = new FragmentGroupScan(rejecting, 3).run(List.of(0, 1, 2, 3, 4, 5), group -> {
            scans.incrementAndGet();
            assertEquals(caller, Thread.currentThread().getName());
            return group.size();
        });
        assertEquals(List.of(2, 2, 2), sizes);
        assertEquals(3, scans.get());
    }

    public void testCallerDoesNotWaitForATaskThePoolNeverStarted() throws Exception {
        // The executor accepts the task and never runs it until after the
        // caller finished; the caller must still return with every group.
        List<Runnable> parked = new ArrayList<>();
        Executor parking = parked::add;
        List<Integer> sizes = new FragmentGroupScan(parking, 4).run(List.of(0, 1, 2, 3, 4, 5, 6, 7), List::size);
        assertEquals(List.of(2, 2, 2, 2), sizes);
        assertEquals(3, parked.size());
        // Running the parked tasks afterwards finds nothing left to do.
        for (Runnable task : parked) {
            task.run();
        }
    }

    public void testFirstFailureIsThrownAndStopsFurtherGroups() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch failed = new CountDownLatch(1);
            AtomicInteger started = new AtomicInteger();
            FragmentGroupScan scan = new FragmentGroupScan(pool, 8);
            List<Integer> fragments = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                fragments.add(i);
            }
            IOException thrown = expectThrows(IOException.class, () -> scan.run(fragments, group -> {
                started.incrementAndGet();
                if (group.get(0) == 0) {
                    failed.countDown();
                    throw new IOException("group " + group + " failed");
                }
                // Groups picked up before the failure finish; the ones
                // not started by then are never started.
                assertTrue(failed.await(5, TimeUnit.SECONDS));
                Thread.sleep(50);
                return group.size();
            }));
            assertEquals("group [0] failed", thrown.getMessage());
            assertTrue("not every group ran after the failure: " + started.get(), started.get() < 8);
        } finally {
            pool.shutdownNow();
        }
    }
}
