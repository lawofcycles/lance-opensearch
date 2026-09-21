/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runs one Lance scan per contiguous group of fragments, the groups side
 * by side on an executor, so a request whose fragments would otherwise
 * be consumed by one thread uses several cores of the node.
 *
 * <p>A Lance scan decodes its batches on Lance's own thread pool, but the
 * Java side that consumes the {@code ArrowReader} (reading the values
 * into heap or off-heap arrays, interning keyword terms, aggregating the
 * rows of a Substrait plan) runs on the calling thread. On a node with
 * many fragments that one thread is the limit of the whole request.
 * {@link #splitContiguous} cuts the fragment list into
 * {@code min(fragments, parallelism)} runs of consecutive fragments and
 * {@link #run} scans every group with its own scan, the first group on the
 * calling thread and the others as tasks on the executor. Consecutive
 * rather than round robin so each scan reads fragments that are adjacent
 * in the manifest, the order they were written in.
 *
 * <p>The tasks and the caller draw group indexes from one shared counter,
 * so a task the executor has not started by the time the caller runs out
 * of groups has nothing left to do: the caller marks it as taken over and
 * does not wait for it. This keeps {@link #run} from blocking on a
 * saturated pool (and from deadlocking when every thread of that pool is
 * a caller waiting here) or on a task the pool rejected. Only tasks that
 * did start are awaited. A saturated pool therefore degrades a request to
 * one scan on its own thread instead of parking it.
 *
 * <p>When one group fails no further group is started, the groups already
 * running are left to finish (a Lance scan has no cancel from the Java
 * side) and the first failure is thrown with the others suppressed.
 */
public final class FragmentGroupScan {

    /** One scan per request: every fragment in one group, on the calling thread. */
    public static final FragmentGroupScan SEQUENTIAL = new FragmentGroupScan(Runnable::run, 1);

    /** The scan of one group of fragments; {@code fragmentIds} is {@code null} when the group is "every fragment". */
    @FunctionalInterface
    public interface GroupScan<T> {
        T scan(List<Integer> fragmentIds) throws Exception;
    }

    private final Executor executor;
    private final int parallelism;

    /**
     * @param executor    pool the groups after the first run on; the
     *                    caller's thread scans the first group and whatever
     *                    the pool does not pick up
     * @param parallelism upper bound of groups per request; 1 means one
     *                    scan over every fragment
     */
    public FragmentGroupScan(Executor executor, int parallelism) {
        this.executor = executor;
        this.parallelism = Math.max(1, parallelism);
    }

    /** Upper bound of groups per request. */
    public int parallelism() {
        return parallelism;
    }

    /**
     * Cuts {@code fragmentIds} into {@code min(size, parallelism)} runs of
     * consecutive fragments, as close to equal in count as the division
     * allows. A null or empty list (every fragment, handed to Lance as no
     * fragment restriction) or a single fragment stays one group, as does
     * a parallelism of 1.
     */
    public static List<List<Integer>> splitContiguous(List<Integer> fragmentIds, int parallelism) {
        if (fragmentIds == null || fragmentIds.isEmpty()) {
            return Collections.singletonList(null);
        }
        if (fragmentIds.size() == 1 || parallelism <= 1) {
            return Collections.singletonList(fragmentIds);
        }
        int count = fragmentIds.size();
        int groupCount = Math.min(count, parallelism);
        List<List<Integer>> groups = new ArrayList<>(groupCount);
        for (int g = 0; g < groupCount; g++) {
            int from = (int) ((long) count * g / groupCount);
            int to = (int) ((long) count * (g + 1) / groupCount);
            groups.add(List.copyOf(fragmentIds.subList(from, to)));
        }
        return groups;
    }

    /**
     * Scans {@code fragmentIds} in up to {@link #parallelism()} groups and
     * returns the group results in group order (the concatenation of the
     * groups is {@code fragmentIds} in its original order). A single group
     * is scanned on the calling thread without touching the executor.
     */
    public <T> List<T> run(List<Integer> fragmentIds, GroupScan<T> scan) throws Exception {
        return runGroups(splitContiguous(fragmentIds, parallelism), scan);
    }

    /**
     * {@link #run(List, GroupScan)} over groups the caller already cut,
     * for a caller that needs the groups themselves (to size the results
     * or to log them).
     */
    public <T> List<T> runGroups(List<List<Integer>> groups, GroupScan<T> scan) throws Exception {
        int groupCount = groups.size();
        if (groupCount == 1) {
            return Collections.singletonList(scan.scan(groups.get(0)));
        }
        Object[] results = new Object[groupCount];
        AtomicInteger next = new AtomicInteger();
        AtomicReference<Exception> failure = new AtomicReference<>();
        Runnable drain = () -> {
            int index;
            while (failure.get() == null && (index = next.getAndIncrement()) < groupCount) {
                try {
                    results[index] = scan.scan(groups.get(index));
                } catch (Exception e) {
                    if (!failure.compareAndSet(null, e)) {
                        failure.get().addSuppressed(e);
                    }
                }
            }
        };
        List<GroupTask> tasks = new ArrayList<>(groupCount - 1);
        for (int i = 1; i < groupCount; i++) {
            GroupTask task = new GroupTask(drain);
            try {
                executor.execute(task);
                tasks.add(task);
            } catch (RejectedExecutionException rejected) {
                // The pool is full; the calling thread scans what the
                // running tasks leave over.
                break;
            }
        }
        drain.run();
        for (GroupTask task : tasks) {
            task.awaitIfStarted();
        }
        if (failure.get() != null) {
            throw failure.get();
        }
        @SuppressWarnings("unchecked")
        List<T> typed = (List<T>) Arrays.asList(results);
        return typed;
    }

    /**
     * One executor task of {@link #runGroups}. Whoever flips {@code taken}
     * first owns the task: the pool thread runs the drain and signals
     * {@code done}, or the caller declares the task never started and
     * skips the wait, after which the pool thread returns at once when it
     * eventually gets to it.
     */
    private static final class GroupTask implements Runnable {
        private final Runnable drain;
        private final AtomicBoolean taken = new AtomicBoolean();
        private final CountDownLatch done = new CountDownLatch(1);

        GroupTask(Runnable drain) {
            this.drain = drain;
        }

        @Override
        public void run() {
            if (!taken.compareAndSet(false, true)) {
                return;
            }
            try {
                drain.run();
            } finally {
                done.countDown();
            }
        }

        void awaitIfStarted() throws InterruptedException {
            if (taken.compareAndSet(false, true)) {
                return;
            }
            done.await();
        }
    }
}
