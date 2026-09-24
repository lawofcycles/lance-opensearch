/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lance.namespace;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.lance.namespace.LanceNamespace;
import org.opensearch.common.CheckedFunction;
import org.opensearch.secure_sm.AccessController;

/**
 * A cached catalog handle whose release waits for the calls in flight
 * against it.
 *
 * <p>{@code DirectoryNamespace.close} frees the Rust object behind the
 * handle's raw pointer with no synchronisation of its own, and every
 * other native method dereferences that pointer unchecked. A
 * {@code listTables} still running on one thread while {@code close}
 * runs on another therefore reads freed memory and takes the node down
 * with a SIGSEGV instead of an exception. The poll (on the cluster
 * manager) and the tables preview (on any node) call into a handle from
 * the generic pool, and the cluster state applier hands a removed
 * registration's handle to that same pool for closing, so the two can
 * overlap whenever a registration is removed while a poll cycle is in
 * progress.
 *
 * <p>Calls take the read side of the lock and run concurrently with
 * each other; the Rust implementations take {@code &self} and are
 * {@code Sync}, so concurrent calls on a live handle are safe.
 * {@link #close} takes the write side, which waits for every in-flight
 * call to return before the native release runs, and releases once.
 * A call that arrives after the release fails with
 * {@link ReleasedException} rather than reaching native code.
 */
final class LanceNamespaceHandle {

    private final LanceNamespace namespace;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    /**
     * Written under the write lock; volatile so {@link #isClosed} can
     * answer without queueing behind a pending close.
     */
    private volatile boolean closed;

    LanceNamespaceHandle(LanceNamespace namespace) {
        this.namespace = namespace;
    }

    /**
     * Thrown by {@link #call} once the handle has been released: the
     * registration behind it was removed while the caller was between
     * looking the handle up and using it.
     */
    static final class ReleasedException extends IllegalStateException {
        ReleasedException() {
            super("namespace handle already released");
        }
    }

    /**
     * Run {@code call} against the namespace while holding it open.
     *
     * <p>The call runs inside {@code doPrivileged}. The Java agent's
     * permission check intersects every protection domain on the stack
     * up to the nearest {@code doPrivileged} frame, and the callers (the
     * poll's schedule on the cluster manager, the tables preview's fork)
     * reach here through server frames whose domain has no read grant
     * under the user's home. The Glue client resolves the AWS SDK's
     * default credential chain on its first request, which reads
     * {@code ~/.aws/credentials} and {@code ~/.aws/config} of the
     * process user; the frame here stops the walk at the plugin's own
     * jars, whose policy carries the read grant for that directory.
     * Every namespace type (rest, iceberg, polaris, unity, glue) passes
     * through this method, and all of them run under the same
     * privileged frame on purpose: this is the one choke point for
     * catalog calls, and the plugin policy is narrow enough that the
     * types which read no local file gain nothing from the elevation.
     *
     * @throws ReleasedException if the handle has been released
     */
    <T> T call(CheckedFunction<LanceNamespace, T, Exception> call) throws Exception {
        lock.readLock().lock();
        try {
            if (closed) {
                throw new ReleasedException();
            }
            return AccessController.doPrivilegedChecked(() -> call.apply(namespace));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Release the namespace once every in-flight call has returned.
     * Idempotent: a second call is a no-op. Implementations that are
     * not {@link AutoCloseable} (the HTTP client based catalogs) have
     * nothing to release and only stop accepting calls.
     */
    void close() throws Exception {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (namespace instanceof AutoCloseable closeable) {
                closeable.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Visible for tests: whether {@link #close} has run. Reads the flag
     * without the lock: a reader arriving while a close is queued would
     * otherwise wait for that close, which is the right behaviour for
     * {@link #call} but not for a status probe.
     */
    boolean isClosed() {
        return closed;
    }
}
