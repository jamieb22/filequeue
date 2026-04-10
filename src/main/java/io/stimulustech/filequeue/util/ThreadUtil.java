/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stimulustech.filequeue.util;

import java.util.concurrent.*;

/**
 * Thread and executor utility methods.
 *
 * <p>In Java 21 the {@link Thread.Builder} API replaces hand-written
 * {@link ThreadFactory} implementations. {@code getFlexibleThreadFactory} and
 * {@code getGroupThreadFactory} now delegate directly to
 * {@link Thread#ofPlatform()}.
 * </p>
 */
public class ThreadUtil {

    /** Utility class; do not instantiate. */
    private ThreadUtil() {}

    /** Default core pool size used when constructing executor services. */
    public static final int DEFAULT_CORE_POOL_SIZE = 1;

    /**
     * Returns a {@link ThreadFactory} that creates named platform threads.
     *
     * @param name   prefix used for thread names (a counter is appended)
     * @return thread factory
     */
    public static ThreadFactory getFlexibleThreadFactory(String name) {
        return Thread.ofPlatform().name(name + "-", 1).factory();
    }

    /**
     * Returns a {@link ThreadFactory} that creates named platform threads with the
     * specified daemon status.
     *
     * @param name   prefix used for thread names (a counter is appended)
     * @param daemon {@code true} if the created threads should be daemon threads
     * @return thread factory
     */
    public static ThreadFactory getFlexibleThreadFactory(String name, boolean daemon) {
        return Thread.ofPlatform().name(name + "-", 1).daemon(daemon).factory();
    }

    /**
     * Returns a {@link ThreadFactory} that creates named platform threads belonging
     * to the given {@link ThreadGroup}.
     *
     * @param group      thread group, or {@code null} to use the calling thread's group
     * @param threadName prefix for thread names (a counter is appended)
     * @param daemon     {@code true} if the created threads should be daemon threads
     * @return thread factory
     */
    public static ThreadFactory getGroupThreadFactory(ThreadGroup group, String threadName, boolean daemon) {
        var builder = Thread.ofPlatform()
                .group(group != null ? group : Thread.currentThread().getThreadGroup())
                .name(threadName + "-", 1)
                .daemon(daemon);
        return builder.factory();
    }

    /**
     * Shuts down the given executor, waiting up to {@code timeout} for tasks to finish,
     * then forcing a shutdown if tasks have not completed.
     *
     * @param pool     executor to shut down
     * @param timeout  maximum wait time
     * @param timeUnit unit for {@code timeout}
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public static void shutdownAndAwaitTermination(ExecutorService pool, long timeout, TimeUnit timeUnit)
            throws InterruptedException {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(timeout, timeUnit)) {
                pool.shutdownNow();
                pool.awaitTermination(timeout, timeUnit);
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            throw ie;
        }
    }

    /**
     * Throws {@link InterruptedException} if the current thread's interrupt flag is set,
     * clearing the flag in the process.
     *
     * @throws InterruptedException if the current thread has been interrupted
     */
    public static void checkInterrupt() throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException("interrupted");
        }
    }

    /**
     * Creates a flexible thread pool where threads are created on demand up to
     * {@code maximumPoolSize} and expire after 60 seconds of inactivity.
     *
     * @param corePoolSize    minimum number of threads to keep alive
     * @param maximumPoolSize maximum number of threads
     * @param threadFactory   factory for new threads
     * @return configured {@link ExecutorService}
     */
    public static ExecutorService newFlexiThreadPool(int corePoolSize, int maximumPoolSize,
                                                     ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(corePoolSize, maximumPoolSize,
                60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
    }

    /**
     * Creates a flexible thread pool with a single core thread and up to
     * {@code maximumPoolSize} threads, using the supplied factory.
     *
     * @param maximumPoolSize maximum number of threads
     * @param threadFactory   factory for new threads
     * @return configured {@link ExecutorService}
     */
    public static ExecutorService newFlexiThreadPool(int maximumPoolSize, ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(DEFAULT_CORE_POOL_SIZE, maximumPoolSize,
                60L, TimeUnit.SECONDS, new SynchronousQueue<>(), threadFactory);
    }
}
