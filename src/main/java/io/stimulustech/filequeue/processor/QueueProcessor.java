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

package io.stimulustech.filequeue.processor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.util.concurrent.MoreExecutors;
import io.stimulustech.filequeue.FileQueueItem;
import io.stimulustech.filequeue.FileQueue;
import io.stimulustech.filequeue.store.MVStoreQueue;
import io.stimulustech.filequeue.util.AdjustableSemaphore;
import io.stimulustech.filequeue.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Internal queue processor responsible for dispatching {@link FileQueueItem} instances to
 * a {@link Consumer}, persisting items that cannot be immediately processed, and retrying
 * them according to the configured delay strategy.
 * <p>
 * Items are first offered directly to the thread pool supplied via {@link QueueProcessorBuilder}.
 * When no thread is available the item is serialised to an H2 MVStore database on disk.
 * A background cleaner task periodically polls the database, deserialises each item, and
 * re-submits it for processing once the configured retry delay has elapsed.
 * </p>
 * <p>
 * Retry behaviour is controlled by {@link RetryDelayAlgorithm}: {@code FIXED} applies a
 * constant interval, while {@code EXPONENTIAL} doubles the delay on each attempt up to
 * {@code maxRetryDelay}. Once an item exceeds {@code maxTries} the optional
 * {@link Expiration} handler is called and the item is discarded.
 * </p>
 * <p>
 * This class is for internal use only. Use {@link FileQueue}
 * as the public entry point.
 * </p>
 *
 * @param <T> the type of {@link FileQueueItem} managed by this processor
 * @author Valentin Popov
 * @author Jamie Band
 */
public class QueueProcessor<T extends FileQueueItem> {

    private static final Logger logger = LoggerFactory.getLogger(QueueProcessor.class);

    private static final ScheduledExecutorService mvstoreCleanUPScheduler =
            Executors.newSingleThreadScheduledExecutor(
                    ThreadUtil.getFlexibleThreadFactory("mvstore-cleanup", true));

    static {
        MoreExecutors.addDelayedShutdownHook(mvstoreCleanUPScheduler, 60L, TimeUnit.SECONDS);
    }

    /**
     * Shuts down the shared MVStore cleanup scheduler and awaits termination for up to
     * 60 seconds. Call this once, at application exit, to cleanly release background threads.
     */
    public static void destroy() {
        MoreExecutors.shutdownAndAwaitTermination(mvstoreCleanUPScheduler, 60L, TimeUnit.SECONDS);
    }

    /**
     * Algorithm used to calculate the delay between successive delivery attempts.
     */
    public enum RetryDelayAlgorithm {
        /** Applies a constant delay between each retry attempt. */
        FIXED,
        /** Doubles the delay after each failed attempt, up to {@code maxRetryDelay}. */
        EXPONENTIAL
    }

    private final ExecutorService executorService;
    private final ObjectMapper objectMapper;
    private final MVStoreQueue mvStoreQueue;
    private final Type type;
    private final JavaType javaType;
    private final Consumer<? super T> consumer;
    private final Expiration<T> expiration;
    private final Phaser restorePolled = new Phaser();
    private final ScheduledFuture<?> cleanupTaskScheduler;
    /**
     * Held by {@link MVStoreCleaner#run()} for its entire execution so that {@link #close()}
     * can acquire it and be guaranteed the cleaner has finished before the MVStore is closed.
     */
    private final ReentrantLock cleanerLock = new ReentrantLock();
    /**
     * Guards {@link #close()} against being called more than once. A second call is a no-op.
     */
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile boolean doRun = true;
    private final int maxTries;
    private final int retryDelay;
    private final int persistRetryDelay;
    private final int maxRetryDelay;
    private final Path queuePath;
    private final String queueName;
    private final TimeUnit retryDelayUnit;
    private final TimeUnit persistRetryDelayUnit;
    private final RetryDelayAlgorithm retryDelayAlgorithm;
    private final AdjustableSemaphore permits = new AdjustableSemaphore();

    /**
     * Creates a new {@code QueueProcessor} from the supplied builder configuration.
     *
     * @param builder queue processor builder
     * @throws IllegalStateException    if the queue is not running
     * @throws IllegalArgumentException if the type cannot be serialized by Jackson
     * @throws IOException              if the item could not be serialized
     */
    QueueProcessor(QueueProcessorBuilder<T> builder) throws IOException, IllegalStateException, IllegalArgumentException, InterruptedException {
        if (builder.queueName == null) throw new IllegalArgumentException("queue name must be specified");
        if (builder.queuePath == null) throw new IllegalArgumentException("queue path must be specified");
        if (builder.type == null) throw new IllegalArgumentException("item type must be specified");
        if (builder.consumer == null) throw new IllegalArgumentException("consumer must be specified");
        if (builder.executorService == null) throw new IllegalArgumentException("executor service must be specified");
        objectMapper = builder.objectMapper != null ? builder.objectMapper : createObjectMapper();
        if (!objectMapper.canSerialize(objectMapper.constructType(builder.type).getRawClass()))
            throw new IllegalArgumentException("The given type is not serializable. it cannot be serialized by jackson");
        this.javaType = objectMapper.constructType(builder.type);
        this.queueName = builder.queueName;
        this.queuePath = builder.queuePath;
        this.consumer = builder.consumer;
        this.executorService = builder.executorService;
        this.expiration = builder.expiration;
        this.type = builder.type;
        this.maxTries = builder.maxTries;
        this.retryDelay = builder.retryDelay;
        this.retryDelayUnit = builder.retryDelayUnit;
        this.maxRetryDelay = builder.maxRetryDelay;
        this.retryDelayAlgorithm = builder.retryDelayAlgorithm;
        mvStoreQueue = new MVStoreQueue(builder.queuePath, builder.queueName);
        this.persistRetryDelay = builder.persistRetryDelay <= 0
                ? (retryDelay <= 1 ? 1 : retryDelay / 2)
                : builder.persistRetryDelay;
        this.persistRetryDelayUnit = builder.persistRetryDelayUnit;
        cleanupTaskScheduler = mvstoreCleanUPScheduler.scheduleWithFixedDelay(
                new MVStoreCleaner(), 0, persistRetryDelay, persistRetryDelayUnit);
        setMaxQueueSize(builder.maxQueueSize);
    }

    /**
     * Submits an item for immediate processing. If the thread pool cannot accept the item
     * within the specified wait time, an {@link IllegalStateException} is thrown. If the
     * pool is busy the item is persisted to disk and retried later.
     *
     * @param item            queue item to process
     * @param acquireWait     maximum time to wait for a free slot
     * @param acquireWaitUnit time unit for {@code acquireWait}
     * @throws IllegalStateException if the queue is not running or the slot could not be
     *                               acquired within the wait period
     * @throws InterruptedException  if the calling thread is interrupted while waiting
     * @throws IOException           if the item cannot be serialised to the persistent store
     */
    public void submit(final T item, int acquireWait, TimeUnit acquireWaitUnit) throws IOException, InterruptedException {
        if (!doRun)
            throw new IllegalStateException("file queue {" + getQueueBaseDir() + "} is not running");
        if (!permits.tryAcquire(1, acquireWait, acquireWaitUnit))
            throw new IllegalStateException(
                    "filequeue %s is full. {maxQueueSize='%d'}".formatted(queuePath, permits.getNumberOfPermits()));
        // Re-check after acquiring: close() may have released extra permits to unblock us.
        if (!doRun) {
            permits.release();
            throw new IllegalStateException("file queue {" + getQueueBaseDir() + "} is not running");
        }
        var submitted = false;
        try {
            dispatchOrPersist(item);
            submitted = true;
        } finally {
            if (!submitted) permits.release();
        }
    }

    /**
     * Submits an item for processing, blocking indefinitely until a free slot becomes
     * available. If the thread pool is busy, the item is persisted to disk and retried later.
     *
     * @param item queue item to process
     * @throws IllegalStateException if the queue is not running
     * @throws IOException           if the item could not be serialized
     * @throws InterruptedException  if the current thread is interrupted
     */
    public void submit(final T item) throws IllegalStateException, IOException, InterruptedException {
        if (!doRun)
            throw new IllegalStateException("file queue {" + getQueueBaseDir() + "} is not running");
        permits.acquire(1);
        // Re-check after acquiring: close() may have released extra permits to unblock us.
        if (!doRun) {
            permits.release();
            throw new IllegalStateException("file queue {" + getQueueBaseDir() + "} is not running");
        }
        var submitted = false;
        try {
            dispatchOrPersist(item);
            submitted = true;
        } finally {
            if (!submitted) permits.release();
        }
    }

    /**
     * Attempts to dispatch the item directly to the thread pool. If the pool rejects the
     * task, the item is serialised and pushed to the on-disk MVStore queue for later retry.
     * <p>
     * {@link Phaser#register()} is called <em>before</em> the try block so that the
     * deregistration in {@code finally} is only reached when {@code register()} succeeded.
     * When the task is successfully dispatched, ownership of the Phaser registration is
     * transferred to {@link ProcessItem#run()}, which calls
     * {@link Phaser#arriveAndDeregister()} after all processing—including any push-back—
     * is complete. This ensures that {@link #close()} waits for the full processing
     * lifecycle, not just for the submission window.
     * </p>
     *
     * @param item item to dispatch or persist
     * @throws IllegalStateException if the {@link Phaser} is terminated
     * @throws IOException           if the item cannot be written to the persistent store
     */
    private void dispatchOrPersist(final T item) throws IllegalStateException, IOException {
        restorePolled.register();
        boolean dispatched = false;
        try {
            executorService.execute(new ProcessItem(item));
            dispatched = true; // ProcessItem.run() now owns the Phaser deregistration
        } catch (RejectedExecutionException rejected) {
            try {
                mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
            } catch (Throwable t) {
                if (t instanceof IOException ioe) throw ioe;
                if (t instanceof RuntimeException rte) throw rte;
                throw new RuntimeException(t);
            }
        } finally {
            // Only deregister here if the task was NOT dispatched.
            // If it was dispatched, ProcessItem.run() is responsible.
            if (!dispatched) restorePolled.arriveAndDeregister();
        }
    }

    /**
     * Stops the queue processor and waits for all in-flight items to finish processing.
     * <p>
     * Shutdown sequence:
     * <ol>
     *   <li>Set {@code doRun = false} — no new submissions will be accepted.</li>
     *   <li>Release at least one extra permit (and up to {@code maxQueueSize} extra permits)
     *       so any threads blocked in {@link #submit} can acquire, re-check {@code doRun},
     *       and exit cleanly with {@link IllegalStateException}.</li>
     *   <li>Cancel the background cleanup scheduler (without interrupting the shared scheduler
     *       thread, so in-flight cleanup runs for other queues are unaffected).</li>
     *   <li>Acquire {@code cleanerLock} to wait for any currently-executing
     *       {@link MVStoreCleaner} run to finish — this guarantees the MVStore is still
     *       open for the cleaner's entire execution before we close it.</li>
     *   <li>Wait on the {@link Phaser} for every dispatched {@link ProcessItem} to
     *       complete its {@code run()} method — including any push-back to disk.</li>
     *   <li>Close the underlying MVStore (safe because all push-back is already done).</li>
     * </ol>
     * <p>
     * This method is idempotent; a second call is a no-op.
     * </p>
     * <p>
     * <strong>Executor lifecycle:</strong> the {@link java.util.concurrent.ExecutorService}
     * supplied at construction is <em>not</em> shut down by this method — the caller owns
     * it and is responsible for shutting it down after this method returns.
     * The executor must <em>not</em> be shut down while the queue is running; doing so
     * causes rejected tasks to be pushed to disk rather than executed.
     * </p>
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) return; // idempotent
        doRun = false;
        // Unblock any threads waiting in submit() for a permit slot so they can
        // re-check doRun and throw IllegalStateException cleanly.
        // Use max(N, 1) so we release at least one permit even when maxQueueSize == 0,
        // covering the race window between the first doRun check and acquire().
        permits.release(Math.max(permits.getNumberOfPermits(), 1));
        // cancel(false): we do NOT interrupt the shared scheduler thread because it serves
        // all QueueProcessor instances — an interrupt would corrupt a concurrently running
        // cleanup task belonging to a different queue.
        cleanupTaskScheduler.cancel(false);
        // Wait for any currently-executing MVStoreCleaner.run() to finish before we close
        // the MVStore. Use tryLock with a timeout so close() cannot block forever if the
        // cleaner stalls on a filesystem operation.
        boolean cleanerStopped = false;
        try {
            if (!cleanerLock.tryLock(60, TimeUnit.SECONDS)) {
                logger.warn("timed out waiting for MVStoreCleaner to finish for queue '{}' — skipping close to avoid store/cleaner race; retry close() later", queueName);
                closed.set(false);
                return;
            }
            cleanerStopped = true;
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.warn("interrupted while waiting for MVStoreCleaner for queue '{}' — skipping close to avoid store/cleaner race", queueName);
            closed.set(false);
            return;
        } finally {
            if (cleanerLock.isHeldByCurrentThread())
                cleanerLock.unlock();
        }
        if (!cleanerStopped) return;
        restorePolled.register();
        restorePolled.arriveAndAwaitAdvance(); // waits for all ProcessItem.run() to finish
        mvStoreQueue.close();
    }

    /**
     * Adjusts the maximum number of items that may be held concurrently by the queue.
     *
     * @param maxQueueSize new maximum queue size; must be &gt;= 0
     * @throws IllegalArgumentException if {@code maxQueueSize} is negative
     */
    public void setMaxQueueSize(int maxQueueSize) {
        permits.setMaxPermits(maxQueueSize);
    }

    /**
     * Returns the number of items currently persisted in the on-disk queue store.
     *
     * @return number of items in the persistent store
     */
    public long size() {
        return mvStoreQueue.size();
    }

    /**
     * Records a delivery attempt against the given item by updating its try-date to now
     * and incrementing its try counter.
     *
     * @param item item whose attempt counters should be updated
     */
    private void tryItem(T item) {
        item.setTryDate(Instant.now());
        item.incTryCount();
    }

    /**
     * Creates the {@link ObjectMapper} used for serializing, with {@link JavaTimeModule}
     * registered so that {@link Instant} fields are handled correctly.
     *
     * @return the configured {@link ObjectMapper}
     */
    private ObjectMapper createObjectMapper() {
        var mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }

    /**
     * Returns {@code true} if the item has not yet exhausted the maximum number of
     * delivery attempts, or if unlimited retries are configured ({@code maxTries <= 0}).
     *
     * @param item item to check
     * @return {@code true} if another delivery attempt should be made
     */
    private boolean isNeedRetry(T item) {
        return maxTries <= 0 || item.getTryCount() < maxTries;
    }

    /**
     * Returns {@code true} if enough time has elapsed since the item's last delivery
     * attempt, according to the configured retry delay and algorithm.
     *
     * @param item item to check
     * @return {@code true} if the retry window has passed
     */
    private boolean isTimeToRetry(T item) {
        long delay = switch (retryDelayAlgorithm) {
            case EXPONENTIAL -> {
                // First retry waits retryDelay, then doubles each attempt up to maxRetryDelay.
                int exponent = Math.max(0, item.getTryCount() - 1);
                long multiplier = exponent >= Long.SIZE - 2 ? Long.MAX_VALUE : (1L << exponent);
                long scaledDelay = multiplier > Long.MAX_VALUE / Math.max(retryDelay, 1)
                        ? Long.MAX_VALUE
                        : multiplier * Math.max(retryDelay, 1);
                yield Math.min(scaledDelay, Math.max(maxRetryDelay, 1));
            }
            case FIXED -> retryDelay;
        };
        return isTimeToRetry(item, delay, retryDelayUnit);
    }

    /**
     * Returns {@code true} if the time elapsed since {@code item}'s last try-date exceeds
     * {@code retryDelay} in the given {@code timeUnit}.
     *
     * @param item       item to check
     * @param retryDelay delay threshold
     * @param timeUnit   unit for {@code retryDelay}
     * @return {@code true} if the item is eligible for another delivery attempt
     */
    private boolean isTimeToRetry(T item, long retryDelay, TimeUnit timeUnit) {
        var tryDate = item.getTryDate();
        if (tryDate == null) return true;
        return (Instant.now().toEpochMilli() - tryDate.toEpochMilli()) > timeUnit.toMillis(retryDelay);
    }

    /**
     * Returns the base directory used by the underlying MVStore queue.
     *
     * @return path to the queue database directory
     */
    public Path getQueueBaseDir() {
        return mvStoreQueue.getQueueDir();
    }

    /**
     * Reopens the underlying MVStore queue after it has been closed.
     *
     * @throws IllegalStateException if the store cannot be reopened
     */
    public void reopen() throws IllegalStateException {
        mvStoreQueue.reopen();
    }

    /**
     * Returns the number of permits currently available, i.e. the number of additional
     * items that may be submitted without blocking.
     *
     * @return number of available queue slots
     */
    public int availablePermits() {
        return permits.availablePermits();
    }

    /**
     * Deserialises raw bytes from the persistent store into a queue item of type {@code T}.
     * Returns {@code null} and logs an error if deserialisation fails.
     *
     * @param data raw bytes to deserialise
     * @return deserialised item, or {@code null} on failure
     */
    private T deserialize(final byte[] data) {
        if (data == null) return null;
        try {
            return objectMapper.readValue(data, javaType);
        } catch (IOException e) {
            logger.error("failed to deserialize item ({} bytes) — entry will be dropped", data.length, e);
            return null;
        }
    }

    /**
     * {@link Runnable} task that processes a single queue item.
     * <p>
     * On success the permit is released. On a retriable failure
     * ({@link Consumer.Result#FAIL_REQUEUE} or {@link IllegalStateException}) the item is
     * serialised back to the on-disk store — even during shutdown, because the
     * {@link Phaser} in {@link #close()} guarantees that the MVStore is still open for
     * the full duration of {@code run()}. Unrecoverable exceptions are logged and the
     * permit is released.
     * </p>
     * <p>
     * This class owns the {@link Phaser} registration created by
     * {@link #dispatchOrPersist(FileQueueItem)} and calls
     * {@link Phaser#arriveAndDeregister()} at the very end of {@code run()}.
     * </p>
     */
    private final class ProcessItem implements Runnable {

        private final T item;
        private boolean pushback = false;

        ProcessItem(T item) {
            this.item = item;
        }

        /**
         * Persists the item back to the on-disk store if flagged.
         * The {@code doRun} guard is intentionally absent: {@link #close()} waits on
         * the {@link Phaser} until {@code run()} completes, so the MVStore is guaranteed
         * open for the full lifetime of this call.
         *
         * @return {@code true} if the item was successfully pushed back
         */
        private boolean pushBack() {
            if (pushback) {
                try {
                    mvStoreQueue.push(objectMapper.writeValueAsBytes(item));
                    return true;
                } catch (Throwable e1) {
                    logger.error("failed to persist item during push-back — item will be lost"
                            + " [item={}, class={}, tryCount={}, exception={}]",
                            item, item.getClass().getSimpleName(), item.getTryCount(), e1, e1);
                }
            }
            return false;
        }

        /** Marks this item as requiring a push-back to the persistent store. */
        private void flagPush() {
            pushback = true;
        }

        @Override
        public void run() {
            try {
                tryItem(item);
                Consumer.Result result = consumer.consume(item);
                if (result == null) {
                    logger.warn("consumer returned null for item {} [class={}, tryCount={}]"
                                    + " — treating as {} (item will not be retried)",
                            item, item.getClass().getSimpleName(), item.getTryCount(),
                            Consumer.Result.FAIL_NOQUEUE);
                } else if (result == Consumer.Result.FAIL_REQUEUE) {
                    flagPush();
                }
            } catch (IllegalStateException e) {
                logger.debug("consumer threw IllegalStateException for item {} [class={}, tryCount={}]"
                        + " — requeueing for later retry",
                        item, item.getClass().getSimpleName(), item.getTryCount(), e);
                flagPush();
            } catch (Throwable e) {
                logger.error("unhandled exception while processing item {} [class={}, tryCount={}, maxTries={}, exception={}]"
                        + " — item will NOT be retried; return {} from consume() to requeue instead",
                        item, item.getClass().getSimpleName(), item.getTryCount(), maxTries,
                        e, Consumer.Result.FAIL_REQUEUE, e);
            } finally {
                try {
                    if (!pushBack())
                        permits.release();
                } finally {
                    // Deregister from the Phaser last, after all push-back is complete,
                    // so close() cannot proceed to mvStoreQueue.close() prematurely.
                    restorePolled.arriveAndDeregister();
                }
            }
        }
    }

    /**
     * Background task that periodically drains the on-disk MVStore queue and re-submits
     * items that are ready for another delivery attempt.
     * <p>
     * Items whose retry window has not yet elapsed are pushed back immediately.
     * Items that have exceeded {@code maxTries} are passed to the {@link Expiration}
     * handler (if configured) and then discarded.
     * </p>
     * <p>
     * This is a non-static inner class and accesses all {@link QueueProcessor} fields
     * via the implicit outer-class reference.
     * </p>
     * <p>
     * The {@link #cleanerLock} is held for the entire duration of {@code run()} so that
     * {@link #close()} can acquire it to ensure no cleaner run is in progress before the
     * MVStore is closed.
     * </p>
     */
    private final class MVStoreCleaner implements Runnable {

        @Override
        public void run() {
            cleanerLock.lock();
            try {
                if (!doRun || mvStoreQueue.isEmpty()) return;
                var initialSize = mvStoreQueue.size();
                var processed = 0L;
                byte[] toDeserialize;
                while (processed < initialSize && (toDeserialize = mvStoreQueue.poll()) != null) {
                    processed++;
                    T item = null;
                    try {
                        if (!doRun) {
                            mvStoreQueue.push(toDeserialize);
                            break;
                        }
                        item = deserialize(toDeserialize);
                        if (item == null) {
                            permits.release();
                            continue;
                        }
                        if (isNeedRetry(item)) {
                            if (isTimeToRetry(item))
                                dispatchOrPersist(item);
                            else
                                mvStoreQueue.push(toDeserialize);
                        } else {
                            permits.release();
                            if (expiration != null) {
                                try {
                                    expiration.expire(item);
                                } catch (Exception expirationEx) {
                                    logger.error("expiration handler threw for expired item {} "
                                                    + "[class={}, tryCount={}] — item will be dropped",
                                            item, item.getClass().getSimpleName(),
                                            item.getTryCount(), expirationEx);
                                }
                            }
                        }
                    } catch (Exception e) {
                       logger.error("error processing item during cleanup [item={}, class={}, tryCount={}, exception={}]"
                                + " — pushing back to queue for retry",
                                item, item != null ? item.getClass().getSimpleName() : "unknown",
                                item != null ? item.getTryCount() : "unknown", e, e);
                        try {
                            mvStoreQueue.push(toDeserialize);
                        } catch (Exception pushEx) {
                            logger.error("failed to push item back to store — item will be lost"
                                    + " [item={}, class={}]",
                                    item, item != null ? item.getClass().getSimpleName() : "unknown", pushEx);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("unexpected error in MVStore cleanup task for queue '{}' — cleanup cycle aborted",
                        queueName, e);
            } finally {
                try {
                    mvStoreQueue.commit();
                } finally {
                    cleanerLock.unlock();
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    /**
     * Returns the queue database path.
     *
     * @return queue database path
     */
    public Path getQueuePath() { return queuePath; }

    /**
     * Returns the queue name.
     *
     * @return queue name
     */
    public String getQueueName() { return queueName; }

    /**
     * Returns the consumer callback.
     *
     * @return consumer callback
     */
    public Consumer<? super T> getConsumer() { return consumer; }

    /**
     * Returns the queue item type.
     *
     * @return queue item type
     */
    public Type getType() { return type; }

    /**
     * Returns the maximum number of tries (0 = unlimited).
     *
     * @return maximum number of tries (0 = unlimited)
     */
    public int getMaxTries() { return maxTries; }

    /**
     * Returns the fixed retry delay.
     *
     * @return fixed retry delay
     */
    public int getRetryDelay() { return retryDelay; }

    /**
     * Returns the maximum retry delay (exponential back-off cap).
     *
     * @return maximum retry delay (exponential back-off cap)
     */
    public int getMaxRetryDelay() { return maxRetryDelay; }

    /**
     * Returns the retry delay time unit.
     *
     * @return retry delay time unit
     */
    public TimeUnit getRetryDelayUnit() { return retryDelayUnit; }

    /**
     * Returns the retry delay algorithm.
     *
     * @return retry delay algorithm
     */
    public RetryDelayAlgorithm getRetryDelayAlgorithm() { return retryDelayAlgorithm; }

    /**
     * Returns the expiration handler, or {@code null} if not set.
     *
     * @return expiration handler, or {@code null} if not set
     */
    public Expiration<T> getExpiration() { return expiration; }

    /**
     * Returns the persistent retry delay.
     *
     * @return persistent retry delay
     */
    public int getPersistRetryDelay() { return persistRetryDelay; }

    /**
     * Returns the persistent retry delay time unit.
     *
     * @return persistent retry delay time unit
     */
    public TimeUnit getPersistRetryDelayUnit() { return persistRetryDelayUnit; }
}
