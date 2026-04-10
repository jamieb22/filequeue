/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an &quot;AS IS&quot; BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stimulustech.filequeue;

import com.google.common.base.Preconditions;
import io.stimulustech.filequeue.processor.Consumer;
import io.stimulustech.filequeue.processor.Expiration;
import io.stimulustech.filequeue.processor.QueueProcessor;
import io.stimulustech.filequeue.processor.QueueProcessorBuilder;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * FileQueue is a fast, efficient, persistent queue written in Java and backed by H2 MVStore.
 * It supports retry behavior for items that cannot be processed immediately. Because the queue is
 * persistent, processing resumes where it left off after a restart. Refer to
 * <a href="https://github.com/jamieb22/filequeue">the FileQueue GitHub repository</a> for more information.<br>
 * For an example, refer to io.stimulustech.filequeue.FileQueueTest.<br>
 * <p>
 * For higher performance, FileQueue transfers queued items directly to consumers without hitting
 * the database when consumers are available. If all consumers are busy, FileQueue automatically
 * persist queued items to the database.
 * </p>
 * <p>
 * FileQueue supports both fixed and exponential backoff retry strategies.
 * </p>
 * <p>
 * Implementation strategy:<br>
 * 1) Create a Jackson-serializable POJO by extending FileQueueItem.<br>
 * 2) Implement a consumer by implementing Consumer.<br>
 * 3) Implement consume(item) with your processing logic.<br>
 * 4) Call config() to configure FileQueue.<br>
 * 5) Call startQueue(config) to start processing.<br>
 * 6) Call stopQueue() to stop processing.<br>
 * </p>
 * Example usage:<br>
 * <pre>{@code
 * FileQueue queue = FileQueue.fileQueue();
 * ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
 * FileQueue.Config config = FileQueue.config(queueName, queuePath, TestFileQueueItem.class, new TestConsumer(), executor)
 *                           .maxQueueSize(MAXQUEUESIZE)
 *                           .retryDelayAlgorithm(QueueProcessor.RetryDelayAlgorithm.EXPONENTIAL)
 *                           .retryDelay(RETRYDELAY)
 *                           .maxRetryDelay(MAXRETRYDELAY)
 *                           .maxTries(0)
 *                           .persistRetryDelay(PERSISTENTRETRYDELAY);
 * queue.startQueue(config);
 * for (int i = 0; i < ROUNDS; i++)
 *     queue.queueItem(new TestFileQueueItem(i));
 * // When finished, call stopQueue.
 * queue.stopQueue();
 * }</pre>
 * <p>
 * For a full example, see {@code io.stimulustech.filequeue.FileQueueTest}.
 * </p>
 *
 * @param <T> the type of {@link FileQueueItem} managed by this queue
 * @author Jamie Band 
 * @author Valentin Popov 
 */

public final class FileQueue<T extends FileQueueItem> {


    private ShutdownHook shutdownHook;
    private final AtomicBoolean isStarted = new AtomicBoolean();
    private volatile QueueProcessor<T> transferQueue;
    private volatile Config<T> config;

    /**
     * Creates a new, uninitialised {@link FileQueue} instance.
     * Call {@link #startQueue(Config)} to begin processing.
     */
    public FileQueue() {
    }

    /**
     * Starts the queue engine using the supplied configuration.
     * Registers a JVM shutdown hook to ensure the queue is cleanly stopped on exit.
     * <p>
     * <strong>Executor lifecycle:</strong> the {@link java.util.concurrent.ExecutorService}
     * embedded in {@code config} is owned by the caller. It must remain running for the
     * full lifetime of the queue. Shutting down the executor while the queue is running
     * will cause tasks to be rejected and pushed to the persistent store instead of being
     * executed. Call {@link #stopQueue()} first, then shut down the executor.
     * </p>
     *
     * @param config queue configuration; call {@link #config(String, Path, Class, Consumer, ExecutorService)}
     *               to build one
     * @throws IOException              if the persistent store cannot be opened
     * @throws InterruptedException     if the thread is interrupted during initialisation
     * @throws IllegalArgumentException if {@code config} is {@code null} or any configuration value is invalid
     * @throws IllegalStateException    if the queue has already been started
     */
    public synchronized void startQueue(Config<T> config) throws IOException, IllegalStateException, IllegalArgumentException, InterruptedException {
        if (config == null) throw new IllegalArgumentException("config must not be null");
        if (isStarted.get()) throw new IllegalStateException("already started");
        this.config = config;
        transferQueue = config.buildProcessor();
        shutdownHook = new ShutdownHook(this);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        isStarted.set(true);
    }

    /**
     * Returns the active queue configuration.
     * <p>
     * <strong>Warning:</strong> the returned object is the live, mutable {@link Config}
     * instance passed to {@link #startQueue(Config)}. Calling setters on it after the
     * queue has started does <em>not</em> affect the running processor — the
     * {@link QueueProcessor} was already built from a snapshot of the config values.
     * The only property that can be changed at runtime is the queue size, via
     * {@link #setMaxQueueSize(int)}.
     * </p>
     *
     * @return currently active {@link Config}
     * @throws IllegalStateException if the queue has not been started
     */
    public Config<T> getConfig() throws IllegalStateException {
        if (!isStarted.get()) throw new IllegalStateException("queue not started");
        return config;
    }

    /**
     * Stops the queue engine and blocks until all in-flight items have either been
     * processed or persisted back to the on-disk store.
     * <p>
     * Items that are mid-{@code consume()} when this is called will run to completion
     * before this method returns. Any item that returns {@link Consumer.Result#FAIL_REQUEUE}
     * or throws {@link IllegalStateException} during that final processing round is
     * persisted to the on-disk store and will be retried on the next start.
     * </p>
     * <p>
     * The JVM shutdown hook registered by {@link #startQueue} is removed.
     * Calling this method on a queue that is not running is a no-op.
     * </p>
     * <p>
     * <strong>After this method returns</strong>, the caller should shut down the
     * {@link java.util.concurrent.ExecutorService} that was supplied in the configuration.
     * </p>
     */
    public synchronized void stopQueue() {
        if (isStarted.compareAndSet(true, false)) {
            try {
                transferQueue.close();
            } finally {
                try {
                    if (shutdownHook != null)
                        Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // JVM shutdown is already in progress; hook cannot be removed
                }
            }
        }
    }

    /**
     * Immutable-style configuration builder for {@link FileQueue}.
     * <p>
     * Encapsulates all options that control queue behaviour: storage path, retry policy,
     * concurrency limits, item type, consumer, and expiration handler. Obtain an instance
     * via {@link FileQueue#config(String, Path, Class, Consumer, ExecutorService)} and
     * chain the setter methods before passing the result to
     * {@link FileQueue#startQueue(Config)}.
     * </p>
     *
     * @param <T> the type of {@link FileQueueItem} this configuration applies to
     */
    public static final class Config<T extends FileQueueItem> {

        private Consumer<? super T> consumer;

        @SuppressWarnings("unchecked")
        private final QueueProcessorBuilder<T> builder = (QueueProcessorBuilder<T>) QueueProcessorBuilder.builder();

        /**
         * Creates a fully-populated configuration with the mandatory fields set.
         *
         * @param queueName       unique name for the queue
         * @param queuePath       writable path where the queue database will be stored
         * @param type            concrete {@link FileQueueItem} subclass to be queued
         * @param consumer        callback invoked to process each dequeued item
         * @param executorService thread pool used to execute processing tasks
         */
        public Config(String queueName, Path queuePath, Class<T> type, Consumer<? super T> consumer, ExecutorService executorService) {
            builder.type(type).queueName(queueName).queuePath(queuePath).executorService(executorService);
            this.consumer = consumer;
        }

        /**
         * Creates an empty configuration. All mandatory fields must be set via the
         * fluent setter methods before calling {@link FileQueue#startQueue(Config)}.
         */
        public Config() {
        }

        /**
         * Builds the underlying {@link QueueProcessor}, wiring in the consumer.
         * Called internally by {@link FileQueue#startQueue(Config)}.
         */
        QueueProcessor<T> buildProcessor() throws IOException, IllegalStateException, IllegalArgumentException, InterruptedException {
            return builder.consumer(consumer).build();
        }

        /**
         * Sets the path to the queue database directory.
         *
         * @param queuePath writable path where the queue database will be stored
         * @return this config
         */
        public Config<T> queuePath(Path queuePath) {
            builder.queuePath(queuePath);
            return this;
        }

        /**
         * Returns the configured queue database path.
         *
         * @return queue database path
         */
        public Path getQueuePath() {
            return builder.getQueuePath();
        }

        /**
         * Sets a human-readable name for the queue.
         *
         * @param queueName unique, friendly name for the queue
         * @return this config
         */
        public Config<T> queueName(String queueName) {
            builder.queueName(queueName);
            return this;
        }

        /**
         * Returns the configured queue name.
         *
         * @return queue name
         */
        public String getQueueName() {
            return builder.getQueueName();
        }

        /**
         * Sets the concrete {@link FileQueueItem} subclass stored by this queue.
         *
         * @param type subclass of {@link FileQueueItem}; must not be {@code FileQueueItem.class} itself
         * @return this config
         * @throws IllegalArgumentException if {@code type} is not a proper subclass of {@link FileQueueItem}
         */
        public Config<T> type(Type type) throws IllegalArgumentException {
            if (!(type instanceof Class<?> clazz)
                    || clazz == FileQueueItem.class
                    || !FileQueueItem.class.isAssignableFrom(clazz))
                throw new IllegalArgumentException("type must be a subclass of filequeueitem");
            builder.type((Class<? extends T>) clazz);
            return this;
        }

        /**
         * Returns the configured queue item type.
         *
         * @return queue item type
         */
        public Type getType() {
            return builder.getType();
        }

        /**
         * Sets the maximum number of delivery attempts before an item is considered expired.
         * Set to {@code 0} for unlimited retries.
         *
         * @param maxTries maximum number of delivery attempts; {@code 0} means unlimited
         * @return this config
         */
        public Config<T> maxTries(int maxTries) {
            builder.maxTries(maxTries);
            return this;
        }

        /**
         * Returns the maximum number of delivery attempts ({@code 0} means unlimited).
         *
         * @return maximum tries
         */
        public int getMaxTries() {
            return builder.getMaxTries();
        }

        /**
         * Sets the fixed delay between delivery attempts, expressed in
         * {@link #retryDelayUnit(TimeUnit)}. When exponential back-off is enabled this
         * value acts as the minimum delay.
         *
         * @param retryDelay delay between retries; must be &gt;= 1
         * @return this config
         */
        public Config<T> retryDelay(int retryDelay) {
            builder.retryDelay(retryDelay);
            return this;
        }

        /**
         * Returns the configured retry delay.
         *
         * @return retry delay
         */
        public int getRetryDelay() {
            return builder.getRetryDelay();
        }

        /**
         * Sets the interval at which items persisted to disk are re-examined for
         * reprocessing, expressed in {@link #persistRetryDelayUnit(TimeUnit)}.
         * Items are only written to disk when all in-memory consumer slots are occupied.
         *
         * @param persistRetryDelay polling interval for the on-disk queue; must be &gt; 0
         * @return this config
         */
        public Config<T> persistRetryDelay(int persistRetryDelay) {
            builder.persistRetryDelay(persistRetryDelay);
            return this;
        }

        /**
         * Returns the configured persistent retry delay.
         *
         * @return persistent retry delay
         */
        public int getPersistRetryDelay() {
            return builder.getPersistRetryDelay();
        }

        /**
         * Sets the time unit for {@link #persistRetryDelay(int)}.
         * Defaults to {@link TimeUnit#SECONDS}.
         *
         * @param persistRetryDelayUnit time unit for the persistent retry delay
         * @return this config
         */
        public Config<T> persistRetryDelayUnit(TimeUnit persistRetryDelayUnit) {
            builder.persistRetryDelayUnit(persistRetryDelayUnit);
            return this;
        }

        /**
         * Returns the configured persistent retry delay time unit.
         *
         * @return persistent retry delay time unit
         */
        public TimeUnit getPersistRetryDelayUnit() {
            return builder.getPersistRetryDelayUnit();
        }

        /**
         * Sets the maximum delay between retries when exponential back-off is enabled,
         * expressed in {@link #retryDelayUnit(TimeUnit)}. Ignored when using fixed delay.
         *
         * @param maxRetryDelay upper bound for exponential back-off delay
         * @return this config
         */
        public Config<T> maxRetryDelay(int maxRetryDelay) {
            builder.maxRetryDelay(maxRetryDelay);
            return this;
        }

        /**
         * Returns the configured maximum retry delay.
         *
         * @return maximum retry delay
         */
        public int getMaxRetryDelay() {
            return builder.getMaxRetryDelay();
        }

        /**
         * Sets the time unit for {@link #retryDelay(int)} and {@link #maxRetryDelay(int)}.
         * Defaults to {@link TimeUnit#SECONDS}.
         *
         * @param retryDelayUnit time unit for retry delays
         * @return this config
         */
        public Config<T> retryDelayUnit(TimeUnit retryDelayUnit) {
            builder.retryDelayUnit(retryDelayUnit);
            return this;
        }

        /**
         * Returns the configured retry delay time unit.
         *
         * @return retry delay time unit
         */
        public TimeUnit getRetryDelayUnit() {
            return builder.getRetryDelayUnit();
        }

        /**
         * Sets the retry delay algorithm.
         * Use {@link QueueProcessor.RetryDelayAlgorithm#FIXED} for a constant interval between
         * attempts, or {@link QueueProcessor.RetryDelayAlgorithm#EXPONENTIAL} for exponential
         * back-off capped at {@link #maxRetryDelay(int)}.
         *
         * @param retryDelayAlgorithm retry delay algorithm to apply
         * @return this config
         */
        public Config<T> retryDelayAlgorithm(QueueProcessor.RetryDelayAlgorithm retryDelayAlgorithm) {
            builder.retryDelayAlgorithm(retryDelayAlgorithm);
            return this;
        }

        /**
         * Returns the configured retry delay algorithm.
         *
         * @return retry delay algorithm
         */
        public QueueProcessor.RetryDelayAlgorithm getRetryDelayAlgorithm() {
            return builder.getRetryDelayAlgorithm();
        }

        /**
         * Sets the consumer callback that processes items dequeued from the queue.
         *
         * @param consumer consumer to invoke for each queue item
         * @return this config
         */
        public Config<T> consumer(Consumer<? super T> consumer) {
            this.consumer = consumer;
            return this;
        }

        /**
         * Returns the configured consumer callback.
         *
         * @return consumer callback
         */
        public Consumer<? super T> getConsumer() {
            return consumer;
        }

        /**
         * Sets the expiration handler invoked when an item exceeds the maximum number of
         * delivery attempts. If not set, expired items are silently discarded.
         *
         * @param expiration handler called with each expired queue item
         * @return this config
         */
        public Config<T> expiration(Expiration<T> expiration) {
            builder.expiration(expiration);
            return this;
        }

        /**
         * Returns the configured expiration handler.
         *
         * @return expiration handler, or {@code null} if not set
         */
        public Expiration<T> getExpiration() {
            return builder.getExpiration();
        }

        /**
         * Sets the maximum number of items the queue will hold before blocking.
         * Defaults to {@link Integer#MAX_VALUE} (effectively unbounded).
         *
         * @param maxQueueSize maximum number of concurrently queued items; must be &gt; 0
         * @return this config
         */
        public Config<T> maxQueueSize(int maxQueueSize) {
            builder.maxQueueSize(maxQueueSize);
            return this;
        }

        /**
         * Returns the configured maximum queue size.
         *
         * @return maximum queue size
         */
        public int getMaxQueueSize() {
            return builder.getMaxQueueSize();
        }

    }

    /**
     * Creates a {@link Config} with all mandatory fields set, ready for further
     * customisation via its fluent setter methods.
     *
     * @param <T>             the queue item type
     * @param queueName       unique name for the queue
     * @param queuePath       writable path where the queue database will be stored
     * @param type            concrete {@link FileQueueItem} subclass to be queued
     * @param consumer        callback invoked to process each dequeued item
     * @param executorService thread pool used to execute processing tasks
     * @return a new {@link Config} ready to be passed to {@link #startQueue(Config)}
     */
    public static <T extends FileQueueItem> Config<T> config(String queueName, Path queuePath, Class<T> type, Consumer<? super T> consumer, ExecutorService executorService) {
        return new Config<>(queueName, queuePath, type, consumer, executorService);
    }

    /**
     * Queues an item for delivery, waiting up to {@code acquireWait} for a free slot.
     * If no slot becomes available within the timeout an {@link IllegalStateException} is thrown.
     *
     * @param <T1>            the concrete item type, must extend {@code T}
     * @param fileQueueItem   item for queuing; must not be {@code null}
     * @param acquireWait     maximum time to wait for a free slot; must be &gt;= 0
     * @param acquireWaitUnit time unit for {@code acquireWait}; must not be {@code null}
     * @throws IOException              if the item could not be saved to the persistent store
     * @throws InterruptedException     if the calling thread is interrupted while waiting
     * @throws NullPointerException     if {@code fileQueueItem} or {@code acquireWaitUnit} is {@code null}
     * @throws IllegalArgumentException if {@code acquireWait} is negative
     * @throws IllegalStateException    if the queue is not started, or no slot was available within the timeout
     */
    public <T1 extends T> void queueItem(@Nonnull final T1 fileQueueItem,
                                         int acquireWait, @Nonnull TimeUnit acquireWaitUnit) throws IOException, InterruptedException {
        Objects.requireNonNull(fileQueueItem, "fileQueueItem cannot be null");
        Objects.requireNonNull(acquireWaitUnit, "acquireWaitUnit cannot be null");
        Preconditions.checkArgument(acquireWait >= 0, "acquireWait should be >= 0");

        if (!isStarted.get())
            throw new IllegalStateException("queue not started");

        try {
            transferQueue.submit(fileQueueItem, acquireWait, acquireWaitUnit);
            // mvstore throws a null ptr exception when out of disk space
        } catch (NullPointerException npe) {
            throw new IOException("not enough disk space");
        }
    }

    /**
     * Queues an item for delivery, blocking indefinitely until a free slot is available.
     *
     * @param fileQueueItem item for queuing; must not be {@code null}
     * @throws NullPointerException     if {@code fileQueueItem} is {@code null}
     * @throws IllegalStateException    if the queue has not been started
     * @throws IOException              if the item could not be serialized or persisted
     * @throws InterruptedException     if the calling thread is interrupted while waiting
     */
    public void queueItem(final T fileQueueItem) throws IOException, InterruptedException {
        Objects.requireNonNull(fileQueueItem, "fileQueueItem cannot be null");

        if (!isStarted.get())
            throw new IllegalStateException("queue not started");

        try {
            transferQueue.submit(fileQueueItem);
            // NOTE: older MVStore versions throw NullPointerException when the disk is full.
            // Catching NPE here is a known workaround; genuine null-bugs in the call path
            // would be incorrectly reported as disk-space errors.
        } catch (NullPointerException npe) {
            throw new IOException("not enough disk space");
        }
    }


    /**
     * Returns the number of items currently persisted in the on-disk queue store.
     *
     * @return number of items in the persistent queue
     * @throws IllegalStateException if the queue has not been started
     */
    public long getQueueSize() throws IllegalStateException {
        if (!isStarted.get()) throw new IllegalStateException("queue not started");
        return transferQueue.size();
    }

    /**
     * Adjusts the maximum number of items the queue will hold concurrently.
     * Takes effect immediately on both the active configuration and the running processor.
     *
     * @param queueSize new maximum size; must be &gt;= 0
     * @throws IllegalArgumentException if {@code queueSize} is negative
     */
    public synchronized void setMaxQueueSize(int queueSize) {
        if (queueSize <= 0) throw new IllegalArgumentException("maxQueueSize must be > 0; got " + queueSize);
        if (config != null)
            config.maxQueueSize(queueSize);
        if (transferQueue != null)
            transferQueue.setMaxQueueSize(queueSize);
    }



    /**
     * JVM shutdown hook that stops the queue cleanly when the process exits.
     * Uses a {@link WeakReference} to avoid preventing garbage collection of the queue.
     */
    private static class ShutdownHook extends Thread {

        private final WeakReference<FileQueue<?>> queueRef;

        ShutdownHook(FileQueue<?> queue) {
            this.queueRef = new WeakReference<>(queue);
        }

        @Override
        public void run() {
            FileQueue<?> queue = queueRef.get();
            if (queue != null) {
                try {
                    queue.stopQueue();
                } catch (Exception ignored) {
                }
            }
        }
    }

    /**
     * Shuts down the shared processor thread pool. Call once at application exit to
     * release background threads managed by {@link QueueProcessor}.
     */
    public static void destroy() {
        QueueProcessor.destroy();
    }

    /**
     * Factory method that creates a new, uninitialised {@link FileQueue} instance.
     * Equivalent to {@code new FileQueue<>()}.
     *
     * @param <T> the queue item type
     * @return a new {@link FileQueue} ready to be started
     */
    public static <T extends FileQueueItem> FileQueue<T> fileQueue() {
        return new FileQueue<>();
    }

    /**
     * Returns the number of permits currently available, i.e. the number of additional
     * items that may be submitted to the queue without blocking.
     *
     * @return number of available queue slots
     * @throws IllegalStateException if the queue has not been started
     */
    public int availablePermits() {
        if (!isStarted.get()) throw new IllegalStateException("queue not started");
        return transferQueue.availablePermits();
    }

}
