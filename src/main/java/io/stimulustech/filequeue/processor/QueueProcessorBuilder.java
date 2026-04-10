package io.stimulustech.filequeue.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stimulustech.filequeue.FileQueueItem;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Builder for constructing a {@link QueueProcessor}.
 * <p>
 * Provides a fluent API for configuring all aspects of queue processing, including
 * retry behaviour, persistence, thread-pool wiring, and item expiration. Obtain an
 * instance via one of the static {@code builder()} factory methods, set the desired
 * options, then call {@link #build()} to create the configured {@link QueueProcessor}.
 * </p>
 * <p>
 * Mandatory fields are: queue name, queue path, item type, and consumer.
 * </p>
 *
 * @param <T> the type of {@link FileQueueItem} processed by the queue
 * @author Valentin Popov
 * @author Jamie Band
 */
public final class QueueProcessorBuilder<T extends FileQueueItem> {

    Path queuePath;
    String queueName;
    Class<? extends T> type;
    int maxTries = 0;
    int retryDelay = 1;
    int persistRetryDelay = 0;
    int maxRetryDelay = 1;
    TimeUnit retryDelayUnit = TimeUnit.SECONDS;
    TimeUnit persistRetryDelayUnit = TimeUnit.SECONDS;
    Consumer<? super T> consumer;
    Expiration<T> expiration;
    ExecutorService executorService;
    QueueProcessor.RetryDelayAlgorithm retryDelayAlgorithm = QueueProcessor.RetryDelayAlgorithm.FIXED;
    ObjectMapper objectMapper = null;
    int maxQueueSize = Integer.MAX_VALUE;

    /**
     * Creates a pre-populated builder with all mandatory fields set.
     *
     * @param <T1>       the queue item type
     * @param queueName  unique name for the queue
     * @param queuePath  writable path where the queue database will be stored
     * @param type       class of the queue item
     * @param consumer   callback invoked to process each item
     * @param executor   thread pool used to execute processing tasks
     * @return a new {@code QueueProcessorBuilder} with mandatory fields initialised
     * @throws IllegalArgumentException if any mandatory parameter is {@code null}
     */
    public static <T1 extends FileQueueItem> QueueProcessorBuilder<T1> builder(String queueName, Path queuePath, Class<T1> type,
                                                     Consumer<T1> consumer, ExecutorService executor) throws IllegalArgumentException {
        return new QueueProcessorBuilder<>(queueName, queuePath, type, consumer, executor);
    }

    /**
     * Creates an empty builder. All mandatory fields must be set before calling {@link #build()}.
     *
     * @return a new, unconfigured {@code QueueProcessorBuilder}
     */
    public static QueueProcessorBuilder<FileQueueItem> builder() {
        return new QueueProcessorBuilder<>();
    }

    /**
     * No-arg constructor used by {@link #builder()}. All fields must be configured
     * explicitly before calling {@link #build()}.
     */
    private QueueProcessorBuilder() {

    }

    /**
     * Constructor used by {@link #builder(String, Path, Class, Consumer, ExecutorService)}.
     * Validates and sets all mandatory fields.
     *
     * @param queueName       unique name for the queue
     * @param queuePath       writable path where the queue database will be stored
     * @param type            class of the queue item
     * @param consumer        callback invoked to process each item
     * @param executorService thread pool used to execute processing tasks
     * @throws IllegalArgumentException if any mandatory parameter is {@code null}
     */
    private QueueProcessorBuilder(String queueName, Path queuePath, Class<? extends T> type, Consumer<? super T> consumer, ExecutorService executorService) throws IllegalArgumentException {
        if (queueName == null) throw new IllegalArgumentException("queue name must be specified");
        if (queuePath == null) throw new IllegalArgumentException("queue path must be specified");
        if (type == null) throw new IllegalArgumentException("item type must be specified");
        if (consumer == null) throw new IllegalArgumentException("consumer must be specified");
        this.queueName = queueName;
        this.queuePath = queuePath;
        this.type = type;
        this.consumer = consumer;
        this.executorService = executorService;
    }

    /**
     * Sets the path to the queue database directory.
     *
     * @param queuePath writable path where the queue database will be stored
     * @return this builder
     */
    public QueueProcessorBuilder<T> queuePath(Path queuePath) {
        this.queuePath = queuePath;
        return this;
    }

    /**
     * Returns the configured queue database path.
     *
     * @return queue database path, or {@code null} if not yet set
     */
    public Path getQueuePath() {
        return queuePath;
    }

    /**
     * Sets a human-readable name for the queue.
     *
     * @param queueName unique, friendly name for the queue
     * @return this builder
     */
    public QueueProcessorBuilder<T> queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    /**
     * Returns the configured queue name.
     *
     * @return queue name, or {@code null} if not yet set
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Sets the class of items stored in the queue. Must be a concrete subclass of
     * {@link FileQueueItem} and serialisable by Jackson.
     *
     * @param type queue item class
     * @return this builder
     */
    public QueueProcessorBuilder<T> type(Class<? extends T> type) {
        this.type = type;
        return this;
    }

    /**
     * Returns the configured queue item type.
     *
     * @return queue item type, or {@code null} if not yet set
     */
    public Type getType() {
        return type;
    }

    /**
     * Sets the maximum number of items the queue will hold in memory before blocking.
     * Defaults to {@link Integer#MAX_VALUE} (effectively unbounded).
     *
     * @param maxQueueSize maximum number of concurrently queued items; must be &gt; 0
     * @return this builder
     */
    public QueueProcessorBuilder<T> maxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
        return this;
    }

    /**
     * Returns the configured maximum queue size.
     *
     * @return maximum queue size
     */
    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    /**
     * Sets the maximum number of delivery attempts before an item is considered expired.
     * Set to {@code 0} for unlimited retries.
     *
     * @param maxTries maximum number of delivery attempts; {@code 0} means unlimited
     * @return this builder
     */
    public QueueProcessorBuilder<T> maxTries(int maxTries) {
        this.maxTries = maxTries;
        return this;
    }

    /**
     * Returns the configured maximum number of delivery attempts.
     *
     * @return maximum tries, or {@code 0} for unlimited
     */
    public int getMaxTries() {
        return maxTries;
    }

    /**
     * Sets the fixed delay between delivery attempts, expressed in {@link #retryDelayUnit(TimeUnit)}.
     * When exponential back-off is enabled this value acts as the minimum delay.
     *
     * @param retryDelay delay between retries; must be &gt;= 1
     * @return this builder
     */
    public QueueProcessorBuilder<T> retryDelay(int retryDelay) {
        this.retryDelay = retryDelay;
        return this;
    }

    /**
     * Returns the configured retry delay.
     *
     * @return retry delay
     */
    public int getRetryDelay() {
        return retryDelay;
    }

    /**
     * Sets the maximum delay between retries when exponential back-off is enabled,
     * expressed in {@link #retryDelayUnit(TimeUnit)}. Ignored when using fixed delay.
     *
     * @param maxRetryDelay upper bound for exponential back-off delay
     * @return this builder
     */
    public QueueProcessorBuilder<T> maxRetryDelay(int maxRetryDelay) {
        this.maxRetryDelay = maxRetryDelay;
        return this;
    }

    /**
     * Returns the configured maximum retry delay.
     *
     * @return maximum retry delay
     */
    public int getMaxRetryDelay() {
        return maxRetryDelay;
    }

    /**
     * Sets the interval at which items persisted to disk are re-examined for reprocessing,
     * expressed in {@link #persistRetryDelayUnit(TimeUnit)}.
     * Items are only written to disk when all in-memory consumer slots are occupied.
     * Defaults to half the {@link #retryDelay(int)} value when not specified.
     *
     * @param persistRetryDelay polling interval for the on-disk queue; must be &gt; 0
     * @return this builder
     */
    public QueueProcessorBuilder<T> persistRetryDelay(int persistRetryDelay) {
        this.persistRetryDelay = persistRetryDelay;
        return this;
    }

    /**
     * Returns the configured persistent retry delay.
     *
     * @return persistent retry delay
     */
    public int getPersistRetryDelay() {
        return persistRetryDelay;
    }

    /**
     * Sets the time unit for {@link #persistRetryDelay(int)}. Defaults to {@link TimeUnit#SECONDS}.
     *
     * @param persistRetryDelayUnit time unit for the persistent retry delay
     * @return this builder
     */
    public QueueProcessorBuilder<T> persistRetryDelayUnit(TimeUnit persistRetryDelayUnit) {
        this.persistRetryDelayUnit = persistRetryDelayUnit;
        return this;
    }

    /**
     * Returns the configured persistent retry delay time unit.
     *
     * @return persistent retry delay time unit
     */
    public TimeUnit getPersistRetryDelayUnit() {
        return persistRetryDelayUnit;
    }

    /**
     * Sets the time unit for {@link #retryDelay(int)} and {@link #maxRetryDelay(int)}.
     * Defaults to {@link TimeUnit#SECONDS}.
     *
     * @param retryDelayUnit time unit for retry delays
     * @return this builder
     */
    public QueueProcessorBuilder<T> retryDelayUnit(TimeUnit retryDelayUnit) {
        this.retryDelayUnit = retryDelayUnit;
        return this;
    }

    /**
     * Returns the configured retry delay time unit.
     *
     * @return retry delay time unit
     */
    public TimeUnit getRetryDelayUnit() {
        return retryDelayUnit;
    }

    /**
     * Sets the retry delay algorithm.
     * Use {@link QueueProcessor.RetryDelayAlgorithm#FIXED} for a constant interval between
     * attempts, or {@link QueueProcessor.RetryDelayAlgorithm#EXPONENTIAL} for exponential
     * back-off capped at {@link #maxRetryDelay(int)}.
     *
     * @param retryDelayAlgorithm retry delay algorithm to apply
     * @return this builder
     */
    public QueueProcessorBuilder<T> retryDelayAlgorithm(QueueProcessor.RetryDelayAlgorithm retryDelayAlgorithm) {
        this.retryDelayAlgorithm = retryDelayAlgorithm;
        return this;
    }

    /**
     * Returns the configured retry delay algorithm.
     *
     * @return retry delay algorithm
     */
    public QueueProcessor.RetryDelayAlgorithm getRetryDelayAlgorithm() {
        return retryDelayAlgorithm;
    }

    /**
     * Sets the consumer callback that processes items dequeued from the queue.
     *
     * @param consumer consumer to invoke for each queue item
     * @return this builder
     */
    public QueueProcessorBuilder<T> consumer(Consumer<? super T> consumer) {
        this.consumer = consumer;
        return this;
    }

    /**
     * Returns the configured consumer.
     *
     * @return consumer callback, or {@code null} if not yet set
     */
    public Consumer<? super T> getConsumer() {
        return consumer;
    }

    /**
     * Sets the {@link ExecutorService} used to dispatch item-processing tasks.
     *
     * @param executorService thread pool for processing tasks
     * @return this builder
     */
    public QueueProcessorBuilder<T> executorService(ExecutorService executorService) {
        this.executorService = executorService;
        return this;
    }

    /**
     * Returns the configured executor service.
     *
     * @return executor service, or {@code null} if not yet set
     */
    public ExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * Sets the expiration handler invoked when an item exceeds the maximum number of
     * delivery attempts. If not set, expired items are silently discarded.
     *
     * @param expiration handler called with each expired queue item
     * @return this builder
     */
    public QueueProcessorBuilder<T> expiration(Expiration<T> expiration) {
        this.expiration = expiration;
        return this;
    }

    /**
     * Returns the configured expiration handler.
     *
     * @return expiration handler, or {@code null} if not set
     */
    public Expiration<T> getExpiration() {
        return expiration;
    }

    /**
     * Supplies a custom {@link ObjectMapper} for serialising and deserialising queue items.
     * If not set, a default mapper with {@code FAIL_ON_UNKNOWN_PROPERTIES} disabled is used.
     *
     * @param objectMapper custom Jackson {@code ObjectMapper}
     * @return this builder
     */
    public QueueProcessorBuilder<T> objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        return this;
    }

    /**
     * Builds and returns a configured {@link QueueProcessor}.
     *
     * @return a new {@link QueueProcessor} ready to accept items
     * @throws IOException              if the underlying queue store cannot be opened
     * @throws IllegalStateException    if mandatory fields have not been set
     * @throws IllegalArgumentException if any field value is invalid
     * @throws InterruptedException     if the thread is interrupted during initialisation
     */
    public QueueProcessor<T> build() throws IOException, IllegalStateException, IllegalArgumentException, InterruptedException {
        return new QueueProcessor<>(this);
    }
}
