# FileQueue

FileQueue offers a lightweight, high-performance, simple, reliable, and persistent queue for Java applications. Producers and consumers run within a single JVM. For persistence, FileQueue uses the H2 MVStore engine. Queue items are regular Java POJOs serialized to JSON with Jackson.

For higher throughput, FileQueue delivers items directly to consumers when worker capacity is available. If all consumers are busy, FileQueue automatically persists queued items to disk. FileQueue also provides retry logic for items that fail processing.

Refer to https://github.com/jamieb22/filequeue for usage details.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this library except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

## Requirements

- Java 21+
- Maven or Gradle project

## Maven

```xml
<dependency>
  <groupId>io.stimulustech</groupId>
  <artifactId>filequeue</artifactId>
  <version>2.0.3</version>
</dependency>
```

## Javadocs

- <a href="https://jamieb22.github.io/filequeue/">FileQueue Javadocs</a>

Generate/update docs locally:

```bash
mvn -DskipTests package
```

Then commit the updated `docs/` contents.

## Quick Start

```java
import io.stimulustech.filequeue.FileQueue;
import io.stimulustech.filequeue.FileQueueItem;
import processor.io.stimulustech.filequeue.Consumer;
import processor.io.stimulustech.filequeue.QueueProcessor;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Example {

  static final class WorkItem extends FileQueueItem {
    private String id;

    public WorkItem() {
    }

    WorkItem(String id) {
      this.id = id;
    }

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return id;
    }
  }

  static final class WorkConsumer implements Consumer<WorkItem> {
    @Override
    public Result consume(WorkItem item) {
      try {
        // Do useful work.
        return Result.SUCCESS;
      } catch (RuntimeException transientFailure) {
        return Result.FAIL_REQUEUE;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors());
    FileQueue<WorkItem> queue = FileQueue.fileQueue();

    FileQueue.Config<WorkItem> config = FileQueue.config(
                    "orders",
                    Path.of("./queue-data"),
                    WorkItem.class,
                    new WorkConsumer(),
                    executor)
            .maxQueueSize(10_000)
            .maxTries(10) // 0 = unlimited retries
            .retryDelayAlgorithm(QueueProcessor.RetryDelayAlgorithm.EXPONENTIAL)
            .retryDelay(2)
            .retryDelayUnit(TimeUnit.SECONDS)
            .maxRetryDelay(60)
            .persistRetryDelay(1)
            .persistRetryDelayUnit(TimeUnit.SECONDS);

    queue.startQueue(config);
    queue.queueItem(new WorkItem("A-100"));
    queue.queueItem(new WorkItem("A-101"), 5, TimeUnit.SECONDS);

    // ... run application ...

    queue.stopQueue();
    executor.shutdown();
    FileQueue.destroy();
  }
}
```

## API Notes

### Lifecycle

- `startQueue(config)` starts processing and registers a JVM shutdown hook.
- `stopQueue()` blocks until in-flight work is processed or persisted.
- `FileQueue.destroy()` shuts down the shared static scheduler used by all queues.
  Call this once at application shutdown.

### Queue Submission

- `queueItem(item)` blocks until a slot is available.
- `queueItem(item, wait, unit)` waits up to the timeout, then throws `IllegalStateException` if full.

### Retry Semantics

- `Consumer.Result.SUCCESS`: done, item removed.
- `Consumer.Result.FAIL_REQUEUE`: item is retried (subject to retry policy/max tries).
- `Consumer.Result.FAIL_NOQUEUE`: item is dropped immediately.
- Throwing `IllegalStateException` from `consume()` is treated as retriable and requeued.

### Expiry Handling

- Configure `.expiration(expirationHandler)` to receive items that exceed `maxTries`.
- `maxTries(0)` means unlimited retries.

### Queue Capacity

- Configure capacity with `.maxQueueSize(n)`.
- You can change it at runtime via `setMaxQueueSize(int)` on `FileQueue`.

### Executor Ownership

- The `ExecutorService` passed in config is owned by the caller.
- Do not shut it down while the queue is running.
- Recommended order: `stopQueue()` then `executor.shutdown()`.

## Operational Guidance

- Keep queued item classes Jackson-friendly (default constructor + getters/setters).
- Use a durable local filesystem path for queue data.
- Monitor `availablePermits()` and persistent `getQueueSize()` for backpressure.
- If you use exponential retry, choose `retryDelay` and `maxRetryDelay` in the same unit.

## Testing Reference

See `src/test/java/com/stimulustech/filequeue/FileQueueTest.java` and
`src/test/java/com/stimulustech/filequeue/MVStoreQueueTest.java` for runnable usage patterns.

## Credits

Implemented by Valentin Popov and Jamie Band.
Copyright Jamie Band.

## License

Apache License 2.0: http://www.apache.org/licenses/LICENSE-2.0
