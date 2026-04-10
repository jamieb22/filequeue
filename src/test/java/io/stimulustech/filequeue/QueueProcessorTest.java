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
 *
 * @author Valentin Popov
 */

package io.stimulustech.filequeue;

import io.stimulustech.filequeue.processor.Consumer;
import io.stimulustech.filequeue.processor.QueueProcessor;
import io.stimulustech.filequeue.processor.QueueProcessorBuilder;
import io.stimulustech.filequeue.util.ThreadUtil;
import org.h2.mvstore.MVStore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/* DB Processing Queue
 * Tests internal QueueProcessor class. Rather extend FileQueue and RetryFileQueueItem.
 */

public class QueueProcessorTest {

    @Test
    public void exponentialDelayScalesFromRetryDelay() throws Exception {
        Path dir = Files.createTempDirectory("queueprocessor-exponential-delay");
        ExecutorService executor = Executors.newSingleThreadExecutor(
                ThreadUtil.getFlexibleThreadFactory("qp-test", true));
        QueueProcessor<FileQueueItem> processor = null;
        try {
            processor = QueueProcessorBuilder.builder()
                    .queueName("test")
                    .queuePath(dir)
                    .type(TestItem.class)
                    .consumer(item -> Consumer.Result.SUCCESS)
                    .executorService(executor)
                    .retryDelayAlgorithm(QueueProcessor.RetryDelayAlgorithm.EXPONENTIAL)
                    .retryDelay(50)
                    .maxRetryDelay(500)
                    .retryDelayUnit(TimeUnit.MILLISECONDS)
                    .persistRetryDelay(1)
                    .persistRetryDelayUnit(TimeUnit.SECONDS)
                    .maxQueueSize(10)
                    .build();

            Method isTimeToRetry = QueueProcessor.class.getDeclaredMethod("isTimeToRetry", FileQueueItem.class);
            isTimeToRetry.setAccessible(true);

            TestItem item = new TestItem(1);
            item.setTryCount(1); // first retry uses base delay
            item.setTryDate(java.time.Instant.now().minusMillis(20));
            assertFalse((Boolean) isTimeToRetry.invoke(processor, item));
            item.setTryDate(java.time.Instant.now().minusMillis(90));
            assertTrue((Boolean) isTimeToRetry.invoke(processor, item));

            item.setTryCount(2); // second retry doubles base delay
            item.setTryDate(java.time.Instant.now().minusMillis(70));
            assertFalse((Boolean) isTimeToRetry.invoke(processor, item));
            item.setTryDate(java.time.Instant.now().minusMillis(140));
            assertTrue((Boolean) isTimeToRetry.invoke(processor, item));
        } finally {
            if (processor != null) processor.close();
            executor.shutdownNow();
        }
    }

    @Test
    public void closeInterruptedLeavesProcessorRetryableAndStoreOpen() throws Exception {
        Path dir = Files.createTempDirectory("queueprocessor-close-interrupt");
        ExecutorService executor = Executors.newSingleThreadExecutor(
                ThreadUtil.getFlexibleThreadFactory("qp-test", true));
        QueueProcessor<FileQueueItem> processor = QueueProcessorBuilder.builder()
                .queueName("test")
                .queuePath(dir)
                .type(TestItem.class)
                .consumer(item -> Consumer.Result.SUCCESS)
                .executorService(executor)
                .retryDelay(1)
                .maxRetryDelay(1)
                .retryDelayUnit(TimeUnit.SECONDS)
                .persistRetryDelay(1)
                .persistRetryDelayUnit(TimeUnit.SECONDS)
                .maxQueueSize(10)
                .build();
        try {
            Thread.currentThread().interrupt();
            processor.close();

            Field closedField = QueueProcessor.class.getDeclaredField("closed");
            closedField.setAccessible(true);
            AtomicBoolean closed = (AtomicBoolean) closedField.get(processor);
            assertFalse(closed.get());

            Field queueField = QueueProcessor.class.getDeclaredField("mvStoreQueue");
            queueField.setAccessible(true);
            Object mvStoreQueue = queueField.get(processor);
            Field storeField = mvStoreQueue.getClass().getDeclaredField("store");
            storeField.setAccessible(true);
            MVStore store = (MVStore) storeField.get(mvStoreQueue);
            assertFalse(store.isClosed());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
            processor.close();
            executor.shutdownNow();
        }
    }

    static class TestItem extends FileQueueItem {
        final int id;

        TestItem(int id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return Integer.toString(id);
        }
    }
}