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
 * @author Jamie Band
 * @author Valentin Popov
 */
package io.stimulustech.filequeue.processor;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * DelayRejectPolicy
 *
 * @author Jamie Band 
 * @author Valentin Popov 
 */

public class DelayRejectPolicy implements RejectedExecutionHandler {
    private final long timeOut;
    private final TimeUnit timeUnit;


    /**
     * Creates a {@code DelayRejectPolicy} for the given executor.
     */
    public DelayRejectPolicy() {
        this.timeOut = 1L;
        this.timeUnit = TimeUnit.SECONDS;
    }

    /**
     * Creates a {@code DelayRejectPolicy} that waits up to {@code timeOut} in the given
     * {@code timeUnit} before throwing a {@link RejectedExecutionException}.
     *
     * @param timeOut  maximum time to wait for space in the queue
     * @param timeUnit time unit for {@code timeOut}
     */
    public DelayRejectPolicy(long timeOut, TimeUnit timeUnit) {
        this.timeOut = timeOut;
        this.timeUnit = timeUnit;
    }

    /**
     * Obtains and ignores the next task that the executor
     * would otherwise execute, if one is immediately available,
     * and then retries execution of task r, unless the executor
     * is shut down, in which case task r is instead discarded.
     *
     * @param r the runnable task requested to be executed
     * @param e the executor attempting to execute this task
     */
    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
        if (!e.isShutdown()) {
            try {
                if (!e.getQueue().offer(r, timeOut, timeUnit))
                    throw new RejectedExecutionException(
                            "Task %s needs to be saved to disk.".formatted(r));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException(
                        "Task %s rejected from %s due to interruption.".formatted(r, e));
            }
        } else {
            throw new RejectedExecutionException(
                    "Task %s rejected from %s: executor is shut down.".formatted(r, e));
        }
    }
}
