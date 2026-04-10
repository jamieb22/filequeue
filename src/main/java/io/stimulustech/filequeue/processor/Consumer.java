package io.stimulustech.filequeue.processor;

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
 */

/**
 * Implement this interface to consume items on the queue. File Queue will call the consume method each time an item
 * is available for processing.
 *
 * @param <T> the type of item this consumer handles
 * @author Jamie Band 
 * @author Valentin Popov 
 */

public interface Consumer<T> {

    /**
     * Result of consumption.
     *
     **/

    enum Result {

        /**
         * Processing of item was successful. Do not requeue.
         **/

        SUCCESS, /* process was successful */

        /**
         * Processing of item failed, however retry later.
         * <p>When {@code maxTries} is reached the framework will call the configured
         * {@link Expiration} handler and discard the item.</p>
         **/
        FAIL_REQUEUE,  /* process failed, but must be requeued */

        /**
         * Processing of item failed permanently. No retry.
         * <p><strong>Note:</strong> returning {@code FAIL_NOQUEUE} drops the item immediately
         * without invoking the {@link Expiration} handler. Use this when the consumer itself
         * has decided the item should be discarded. Return {@link #FAIL_REQUEUE} instead if
         * you want the framework to manage expiry via the {@link Expiration} callback.</p>
         **/

        FAIL_NOQUEUE /* process failed, don't requeue */
    }

    /**
     * Consume the given item. This callback is called by FileQueue when an item is available for processing.
     *
     * @param item to handle.
     * @return {@code SUCCESS} if the item was processed successfully and shall be removed from the filequeue.
     * @throws IllegalStateException if consumer can't process item exact now
     */

    Result consume(T item);

}