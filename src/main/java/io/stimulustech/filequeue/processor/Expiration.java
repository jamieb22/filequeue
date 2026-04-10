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
 * Implement this interface to receive notification when a queued item has expired due to maximum number of
 * retries exceeded.
 *
 * @param <T> the type of item that can expire
 * @author Jamie Band 
 * @author Valentin Popov 
 */

public interface Expiration<T> {

    /**
     * Expiration notification
     *
     * @param item expired item
     */
    void expire(T item);

}