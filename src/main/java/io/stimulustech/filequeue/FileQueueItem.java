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
 * @author Jamie Band (adaptation to MVStore, added multithreaded consumer support & retry delay option)
 * @author Valentin Popov
 */

/*
 *  Your custom QueueItem should extends this abstract class.
 *  Use basic fields that can be serialized using Jackson JSON.
 */

package io.stimulustech.filequeue;

import com.google.common.base.Preconditions;

import java.time.Instant;

/**
 * Extend this abstract class to include the properties of a queue item.
 * For example, an ID that refers to a queued element. Fields are serialized
 * via Jackson; the class does not need to implement {@link java.io.Serializable}.
 * <p>
 * Each instance is only ever accessed by one thread at a time: the framework
 * serialises items to/from JSON across every thread hand-off, so no concurrent
 * access to the same object occurs and atomic types are unnecessary.
 * </p>
 *
 * @author Jamie Band 
 * @author Valentin Popov 
 */
public abstract class FileQueueItem {

    private int tryCount;
    private Instant tryDate;

    /**
     * Default constructor for subclasses and Jackson deserialisation.
     */
    protected FileQueueItem() {
    }

    /**
     * Returns the timestamp of the last delivery attempt, or {@code null} if the item
     * has not yet been attempted. {@link Instant} is immutable so no defensive copy is needed.
     *
     * @return last try timestamp, or {@code null}
     */
    public Instant getTryDate() {
        return tryDate;
    }

    /**
     * Sets the timestamp of the last delivery attempt.
     *
     * @param instant timestamp to record, or {@code null} to clear
     */
    public void setTryDate(Instant instant) {
        this.tryDate = instant;
    }

    /**
     * Returns the number of delivery attempts made so far.
     *
     * @return current try count
     */
    public int getTryCount() {
        return tryCount;
    }

    /**
     * Overrides the try count.
     *
     * @param tryCount new count; must be &gt;= 0
     * @throws IllegalArgumentException if {@code tryCount} is negative
     */
    public void setTryCount(int tryCount) {
        Preconditions.checkArgument(tryCount >= 0, "tryCount can't be less 0");
        this.tryCount = tryCount;
    }

    /** Increments the try count by one. */
    public void incTryCount() {
        tryCount++;
    }

    @Override
    public abstract String toString();

}