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

package io.stimulustech.filequeue.util;

import java.util.concurrent.Semaphore;

/**
 * A {@link Semaphore} whose maximum number of permits can be adjusted at runtime.
 * <p>
 * Unlike a standard semaphore, whose permit count is fixed at construction time,
 * {@code AdjustableSemaphore} allows the upper bound to be raised or lowered while
 * the semaphore is in use. Raising the limit immediately releases additional permits;
 * lowering it reduces the available permits without affecting threads that already hold
 * a permit.
 * </p>
 * <p>
 * All operations are fair and thread-safe.
 * </p>
 */
public class AdjustableSemaphore extends Semaphore {

    /** The configured maximum number of permits; adjusted by {@link #setMaxPermits(int)}. */
    private int numberOfPermits = 0;

    /**
     * Creates a new {@code AdjustableSemaphore} with zero initial permits and fair ordering.
     * Call {@link #setMaxPermits(int)} before using the semaphore.
     */
    public AdjustableSemaphore() {
        super(0, true);
    }

    /**
     * Sets the maximum number of permits available to this semaphore.
     * <p>
     * If {@code desiredPermits} is greater than the current maximum, the difference is
     * immediately released. If it is less, the difference is drained so that no new
     * acquisitions can succeed until enough permits have been returned.
     * </p>
     *
     * @param desiredPermits the new permit limit; must be &gt;= 0
     * @throws IllegalArgumentException if {@code desiredPermits} is negative
     */
    public synchronized void setMaxPermits(int desiredPermits) {
        if (desiredPermits > numberOfPermits)
            release(desiredPermits - numberOfPermits);
        else if (desiredPermits < numberOfPermits)
            reducePermits(numberOfPermits - desiredPermits);
        numberOfPermits = desiredPermits;
    }

    /**
     * Returns the maximum number of permits configured for this semaphore.
     * This is the value last set by {@link #setMaxPermits(int)}, not the number
     * of permits currently available.
     *
     * @return the configured permit limit
     */
    public synchronized int getNumberOfPermits() {
        return numberOfPermits;
    }
}