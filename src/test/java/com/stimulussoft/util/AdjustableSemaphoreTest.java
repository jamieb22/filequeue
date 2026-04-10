package io.stimulustech.util;

import io.stimulustech.filequeue.util.AdjustableSemaphore;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by Valentin Popov valentin@archiva.ru on 04.04.2023.
 */
public class AdjustableSemaphoreTest {

    @Test
    public void setMaxPermits() {
        AdjustableSemaphore semaphore = new AdjustableSemaphore();
        Assert.assertEquals(0, semaphore.availablePermits());

        for (int i = 1; i < 10; i++) {
            semaphore.setMaxPermits(i);
            Assert.assertEquals(i, semaphore.availablePermits());
        }

        for (int i = 10; i > 0; i--) {
            semaphore.setMaxPermits(i);
            Assert.assertEquals(i, semaphore.availablePermits());
        }

    }

    @Test
    public void setPermits() throws InterruptedException {
        AdjustableSemaphore semaphore = new AdjustableSemaphore();
        Assert.assertEquals(0, semaphore.availablePermits());
        semaphore.setMaxPermits(10);
        Assert.assertEquals(10, semaphore.drainPermits());
        semaphore.release(3);

        semaphore.setMaxPermits(9);
        semaphore.acquire(2);

        Assert.assertEquals(0, semaphore.drainPermits());

        semaphore.setMaxPermits(8);
        Assert.assertEquals(-1, semaphore.drainPermits());

    }
}