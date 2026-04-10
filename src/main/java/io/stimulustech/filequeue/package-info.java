/**
 * FileQueue offers a lightweight, high-performance, simple, reliable, and persistent queue for Java applications.
 * Producers and consumers run within a single JVM. For persistence, FileQueue uses the H2 MVStore engine.
 * Queue items are regular Java POJOs serialized to JSON with Jackson.
 * <p>
 * For higher throughput, FileQueue delivers items directly to consumers when worker capacity is available.
 * If all consumers are busy, FileQueue automatically persists queued items to disk.
 * FileQueue also provides retry logic for items that fail processing.
 * <p>
 * Refer to <a href="https://github.com/jamieb22/filequeue">https://github.com/jamieb22/filequeue</a>
 * for usage details.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this library except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0.
 * <p>
 * FileQueue is copyright Jamie Band, implemented by Valentin Popov and Jamie Band.
 *
 * @see io.stimulustech.filequeue
 * @since 1.0
 */
package io.stimulustech.filequeue;
