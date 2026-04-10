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

package io.stimulustech.filequeue.store;


import com.google.common.base.Preconditions;
import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.store.fs.FilePath;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

/**
 * Fast queue implementation on top of MVStore. This class is thread-safe.
 *
 * @author Valentin Popov
 * @author Jamie Band
 */

public final class MVStoreQueue implements Comparable<MVStoreQueue> {

    private final String queueName;
    private MVMap<Long, byte[]> mvMap;
    private MVStore store;
    private final Path queueDir;
    /** Monotonically-increasing key used as the queue tail pointer. Always accessed inside a {@code synchronized} method. */
    private long tailKey = 0;

    static {
        FilePath.register(new JimFSDecorator());
    }

    /**
     * This class is for internal use only. Please refer to FileQueue class.
     *
     * @param queueDir  filequeue database environment directory path
     * @param queueName descriptive filequeue name
     * @throws IOException thrown when the given queueEnvPath does not exist and cannot be created.
     */
    public MVStoreQueue(final Path queueDir,
                        final String queueName) throws IOException {
        Files.createDirectories(queueDir);
        this.queueDir = queueDir.toAbsolutePath();
        this.queueName = queueName;
        reopen();
    }

    private Path getDBName() {
        return queueDir.resolve(queueName);
    }

    /**
     * Reopens the underlying MVStore, discarding any previous open handle.
     * Safe to call after {@link #close()} to re-use the same queue directory.
     *
     * @throws IllegalStateException if the store cannot be opened
     */
    public synchronized void reopen() throws IllegalStateException {
        try {
            if (store != null && !store.isClosed()) store.close();
        } catch (Exception ignored) {
        }
        store = getOpenStore();
        mvMap = store.openMap(queueName);
        if (!mvMap.isEmpty())
            tailKey = mvMap.lastKey();
        else
            tailKey = 0;
    }

    private MVStore getOpenStore() {
        Path dbName = getDBName();
        String path = dbName.toUri().getScheme().equals("jimfs") ?
                dbName.toUri().toString() : dbName.toString();
        return new MVStore.Builder().fileName(path).cacheSize(1).autoCommitDisabled().open();
    }

    /**
     * Returns the directory where the queue database files are stored.
     *
     * @return absolute path of the queue database directory
     */
    public Path getQueueDir() {
        return queueDir;
    }

    /**
     * Retrieves and and removes element from the head of this filequeue.
     *
     * @return element from the tail of the filequeue or null if filequeue is empty
     */
    public synchronized byte[] poll() {
        if (mvMap.isEmpty()) {
            tailKey = 0;
            return null;
        }
        return mvMap.remove(mvMap.firstKey());
    }

    /**
     * Pushes element to the tail of this filequeue.
     *
     * @param element byte array containing element data
     */
    public synchronized void push(final byte[] element) {
        Preconditions.checkNotNull(element, "cant insert null");
        mvMap.put(++tailKey, element);
    }

    /**
     * Removes all items from the queue map. The store is not compacted or committed;
     * call {@link #commit()} afterwards if persistence is required.
     */
    public synchronized void clear() {
        mvMap.clear();
    }

    /**
     * Returns the size of this filequeue.
     *
     * @return the size of the filequeue
     */
    public synchronized long size() {
        return mvMap.size();
    }

    /**
     * Determines if this filequeue is empty (equivalent to <code>{@link #size()} == 0</code>).
     *
     * @return <code>true</code> if this filequeue is empty, otherwise <code>false</code>.
     */
    public synchronized boolean isEmpty() {
        return mvMap.isEmpty();
    }

    /**
     * Closes this filequeue and frees up all resources associated to it.
     */
    public synchronized void close() {
        if (store != null && !store.isClosed()) {
            store.sync();
            store.close();
        }
    }

    /**
     * Compares this queue to another by directory path then name, consistent with {@link #equals}.
     * Two queues are ordered by their storage directory first; within the same directory they are
     * ordered by queue name.
     */
    @Override
    public int compareTo(@Nonnull MVStoreQueue o) {
        int c = queueDir.compareTo(o.queueDir);
        return c != 0 ? c : queueName.compareTo(o.queueName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MVStoreQueue that)) return false;
        return queueName.equals(that.queueName)
                && getQueueDir().equals(that.getQueueDir());
    }

    @Override
    public int hashCode() {
        return 31 * queueName.hashCode() + getQueueDir().hashCode();
    }

    /**
     * Commits any pending changes to the underlying MVStore.
     * This method is a no-op when the store has already been closed, making it safe
     * to call from a {@code finally} block even after {@link #close()} has returned.
     */
    public synchronized void commit() {
        if (store != null && !store.isClosed()) {
            store.commit();
        }
    }

    private static class JimFSDecorator extends FilePath {

        private Path decorated;

        public JimFSDecorator() {
        }

        private JimFSDecorator(Path path) {
            this.name = path.toString();
            this.decorated = path;
        }

        @Override
        public long size() {
            try {
                return Files.size(decorated);
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public void moveTo(FilePath filePath, boolean atomicReplace) {
            try {
                Path target = (filePath instanceof JimFSDecorator)
                        ? ((JimFSDecorator) filePath).decorated
                        : Paths.get(filePath.toString());
                if (atomicReplace) {
                    Files.move(decorated, target,
                            java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                            java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                } else {
                    Files.move(decorated, target);
                }
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public boolean createFile() {
            try {
                Files.createFile(decorated);
                return true;
            } catch (FileAlreadyExistsException e) {
                // contract: return false when the file already exists
                return false;
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public boolean exists() {
            return Files.exists(decorated);
        }

        @Override
        public void delete() {
            try {
                Files.deleteIfExists(decorated);
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public List<FilePath> newDirectoryStream() {
            try (Stream<Path> pathStream = Files.list(decorated)) {
                return pathStream.<FilePath>map(JimFSDecorator::new).toList();
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public FilePath toRealPath() {
            return getPath(decorated);
        }

        @Override
        public FilePath getParent() {
            Path parent = decorated.getParent();
            return parent == null ? null : getPath(parent);
        }

        @Override
        public boolean isDirectory() {
            return Files.isDirectory(decorated);
        }

        @Override
        public boolean isAbsolute() {
            return decorated.isAbsolute();
        }

        @Override
        public long lastModified() {
            try {
                return Files.getLastModifiedTime(decorated).toMillis();
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public boolean canWrite() {
            return Files.isWritable(decorated);
        }

        @Override
        public void createDirectory() {
            try {
                Files.createDirectories(decorated);
            } catch (IOException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        @Override
        public OutputStream newOutputStream(boolean append) throws IOException {
            return append ? Files.newOutputStream(decorated, StandardOpenOption.APPEND)
                    : Files.newOutputStream(decorated);
        }

        @Override
        public FileChannel open(String s) throws IOException {
            try {
                Files.createFile(decorated);
            } catch (FileAlreadyExistsException exist) {
                // file already exists — this is expected on re-open, safe to continue
            }

            if ("r".equalsIgnoreCase(s)) return FileChannel.open(decorated, StandardOpenOption.READ);
            return FileChannel.open(decorated, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }

        @Override
        public InputStream newInputStream() throws IOException {
            return Files.newInputStream(decorated);
        }

        @Override
        public boolean setReadOnly() {
            try {
                return decorated.toFile().setReadOnly();
            } catch (UnsupportedOperationException e) {
                // JimFS and other non-default filesystems don't support toFile()
                return false;
            }
        }

        @Override
        public String getScheme() {
            return "jimfs";
        }

        @Override
        public FilePath getPath(String path) {
            try {
                URI uri = new URI(path);
                String scheme = uri.getScheme();
                if (scheme == null || !scheme.equalsIgnoreCase(getScheme()))
                    throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, String.valueOf(scheme) + " not exists");
                return new JimFSDecorator(Paths.get(uri));
            } catch (URISyntaxException e) {
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, name, e.getMessage());
            }
        }

        private FilePath getPath(Path path) {
            return new JimFSDecorator(path);
        }

    }
}