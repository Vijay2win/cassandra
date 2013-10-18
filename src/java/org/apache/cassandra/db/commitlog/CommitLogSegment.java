/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/*
 * A single commit log file on disk. Manages creation of the file and writing row mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accomidate a larger value if necessary.
 */
public class CommitLogSegment
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogSegment.class);

    private final static long idBase = System.currentTimeMillis();
    private final static AtomicInteger nextId = new AtomicInteger(1);

    // The commit log entry overhead in bytes (int: length + long: head checksum + long: tail checksum)
    static final int ENTRY_OVERHEAD_SIZE = 4 + 8 + 8;

    private AtomicInteger position = new AtomicInteger(4); //reserve first 4 bytes.
    private AtomicInteger lastSynced = new AtomicInteger(0);
    // cache which cf is dirty in this segment to avoid having to lookup all ReplayPositions to decide if we can delete this segment
    private final LoadingCache<UUID, AtomicInteger> cfLastWrite = CacheBuilder.newBuilder().build(new CacheLoader<UUID, AtomicInteger>()
    {
        public AtomicInteger load(UUID key) throws Exception
        {
            return new AtomicInteger(0);
        }
    });

    public final long id;

    private final File logFile;
    private final RandomAccessFile logFileAccessor;

    private final MappedByteBuffer buffer;

    public final CommitLogDescriptor descriptor;

    /**
     * @return a newly minted segment file
     */
    public static CommitLogSegment freshSegment()
    {
        return new CommitLogSegment(null);
    }

    public static long getNextId()
    {
        return idBase + nextId.getAndIncrement();
    }

    /**
     * Constructs a new segment file.
     *
     * @param filePath  if not null, recycles the existing file by renaming it and truncating it to CommitLog.SEGMENT_SIZE.
     */
    CommitLogSegment(String filePath)
    {
        id = getNextId();
        descriptor = new CommitLogDescriptor(id);
        logFile = new File(DatabaseDescriptor.getCommitLogLocation(), descriptor.fileName());
        boolean isCreating = true;

        try
        {
            if (filePath != null)
            {
                File oldFile = new File(filePath);

                if (oldFile.exists())
                {
                    logger.debug("Re-using discarded CommitLog segment for {} from {}", id, filePath);
                    if (!oldFile.renameTo(logFile))
                        throw new IOException("Rename from " + filePath + " to " + id + " failed");
                    isCreating = false;
                }
            }

            // Open the initial the segment file
            logFileAccessor = new RandomAccessFile(logFile, "rw");

            if (isCreating)
                logger.debug("Creating new commit log segment {}", logFile.getPath());

            // Map the segment, extending or truncating it to the standard segment size
            logFileAccessor.setLength(DatabaseDescriptor.getCommitLogSegmentSize());

            buffer = logFileAccessor.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, DatabaseDescriptor.getCommitLogSegmentSize());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }
    }

    /**
     * Completely discards a segment file by deleting it. (Potentially blocking operation)
     */
    public void discard(boolean deleteFile)
    {
        // TODO shouldn't we close the file when we're done writing to it, which comes (potentially) much earlier than it's eligible for recyling?
        close(deleteFile);
        if (deleteFile)
            FileUtils.deleteWithConfirm(logFile);
    }

    /**
     * Recycle processes an unneeded segment file for reuse.
     *
     * @return a new CommitLogSegment representing the newly reusable segment.
     */
    public CommitLogSegment recycle()
    {
        try
        {
            position.set(4); // remember first 4 bytes are reserved.
            sync();
        }
        catch (FSWriteError e)
        {
            logger.error("I/O error flushing {} {}", this, e.getMessage());
            throw e;
        }

        close(false);

        return new CommitLogSegment(getPath());
    }

    /**
     * @return true if there is room to write() @param size to this segment
     */
    public boolean hasCapacityFor(long size)
    {
        return size <= (buffer.remaining() - position.get());
    }

    /**
     * mark all of the column families we're modifying as dirty at this position
     */
    private void markDirty(RowMutation rowMutation, int position)
    {
        for (ColumnFamily columnFamily : rowMutation.getColumnFamilies())
        {
            // check for null cfm in case a cl write goes through after the cf is
            // defined but before a new segment is created.
            CFMetaData cfm = Schema.instance.getCFMetaData(columnFamily.id());
            if (cfm == null)
            {
                logger.error("Attempted to write commit log entry for unrecognized column family: {}", columnFamily.id());
            }
            else
            {
                markCFDirty(cfm.cfId, position);
            }
        }
    }

    public ByteBuffer allocate(RowMutation mutation, int size)
    {
        while (true)
        {
            int current = position.get();
            int newCurrent = current + size;
            if (newCurrent >= buffer.capacity())
                return null;
            if (position.compareAndSet(current, newCurrent))
            {
                markDirty(mutation, current);
                return (ByteBuffer) buffer.duplicate().position(current).limit(newCurrent);
            }
            // we need to get another one, its already taken.
        }
    }

    /**
     * Forces a disk flush for this segment file.
     */
    public void sync()
    {
        try
        {
            int current = position.get();
            if (current != lastSynced.get())
            {
                buffer.putInt(0, current);
                buffer.force();
                lastSynced.set(current);
            }
        }
        catch (Exception e) // MappedByteBuffer.force() does not declare IOException but can actually throw it
        {
            throw new FSWriteError(e, getPath());
        }
    }

    /**
     * @return the current ReplayPosition for this log segment
     */
    public ReplayPosition getContext()
    {
        return new ReplayPosition(id, position.get());
    }

    /**
     * @return the file path to this segment
     */
    public String getPath()
    {
        return logFile.getPath();
    }

    /**
     * @return the file name of this segment
     */
    public String getName()
    {
        return logFile.getName();
    }

    /**
     * Close the segment file.
     */
    public void close(boolean hardClose)
    {
        try
        {
            FileUtils.clean(buffer);
            if (hardClose)
                logFileAccessor.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    /**
     * Records the CF as dirty at a certain position.
     *
     * @param cfId      the column family ID that is now dirty
     * @param position  the position the last write for this CF was written at
     */
    private void markCFDirty(UUID cfId, Integer position)
    {
        try
        {
            while (true)
            {
                AtomicInteger lastWrite = cfLastWrite.get(cfId);
                int lastposition = lastWrite.get();
                if (lastposition >= position)
                    return;
                if (lastWrite.compareAndSet(lastposition, position))
                    return;
            }
        }
        catch (ExecutionException e)
        {
            // this cannot happen
            throw new AssertionError(e);
        }
    }

    /**
     * Marks the ColumnFamily specified by cfId as clean for this log segment. If the
     * given context argument is contained in this file, it will only mark the CF as
     * clean if no newer writes have taken place.
     *
     * @param cfId    the column family ID that is now clean
     * @param context the optional clean offset
     */
    public void markClean(UUID cfId, ReplayPosition context)
    {
        cfLastWrite.invalidate(cfId);
    }

    /**
     * @return a collection of dirty CFIDs for this segment file.
     */
    public Collection<UUID> getDirtyCFIDs()
    {
        return cfLastWrite.asMap().keySet();
    }

    /**
     * @return true if this segment is unused and safe to recycle or delete
     */
    public boolean isUnused()
    {
        return cfLastWrite.size() == 0;
    }

    /**
     * Check to see if a certain ReplayPosition is contained by this segment file.
     *
     * @param   context the replay position to be checked
     * @return  true if the replay position is contained by this segment file.
     */
    public boolean contains(ReplayPosition context)
    {
        return context.segment == id;
    }

    // For debugging, not fast
    public String dirtyString()
    {
        StringBuilder sb = new StringBuilder();
        for (UUID cfId : cfLastWrite.asMap().keySet())
        {
            CFMetaData m = Schema.instance.getCFMetaData(cfId);
            sb.append(m == null ? "<deleted>" : m.cfName).append(" (").append(cfId).append("), ");
        }
        return sb.toString();
    }

    @Override
    public String toString()
    {
        return "CommitLogSegment(" + getPath() + ')';
    }

    public int position()
    {
        return position.get();
    }


    public static class CommitLogSegmentFileComparator implements Comparator<File>
    {
        public int compare(File f, File f2)
        {
            CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(f.getName());
            CommitLogDescriptor desc2 = CommitLogDescriptor.fromFileName(f2.getName());
            return (int) (desc.id - desc2.id);
        }
    }
}
