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
package org.apache.cassandra.io.sstable;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;

import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndexSummaryBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryBuilder.class);

    private final ArrayList<Long> positions;
    private final ArrayList<byte[]> keys;
    private long keysWritten = 0;
    private long offheapSize = 0;

    public IndexSummaryBuilder(long expectedKeys, int indexInterval)
    {
        long expectedEntries = expectedKeys / indexInterval;
        if (expectedEntries > Integer.MAX_VALUE)
        {
            // that's a _lot_ of keys, and a very low interval
            int effectiveInterval = (int) Math.ceil((double) Integer.MAX_VALUE / expectedKeys);
            expectedEntries = expectedKeys / effectiveInterval;
            assert expectedEntries <= Integer.MAX_VALUE : expectedEntries;
            logger.warn("Index interval of {} is too low for {} expected keys; using interval of {} instead",
                    indexInterval, expectedKeys, effectiveInterval);
        }
        positions = new ArrayList<Long>((int)expectedEntries);
        keys = new ArrayList<byte[]>((int)expectedEntries);
    }

    public IndexSummaryBuilder maybeAddEntry(DecoratedKey decoratedKey, int indexInterval, long indexPosition)
    {
        if (keysWritten % indexInterval == 0)
        {
            byte[] key = ByteBufferUtil.getArray(decoratedKey.key);
            keys.add(key);
            offheapSize += key.length;
            positions.add(indexPosition);
            offheapSize += TypeSizes.NATIVE.sizeof(indexPosition);
        }
        keysWritten++;

        return this;
    }

    public IndexSummary build(IPartitioner partitioner, int indexInterval)
    {
        assert keys != null && keys.size() > 0;
        assert keys.size() == positions.size();

        Memory memory = Memory.allocate(offheapSize);
        MemoryOutputStream out = new MemoryOutputStream(memory);
        int[] summaryIdx = new int[keys.size()];
        try
        {
            for (int i = 0; i < keys.size(); i++)
            {
                summaryIdx[i] = out.position();
                out.write(keys.get(i));
                out.write(Longs.toByteArray(positions.get(i)));
            }
        }
        catch (IOException ex)
        {
            throw new IOError(ex);
        }
        finally
        {
            FileUtils.closeQuietly(out);
        }
        return new IndexSummary(partitioner, memory, summaryIdx, indexInterval);
    }
}
