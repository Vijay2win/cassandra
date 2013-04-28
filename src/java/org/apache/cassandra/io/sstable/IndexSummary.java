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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.MemoryInputStream;
import org.apache.cassandra.io.util.MemoryOutputStream;
import org.apache.cassandra.utils.FBUtilities;

public class IndexSummary
{
    public static final IndexSummarySerializer serializer = new IndexSummarySerializer();
    private final int indexInterval;
    private final IPartitioner partitioner;
    private int[] summaryIdx;
    private Memory memory;

    public IndexSummary(IPartitioner partitioner, Memory memory, int[] summaryIdx, int indexInterval)
    {
        this.partitioner = partitioner;
        this.indexInterval = indexInterval;
        this.summaryIdx = summaryIdx;
        this.memory = memory;
    }

    public byte[][] getKeys()
    {
        //return keys;
        return null;
    }

    // binary search is notoriously more difficult to get right than it looks; this is lifted from
    // Harmony's Collections implementation
    public int binarySearch(RowPosition key)
    {
        int low = 0, mid = summaryIdx.length, high = mid - 1, result = -1;
        while (low <= high)
        {
            mid = (low + high) >> 1;
            result = -partitioner.decorateKey(getKey(mid)).compareTo(key);
            if (result > 0)
            {
                low = mid + 1;
            }
            else if (result == 0)
            {
                return mid;
            }
            else
            {
                high = mid - 1;
            }
        }

        return -mid - (result < 0 ? 1 : 2);
    }

    public ByteBuffer getKey(int index)
    {
        byte[] temp = getIndexPair(index);
        return ByteBuffer.wrap(temp, 0, temp.length - 8);
    }

    public byte[] getIndexPair(int index)
    {
        int end = (index == (summaryIdx.length - 1)) ? (int) memory.size() : summaryIdx[index + 1];
        int byteSize = (end - summaryIdx[index]);
        byte[] temp = new byte[byteSize];
        memory.getBytes((long) summaryIdx[index], temp, 0, byteSize);
        return temp;
    }

    public long getPosition(int index)
    {
        byte[] temp = getIndexPair(index);
        return ByteBuffer.wrap(temp, temp.length - 8, 8).getLong();
    }

    public int getIndexInterval()
    {
        return indexInterval;
    }

    public int size()
    {
        return summaryIdx.length;
    }

    public static class IndexSummarySerializer
    {
        public void serialize(IndexSummary t, DataOutputStream out) throws IOException
        {
            out.writeInt(t.indexInterval);
            out.writeInt(t.summaryIdx.length);
            for (int i = 0; i < t.summaryIdx.length; i++)
                out.writeInt(t.summaryIdx[i]);
            out.writeLong(t.memory.size());
            FBUtilities.copy(new MemoryInputStream(t.memory), out, t.memory.size());
        }

        public IndexSummary deserialize(DataInputStream in, IPartitioner partitioner) throws IOException
        {
            int indexInterval = in.readInt();
            int size = in.readInt();
            int[] summaryIdx = new int[size];
            for (int i = 0; i < size; i++)
                summaryIdx[i] = in.readInt();
            long offheapSize = in.readLong();
            Memory memory = Memory.allocate(offheapSize);
            FBUtilities.copy(in, new MemoryOutputStream(memory), offheapSize);
            return new IndexSummary(partitioner, memory, summaryIdx, indexInterval);
        }
    }
}
