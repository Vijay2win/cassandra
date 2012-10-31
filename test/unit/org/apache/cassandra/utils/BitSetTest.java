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
package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.KeyGenerator.WordGenerator;
import org.apache.cassandra.utils.obs.BitSet;
import org.apache.cassandra.utils.obs.OffHeapBitSet;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.junit.Test;

import com.google.common.collect.Lists;

public class BitSetTest
{
    /**
     * OffheapBitSet uses OpenBitSet.bits2words because of this test.
     */
    @Test
    public void compareBitSets()
    {
        BloomFilter bf2 = (BloomFilter) FilterFactory.getFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTestHelper.MAX_FAILURE_RATE, false);
        BloomFilter bf3 = (BloomFilter) FilterFactory.getFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTestHelper.MAX_FAILURE_RATE, true);
        int skipEven = KeyGenerator.WordGenerator.WORDS % 2 == 0 ? 0 : 2;
        WordGenerator gen1 = new KeyGenerator.WordGenerator(skipEven, 2);

        // make sure both bitsets are empty.
        for (long i = 0; i < bf2.bitset.capacity(); i++)
            Assert.assertEquals(bf2.bitset.get(i), bf3.bitset.get(i));

        addPositives(bf2, bf3, gen1);

        for (long i = 0; i < bf2.bitset.capacity(); i++)
            Assert.assertEquals(bf2.bitset.get(i), bf3.bitset.get(i));
    }

    public void addPositives(BloomFilter f1, BloomFilter f2, ResetableIterator<ByteBuffer> keys)
    {
        while (keys.hasNext())
        {
            ByteBuffer key = keys.next();
            f1.add(key);
            f2.add(key);
        }
    }

    private static final String LEGACY_SST_FILE = "test/data/legacy-sstables/hb/Keyspace1/Keyspace1-Standard1-hb-0-Filter.db";

    /**
     * Test with some real data.
     */
    @Test
    public void testExpectedCompatablity() throws IOException
    {
        DataInputStream dis = new DataInputStream(new FileInputStream(new File(LEGACY_SST_FILE)));
        dis.readInt();
        OpenBitSet bs = OpenBitSet.deserialize(dis);

        dis = new DataInputStream(new FileInputStream(new File(LEGACY_SST_FILE)));
        dis.readInt();
        OffHeapBitSet obs = OffHeapBitSet.deserialize(dis);

        // compare to make sure we are good to go.
        for (long i = 0; i < bs.size(); i++)
            compare(obs, bs, i);
    }

    private static final Random random = new Random();

    public void populateRandom(OffHeapBitSet offbs, long index)
    {
        if (random.nextBoolean())
            offbs.set(index);
    }

    /**
     * Test serialization and de-serialization.
     */
    @Test
    public void testSerialization() throws IOException
    {
        OffHeapBitSet bs = new OffHeapBitSet(Integer.MAX_VALUE / 4000);
        for (long i = 0; i < Integer.MAX_VALUE / 40000; i++)
            populateRandom(bs, i);

        DataOutputBuffer dos = new DataOutputBuffer();
        bs.serialize(dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(dos.getData()));
        OffHeapBitSet newbs = OffHeapBitSet.deserialize(dis);
        for (long i = 0; i < Integer.MAX_VALUE / 40000; i++)
            compare(bs, newbs, i);
    }

    public void compare(BitSet offbs, BitSet obs, long index)
    {
        Assert.assertEquals(offbs.get(index), obs.get(index));
    }

    @Test
    public void testBitClear() throws IOException
    {
        int size = Integer.MAX_VALUE / 4000;
        OffHeapBitSet bitset = new OffHeapBitSet(size);
        List<Integer> randomBits = Lists.newArrayList();
        for (int i = 0; i < 10; i++)
            randomBits.add(random.nextInt(size));

        for (long randomBit : randomBits)
            bitset.set(randomBit);

        for (long randomBit : randomBits)
            Assert.assertEquals(true, bitset.get(randomBit));

        for (long randomBit : randomBits)
            bitset.clear(randomBit);

        for (long randomBit : randomBits)
            Assert.assertEquals(false, bitset.get(randomBit));
        bitset.close();
    }
}
