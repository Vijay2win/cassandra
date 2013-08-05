package org.apache.cassandra.cache;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import junit.framework.Assert;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamily;

import com.google.common.collect.Sets;
import com.googlecode.concurrentlinkedhashmap.Weighers;
import org.apache.cassandra.db.TreeMapBackedSortedColumns;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;

import static org.apache.cassandra.Util.column;
import static org.junit.Assert.*;

public class CacheProviderTest extends SchemaLoader
{
    MeasureableString key1 = new MeasureableString("key1");
    MeasureableString key2 = new MeasureableString("key2");
    MeasureableString key3 = new MeasureableString("key3");
    MeasureableString key4 = new MeasureableString("key4");
    MeasureableString key5 = new MeasureableString("key5");
    private static final long CAPACITY = 4;
    private String keyspaceName = "Keyspace1";
    private String cfName = "Standard1";

    private <T> void simpleCase(T cf, ICache<MeasureableString, T> cache)
    {
        cache.put(key1, cf);
        assert cache.get(key1) != null;

        assertDigests(cache.get(key1), cf);
        cache.put(key2, cf);
        cache.put(key3, cf);
        cache.put(key4, cf);
        cache.put(key5, cf);

        assertEquals(CAPACITY, cache.size());
    }

    private <T> void assertDigests(T one, T two)
    {
        // CF does not implement .equals
        if (two instanceof QueryCacheValue)
            assert ColumnFamily.digest(((QueryCacheValue)one).data).equals(ColumnFamily.digest(((QueryCacheValue)two).data));
        else if (one instanceof ColumnFamily)
            assert ColumnFamily.digest((ColumnFamily)one).equals(ColumnFamily.digest((ColumnFamily)two));
        else
            fail();
    }

    // TODO this isn't terribly useful
    private <T> void concurrentCase(final T cf, final ICache<MeasureableString, T> cache) throws InterruptedException
    {
        Runnable runable = new Runnable()
        {
            public void run()
            {
                for (int j = 0; j < 10; j++)
                {
                    cache.put(key1, cf);
                    cache.put(key2, cf);
                    cache.put(key3, cf);
                    cache.put(key4, cf);
                    cache.put(key5, cf);
                }
            }
        };

        List<Thread> threads = new ArrayList<Thread>(100);
        for (int i = 0; i < 100; i++)
        {
            Thread thread = new Thread(runable);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads)
            thread.join();
    }

    private ColumnFamily createCF()
    {
        ColumnFamily cf = TreeMapBackedSortedColumns.factory.create(keyspaceName, cfName);
        cf.addColumn(column("vijay", "great", 1));
        cf.addColumn(column("awesome", "vijay", 1));
        return cf;
    }

    @Test
    public void testSerializingCache() throws InterruptedException
    {
        ICache<MeasureableString, QueryCacheValue> cache = SerializingCache.create(CAPACITY, Weighers.<RefCountedMemory> singleton(), new QueryCacheValue.QueryCacheValueSerializer());
        QueryCacheValue cf = new QueryCacheValue(Sets.<IDiskAtomFilter> newHashSet(new IdentityQueryFilter()), createCF());
        simpleCase(cf, cache);
        concurrentCase(cf, cache);
    }

    @Test
    public void testQueryCache() throws InterruptedException
    {
        QueryCache cache = QueryCache.create(1024 * 1024 * 1024);
        cache.put(createQK("key1"), createCF());
        cache.put(createQK("key2"), createCF());
        cache.put(createQK("key3"), createCF());

        System.out.println(cache.size());
        Assert.assertEquals(createCF(), cache.get(createQK("key1")));
        Assert.assertEquals(createCF(), cache.get(createQK("key2")));
        Assert.assertEquals(createCF(), cache.get(createQK("key3")));
    }

    private QueryCacheKey createQK(String string)
    {
       return new QueryCacheKey(new UUID(0L, 0L), string.getBytes(), new IdentityQueryFilter());
    }

    @Test
    public void testKeys()
    {
        UUID cfId = UUID.randomUUID();

        byte[] b1 = {1, 2, 3, 4};
        RowCacheKey key1 = new RowCacheKey(cfId, ByteBuffer.wrap(b1));
        byte[] b2 = {1, 2, 3, 4};
        RowCacheKey key2 = new RowCacheKey(cfId, ByteBuffer.wrap(b2));
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
        
        byte[] b3 = {1, 2, 3, 5};
        RowCacheKey key3 = new RowCacheKey(cfId, ByteBuffer.wrap(b3));
        assertNotSame(key1, key3);
        assertNotSame(key1.hashCode(), key3.hashCode());
    }

    private class MeasureableString implements IMeasurableMemory
    {
        public final String string;

        public MeasureableString(String input)
        {
            this.string = input;
        }

        public long memorySize()
        {
            return string.length();
        }
    }
}
