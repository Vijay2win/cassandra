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


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cache.ICacheProvider.RowKeySerializer;
import org.apache.cassandra.cache.ICacheProvider.RowValueSerializer;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.CacheService;

import static org.apache.cassandra.Util.column;
import static org.junit.Assert.*;

public class CacheProviderTest
{
    private static final String KEYSPACE1 = "CacheProviderTest1";
    private static final String CF_STANDARD1 = "Standard1";

    private final RowCacheKey key1 = new RowCacheKey(new UUID(0, 0), "key1".getBytes());
    private final RowCacheKey key2 = new RowCacheKey(new UUID(0, 0), "key2".getBytes());
    private final RowCacheKey key3 = new RowCacheKey(new UUID(0, 0), "key3".getBytes());
    private final RowCacheKey key4 = new RowCacheKey(new UUID(0, 0), "key4".getBytes());
    private final RowCacheKey key5 = new RowCacheKey(new UUID(0, 0), "key5".getBytes());

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    SimpleStrategy.class,
                                    KSMetaData.optsWithRF(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1));
    }

    private void simpleCase(ColumnFamily cf, AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache)
    {
        rowCache.put(key1, cf);
        assertNotNull(rowCache.get(key1));

        assertDigests(rowCache.get(key1), cf);
        rowCache.putIfAbsent(key2, cf);
        rowCache.putIfAbsent(key3, cf);
        rowCache.putIfAbsent(key4, cf);
        rowCache.putIfAbsent(key5, cf);

        assertEquals(5, rowCache.size());
    }

    private void assertDigests(IRowCacheEntry one, ColumnFamily two)
    {
        // CF does not implement .equals
        assertTrue(one instanceof ColumnFamily);
        assertEquals(ColumnFamily.digest((ColumnFamily)one), ColumnFamily.digest(two));
    }

    // TODO this isn't terribly useful
    private void concurrentCase(final ColumnFamily cf, final AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache) throws InterruptedException
    {
        Runnable runable = new Runnable()
        {
            public void run()
            {
                for (int j = 0; j < 10; j++)
                {
                    rowCache.putIfAbsent(key1, cf);
                    rowCache.putIfAbsent(key2, cf);
                    rowCache.putIfAbsent(key3, cf);
                    rowCache.putIfAbsent(key4, cf);
                    rowCache.putIfAbsent(key5, cf);
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
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(KEYSPACE1, CF_STANDARD1);
        cf.addColumn(column("vijay", "great", 1));
        cf.addColumn(column("awesome", "vijay", 1));
        cf.addColumn(column("vijay2", "great", 1));
        cf.addColumn(column("awesome2", "vijay", 1));
        cf.addColumn(column("vijay3", "vijay", 1));
        return cf;
    }

    @Test
    public void testSerializingCache() throws InterruptedException
    {
        ColumnFamily cf = createCF();
        CacheService.instance.rowCache.clear();
        simpleCase(cf, CacheService.instance.rowCache);
        concurrentCase(cf, CacheService.instance.rowCache);
    }
    
    @Test
    public void testKeys() throws IOException
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

    @Test
    public void testKeySerialization() throws IOException
    {
        RowKeySerializer serializer = new RowKeySerializer();
        DataOutputBuffer buffer = new DataOutputBuffer();
        serializer.serialize(buffer, key3);

        assertEquals(serializer.serializedSize(key3), buffer.getLength());

        DataInputStream input = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));
        RowCacheKey clone = serializer.deserialize(input, buffer.getLength());
        assertEquals(clone, key3);
    }

    @Test
    public void testValueSerialization() throws IOException
    {
        RowValueSerializer serializer = new RowValueSerializer();
        DataOutputBuffer buffer = new DataOutputBuffer();
        ColumnFamily original = createCF();
        serializer.serialize(buffer, original);

        assertEquals(serializer.serializedSize(original), buffer.getLength());

        DataInputStream input = new DataInputStream(new ByteArrayInputStream(buffer.toByteArray()));
        IRowCacheEntry clone = serializer.deserialize(input, buffer.getLength());
        assertEquals(clone, original);
    }
}
