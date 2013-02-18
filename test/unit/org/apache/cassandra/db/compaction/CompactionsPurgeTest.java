/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import junit.framework.Assert;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.Util;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;
import static org.apache.cassandra.db.TableTest.assertColumns;
import static org.junit.Assert.assertEquals;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;


public class CompactionsPurgeTest extends SchemaLoader
{
    public static final String TABLE1 = "Keyspace1";
    public static final String TABLE2 = "Keyspace2";

    @Test
    public void testMajorCompactionPurge() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        DecoratedKey key = Util.dk("key1");
        RowMutation rm;

        // inserts
        rm = new RowMutation(TABLE1, key.key);
        for (int i = 0; i < 10; i++)
        {
            rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        }
        rm.apply();
        cfs.forceBlockingFlush();

        // deletes
        for (int i = 0; i < 10; i++)
        {
            rm = new RowMutation(TABLE1, key.key);
            rm.delete(cfName, ByteBufferUtil.bytes(String.valueOf(i)), 1);
            rm.apply();
        }
        cfs.forceBlockingFlush();

        // resurrect one column
        rm = new RowMutation(TABLE1, key.key);
        rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(5)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 2);
        rm.apply();
        cfs.forceBlockingFlush();

        // major compact and test that all columns but the resurrected one is completely gone
        CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE).get();
        cfs.invalidateCachedRow(key);
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName));
        assertColumns(cf, "5");
        assert cf.getColumn(ByteBufferUtil.bytes(String.valueOf(5))) != null;
    }

    @Test
    public void testCleanupDuringCompaction() throws Exception
    {
        DatabaseDescriptor.setCleanupDuringCompaction(true);
        CompactionManager.instance.disableAutoCompaction();
        Table table = Table.open(TABLE1);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);
        // inserts
        for (int i = 0; i < 10; i++)
        {
            DecoratedKey key = Util.dk("key" + i);
            RowMutation rm = new RowMutation(TABLE1, key.key);
            rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
            rm.apply();
        }
        cfs.forceBlockingFlush();

        List<Row> rows = Util.getRangeSlice(cfs);
        assertNotSame(0, rows.size());
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        byte[] tk1 = new byte[1], tk2 = new byte[1];
        tk1[0] = 2;
        tk2[0] = 1;
        tmd.updateNormalToken(new BytesToken(tk1), InetAddress.getByName("127.0.0.1"));
        tmd.updateNormalToken(new BytesToken(tk2), InetAddress.getByName("127.0.0.2"));
        CompactionManager.instance.submitMaximal(cfs, Integer.MAX_VALUE).get();
        tmd.removeEndpoint(InetAddress.getByName("127.0.0.2"));
        rows = Util.getRangeSlice(cfs);
        assertEquals(0, rows.size());   
    }

    @Test
    public void testMinorCompactionPurge() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE2);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        RowMutation rm;
        for (int k = 1; k <= 2; ++k) {
            DecoratedKey key = Util.dk("key" + k);

            // inserts
            rm = new RowMutation(TABLE2, key.key);
            for (int i = 0; i < 10; i++)
            {
                rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
            }
            rm.apply();
            cfs.forceBlockingFlush();

            // deletes
            for (int i = 0; i < 10; i++)
            {
                rm = new RowMutation(TABLE2, key.key);
                rm.delete(cfName, ByteBufferUtil.bytes(String.valueOf(i)), 1);
                rm.apply();
            }
            cfs.forceBlockingFlush();
        }

        DecoratedKey key1 = Util.dk("key1");
        DecoratedKey key2 = Util.dk("key2");

        // flush, remember the current sstable and then resurrect one column
        // for first key. Then submit minor compaction on remembered sstables.
        cfs.forceBlockingFlush();
        Collection<SSTableReader> sstablesIncomplete = cfs.getSSTables();
        rm = new RowMutation(TABLE2, key1.key);
        rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(5)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 2);
        rm.apply();
        cfs.forceBlockingFlush();
        new CompactionTask(cfs, sstablesIncomplete, Integer.MAX_VALUE).execute(null);

        // verify that minor compaction does not GC when key is present
        // in a non-compacted sstable
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key1, cfName));
        Assert.assertEquals(10, cf.getColumnCount());

        // verify that minor compaction does GC when key is provably not
        // present in a non-compacted sstable
        cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key2, cfName));
        assert cf == null;
    }

    @Test
    public void testMinTimestampPurge() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();
        Table table = Table.open(TABLE2);
        String cfName = "Standard1";
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        RowMutation rm;
        DecoratedKey key3 = Util.dk("key3");
        // inserts
        rm = new RowMutation(TABLE2, key3.key);
        rm.add(cfName, ByteBufferUtil.bytes("c1"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 8);
        rm.add(cfName, ByteBufferUtil.bytes("c2"), ByteBufferUtil.EMPTY_BYTE_BUFFER, 8);
        rm.apply();
        cfs.forceBlockingFlush();
        // deletes
        rm = new RowMutation(TABLE2, key3.key);
        rm.delete(cfName, ByteBufferUtil.bytes("c1"), 10);
        rm.apply();
        cfs.forceBlockingFlush();
        Collection<SSTableReader> sstablesIncomplete = cfs.getSSTables();

        // delete so we have new delete in a diffrent SST.
        rm = new RowMutation(TABLE2, key3.key);
        rm.delete(cfName, ByteBufferUtil.bytes("c2"), 9);
        rm.apply();
        cfs.forceBlockingFlush();
        new CompactionTask(cfs, sstablesIncomplete, Integer.MAX_VALUE).execute(null);

        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key3, cfName));
        Assert.assertTrue(!cf.getColumn(ByteBufferUtil.bytes("c2")).isLive());
        Assert.assertEquals(1, cf.getColumnCount());
    }

    @Test
    public void testCompactionPurgeOneFile() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        Table table = Table.open(TABLE1);
        String cfName = "Standard2";
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        DecoratedKey key = Util.dk("key1");
        RowMutation rm;

        // inserts
        rm = new RowMutation(TABLE1, key.key);
        for (int i = 0; i < 5; i++)
        {
            rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        }
        rm.apply();

        // deletes
        for (int i = 0; i < 5; i++)
        {
            rm = new RowMutation(TABLE1, key.key);
            rm.delete(cfName, ByteBufferUtil.bytes(String.valueOf(i)), 1);
            rm.apply();
        }
        cfs.forceBlockingFlush();
        assert cfs.getSSTables().size() == 1 : cfs.getSSTables(); // inserts & deletes were in the same memtable -> only deletes in sstable

        // compact and test that the row is completely gone
        Util.compactAll(cfs).get();
        assert cfs.getSSTables().isEmpty();
        ColumnFamily cf = table.getColumnFamilyStore(cfName).getColumnFamily(QueryFilter.getIdentityFilter(key, cfName));
        assert cf == null : cf;
    }

    @Test
    public void testCompactionPurgeCachedRow() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        String tableName = "RowCacheSpace";
        String cfName = "CachedCF";
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        DecoratedKey key = Util.dk("key3");
        RowMutation rm;

        // inserts
        rm = new RowMutation(tableName, key.key);
        for (int i = 0; i < 10; i++)
        {
            rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        }
        rm.apply();

        // move the key up in row cache
        cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName));

        // deletes row
        rm = new RowMutation(tableName, key.key);
        rm.delete(cfName, 1);
        rm.apply();

        // flush and major compact
        cfs.forceBlockingFlush();
        Util.compactAll(cfs).get();

        // re-inserts with timestamp lower than delete
        rm = new RowMutation(tableName, key.key);
        for (int i = 0; i < 10; i++)
        {
            rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, 0);
        }
        rm.apply();

        // Check that the second insert did went in
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName));
        assertEquals(10, cf.getColumnCount());
        for (Column c : cf)
            assert !c.isMarkedForDelete();
    }

    @Test
    public void testCompactionPurgeTombstonedRow() throws IOException, ExecutionException, InterruptedException
    {
        CompactionManager.instance.disableAutoCompaction();

        String tableName = "Keyspace1";
        String cfName = "Standard1";
        Table table = Table.open(tableName);
        ColumnFamilyStore cfs = table.getColumnFamilyStore(cfName);

        DecoratedKey key = Util.dk("key3");
        RowMutation rm;

        // inserts
        rm = new RowMutation(tableName, key.key);
        for (int i = 0; i < 10; i++)
        {
            rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, i);
        }
        rm.apply();

        // deletes row with timestamp such that not all columns are deleted
        rm = new RowMutation(tableName, key.key);
        rm.delete(cfName, 4);
        rm.apply();

        // flush and major compact (with tombstone purging)
        cfs.forceBlockingFlush();
        Util.compactAll(cfs).get();

        // re-inserts with timestamp lower than delete
        rm = new RowMutation(tableName, key.key);
        for (int i = 0; i < 5; i++)
        {
            rm.add(cfName, ByteBufferUtil.bytes(String.valueOf(i)), ByteBufferUtil.EMPTY_BYTE_BUFFER, i);
        }
        rm.apply();

        // Check that the second insert did went in
        ColumnFamily cf = cfs.getColumnFamily(QueryFilter.getIdentityFilter(key, cfName));
        assertEquals(10, cf.getColumnCount());
        for (Column c : cf)
            assert !c.isMarkedForDelete();
    }
}
