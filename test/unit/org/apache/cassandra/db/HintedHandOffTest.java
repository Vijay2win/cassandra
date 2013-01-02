package org.apache.cassandra.db;
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


import static junit.framework.Assert.assertEquals;
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class HintedHandOffTest extends SchemaLoader
{

    public static final String TABLE4 = "Keyspace4";
    public static final String STANDARD1_CF = "Standard1";
    public static final String COLUMN1 = "column1";

    // Test compaction of hints column family. It shouldn't remove all columns on compaction.
    @Test
    public void testCompactionOfHintsCF() throws Exception
    {
        // prepare hints column family
        Table systemTable = Table.open("system");
        ColumnFamilyStore hintStore = systemTable.getColumnFamilyStore(SystemTable.HINTS_CF);
        hintStore.clearUnsafe();
        hintStore.metadata.gcGraceSeconds(36000); // 10 hours
        hintStore.setCompactionStrategyClass(SizeTieredCompactionStrategy.class.getCanonicalName());
        hintStore.disableAutoCompaction();

        // insert 1 hint
        RowMutation rm = new RowMutation(TABLE4, ByteBufferUtil.bytes(1));
        rm.add(new QueryPath(STANDARD1_CF,
                             null,
                             ByteBufferUtil.bytes(String.valueOf(COLUMN1))),
               ByteBufferUtil.EMPTY_BYTE_BUFFER,
               System.currentTimeMillis());

        RowMutation.hintFor(rm, UUID.randomUUID()).apply();

        // flush data to disk
        hintStore.forceBlockingFlush();
        assertEquals(1, hintStore.getSSTables().size());

        // submit compaction
        FBUtilities.waitOnFuture(HintedHandOffManager.instance.compact());
        while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0)
            TimeUnit.SECONDS.sleep(1);

        // single row should not be removed because of gc_grace_seconds
        // is 10 hours and there are no any tombstones in sstable
        assertEquals(1, hintStore.getSSTables().size());
    }

    @Test
    public void testHintsMetrics() throws UnknownHostException
    {
        for (int i = 0; i < 100; i++)
            HintedHandOffManager.instance.metrics.incrHintsCount();
        for (int i = 0; i < 100; i++)
            HintedHandOffManager.instance.metrics.incrPastWindow(InetAddress.getLocalHost());
        HintedHandOffManager.instance.metrics.log();

        Assert.assertEquals(100, HintedHandOffManager.instance.metrics.hintsCount());

        UntypedResultSet rows = processInternal("SELECT message FROM system." + SystemTable.EVENTS_CF);
        String returned = rows.one().getString("message");
        Assert.assertTrue(returned.contains("100"));

        for (int i = 0; i < 88; i++)
            HintedHandOffManager.instance.metrics.incrPastWindow(InetAddress.getLocalHost());
        HintedHandOffManager.instance.metrics.log();
        rows = processInternal("SELECT message FROM system." + SystemTable.EVENTS_CF);
        returned = Iterators.getLast(rows.iterator(), null).getString("message");
        Assert.assertTrue(returned.contains("88"));
    }
}
