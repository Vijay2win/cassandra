package org.apache.cassandra.db;

import static org.apache.cassandra.Util.column;
import static org.apache.cassandra.Util.counterColumn;
import static org.junit.Assert.*;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cache.KeyCacheKey;
import org.apache.cassandra.cache.RowCacheKey;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnIndex.Builder;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.IndexHelper.IndexInfo;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Filter;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.HeapAllocator;
import org.apache.cassandra.utils.IntervalTree;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.github.jamm.MemoryMeter;
import org.junit.Test;

import com.google.common.collect.Lists;

import edu.stanford.ppl.concurrent.SnapTreeMap;

public class ObjectSizesTest extends SchemaLoader
{
    public static final MemoryMeter meter = new MemoryMeter();
    private String tableName = "Keyspace1";
    private String counterCFName = "Counter1";

    public static class TestObject
    {
        public int test = 100;
        public byte[] bytes = new byte[100];
        public ByteBuffer buffer = ByteBuffer.wrap(new byte[100]);

        public long getMemorySize()
        {
            return ObjectSizes.getFieldSize(4L + ObjectSizes.getSizeWithRef(bytes) + // ByteArray
                    ObjectSizes.getSizeWithRef(buffer)); // BB
        }
    }

    @Test
    public void testPrimitiveSizes()
    {
        assertEquals(ObjectSizes.getArraySize(0), meter.measureDeep(new byte[0]));
        assertEquals(ObjectSizes.getArraySize(8), meter.measureDeep(new byte[8]));
        assertEquals(ObjectSizes.getArraySize(9), meter.measureDeep(new byte[9]));
        assertEquals(ObjectSizes.getArraySize(16), meter.measureDeep(new byte[16]));
        ByteBuffer buffer = ByteBuffer.wrap(new byte[100]);
        assertEquals(ObjectSizes.getSize(buffer), meter.measureDeep(buffer));
        assertEquals(meter.measureDeep(new TestObject()), new TestObject().getMemorySize());
    }

    @Test
    public void testKeyCacheKey()
    {
        Descriptor desc = new Descriptor(new File(""), "ksname", "cfname", 10, false);
        KeyCacheKey key = new KeyCacheKey(desc, ByteBuffer.wrap(new byte[0]));
        assertEquals(meter.measureDeep(key) - meter.measureDeep(desc), key.memorySize());

        RowCacheKey rkey = new RowCacheKey(123, ByteBuffer.wrap(new byte[11]));
        assertEquals(meter.measureDeep(rkey), rkey.memorySize());
    }

    private ColumnFamily createCounterCF()
    {
        ColumnFamily cf = ColumnFamily.create(tableName, counterCFName);
        cf.addColumn(counterColumn("vijay", 1L, 1));
        cf.addColumn(counterColumn("wants", 1000000, 1));
        return cf;
    }

    @Test
    public void testListSizes()
    {
        int count = 10;
        ArrayList<Long> lst = new ArrayList<Long>(count);
        for (long i = 0; i < count; i++)
            lst.add(i);
        lst.trimToSize();
        assertEquals(meter.measureDeep(lst), ObjectSizes.getSize(lst, ObjectSizes.getFieldSize(8L) * count));
    }

    @Test
    public void testBloomFilterSizes()
    {
        OpenBitSet bitset = new OpenBitSet(10000L);
        assertEquals(meter.measureDeep(new OpenBitSet(10000L)), ObjectSizes.getSize(bitset));

        Filter filter = FilterFactory.getFilter(1000L, 0.99);
        assertEquals(meter.measureDeep(filter), filter.memorySize());
    }

    @Test
    public void testKeyCacheValues() throws IOException
    {
        IndexInfo indexInfo = new IndexHelper.IndexInfo(ByteBuffer.wrap("1".getBytes()), ByteBuffer.wrap("100".getBytes()), 0, 1000);
        assertEquals(meter.measureDeep(indexInfo), indexInfo.memorySize());
        ArrayList<IndexInfo> infos = Lists.newArrayList(indexInfo);
        infos.trimToSize();
        assertEquals(meter.measureDeep(infos), ObjectSizes.getSize(infos, indexInfo.memorySize()));

        RangeTombstone tombstone = new RangeTombstone(ByteBuffer.wrap("1".getBytes()), ByteBuffer.wrap("9".getBytes()), System.currentTimeMillis(), 0);
        assertEquals(meter.measureDeep(tombstone), tombstone.memorySize());
        IntervalTree<ByteBuffer, DeletionTime, RangeTombstone> tombstones = IntervalTree.build(Collections.singletonList(tombstone));
        for (RangeTombstone stone : tombstones)
            assertEquals(meter.measureDeep(stone), stone.memorySize());
        assertEquals(meter.measureDeep(tombstones), tombstones.memorySize());
        DeletionTime time = new DeletionTime(System.currentTimeMillis(), 100);
        assertEquals(meter.measureDeep(time), time.memorySize());

        DeletionInfo info = new DeletionInfo(tombstone, BytesType.instance);
        // we dont want to measure the singleton which Memory Meter does.
        assertEquals((meter.measureDeep(info) - meter.measureDeep(BytesType.instance) + 8), info.memorySize());

        DataOutput output = new DataOutputBuffer();
        Builder builder = new ColumnIndex.Builder(createCounterCF(), ByteBuffer.wrap(("" + 1).getBytes()), 100, output);
        for (int i = 0; i < 100; i++)
            builder.add(counterColumn("vijay" + i, 1L, 1));
        for (int i = 0; i < 100; i++)
            builder.add(counterColumn("zzzz" + i, 1L, 1));
        ColumnIndex index = builder.build();
        assertEquals(meter.measureDeep(index), index.memorySize());

        // finally!
        RowIndexEntry rIndex = RowIndexEntry.create(100, info, index);
        // we dont want to measure the singleton which Memory Meter does.
        assertEquals((meter.measureDeep(rIndex) - meter.measureDeep(BytesType.instance)), rIndex.memorySize());
    }

    @Test
    public void testMaps()
    {
        TreeMap<ByteBuffer, Column> map = new TreeMap<ByteBuffer, Column>(BytesType.instance);
        Column col = column("vijay1", "tests", 1);
        Column col1 = column("vijay2", "tests", 1);
        Column col2 = column("vijay3", "tests", 1);
        map.put(col.name, col);
        map.put(col1.name, col1);
        map.put(col2.name, col2);
        int entrySize = 0;
        for (Entry<ByteBuffer, Column> entry : map.entrySet())
            entrySize += entry.getValue().memorySize();
        assertEquals((meter.measureDeep(map) - meter.measureDeep(BytesType.instance)), ObjectSizes.getSize(map, entrySize));

        SnapTreeMap<ByteBuffer, Column> map1 = new SnapTreeMap<ByteBuffer, Column>(BytesType.instance);
        map1.put(col.name, col);
        map1.put(col1.name, col1);
        map1.put(col2.name, col2);
        entrySize = 0;
        for (Entry<ByteBuffer, Column> entry : map1.entrySet())
            entrySize += entry.getValue().memorySize();
        assertEquals((meter.measureDeep(map1) - meter.measureDeep(BytesType.instance)), ObjectSizes.getSize(map1, entrySize));
    }

    @Test
    public void testRowCacheValues() throws IOException
    {
        Column ccol = counterColumn("vijay", 1L, 1);
        assertEquals(meter.measureDeep(ccol), ccol.memorySize());
        Column col = column("vijay", "tests", 1);
        assertEquals(meter.measureDeep(col), col.memorySize());
        ExpiringColumn ecol = new ExpiringColumn(ByteBufferUtil.bytes("vj"), ByteBufferUtil.bytes("trys"), 1000, 100);
        assertEquals(meter.measureDeep(ecol), ecol.memorySize());

        ISortedColumns cols = ArrayBackedSortedColumns.factory.create(BytesType.instance, false);
        for (int i = 0; i < 10; i++)
            cols.addColumn(column("vijay"+i, "tests", 1), HeapAllocator.instance);
        assertEquals(measureSize(cols), cols.memorySize());

        cols = TreeMapBackedSortedColumns.factory.create(BytesType.instance, false);
        for (int i = 0; i < 10; i++)
            cols.addColumn(column("vijay"+i, "tests", 1), HeapAllocator.instance);
        assertEquals(measureSize(cols) + 16, cols.memorySize());

        cols = ThreadSafeSortedColumns.factory.create(BytesType.instance, false);
        for (int i = 0; i < 10; i++)
            cols.addColumn(column("vijay"+i, "tests", 1), HeapAllocator.instance);
        assertTrue("Expected: " + measureSize(cols) + " Found: " + cols.memorySize(), measureSize(cols) <= cols.memorySize());

        cols = AtomicSortedColumns.factory.create(BytesType.instance, false);
        for (int i = 0; i < 10; i++)
            cols.addColumn(column("vijay"+i, "tests", 1), HeapAllocator.instance);
        assertTrue(measureSize(cols) <= cols.memorySize());

        ColumnFamily cf = createCounterCF();
        System.out.println(meter.measureDeep(cf) - meter.measureDeep(Schema.instance.getCFMetaData(tableName, counterCFName)) - meter.measureDeep(DeletionInfo.LIVE));
        System.out.println(cf.memorySize());
    }

    public long measureSize(ISortedColumns cols)
    {
        return meter.measureDeep(cols) - meter.measureDeep(BytesType.instance) - meter.measureDeep(DeletionInfo.LIVE);
    }
}
