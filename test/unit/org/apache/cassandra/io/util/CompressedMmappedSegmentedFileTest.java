package org.apache.cassandra.io.util;

import static org.apache.cassandra.io.sstable.SSTableUtils.tempSSTableFile;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.io.sstable.SSTableMetadata.Collector;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.sstable.SSTableWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import com.google.common.base.Charsets;

public class CompressedMmappedSegmentedFileTest extends SchemaLoader
{
    private long length = ((long) Integer.MAX_VALUE) * 20L;

    /**
     * Test Compressed end to end SST compression using
     * CompressedMappedSegmentFile
     */
    @Test
    public void testSSTableReader() throws IOException
    {
        File tempSS = tempSSTableFile("Keyspace1", "Standard5");
        ColumnFamilyStore cfs = null;
        for (ColumnFamilyStore colfs : ColumnFamilyStore.all())
        {
            if ("Standard5".equals(colfs.columnFamily))
                cfs = colfs;
        }
        SSTableWriter writer = new SSTableWriter(tempSS.getPath(), 2, cfs.metadata, cfs.partitioner, new Collector());
        // Add rows
        int rowCount = 9;
        for (int i = 0; i < rowCount; i++)
        {
            ColumnFamily cfamily = ColumnFamily.create("Keyspace1", "Standard5");
            cfamily.addColumn(new QueryPath("Standard5", null, ByteBufferUtil.bytes("name")), ByteBufferUtil.bytes("val" + i), System.currentTimeMillis());
            writer.append(Util.dk(i + "row"), cfamily);
            cfamily.clear();
        }
        SSTableReader reader = writer.closeAndOpenReader();
        // reader = SSTableReader.open(Descriptor.fromFilename(tempSS.getPath()));
        int i = 0;
        for (; i < rowCount; i++)
        {
            QueryFilter qf = QueryFilter.getNamesFilter(Util.dk(i + "row"), new QueryPath("Standard5", null, null), ByteBufferUtil.bytes("name"));
            OnDiskAtomIterator cf = qf.getSSTableColumnIterator(reader);
            Column atom = (Column) cf.next();
            assertEquals(new String(atom.value().array(), Charsets.UTF_8), "val" + i);
        }
        assertEquals(rowCount, i);
    }
}
