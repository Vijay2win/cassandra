package org.apache.cassandra.io.compress;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.cassandra.io.sstable.SSTableMetadata;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SegmentedFile.Segment;
import org.apache.cassandra.utils.Pair;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class CompressedMappedFileDataInputTest
{
    private String content = "Vijay is trying to test 1234 for a mmaped IO with compressed blocks on reads. " +
    		                 "if you are reading this you have a lot of time... and now this works! count: ";
    private int orbSize = 10000;

    @Test
    public void testFileDataInput() throws IOException
    {
        Pair<File, File> pair = writeCompressedData(orbSize);
        CompressionMetadata meta = new CompressionMetadata(pair.right.getPath(), pair.left.length());
        CompressedMappedFileDataInput r = new CompressedMappedFileDataInput(pair.left.getName(), 0, meta, 
                                                                            new Segment(0, createBuffer(pair.left.getPath(), meta)));
        int i = tryGet(r, 0, orbSize, true);
        assertEquals(orbSize, i);
    }

    @Test
    public void testFileDataInputReadByte() throws IOException
    {
        Pair<File, File> pair = writeCompressedData(orbSize);
        CompressionMetadata meta = new CompressionMetadata(pair.right.getPath(), pair.left.length());
        CompressedMappedFileDataInput r = new CompressedMappedFileDataInput(pair.left.getName(), 0, meta, 
                                                                            new Segment(0, createBuffer(pair.left.getPath(), meta)));
        int i = tryGetByte(r, 0, orbSize, true);
        assertEquals(orbSize, i);
    }

    @Test
    public void testSkip() throws IOException
    {
        Pair<File, File> pair = writeCompressedData(orbSize);
        CompressionMetadata meta = new CompressionMetadata(pair.right.getPath(), pair.left.length());
        CompressedMappedFileDataInput r = new CompressedMappedFileDataInput(pair.left.getName(), 0, meta,
                                                                            new Segment(0, createBuffer(pair.left.getPath(), meta)));
        for (int i = 0; i < 256; i++)
        {
            String temp = content + i;
            r.skip(temp.getBytes(Charset.forName("UTF8")).length);
        }
        int i = tryGet(r, 256, orbSize, false);
        assertEquals(orbSize, i);
    }

    private int tryGet(CompressedMappedFileDataInput r, int i, int orbSize, boolean doAssert) throws IOException
    {
        try
        {
            for (; i < orbSize; i++)
            {
                String temp = content + i;
                ByteBuffer buff = r.readBytes(temp.getBytes(Charset.forName("UTF8")).length);
                if (doAssert)
                    assertEquals(temp, new String(buff.array(), Charset.forName("UTF8")));
            }
        }
        catch (EOFException ex)
        {
            // ignore and move on.
        }

        return i;
    }

    private int tryGetByte(CompressedMappedFileDataInput r, int i, int orbSize, boolean doAssert) throws IOException
    {
        try
        {
            for (; i < orbSize; i++)
            {
                String temp = content + i;
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                for (int j = 0; j < temp.getBytes(Charset.forName("UTF8")).length; j++)
                {
                    os.write(r.read());
                }
                if (doAssert)
                    assertEquals(temp, new String(os.toByteArray(), Charset.forName("UTF8")));
            }
        }
        catch (EOFException ex)
        {
            // ignore and move on.
        }

        return i;
    }

    private Pair<File, File> writeCompressedData(int orbSize) throws IOException
    {
        new File("testCompressedSST").delete();
        File file = new File("testCompressedSST");
        file.deleteOnExit();

        new File(file.getPath() + ".meta").delete();
        File metadata = new File(file.getPath() + ".meta");
        metadata.deleteOnExit();

        SSTableMetadata.Collector sstableMetadataCollector = SSTableMetadata.createCollector().replayPosition(null);
        CompressedSequentialWriter writer = new CompressedSequentialWriter(file, metadata.getPath(), false, new CompressionParameters(SnappyCompressor.instance), sstableMetadataCollector);
        for (int i = 0; i < orbSize; i++)
        {
            String temp = content + i;
            writer.write(temp.getBytes(Charset.forName("UTF8")));
        }
        writer.close();
        return new Pair<File, File>(file, metadata);
    }

    public MappedByteBuffer createBuffer(String path, CompressionMetadata meta) throws IOException
    {
        RandomAccessReader reader = CompressedRandomAccessReader.open(path, meta, false);
        FileChannel channel = reader.getChannel();
        return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    }
}
