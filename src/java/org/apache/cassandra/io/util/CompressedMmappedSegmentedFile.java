package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import org.apache.cassandra.io.compress.CompressedMappedFileDataInput;
import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressionMetadata;

import com.google.common.collect.Lists;

public class CompressedMmappedSegmentedFile extends MmappedSegmentedFile
{
    private CompressionMetadata metadata;

    public CompressedMmappedSegmentedFile(String path, long length, Segment[] segments, CompressionMetadata metadata)
    {
        super(path, length, segments);
        this.metadata = metadata;
    }

    @Override
    public FileDataInput getSegment(long position)
    {
        try
        {
            if (metadata.compressor().handlesDirectByteBuffer())
                return new CompressedMappedFileDataInput(path, position, metadata, segments);

            RandomAccessReader file = CompressedRandomAccessReader.open(path, metadata);
            file.seek(position);
            return file;
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    static class Builder extends SegmentedFile.Builder
    {
        private long currentStart = 0;

        @Override
        public SegmentedFile complete(String path)
        {
            long length = new File(path).length();
            return new CompressedMmappedSegmentedFile(path, length, createSegments(path), CompressionMetadata.create(path));
        }

        public void addPotentialBoundary(long boundry)
        {
            // do nothing here.
        }

        private Segment[] createSegments(String path)
        {
            RandomAccessFile raf = null;
            try
            {
                CompressionMetadata metadata = CompressionMetadata.create(path);
                // use a List because we cannot predict the segment sizes.
                List<Segment> segmentList = Lists.newArrayList();
                raf = new RandomAccessFile(path, "r");
                while (currentStart != raf.length())
                {
                    long mayEnd = currentStart + MAX_SEGMENT_SIZE;
                    long end = (mayEnd >= raf.length()) ? raf.length() : metadata.findStart(mayEnd);
                    MappedByteBuffer segment = raf.getChannel().map(FileChannel.MapMode.READ_ONLY, currentStart, end - currentStart);
                    segmentList.add(new Segment(currentStart, segment));
                    currentStart = end;
                }
                return segmentList.toArray(new Segment[segmentList.size()]);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            finally
            {
                FileUtils.closeQuietly(raf);
            }
        }
    }
}
