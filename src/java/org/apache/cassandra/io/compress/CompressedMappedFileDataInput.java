package org.apache.cassandra.io.compress;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.Checksum;

import org.apache.cassandra.io.compress.CompressionMetadata.Chunk;
import org.apache.cassandra.io.compress.ICompressor.DirectBufferThreadLocal;
import org.apache.cassandra.io.util.AbstractDataInput;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.SegmentedFile.Segment;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.PureJavaCrc32;

/**
 * Given the mapped buffer this class will allow iteration of bytes.
 */
public class CompressedMappedFileDataInput extends AbstractDataInput implements FileDataInput
{
    private static final DirectBufferThreadLocal cachedBuffer = new DirectBufferThreadLocal();
    private final Checksum checksum = new PureJavaCrc32();
    private final ByteBuffer uncompressedBuffer;
    private final Segment[] segments;
    private final CompressionMetadata metadata;
    private final String filename;

    private long position;
    private int uncompressedBuffCursor, uncompressedBuffLimit;
    private byte[] checksumBuffer;

    public CompressedMappedFileDataInput(String filename, long seekto, CompressionMetadata metadata, Segment... segments) throws IOException
    {
        this.segments = segments;
        this.filename = filename;
        this.metadata = metadata;
        uncompressedBuffer = cachedBuffer.get(metadata.chunkLength());
        checksumBuffer = new byte[metadata.chunkLength()];
        seek(seekto);
    }

    /**
     * This method is a helper method which has to be used to skip within a
     * segment.
     */
    public void seek(long seekto) throws IOException
    {
        long maxPosition = position + (uncompressedBuffLimit - uncompressedBuffCursor);
        if (seekto < position || seekto > maxPosition)
        {
            // seek to the target chunk.
            Chunk seekChunk = metadata.chunkFor(seekto);
            position = seekChunk.startPosition;
            reBuffer(seekChunk);
        }
        // now skip bytes within the uncompressed block
        skipBytes(seekto - position);
    }

    @Override
    public String getPath()
    {
        return filename;
    }

    @Override
    public int read() throws IOException
    {
        if (uncompressedBuffCursor >= uncompressedBuffLimit)
            reBuffer(metadata.chunkFor(position)); // throws EOF exception.
        // mark and increment the pointer (uncompressedBuffCursor)
        uncompressedBuffer.position(uncompressedBuffCursor++);
        int returns = uncompressedBuffer.get() & 0xFF;
        position++;
        return returns;
    }

    @Override
    public ByteBuffer readBytes(int length) throws IOException
    {
        if (length == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        byte[] bytes = new byte[length];
        int read = read(bytes, 0, length);
        if (read == -1)
            throw new EOFException();
        return (ByteBuffer) ByteBuffer.wrap(bytes).position(0).limit(read);
    }

    @Override
    public int skipBytes(int bytesToSkip) throws IOException
    {
        return skipBytes((long) bytesToSkip);
    }

    private int skipBytes(long bytesToSkip) throws IOException
    {
        assert bytesToSkip >= 0 : "skipping negative bytes is illegal: " + bytesToSkip;
        if (bytesToSkip == 0)
            return 0;
        int writtenBytes = 0;
        while (writtenBytes < bytesToSkip)
        {
            if (uncompressedBuffCursor >= uncompressedBuffLimit)
                reBuffer(metadata.chunkFor(position)); // throws EOF exception.
            long bytesToWrite = Math.min(uncompressedBuffLimit - uncompressedBuffCursor, bytesToSkip - writtenBytes);
            writtenBytes += bytesToWrite;
            uncompressedBuffCursor += bytesToWrite;
            position += bytesToWrite;
        }
        return writtenBytes;
    }

    /**
     * Marking it as protected for testing.
     */
    @Override
    public int read(byte[] array, int offset, int length) throws IOException
    {
        int writtenBytes = 0, start = offset;
        while (writtenBytes < length)
        {
            if (uncompressedBuffCursor >= uncompressedBuffLimit)
                reBuffer(metadata.chunkFor(position)); // throws EOF exception.
            int bytesToWrite = Math.min(uncompressedBuffLimit - uncompressedBuffCursor, length - writtenBytes);
            uncompressedBuffer.position(uncompressedBuffCursor);
            uncompressedBuffer.get(array, start, bytesToWrite);
            start += bytesToWrite;
            writtenBytes += bytesToWrite;
            uncompressedBuffCursor += bytesToWrite;
            position += bytesToWrite;
        }
        return writtenBytes;
    }

    private void reBuffer(Chunk current) throws IOException
    {
        // initialize
        uncompressedBuffCursor = 0;
        uncompressedBuffer.rewind();

        // find the DirectBB/segment of the file.
        Segment segment = findSegment(current.offset);
        ByteBuffer bytes = segment.right.duplicate();

        int chunkStart = (int) (current.offset - segment.left); 
        int chunkEnd = chunkStart + current.length;
        bytes.position(chunkStart).limit(chunkEnd);
        uncompressedBuffLimit = metadata.compressor().uncompress(bytes, uncompressedBuffer);

        // checksum.
        if (FBUtilities.threadLocalRandom().nextDouble() < metadata.parameters.crcChance)
        {
            uncompressedBuffer.get(checksumBuffer, 0, uncompressedBuffLimit);
            checksum.update(checksumBuffer, 0, uncompressedBuffLimit);
            int storedCRC = ((ByteBuffer) bytes.position(chunkEnd).limit(chunkEnd + 4)).getInt();
            if (storedCRC != (int) checksum.getValue())
                throw new CorruptedBlockException(getPath(), current);
            // reset checksum object back to the original (blank) state
            checksum.reset();
        }
    }

    private Segment findSegment(long position)
    {
        Segment seg = new Segment(position, null);
        int idx = Arrays.binarySearch(segments, seg);
        assert idx != -1 : "Bad position " + position + " in segments " + Arrays.toString(segments);
        if (idx < 0)
            idx = -(idx + 2);
        return segments[idx];
    }

    @Override
    public boolean isEOF() throws IOException
    {
        return position >= metadata.dataLength;
    }

    @Override
    protected int getPosition()
    {
        // this method is called only while throwing exceptions.
        return (int) position;
    }

    protected void seekInternal(int pos)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    @Override
    public void reset(FileMark mark) throws IOException
    {
        assert mark instanceof CompressedMappedFileDataInputMark;
        seek(((CompressedMappedFileDataInputMark) mark).position);
    }

    @Override
    public FileMark mark()
    {
        return new CompressedMappedFileDataInputMark(position);
    }

    @Override
    public long bytesPastMark(FileMark mark)
    {
        assert mark instanceof CompressedMappedFileDataInputMark;
        return ((CompressedMappedFileDataInputMark) mark).position - position;
    }

    @Override
    public long bytesRemaining() throws IOException
    {
        return metadata.dataLength - position;
    }

    public static class CompressedMappedFileDataInputMark implements FileMark
    {
        public long position;

        public CompressedMappedFileDataInputMark(long position)
        {
            this.position = position;
        }
    }

    public long getFilePointer()
    {
        return position;
    }
}
