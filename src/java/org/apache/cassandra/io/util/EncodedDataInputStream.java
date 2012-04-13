package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.IOException;

/**
 * Borrows idea from
 * https://developers.google.com/protocol-buffers/docs/encoding#varints
 * 
 * Should be used with EncodedDataOutputStream
 */
public class EncodedDataInputStream extends AbstractDataInput
{
    private DataInput input;

    public EncodedDataInputStream(DataInput input)
    {
        this.input = input;
    }

    @Override
    public int readInt() throws IOException
    {
        return (int) readVLong();
    }

    @Override
    public long readLong() throws IOException
    {
        return readVLong();
    }

    @Override
    public short readShort() throws IOException
    {
        return (short) readVLong();
    }

    public long readVLong() throws IOException
    {
        byte firstByte = input.readByte();
        int len = decodeVIntSize(firstByte);
        if (len == 1)
            return firstByte;
        long i = 0;
        for (int idx = 0; idx < len - 1; idx++)
        {
            byte b = input.readByte();
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    }

    public boolean isNegativeVInt(byte value)
    {
        return value < -120 || (value >= -112 && value < 0);
    }

    public int decodeVIntSize(byte value)
    {
        if (value >= -112)
        {
            return 1;
        }
        else if (value < -120)
        {
            return -119 - value;
        }
        return -111 - value;
    }

    public int skipBytes(int n) throws IOException
    {
        return input.skipBytes(n);
    }

    public int read() throws IOException
    {
        return input.readByte() & 0xFF;
    }

    protected void seekInternal(int position)
    {
        throw new UnsupportedOperationException();
    }

    protected int getPosition()
    {
        throw new UnsupportedOperationException();
    }
}
