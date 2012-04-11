package org.apache.cassandra.utils;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;

public class OptimizedDataInput implements FileDataInput
{
    private FileDataInput input;
    
    public OptimizedDataInput(FileDataInput input)
    {
        this.input = input;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException
    {
        return input.readByte();
    }

    @Override
    public char readChar() throws IOException
    {
        return input.readChar();
    }

    @Override
    public double readDouble() throws IOException
    {
        return input.readDouble();
    }

    @Override
    public float readFloat() throws IOException
    {
        return input.readFloat();
    }

    @Override
    public void readFully(byte[] arg0) throws IOException
    {
        input.readFully(arg0);
    }

    @Override
    public void readFully(byte[] arg0, int arg1, int arg2) throws IOException
    {
        input.readFully(arg0, arg1, arg2);
    }
    
    @Override
    public String readLine() throws IOException
    {
        return input.readLine();
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


    @Override
    public String readUTF() throws IOException
    {
        return input.readUTF();
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        return input.readUnsignedByte();
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        return input.readUnsignedShort();
    }

    @Override
    public int skipBytes(int arg0) throws IOException
    {
        return input.skipBytes(arg0);
    }

    @Override
    public void close() throws IOException
    {
        input.close();
    }

    @Override
    public String getPath()
    {
        return input.getPath();
    }

    @Override
    public boolean isEOF() throws IOException
    {
        return input.isEOF();
    }

    @Override
    public long bytesRemaining() throws IOException
    {
        return input.bytesRemaining();
    }

    @Override
    public void seek(long pos) throws IOException
    {
        input.seek(pos);
    }

    @Override
    public FileMark mark()
    {
        return input.mark();
    }

    @Override
    public void reset(FileMark mark) throws IOException
    {
        input.reset(mark);
    }

    @Override
    public long bytesPastMark(FileMark mark)
    {
        return input.bytesPastMark(mark);
    }

    @Override
    public long getFilePointer()
    {
        return input.getFilePointer();
    }

    @Override
    public ByteBuffer readBytes(int length) throws IOException
    {
        return input.readBytes(length);
    }

    public long length() throws IOException
    {
        return input.length();
    }

    @Override
    public void reset() throws IOException
    {
        input.reset();
    }

    @Override
    public long bytesPastMark()
    {
        return input.bytesPastMark();
    }

    @Override
    public int read() throws IOException
    {
        return input.read();
    }

    @Override
    public int read(byte[] buffer) throws IOException
    {
        return input.read(buffer);
    }

    @Override
    public int read(byte[] buff, int offset, int length) throws IOException
    {
        return input.read(buff, offset, length);
    }

    @Override
    public void setLength(long newLength) throws IOException
    {
        input.setLength(newLength);
    }
}
