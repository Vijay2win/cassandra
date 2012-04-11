package org.apache.cassandra.utils;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class OptimizedDataOutput implements DataOutput
{
    private DataOutputStream out;

    public OptimizedDataOutput(DataOutputStream out)
    {
        this.out = out;
    }

    @Override
    public void write(int b) throws IOException
    {
        out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        out.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException
    {
        out.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException
    {
        out.writeByte(v);
    }

    @Override
    public void writeBytes(String s) throws IOException
    {
        out.writeBytes(s);
    }

    @Override
    public void writeChar(int v) throws IOException
    {
        out.writeChar(v);
    }

    @Override
    public void writeChars(String s) throws IOException
    {
        out.writeChars(s);
    }

    @Override
    public void writeDouble(double v) throws IOException
    {
        out.writeDouble(v);
    }

    @Override
    public void writeFloat(float v) throws IOException
    {
        out.writeFloat(v);
    }

    public void writeVLong(long i) throws IOException
    {
        if (i >= -112 && i <= 127)
        {
            out.writeByte((byte) i);
            return;
        }
        int len = -112;
        if (i < 0)
        {
            i ^= -1L; // take one's complement'
            len = -120;
        }
        long tmp = i;
        while (tmp != 0)
        {
            tmp = tmp >> 8;
            len--;
        }
        out.writeByte((byte) len);
        len = (len < -120) ? -(len + 120) : -(len + 112);
        for (int idx = len; idx != 0; idx--)
        {
            int shiftbits = (idx - 1) * 8;
            long mask = 0xFFL << shiftbits;
            out.writeByte((byte) ((i & mask) >> shiftbits));
        }
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        writeVLong(v);
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        writeVLong(v);
    }

    @Override
    public void writeShort(int v) throws IOException
    {
        writeVLong(v);
    }

    @Override
    public void writeUTF(String s) throws IOException
    {
        out.writeUTF(s);
    }
    
    
}
