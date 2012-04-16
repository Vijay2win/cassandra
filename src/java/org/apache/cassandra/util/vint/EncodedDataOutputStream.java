package org.apache.cassandra.util.vint;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.io.util.AbstractDataOutput;

/**
 * Borrows idea from
 * https://developers.google.com/protocol-buffers/docs/encoding#varints
 */
public class EncodedDataOutputStream extends AbstractDataOutput
{
    private OutputStream out;

    public EncodedDataOutputStream(OutputStream out)
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

    public void encodeVInt(long i) throws IOException
    {
        if (i >= -112 && i <= 127)
        {
            writeByte((byte) i);
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
        writeByte((byte) len);
        len = (len < -120) ? -(len + 120) : -(len + 112);
        for (int idx = len; idx != 0; idx--)
        {
            int shiftbits = (idx - 1) * 8;
            long mask = 0xFFL << shiftbits;
            writeByte((byte) ((i & mask) >> shiftbits));
        }
    }

    @Override
    public void writeInt(int v) throws IOException
    {
        encodeVInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException
    {
        encodeVInt(v);
    }

    @Override
    public void writeShort(int v) throws IOException
    {
        encodeVInt(v);
    }

    public static class EncodedDBConstant extends DBConstants
    {
        public int sizeofVInt(long i)
        {
            if (i >= -112 && i <= 127)
                return 1;

            int size = 0;
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
            size++;
            len = (len < -120) ? -(len + 120) : -(len + 112);
            size += len;
            return size;
        }

        public int sizeof(long i)
        {
            return sizeofVInt(i);
        }

        @Override
        public int sizeof(boolean i)
        {
            return DBConstants.BOOL_SIZE;
        }

        @Override
        public int sizeof(short i)
        {
            return sizeofVInt(i);
        }

        @Override
        public int sizeof(int i)
        {
            return sizeofVInt(i);
        }
    }
}
