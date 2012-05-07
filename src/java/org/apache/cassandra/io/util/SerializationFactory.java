package org.apache.cassandra.io.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.db.DBTypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.vint.EncodedDataInputStream;
import org.apache.cassandra.utils.vint.EncodedDataOutputStream;

public abstract class SerializationFactory
{
    private int version;

    public SerializationFactory(int version)
    {
        this.version = version;
    }

    public static SerializationFactory get(int version)
    {
        return (version >= MessagingService.VERSION_12) ? new EncodedSerializationFactory(version)
                                                        : new NativeSerializationFactory(version);
    }

    public <T> byte[] serialize(T object, IVersionedSerializer<T> serializer) throws IOException
    {
        int size = (int) serializer.serializedSize(object, getDBTypeSizes(), version);
        DataOutputBuffer out = new DataOutputBuffer(size);
        serializer.serialize(object, getDataOutput(out), version);
        assert out.size() == size && out.getData().length == size : String.format("Final buffer length %s to accomodate data size of %s (predicted %s) for %s",
                                                                                    out.getData().length, out.getLength(), size, object);
        return out.getData();
    }

    public <T> byte[] serializeWithoutSize(T object, IVersionedSerializer<T> serializer) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        serializer.serialize(object, getDataOutput(out), version);
        return out.getData();
    }

    public abstract DataOutput getDataOutput(DataOutputBuffer out);

    public <T> T deserialize(byte[] data, IVersionedSerializer<T> serializer) throws IOException
    {
        DataInput input = getDataInput(data);
        return serializer.deserialize(input, version);
    }

    public DataInput getDataInput(byte[] data) throws IOException
    {
        FastByteArrayInputStream fbin = new FastByteArrayInputStream(data);
        return getDataInput(fbin);
    }

    public abstract DataInput getDataInput(FastByteArrayInputStream fbin);

    public abstract DBTypeSizes getDBTypeSizes();

    private static class EncodedSerializationFactory extends SerializationFactory
    {

        public EncodedSerializationFactory(int version)
        {
            super(version);
        }

        public DataOutput getDataOutput(DataOutputBuffer out)
        {
            return new EncodedDataOutputStream(out);
        }

        public DataInput getDataInput(FastByteArrayInputStream fbin)
        {
            DataInput input = new DataInputStream(fbin);
            return new EncodedDataInputStream(input);
        }

        public DBTypeSizes getDBTypeSizes()
        {
            return DBTypeSizes.VINT;
        }
    }

    private static class NativeSerializationFactory extends SerializationFactory
    {
        public NativeSerializationFactory(int version)
        {
            super(version);
        }

        public DataOutput getDataOutput(DataOutputBuffer out)
        {
            return out;
        }

        public DataInput getDataInput(FastByteArrayInputStream fbin)
        {
            return new DataInputStream(fbin);
        }

        public DBTypeSizes getDBTypeSizes()
        {
            return DBTypeSizes.NATIVE;
        }
    }
}
