package org.apache.cassandra.gms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;

public class EchoMessage
{
    public static IVersionedSerializer<EchoMessage> serializer = new EchoMessageSerializer();

    public static class EchoMessageSerializer implements IVersionedSerializer<EchoMessage>
    {
        private static final int ECHO_MESSAGE = 0xDECAF;

        public void serialize(EchoMessage t, DataOutput out, int version) throws IOException
        {
            out.writeInt(ECHO_MESSAGE);
        }

        public EchoMessage deserialize(DataInput in, int version) throws IOException
        {
            int by = in.readInt();
            if (by != ECHO_MESSAGE)
                throw new AssertionError();
            return new EchoMessage();
        }

        public long serializedSize(EchoMessage t, int version)
        {
            return TypeSizes.NATIVE.sizeof(ECHO_MESSAGE);
        }
    }
}
