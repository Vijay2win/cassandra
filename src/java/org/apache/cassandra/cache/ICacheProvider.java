package org.apache.cassandra.cache;

import java.io.*;
import java.util.UUID;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.vint.EncodedDataInputStream;
import org.apache.cassandra.utils.vint.EncodedDataOutputStream;

import com.lruc.api.ICacheSerializer;

public interface ICacheProvider
{
    ICache<RowCacheKey, IRowCacheEntry> create(long capacity);

    // Package protected for tests
    static class RowKeySerializer implements ICacheSerializer<RowCacheKey>
    {
        public void serialize(OutputStream output, RowCacheKey entry) throws IOException
        {
            try (EncodedDataOutputStream out = new EncodedDataOutputStream(output))
            {
                assert entry != null; // unlike CFS we don't support nulls, since there is no need for that in the cache
                out.writeLong(entry.cfId.getMostSignificantBits());
                out.writeLong(entry.cfId.getLeastSignificantBits());
                out.writeInt(entry.key.length);
                out.write(entry.key);
            }
        }

        public RowCacheKey deserialize(InputStream input, int size) throws IOException
        {
            try (EncodedDataInputStream in = new EncodedDataInputStream(input))
            {
                long msb = in.readLong();
                long lsb = in.readLong();
                UUID cfId = new UUID(msb, lsb);
                byte[] bytes = new byte[in.readInt()];
                in.readFully(bytes);
                return new RowCacheKey(cfId, bytes);
            }
        }

        public int serializedSize(RowCacheKey entry)
        {
            TypeSizes typeSizes = TypeSizes.VINT;
            int size = typeSizes.sizeof(entry.cfId.getMostSignificantBits());
            size += typeSizes.sizeof(entry.cfId.getLeastSignificantBits());
            size += typeSizes.sizeof(entry.key.length);
            size += entry.key.length;
            return size;
        }
    }

    // Package protected for tests
    static class RowValueSerializer implements ICacheSerializer<IRowCacheEntry>, ISerializer<IRowCacheEntry>
    {
        public void serialize(OutputStream output, IRowCacheEntry entry) throws IOException
        {
            try (EncodedDataOutputStream out = new EncodedDataOutputStream(output))
            {
                serialize(entry, out);
            }
        }

        public void serialize(IRowCacheEntry entry, DataOutputPlus out) throws IOException
        {
            assert entry != null; // unlike CFS we don't support nulls, since there is no need for that in the cache
            boolean isSentinel = entry instanceof RowCacheSentinel;
            out.writeBoolean(isSentinel);
            if (isSentinel)
                out.writeLong(((RowCacheSentinel) entry).sentinelId);
            else
                ColumnFamily.serializer.serialize((ColumnFamily) entry, out, MessagingService.current_version);            
        }

        public IRowCacheEntry deserialize(InputStream input, int size) throws IOException
        {
            try (EncodedDataInputStream in = new EncodedDataInputStream(input))
            {
                return deserialize(in);
            }
        }

        public IRowCacheEntry deserialize(DataInput in) throws IOException
        {
            boolean isSentinel = in.readBoolean();
            if (isSentinel)
                return new RowCacheSentinel(in.readLong());
            return ColumnFamily.serializer.deserialize(in, MessagingService.current_version);
        }

        public int serializedSize(IRowCacheEntry entry)
        {
            TypeSizes typeSizes = TypeSizes.VINT;
            return (int) serializedSize(entry, typeSizes);
        }

        public long serializedSize(IRowCacheEntry entry, TypeSizes typeSizes)
        {
            long size = typeSizes.sizeof(true);
            if (entry instanceof RowCacheSentinel)
                size += typeSizes.sizeof(((RowCacheSentinel) entry).sentinelId);
            else
                size += ColumnFamily.serializer.serializedSize((ColumnFamily) entry, typeSizes, MessagingService.current_version);
            return size;
        }
    }
}