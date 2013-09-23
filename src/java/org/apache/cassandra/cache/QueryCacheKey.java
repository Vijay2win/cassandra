package org.apache.cassandra.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

public class QueryCacheKey implements CacheKey
{
    public static final QueryCacheKeySerializer serializer = new QueryCacheKeySerializer();
    public final UUID cfid;
    public final byte[] key;
    public final IDiskAtomFilter filter;

    public QueryCacheKey(UUID cfid, ByteBuffer key, IDiskAtomFilter filter)
    {
        this(cfid, ByteBufferUtil.getArray(key), filter);
    }

    public QueryCacheKey(UUID cfid, byte[] key, IDiskAtomFilter filter)
    {
        this.cfid = cfid;
        this.key = key;
        this.filter = filter;
    }

    @Override
    public long memorySize()
    {
        // QueryCacheKey is a temp object, but creates RowCacheKey.
        return getRowCacheKey().memorySize();
    }

    @Override
    public Pair<String, String> getPathInfo()
    {
        return getRowCacheKey().getPathInfo();
    }

    public RowCacheKey getRowCacheKey()
    {
        return new RowCacheKey(cfid, key);
    }

    public static class QueryCacheKeySerializer implements ISerializer<QueryCacheKey>
    {
        public void serialize(QueryCacheKey t, DataOutput out) throws IOException
        {
            out.writeLong(t.cfid.getLeastSignificantBits());
            out.writeLong(t.cfid.getMostSignificantBits());
            out.writeShort(t.key.length);
            out.write(t.key);
            IDiskAtomFilter.Serializer.instance.serialize(t.filter, out, MessagingService.current_version);
        }

        public QueryCacheKey deserialize(DataInput in) throws IOException
        {
            long lsb = in.readLong();
            long msb = in.readLong();
            UUID cfID = new UUID(msb, lsb);
            int ksize = in.readShort();
            byte[] key = new byte[ksize];
            in.readFully(key);
            IDiskAtomFilter filter = IDiskAtomFilter.Serializer.instance.deserialize(in, MessagingService.current_version);
            return new QueryCacheKey(cfID, key, filter);
        }

        public long serializedSize(QueryCacheKey t, TypeSizes type)
        {
            long size = 8 + 8; // UUID
            size += 2; // key header
            size += t.key.length;
            size += IDiskAtomFilter.Serializer.instance.serializedSize(t.filter, MessagingService.current_version);
            return size;
        }
    }
}
