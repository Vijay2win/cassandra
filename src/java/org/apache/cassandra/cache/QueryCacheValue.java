package org.apache.cassandra.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HeapAllocator;

public class QueryCacheValue
{
    public static final ISerializer<QueryCacheValue> serializer = new QueryCacheValueSerializer();
    public final Set<IDiskAtomFilter> filters;
    public final ColumnFamily data;
    public final RowCacheSentinel sentinel;

    public QueryCacheValue(Set<IDiskAtomFilter> filters, IRowCacheEntry data)
    {
        this(filters, data.hasEntry() ? (ColumnFamily) data : null, 
                      data.hasEntry() ? null : (RowCacheSentinel) data);
    }

    public QueryCacheValue(Set<IDiskAtomFilter> filters, ColumnFamily data, RowCacheSentinel sentinel)
    {
        this.filters = filters;
        this.data = data;
        this.sentinel = sentinel;
    }

    /**
     * Clone with the new filter and value, the value also can be an sentinel.
     */
    public QueryCacheValue cloneWith(IDiskAtomFilter filter, IRowCacheEntry value)
    {
        if (!value.hasEntry())
            return new QueryCacheValue(filters, data, (RowCacheSentinel) value);

        Set<IDiskAtomFilter> f = new HashSet<>(filters);
        f.add(filter);
        ColumnFamily temp = ((ColumnFamily) value).cloneMe();
        if (data != null)
            temp.addAll(data, HeapAllocator.instance);

        return new QueryCacheValue(f, temp);
    }

    /**
     * Verify if the value is contained in the filter, if sentinel then just compare it.
     */
    public boolean hasEqualValues(IRowCacheEntry old)
    {
        if (!old.hasEntry())
            return sentinel != null && sentinel.equals(old);
        return data.equals((ColumnFamily) old);
    }

    @Override
    public boolean equals(Object obj)
    {
        QueryCacheValue other = (QueryCacheValue) obj;
        return FBUtilities.equal(other.filters, filters) && FBUtilities.equal(other.data, data) && FBUtilities.equal(other.sentinel, sentinel);
    }

    public static class QueryCacheValueSerializer implements ISerializer<QueryCacheValue>
    {
        /**
         * First stores the query's and then the data. [[QueryFilter, ...] [sentinel] [Data, ...]]
         */
        public void serialize(QueryCacheValue t, DataOutput out) throws IOException
        {
            out.writeInt(t.filters.size());
            for (IDiskAtomFilter filter : t.filters)
                IDiskAtomFilter.Serializer.instance.serialize(filter, out, MessagingService.current_version);
            boolean hasSentinal = (t.sentinel != null);
            out.writeBoolean(hasSentinal);
            if (hasSentinal)
                out.writeLong(t.sentinel.sentinelId);
            ColumnFamily.serializer.serialize(t.data, out, MessagingService.current_version);
        }

        public QueryCacheValue deserialize(DataInput in) throws IOException
        {
            int size = in.readInt();
            Set<IDiskAtomFilter> filters = new HashSet<>(size);
            for (int i = 0; i < size; i++)
                filters.add(IDiskAtomFilter.Serializer.instance.deserialize(in, MessagingService.current_version));
            boolean hasSentinal = in.readBoolean();
            RowCacheSentinel sentinal = null;
            if (hasSentinal)
                sentinal = new RowCacheSentinel(in.readLong());
            ColumnFamily data = ColumnFamily.serializer.deserialize(in, MessagingService.current_version);
            return new QueryCacheValue(filters, data, sentinal);
        }

        public long serializedSize(QueryCacheValue t, TypeSizes type)
        {
            int size = type.sizeof(t.filters.size());
            for (IDiskAtomFilter filter : t.filters)
                size += IDiskAtomFilter.Serializer.instance.serializedSize(filter, type, MessagingService.current_version);
            boolean hasSentinal = (t.sentinel != null);
            size += type.sizeof(hasSentinal);
            if (hasSentinal)
                size += type.sizeof(t.sentinel.sentinelId);
            size += ColumnFamily.serializer.serializedSize(t.data, type, MessagingService.current_version);
            return size;
        }
    }
}
