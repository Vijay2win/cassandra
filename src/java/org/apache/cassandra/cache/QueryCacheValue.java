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
import org.apache.cassandra.utils.HeapAllocator;

public class QueryCacheValue implements IMeasurableMemory
{
    public static final ISerializer<QueryCacheValue> serializer = new QueryCacheValueSerializer();
    public final Set<IDiskAtomFilter> filters;
    public final ColumnFamily data;

    public QueryCacheValue(Set<IDiskAtomFilter> filters, ColumnFamily data)
    {
        this.filters = filters;
        this.data = data;
    }

    public QueryCacheValue cloneWith(IDiskAtomFilter filter, ColumnFamily value)
    {
        Set<IDiskAtomFilter> f = new HashSet<>(filters);
        f.add(filter);

        ColumnFamily d = data.cloneMe();
        d.addAll(value, HeapAllocator.instance);

        return new QueryCacheValue(f, d);
    }

    public static class QueryCacheValueSerializer implements ISerializer<QueryCacheValue> {
        /**
         * First stores the query's and then the data.
         * [[QueryFilter, ...] [Data, ...]]
         */
        public void serialize(QueryCacheValue t, DataOutput out) throws IOException
        {
            out.writeInt(t.filters.size());
            for (IDiskAtomFilter filter: t.filters)
                IDiskAtomFilter.Serializer.instance.serialize(filter, out, MessagingService.current_version);
            ColumnFamily.serializer.serialize(t.data, out, MessagingService.current_version);
        }

        public QueryCacheValue deserialize(DataInput in) throws IOException
        {
            int size = in.readInt();
            Set<IDiskAtomFilter> filters = new HashSet<>(size);
            for (int i = 0; i < size; i++)
                filters.add(IDiskAtomFilter.Serializer.instance.deserialize(in, MessagingService.current_version));
            ColumnFamily data = ColumnFamily.serializer.deserialize(in, MessagingService.current_version);
            return new QueryCacheValue(filters, data);
        }

        public long serializedSize(QueryCacheValue t, TypeSizes type)
        {
            int size = type.sizeof(t.filters.size());
            for (IDiskAtomFilter filter: t.filters)
                size += IDiskAtomFilter.Serializer.instance.serializedSize(filter, type, MessagingService.current_version);
            size += ColumnFamily.serializer.serializedSize(t.data, type, MessagingService.current_version);
            return size;
        }
    }

    @Override
    public long memorySize()
    {
        return 0;
    }
}
