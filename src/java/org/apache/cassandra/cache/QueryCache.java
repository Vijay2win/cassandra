package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.io.ISerializer;

import com.google.common.collect.Sets;
import com.googlecode.concurrentlinkedhashmap.Weigher;

public class QueryCache implements ICache<QueryCacheKey, IRowCacheEntry>
{
    private final SerializingCache<RowCacheKey, QueryCacheValue> cache;

    public QueryCache(long capacity, Weigher<RefCountedMemory> weigher, ISerializer<QueryCacheValue> serializer)
    {
        this.cache = new SerializingCache<>(capacity, weigher, serializer);
    }

    public static QueryCache create(long capacity)
    {
        return new QueryCache(capacity, new Weigher<RefCountedMemory>()
        {
            public int weightOf(RefCountedMemory value)
            {
                long size = value.size();
                assert size < Integer.MAX_VALUE : "Serialized size cannot be more than 2GB";
                return (int) size;
            }
        }, QueryCacheValue.serializer);
    }

    public void put(QueryCacheKey qf, IRowCacheEntry value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IRowCacheEntry get(QueryCacheKey qf)
    {
        QueryCacheValue value = cache.get(qf.getRowCacheKey());
        if (value != null && value.filters.contains(qf.filter))
            return value.data;
        return null;
    }

    @Override
    public long capacity()
    {
        return cache.capacity();
    }

    @Override
    public void setCapacity(long capacity)
    {
        cache.setCapacity(capacity);
    }

    @Override
    public boolean putIfAbsent(QueryCacheKey qf, IRowCacheEntry value)
    {
        RowCacheKey rk = qf.getRowCacheKey();
        if (cache.putIfAbsent(rk, new QueryCacheValue(Sets.newHashSet(qf.filter), value)))
            return true;
        // We are here since someone added the same or a different filter into cache
        QueryCacheValue existing = cache.get(rk);
        if (existing != null)
            return cache.replace(rk, existing, existing.cloneWith(qf.filter, value));
        return false;
    }

    /**
     * QueryCache only replace the old a sentinal value and is not a replace in an traditional sense.
     */
    @Override
    public boolean replace(QueryCacheKey qf, IRowCacheEntry old, IRowCacheEntry value)
    {
        RowCacheKey rk = qf.getRowCacheKey();
        QueryCacheValue existing = cache.get(rk);
        if (existing != null && existing.hasEqualValues(old))
            return cache.replace(rk, existing, existing.cloneWith(qf.filter, value));
        return false;
    }

    @Override
    public void remove(QueryCacheKey qf)
    {
        // removes the whole row for now.
        cache.remove(qf.getRowCacheKey());
    }

    @Override
    public int size()
    {
        return cache.size();
    }

    @Override
    public long weightedSize()
    {
        return cache.weightedSize();
    }

    @Override
    public void clear()
    {
        cache.clear();
    }

    @Override
    public Set<QueryCacheKey> keySet()
    {
        Set<QueryCacheKey> keys = new HashSet<>();
        for (RowCacheKey key : cache.keySet())
            for (IDiskAtomFilter filter : cache.get(key).filters)
                keys.add(new QueryCacheKey(key.cfId, ByteBuffer.wrap(key.key), filter));
        return keys;
    }

    @Override
    public Set<QueryCacheKey> hotKeySet(int n)
    {
        int i = 0;
        Set<QueryCacheKey> keys = new HashSet<>();
        for (RowCacheKey key : cache.hotKeySet(n))
        {
            for (IDiskAtomFilter filter : cache.get(key).filters)
            {
                if (i >= n)
                    break;
                keys.add(new QueryCacheKey(key.cfId, ByteBuffer.wrap(key.key), filter));
                i++;
            }
        }
        return keys;
    }

    @Override
    public boolean containsKey(QueryCacheKey qf)
    {
        RowCacheKey key = qf.getRowCacheKey();
        return cache.containsKey(key) && cache.get(key).filters.contains(qf.filter);
    }
}
