/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cache;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.lruc.api.ICacheSerializer;
import com.lruc.api.LRUCache;

/**
 * Serializes cache values off-heap.
 */
public class OffHeapCache<K, V> implements ICache<K, V>
{
    private static final Logger logger = LoggerFactory.getLogger(OffHeapCache.class);
    private static final int DEFAULT_CONCURENCY_LEVEL = 64;

    private final LRUCache<K, V> map;

    private OffHeapCache(long capacity, ICacheSerializer<K> keySerializer, ICacheSerializer<V> valueSerializer)
    {
        this.map = LRUCache.<K, V>builder().keySerializer(keySerializer)
                                           .valueSerializer(valueSerializer)
                                           .maxSize(capacity)
                                           .hashpowerInit(DEFAULT_CONCURENCY_LEVEL)
                                           .build();
        logger.info("Intialized OffHeapCache with max memory size {}", capacity);
    }

    public static <K, V> OffHeapCache<K, V> create(long capacity, ICacheSerializer<K> keySerializer, ICacheSerializer<V> valueSerializer)
    {
        return new OffHeapCache<>(capacity, keySerializer, valueSerializer);
    }

    public long capacity()
    {
        return map.capacity();
    }

    public void setCapacity(long capacity)
    {
        map.setCapacity(capacity);
    }

    public boolean isEmpty()
    {
        return map.size() == 0;
    }

    public long size()
    {
        return map.size();
    }

    public long weightedSize()
    {
        return map.memUsed();
    }

    public void clear()
    {
        map.invalidateAll();
    }

    public V get(K key)
    {
        return map.getIfPresent(key);
    }

    public void put(K key, V value)
    {
        map.put(key, value);
    }

    public boolean putIfAbsent(K key, V value)
    {
        return map.putIfAbsent(key, value);
    }

    public boolean replace(K key, V oldToReplace, V value)
    {
        return map.replace(key, oldToReplace, value);
    }

    public void remove(K key)
    {
        map.invalidate(key);
    }

    public Set<K> keySet()
    {
        long size = map.size();
        return Sets.newHashSet(map.hotN(size));
    }

    public Set<K> hotKeySet(int n)
    {
        return Sets.newHashSet(map.hotN(n));
    }

    public boolean containsKey(K key)
    {
        return map.containsKey(key);
    }
}
