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

import com.googlecode.concurrentlinkedhashmap.Weigher;
import com.googlecode.concurrentlinkedhashmap.Weighers;

public class ConcurrentLinkedHashCacheProvider implements IRowCacheProvider
{
    public ICache<RowCacheKey, IRowCacheEntry> create(long capacity, boolean useMemoryWeigher)
    {
        return ConcurrentLinkedHashCache.create(capacity, useMemoryWeigher
                                                            ? createMemoryWeigher()
                                                            : Weighers.<IRowCacheEntry>singleton());
    }

    private static Weigher<IRowCacheEntry> createMemoryWeigher()
    {
        return new Weigher<IRowCacheEntry>()
        {
            public int weightOf(IRowCacheEntry value)
            {
                int size = value.dataSize();
                assert size > 0;
                return size;
            }
        };
    }
}
