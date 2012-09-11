package org.apache.cassandra.io.util;

public interface OffHeapAllocator
{
    OffHeapMemory allocate(long size);
}
