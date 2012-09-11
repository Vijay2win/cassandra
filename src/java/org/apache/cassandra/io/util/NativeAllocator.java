package org.apache.cassandra.io.util;

public class NativeAllocator implements OffHeapAllocator
{
    public OffHeapMemory allocate(long size)
    {
        if (size < 0)
            throw new IllegalArgumentException();
        return new Memory(size);
    }
}
