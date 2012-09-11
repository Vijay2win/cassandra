package org.apache.cassandra.io.util;


import java.util.concurrent.atomic.AtomicInteger;

import com.sun.jna.Library;
import com.sun.jna.Memory;
import com.sun.jna.Native;

public class JEMallocMemory extends Memory implements OffHeapMemory
{
    public interface JEMLibrary extends Library
    {
        long malloc(long size);

        void free(long pointer);
    }
    private static final JEMLibrary INSTANCE = (JEMLibrary) Native.loadLibrary("jemalloc", JEMLibrary.class);

    private final AtomicInteger references = new AtomicInteger(1);

    public JEMallocMemory(long size)
    {
        this.size = size;
        if (size <= 0)
            throw new IllegalArgumentException("Allocation size must be >= 0");
        peer = INSTANCE.malloc(size);
        if (peer == 0)
            throw new OutOfMemoryError("Cannot allocate " + size + " bytes");
    }

    private void free()
    {
        assert peer != 0;
        INSTANCE.free(peer);
        peer = 0;
    }

    public boolean reference()
    {
        while (true)
        {
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
                return true;
        }
    }

    public void unreference()
    {
        if (references.decrementAndGet() == 0)
            free();
    }

    @Override
    protected void finalize()
    {
        assert references.get() <= 0;
        assert peer == 0;
        try
        {
            if (peer != 0)
                free();
        }
        finally
        {
            super.finalize();
        }
    }
}
