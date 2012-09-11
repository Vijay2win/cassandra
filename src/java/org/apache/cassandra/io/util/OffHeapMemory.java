package org.apache.cassandra.io.util;

public interface OffHeapMemory
{
    /**
     * @return true if we succeed in referencing before the reference count
     *         reaches zero. (A FreeableMemory object is created with a
     *         reference count of one.)
     */
    boolean reference();

    /** decrement reference count. if count reaches zero, the object is freed. */
    void unreference();

    byte getByte(long offset);

    void read(long position, byte[] buffer, int offset, int count);

    void setByte(long position, byte b);

    void write(long position, byte[] b, int off, int len);

    long size();
}
