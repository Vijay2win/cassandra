package org.apache.cassandra.db;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.cassandra.utils.obs.OpenBitSet;

import edu.stanford.ppl.concurrent.SnapTreeMap;

/**
 * Modified version of the code from.
 * https://github.com/twitter/commons/blob/master
 * /src/java/com/twitter/common/objectsize/ObjectSizeCalculator.java
 * 
 * Difference is that we don't use reflection.
 */
public class ObjectSizes
{
    public static final MemoryLayoutSpecification SPEC = getEffectiveMemoryLayoutSpecification();

    /**
     * Describes constant memory overheads for various constructs in a JVM
     * implementation.
     */
    public interface MemoryLayoutSpecification
    {
        int getArrayHeaderSize();

        int getObjectHeaderSize();

        int getObjectPadding();

        int getReferenceSize();

        int getSuperclassFieldPadding();
    }

    private static MemoryLayoutSpecification getEffectiveMemoryLayoutSpecification()
    {
        final String dataModel = System.getProperty("sun.arch.data.model");
        if ("32".equals(dataModel))
        {
            // Running with 32-bit data model
            return new MemoryLayoutSpecification()
            {
                public int getArrayHeaderSize()
                {
                    return 12;
                }

                public int getObjectHeaderSize()
                {
                    return 8;
                }

                public int getObjectPadding()
                {
                    return 8;
                }

                public int getReferenceSize()
                {
                    return 4;
                }

                public int getSuperclassFieldPadding()
                {
                    return 4;
                }
            };
        }

        final String strVmVersion = System.getProperty("java.vm.version");
        final int vmVersion = Integer.parseInt(strVmVersion.substring(0, strVmVersion.indexOf('.')));
        if (vmVersion >= 17)
        {
            long maxMemory = 0;
            for (MemoryPoolMXBean mp : ManagementFactory.getMemoryPoolMXBeans())
            {
                maxMemory += mp.getUsage().getMax();
            }
            if (maxMemory < 30L * 1024 * 1024 * 1024)
            {
                // HotSpot 17.0 and above use compressed OOPs below 30GB of RAM
                // total for all memory pools (yes, including code cache).
                return new MemoryLayoutSpecification()
                {
                    public int getArrayHeaderSize()
                    {
                        return 16;
                    }

                    public int getObjectHeaderSize()
                    {
                        return 12;
                    }

                    public int getObjectPadding()
                    {
                        return 8;
                    }

                    public int getReferenceSize()
                    {
                        return 4;
                    }

                    public int getSuperclassFieldPadding()
                    {
                        return 4;
                    }
                };
            }
        }

        /* Worst case we over count. */

        // In other cases, it's a 64-bit uncompressed OOPs object model
        return new MemoryLayoutSpecification()
        {
            public int getArrayHeaderSize()
            {
                return 24;
            }

            public int getObjectHeaderSize()
            {
                return 16;
            }

            public int getObjectPadding()
            {
                return 8;
            }

            public int getReferenceSize()
            {
                return 8;
            }

            public int getSuperclassFieldPadding()
            {
                return 8;
            }
        };
    }

    public static long getFieldSize(long fieldsSize)
    {
        return roundTo(SPEC.getObjectHeaderSize() + fieldsSize, SPEC.getObjectPadding());
    }

    public static long getSuperClassFieldSize(long fieldsSize)
    {
        return roundTo(fieldsSize, SPEC.getSuperclassFieldPadding());
    }

    public static long getArraySize(int length, long elementTotalSize)
    {
        return roundTo(SPEC.getArrayHeaderSize() + elementTotalSize, SPEC.getObjectPadding());
    }

    public static long getArraySize(int length)
    {
        return getArraySize(length, length);
    }

    public static long getSizeWithRef(byte[] bytes)
    {
        return SPEC.getReferenceSize() + getArraySize(bytes.length);
    }

    public static long getSize(OpenBitSet bitset)
    {
        int pageCount = bitset.getPageCount();
        long size = 0;
        for (int p = 0; p < pageCount; p++)
        {
            long[] bits = bitset.getPage(p);
            size += getArraySize(bits.length, 8 * bits.length);
        }
        size += 4; // int wlen;
        size += 4; // int pageCount;
        size += getArraySize(pageCount, getReferenceSize());
        return ObjectSizes.getFieldSize(size);
    }

    public static long getSize(ByteBuffer buffer)
    {
        long size = 0;
        /* Super Class */
        // private int mark;
        // private int position;
        // private int limit;
        // private int capacity;
        // long address;
        size += ObjectSizes.getSuperClassFieldSize(4L + 4 + 4 + 4 + 8);
        /* BB Class */
        // final byte[] hb;
        // final int offset;
        // boolean isReadOnly;
        int cap = buffer.capacity();
        size += ObjectSizes.getFieldSize(1L + 4 + ObjectSizes.getReferenceSize() + ObjectSizes.getArraySize(cap));
        return size;
    }

    public static long getSizeWithRef(ByteBuffer buffer)
    {
        return SPEC.getReferenceSize() + getSize(buffer);
    }

    public static long roundTo(long x, int multiple)
    {
        return ((x + multiple - 1) / multiple) * multiple;
    }

    public static int getReferenceSize()
    {
        return SPEC.getReferenceSize();
    }

    public static <T> long getSize(List<T> input, long elementTotalSize)
    {
        long refSizes = input.size() * ObjectSizes.getReferenceSize();
        long size = getArraySize(input.size(), elementTotalSize + refSizes) + getReferenceSize();
        size += 4; // tracks size
        return getFieldSize(size);
    }

    public static <K, V> long getSize(TreeMap<K, V> map, long entryTotalSize)
    {
        // AbstractMap.keySet + AbstractMap.values
        long superSize = 2 * getReferenceSize(); 
        long size = getSuperClassFieldSize(superSize);
        // entrySet + navigableKeySet + descendingMap
        size += 3 * getReferenceSize();
        // comparator + root
        size += 2 * getReferenceSize();
        size += 3 * getReferenceSize();
        // size + modCount
        size += 4 + 4;

        // key + value + left + right + parent
        long entrySize = 5 * getReferenceSize();
        // color
        entrySize += 1;
        size += map.size() * getFieldSize(entrySize);

        size += entryTotalSize;
        return getFieldSize(size);
    }

    public static <K, V> long getSize(ConcurrentSkipListMap<K, V> map, long entryTotalSize)
    {
        // AbstractMap.keySet + AbstractMap.values
        long superSize = 2 * getReferenceSize();
        long size = getSuperClassFieldSize(superSize);
        size += 2 * getReferenceSize(); // head + comparator
        size += 4;
        // keySet + entrySet + values + descendingMap
        size += 4 * getReferenceSize(); 
        // Node (key + Value + next)
        long entrySize = getFieldSize(3 * getReferenceSize());
        // Index (node + down + right)
        entrySize += getFieldSize(3 * getReferenceSize());
        size += map.size() * getFieldSize(entrySize);
        size += entryTotalSize + (11 * getReferenceSize());
        return getFieldSize(size);
    }

    public static <K, V> long getSize(SnapTreeMap<K, V> map, long entryTotalSize)
    {
        long size = 2 * getReferenceSize(); // comparator + holderRef
        size += 4L; // state
        // K key;
        // Object vOpt;
        // Node<K,V> parent;
        // Node<K,V> left;
        // Node<K,V> right;
        long nodeSize = 6 * getReferenceSize();
        // int height;
        // long shrinkOVL;
        nodeSize += 4L + 8L;
        size += map.size() * getFieldSize(nodeSize);
        // TODO fix me.
        // measured overhead for CopyOnWriteManager
        size += getSuperClassFieldSize(nodeSize) + 240;

        size += entryTotalSize;
        return size;
    }
}