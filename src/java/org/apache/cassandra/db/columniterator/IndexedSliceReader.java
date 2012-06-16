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
package org.apache.cassandra.db.columniterator;

import java.io.IOError;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 *  This is a reader that finds the block for a starting column and returns
 *  blocks before/after it for each next call. This function assumes that
 *  the CF is sorted by name and exploits the name index.
 */
class IndexedSliceReader extends AbstractIterator<OnDiskAtom> implements OnDiskAtomIterator
{
    private final ColumnFamily emptyColumnFamily;

    private final SSTableReader sstable;
    private final List<IndexHelper.IndexInfo> indexes;
    private final FileDataInput originalInput;
    private FileDataInput file;
    private ColumnSlice current;
    private final Iterator<ColumnSlice> rangeIterator;
    private final boolean reversed;

    private final BlockFetcher fetcher;
    private final Deque<OnDiskAtom> blockColumns = new ArrayDeque<OnDiskAtom>();
    private final AbstractType<?> comparator;

    /**
    * This slice reader assumes that ranges are sorted correctly, 
    * 
    * e.g. that for forward lookup ranges are in lexicographic order of start elements and that 
    * for reverse lookup they are in reverse lexicographic order of finish (reverse start) elements. 
    * i.e. forward: [a,b],[d,e],[g,h] reverse: [h,g],[e,d],[b,a].
    *  
    * This reader also assumes that validation has been performed in terms of intervals (no overlapping intervals).
    */
    public IndexedSliceReader(SSTableReader sstable, RowIndexEntry indexEntry, FileDataInput input, List<ColumnSlice> ranges, boolean reversed)
    {
        this.sstable = sstable;
        this.originalInput = input;
        this.rangeIterator = ranges.iterator();
        this.reversed = reversed;
        this.current = rangeIterator.next();
        this.comparator = sstable.metadata.comparator;

        try
        {
            if (sstable.descriptor.version.hasPromotedIndexes)
            {
                this.indexes = indexEntry.columnsIndex();
                if (indexes.isEmpty())
                {
                    setToRowStart(sstable, indexEntry, input);
                    this.emptyColumnFamily = ColumnFamily.create(sstable.metadata);
                    emptyColumnFamily.delete(DeletionInfo.serializer().deserializeFromSSTable(file, sstable.descriptor.version));
                    fetcher = new SimpleBlockFetcher();
                }
                else
                {
                    this.emptyColumnFamily = ColumnFamily.create(sstable.metadata);
                    emptyColumnFamily.delete(indexEntry.deletionInfo());
                    fetcher = new IndexedBlockFetcher(indexEntry);
                }
            }
            else
            {
                setToRowStart(sstable, indexEntry, input);
                IndexHelper.skipBloomFilter(file);
                this.indexes = IndexHelper.deserializeIndex(file);
                this.emptyColumnFamily = ColumnFamily.create(sstable.metadata);
                emptyColumnFamily.delete(DeletionInfo.serializer().deserializeFromSSTable(file, sstable.descriptor.version));
                fetcher = indexes.isEmpty() ? new SimpleBlockFetcher() : new IndexedBlockFetcher();
            }
        }
        catch (IOException e)
        {
            sstable.markSuspect();
            throw new IOError(e);
        }
    }

    private void setToRowStart(SSTableReader reader, RowIndexEntry indexEntry, FileDataInput input) throws IOException
    {
        if (input == null)
        {
            this.file = sstable.getFileDataInput(indexEntry.position);
        }
        else
        {
            this.file = input;
            input.seek(indexEntry.position);
        }
        sstable.decodeKey(ByteBufferUtil.readWithShortLength(file));
        SSTableReader.readRowSize(file, sstable.descriptor);
    }

    public ColumnFamily getColumnFamily()
    {
        return emptyColumnFamily;
    }

    public DecoratedKey getKey()
    {
        throw new UnsupportedOperationException();
    }

    private boolean isColumnNeeded(OnDiskAtom column)
    {
        if (current.left.remaining() == 0 && current.right.remaining() == 0)
            return true;
        else if (current.left.remaining() == 0 && !reversed)
            return comparator.compare(column.name(), current.right) <= 0;
        else if (current.left.remaining() == 0 && reversed)
            return comparator.compare(column.name(), current.right) >= 0;
        else if (current.right.remaining() == 0 && !reversed)
            return comparator.compare(column.name(), current.left) >= 0;
        else if (current.right.remaining() == 0 && reversed)
            return comparator.compare(column.name(), current.left) <= 0;
        else if (!reversed)
            return comparator.compare(column.name(), current.left) >= 0 && comparator.compare(column.name(), current.right) <= 0;
        else // if reversed
            return comparator.compare(column.name(), current.left) <= 0 && comparator.compare(column.name(), current.right) >= 0;
    }

    protected OnDiskAtom computeNext()
    {
        while (true)
        {
            OnDiskAtom column = blockColumns.poll();
            if (column != null)
            {
                while (true)
                {
                    if (isColumnNeeded(column))
                        return column;
                    if ((!reversed && comparator.compare(current.right, column.name()) < 0) ||
                            (reversed && comparator.compare(column.name(), current.right) < 0))
                    {
                        if (rangeIterator.hasNext())
                        {
                            current = rangeIterator.next();
                            continue;
                        }
                    }
                    break;
                }
            }
            try
            {
                if (column == null && !fetcher.getNextBlock())
                {
                    if (!rangeIterator.hasNext())
                        return endOfData();
                    current = rangeIterator.next();
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public void close() throws IOException
    {
        if (originalInput == null && file != null)
            file.close();
    }

    interface BlockFetcher
    {
        public boolean getNextBlock() throws IOException;
    }

    private class IndexedBlockFetcher implements BlockFetcher
    {
        private final long basePosition;
        private int curRangeIndex;

        IndexedBlockFetcher() throws IOException
        {
            file.readInt(); // column count
            basePosition = file.getFilePointer();
            curRangeIndex = IndexHelper.indexFor(current.left, indexes, comparator, reversed);
        }

        IndexedBlockFetcher(RowIndexEntry indexEntry)
        {
            basePosition = indexEntry.position;
            curRangeIndex = IndexHelper.indexFor(current.left, indexes, comparator, reversed);
        }

        public boolean getNextBlock() throws IOException
        {
            if (curRangeIndex < 0 || curRangeIndex >= indexes.size())
                return false;

            /* seek to the correct offset to the data, and calculate the data size */
            IndexHelper.IndexInfo curColPosition = indexes.get(curRangeIndex);

            /* see if this read is really necessary. */
            if (reversed)
            {
                if ((current.right.remaining() > 0 && comparator.compare(current.right, curColPosition.lastName) > 0) ||
                    (current.left.remaining() > 0 && comparator.compare(current.left, curColPosition.firstName) < 0))
                    return false;
            }
            else
            {
                if ((current.left.remaining() > 0 && comparator.compare(current.left, curColPosition.lastName) > 0) ||
                    (current.right.remaining() > 0 && comparator.compare(current.right, curColPosition.firstName) < 0))
                    return false;
            }

            boolean outOfBounds = false;
            long positionToSeek = basePosition + curColPosition.offset;

            // With new promoted indexes, our first seek in the data file will happen at that point.
            if (file == null)
                file = originalInput == null ? sstable.getFileDataInput(positionToSeek) : originalInput;

            OnDiskAtom.Serializer atomSerializer = emptyColumnFamily.getOnDiskSerializer();
            file.seek(positionToSeek);
            FileMark mark = file.mark();
            while (file.bytesPastMark(mark) < curColPosition.width && !outOfBounds)
            {
                OnDiskAtom column = atomSerializer.deserializeFromSSTable(file, sstable.descriptor.version);
                if (reversed)
                    blockColumns.addFirst(column);
                else
                    blockColumns.addLast(column);

                if (!rangeIterator.hasNext())
                {
                    /* see if we can stop seeking. */
                    if (!reversed && current.right.remaining() > 0)
                        outOfBounds = comparator.compare(column.name(), current.right) >= 0;
                    else if (reversed && current.left.remaining() > 0)
                        outOfBounds = comparator.compare(column.name(), current.left) >= 0;
                }
            }

            if (reversed)
                curRangeIndex--;
            else
                curRangeIndex++;
            return true;
        }
    }

    private class SimpleBlockFetcher implements BlockFetcher
    {
        private SimpleBlockFetcher() throws IOException
        {
            OnDiskAtom.Serializer atomSerializer = emptyColumnFamily.getOnDiskSerializer();
            int columns = file.readInt();
            for (int i = 0; i < columns; i++)
            {
                OnDiskAtom column = atomSerializer.deserializeFromSSTable(file, sstable.descriptor.version);
                if (reversed)
                    blockColumns.addFirst(column);
                else
                    blockColumns.addLast(column);

                if (!rangeIterator.hasNext())
                {
                    /* see if we can stop seeking. */
                    boolean outOfBounds = false;
                    if (!reversed && current.right.remaining() > 0)
                        outOfBounds = comparator.compare(column.name(), current.right) >= 0;
                    else if (reversed && current.left.remaining() > 0)
                        outOfBounds = comparator.compare(column.name(), current.left) >= 0;
                    if (outOfBounds)
                        break;
                }
            }
        }

        public boolean getNextBlock() throws IOException
        {
            return false;
        }
    }
}
