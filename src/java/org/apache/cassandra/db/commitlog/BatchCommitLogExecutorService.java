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
package org.apache.cassandra.db.commitlog;

import java.util.ArrayList;
import java.util.concurrent.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

class BatchCommitLogExecutorService extends AbstractCommitLogExecutorService
{
    private final BlockingQueue<CheaterFutureTask<?>> queue;
    private final Thread appendingThread;
    private volatile boolean run = true;

    public BatchCommitLogExecutorService()
    {
        this(DatabaseDescriptor.getConcurrentWriters());
    }

    public BatchCommitLogExecutorService(int queueSize)
    {
        queue = new LinkedBlockingQueue<CheaterFutureTask<?>>(queueSize);
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                while (run)
                {
                    processWithSyncBatch();
                }
            }
        };
        appendingThread = new Thread(runnable, "COMMIT-LOG-WRITER");
        appendingThread.start();

    }

    public long getPendingTasks()
    {
        return queue.size();
    }

    private final ArrayList<CheaterFutureTask<?>> incompleteTasks = new ArrayList<CheaterFutureTask<?>>();
    private boolean processWithSyncBatch() throws Exception
    {
        CheaterFutureTask<?> firstTask = queue.poll(100, TimeUnit.MILLISECONDS);
        if (firstTask == null)
            return false;
        if (!(firstTask.getRawCallable() instanceof LogRecordAdder))
        {
            firstTask.run();
            return true;
        }

        // (this is a little clunky since there is no blocking peek method,
        //  so we have to break it into firstTask / extra tasks)
        incompleteTasks.clear();
        long start = System.nanoTime();
        long window = (long) (1000000 * DatabaseDescriptor.getCommitLogSyncBatchWindow());

        // it doesn't seem worth bothering future-izing the exception
        // since if a commitlog op throws, we're probably screwed anyway
        incompleteTasks.add(firstTask);
        firstTask.set(null);
        while (!queue.isEmpty()
               && queue.peek().getRawCallable() instanceof LogRecordAdder
               && System.nanoTime() - start < window)
            incompleteTasks.add(queue.remove());

        // now sync and set the tasks' values (which allows thread calling get() to proceed)
        CommitLog.instance.sync();
        for (CheaterFutureTask<?> task : incompleteTasks)
            task.set(null);
        return true;
    }


    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value)
    {
        return newTaskFor(Executors.callable(runnable, value));
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable)
    {
        return new CheaterFutureTask<T>(callable);
    }

    public void execute(Runnable command)
    {
        try
        {
            queue.put((CheaterFutureTask<?>)command);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void waitIfNeeded()
    {
        FBUtilities.waitOnFuture(submit(new LogRecordAdder()));
    }

    public void shutdown()
    {
        new Thread(new WrappedRunnable()
        {
            public void runMayThrow() throws InterruptedException
            {
                while (!queue.isEmpty())
                    Thread.sleep(100);
                run = false;
                appendingThread.join();
            }
        }, "Commitlog Shutdown").start();
    }

    public void awaitTermination() throws InterruptedException
    {
        appendingThread.join();
    }

    private static class CheaterFutureTask<V> extends FutureTask<V>
    {
        private final Callable<V> rawCallable;

        public CheaterFutureTask(Callable<V> callable)
        {
            super(callable);
            rawCallable = callable;
        }

        public Callable<V> getRawCallable()
        {
            return rawCallable;
        }

        @Override
        public void set(V v)
        {
            super.set(v);
        }
    }

    class LogRecordAdder implements Callable<Void>
    {
        public Void call()
        {
            return null;
        }
    }
}
