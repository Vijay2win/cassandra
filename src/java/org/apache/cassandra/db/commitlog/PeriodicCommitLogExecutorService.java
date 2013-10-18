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

import java.io.IOException;
import java.util.concurrent.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.WrappedRunnable;

import com.google.common.util.concurrent.Uninterruptibles;

class PeriodicCommitLogExecutorService implements ICommitLogExecutorService
{
    private volatile boolean run = true;

    public PeriodicCommitLogExecutorService(final CommitLog commitLog)
    {
        new Thread(new Runnable()
        {
            public void run()
            {
                while (run)
                {
                    commitLog.sync();
                    Uninterruptibles.sleepUninterruptibly(DatabaseDescriptor.getCommitLogSyncPeriod(), TimeUnit.MILLISECONDS);
                }
            }
        }, "PERIODIC-COMMIT-LOG-SYNCER").start();

    }

    public void waitIfNeeded()
    {
        // noop.
    }

    public <T> Future<T> submit(Callable<T> task)
    {
        throw new UnsupportedOperationException("Multi threaded commit log");
    }

    Thread shutdownThread = new Thread(new WrappedRunnable()
    {
        public void runMayThrow() throws InterruptedException, IOException
        {
            CommitLog.instance.sync();
        }
    }, "Commitlog Shutdown");

    public void shutdown()
    {
        shutdownThread.start();
    }

    public void awaitTermination() throws InterruptedException
    {
        shutdownThread.run();
    }
}