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
package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy.LocalReadRunnable;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractReadExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractReadExecutor.class);
    protected final List<InetAddress> naturalEndpoints;
    protected final ReadCommand command;
    protected final ReadCallback<ReadResponse, Row> handler;

    AbstractReadExecutor(ReadCommand command, ConsistencyLevel consistency_level)
    {
        this.command = command;
        List<InetAddress> endpoints = StorageService.instance.getLiveNaturalEndpoints(command.table, command.key);
        DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), endpoints);
        this.naturalEndpoints = endpoints;
        this.handler = StorageProxy.getReadCallback(getResolver(), command, consistency_level, naturalEndpoints);
    }

    void executeAsync() throws UnavailableException
    {
        handler.assureSufficientLiveNodes();
        assert !handler.endpoints.isEmpty();

        // The data-request message is sent to dataPoint, the node that will actually get the data for us
        InetAddress dataPoint = handler.endpoints.get(0);
        if (dataPoint.equals(FBUtilities.getBroadcastAddress()) && StorageProxy.OPTIMIZE_LOCAL_REQUESTS)
        {
            logger.debug("reading data locally");
            StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
        }
        else
        {
            logger.debug("reading data from {}", dataPoint);
            MessagingService.instance().sendRR(command.createMessage(), dataPoint, handler);
        }

        if (handler.endpoints.size() != 1)
            return;

        // send the other endpoints a digest request
        ReadCommand digestCommand = command.copy();
        digestCommand.setDigestQuery(true);
        MessageOut<?> message = null;
        for (InetAddress digestPoint : handler.endpoints.subList(1, handler.endpoints.size()))
        {
            if (digestPoint.equals(FBUtilities.getBroadcastAddress()))
            {
                logger.debug("reading digest locally");
                StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(digestCommand, handler));
            }
            else
            {
                logger.debug("reading digest from {}", digestPoint);
                // (We lazy-construct the digest Message object since it may not be necessary if we
                // are doing a local digest read, or no digest reads at all.)
                if (message == null)
                    message = digestCommand.createMessage();
                MessagingService.instance().sendRR(message, digestPoint, handler);
            }
        }
    }

    Row get() throws ReadTimeoutException, DigestMismatchException, IOException
    {
        return handler.get(command.getTimeout());
    }

    AbstractRowResolver getResolver()
    {
        return new RowDigestResolver(command.table, command.key);
    }

    public static AbstractReadExecutor getReadExecutor(ReadCommand command, ConsistencyLevel consistency_level)
    {
        ColumnFamilyStore cfs = Table.open(command.table).getColumnFamilyStore(command.getColumnFamilyName());
        switch (cfs.metadata.getSpeculativeRetry())
        {
        case ALL:
            return new ReadAllExecutor(command, consistency_level);
        case AUTO:
            return new SpeculativeReadExecutor(command, cfs, consistency_level);
        default:
            return new DefaultReadExecutor(command, consistency_level);
        }
    }

    private static class DefaultReadExecutor extends AbstractReadExecutor
    {
        DefaultReadExecutor(ReadCommand command, ConsistencyLevel consistency_level)
        {
            super(command, consistency_level);
        }
    }

    private static class SpeculativeReadExecutor extends AbstractReadExecutor
    {
        private ColumnFamilyStore cfs;

        public SpeculativeReadExecutor(ReadCommand command, ColumnFamilyStore cfs, ConsistencyLevel consistency_level)
        {
            super(command, consistency_level);
            this.cfs = cfs;
        }

        Row get() throws ReadTimeoutException, DigestMismatchException, IOException
        {
            long speculativeTimeout = cfs.getReadLatencyRate(command.getTimeout());
            Row row = handler.get(speculativeTimeout);
            if (row == null && naturalEndpoints.size() > 1)
            {
                InetAddress dataPoint = naturalEndpoints.get(1);
                logger.debug("speculating read retry from {}", dataPoint);
                MessagingService.instance().sendRR(command.createMessage(), dataPoint, handler);
                cfs.metric.speculativeRetry.inc();
            }
            return handler.get(command.getTimeout() - speculativeTimeout);
        }
    }

    private static class ReadAllExecutor extends AbstractReadExecutor
    {
        public ReadAllExecutor(ReadCommand command, ConsistencyLevel consistency_level)
        {
            super(command, consistency_level);
        }

        void executeAsync() throws UnavailableException
        {
            handler.assureSufficientLiveNodes();
            assert !handler.endpoints.isEmpty();
            MessageOut<?> message = null;
            for (InetAddress endpoint : handler.endpoints)
            {
                if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                {
                    logger.debug("reading full data locally");
                    StageManager.getStage(Stage.READ).execute(new LocalReadRunnable(command, handler));
                }
                else
                {
                    logger.debug("reading full data from {}", endpoint);
                    if (message == null)
                        message = command.createMessage();
                    MessagingService.instance().sendRR(message, endpoint, handler);
                }
            }
        }

        AbstractRowResolver getResolver()
        {
            return new RowRepairResolver(command.table, command.key);
        }
    }
}
