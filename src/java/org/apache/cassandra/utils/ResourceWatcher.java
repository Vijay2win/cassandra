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
package org.apache.cassandra.utils;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceWatcher
{
    public static void watch(String resource, Runnable callback, int period) throws ConfigurationException
    {
        StorageService.scheduledTasks.scheduleWithFixedDelay(new WatchedResource(resource, callback), period, period, TimeUnit.MILLISECONDS);
    }

    public static void watch(String resource, Runnable callback, long startTime, int period)
    {
        StorageService.scheduledTasks.scheduleWithFixedDelay(new WatchedResource(resource, callback, startTime), period, period, TimeUnit.MILLISECONDS);
    }

    public static class WatchedResource implements Runnable
    {
        private static final Logger logger = LoggerFactory.getLogger(WatchedResource.class);
        private final String resource;
        private final Runnable callback;
        private long lastLoaded = 0;

        /**
         * File watch, searches the classpath for resource.
         */
        public WatchedResource(String resource, Runnable callback) throws ConfigurationException
        {
            this.resource = FBUtilities.resourceToFile(resource);
            this.callback = callback;
        }

        /**
         * Directory watch.
         */
        public WatchedResource(String resource, Runnable callback, long start)
        {
            this.resource = resource;
            this.callback = callback;
            this.lastLoaded = start;
        }

        public void run()
        {
            try
            {
                long lastModified = getLastModified(new File(resource));
                if (lastModified > lastLoaded)
                {
                    callback.run();
                    lastLoaded = lastModified;
                }
            }
            catch (Throwable t)
            {
                logger.error(String.format("Timed run of %s failed.", callback.getClass()), t);
            }
        }

        private long getLastModified(File file)
        {
            if (!file.isDirectory())
                return file.lastModified();

            long largest = 0;
            for (File jar : file.listFiles())
            {
                if (jar.lastModified() > largest)
                    largest = jar.lastModified();
            }
            return largest;
        }
    }
}
