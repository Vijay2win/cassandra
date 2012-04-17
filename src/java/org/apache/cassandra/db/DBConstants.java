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
package org.apache.cassandra.db;

public abstract class DBConstants
{
    public static final DBConstants nativeConstants = new DBConstants.NativeDBConstants();
    private static final int BOOL_SIZE = 1;
    private static final int SHORT_SIZE = 2;
    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;

    public abstract int sizeof(boolean value);
    public abstract int sizeof(short value);
    public abstract int sizeof(int value);
    public abstract int sizeof(long value);

    public static class NativeDBConstants extends DBConstants
    {
        public int sizeof(boolean value)
        {
            return BOOL_SIZE;
        }

        public int sizeof(short value)
        {
            return SHORT_SIZE;
        }

        public int sizeof(int value)
        {
            return INT_SIZE;
        }

        public int sizeof(long value)
        {
            return LONG_SIZE;
        }
    }
}
