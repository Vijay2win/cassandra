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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;

public class MutationContainer
{
    public final ConsistencyLevel consistency_level;
    public Collection rowMutations;
    public Collection counterMutations;

    public MutationContainer(ConsistencyLevel consistency)
    {
        this.consistency_level = consistency;
    }

    public MutationContainer(ConsistencyLevel consistency, RowMutation rm, CFMetaData metadata)
    {
        this.consistency_level = consistency;
        if (metadata.getDefaultValidator().isCommutative())
            counterMutations = Collections.singleton(new CounterMutation(rm, consistency_level));
        else
            rowMutations = Collections.singleton(rm);
    }

    private Collection<? extends IMutation> merged; // cache.

    public Collection<? extends IMutation> mergeMutations()
    {
        if (merged != null)
            return merged;
        merged = rowMutations;
        if (merged == null)
            merged = counterMutations;
        else if (counterMutations != null)
            merged.addAll(counterMutations);
        return merged;
    }

    public boolean isEmpty()
    {
        return (rowMutations == null) && (counterMutations == null);
    }

    public void addCounterMutation(CounterMutation counterMutation)
    {
        if (counterMutations == null)
            counterMutations = new LinkedList<CounterMutation>();
        counterMutations.add(counterMutation);
    }

    public void addRowMutation(RowMutation rm)
    {
        if (rowMutations == null)
            rowMutations = new LinkedList<RowMutation>();
        rowMutations.add(rm);
    }

    public void addCounterMutation(Collection<IMutation> prepareRowMutations)
    {
        if (counterMutations == null)
            counterMutations = new LinkedList<CounterMutation>();
        counterMutations.addAll(prepareRowMutations);
    }

    public void addRowMutation(Collection<IMutation> mutations)
    {
        if (rowMutations == null)
            rowMutations = new LinkedList<RowMutation>();
        rowMutations.addAll(mutations);
    }
}
