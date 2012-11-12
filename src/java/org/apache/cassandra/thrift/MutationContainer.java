package org.apache.cassandra.thrift;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.db.ConsistencyLevel;

public class MutationContainer
{
    public final ConsistencyLevel consistency_level;
    public List rowMutations;
    public List counterMutations;

    public MutationContainer(ConsistencyLevel consistency)
    {
        this.consistency_level = consistency;
    }

    public MutationContainer(ConsistencyLevel consistency, RowMutation rm, CFMetaData metadata)
    {
        this.consistency_level = consistency;
        if (metadata.getDefaultValidator().isCommutative())
            counterMutations = Arrays.asList(new CounterMutation(rm, consistency_level));
        else
            rowMutations = Arrays.asList(rm);
    }

    private List<String> cfamsSeen = new ArrayList<String>();

    public void checkPermission(String keyspace, String cfName, ThriftClientState state) throws UnauthorizedException, InvalidRequestException
    {
        // Avoid unneeded authorizations
        if (!(cfamsSeen.contains(cfName)))
        {
            state.hasColumnFamilyAccess(keyspace, cfName, Permission.UPDATE);
            cfamsSeen.add(cfName);
        }
    }

    public List<? extends IMutation> mergeMutations()
    {
        List<? extends IMutation> merged = rowMutations;
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

    public void addCounter(CounterMutation counterMutation)
    {
        if (counterMutations == null)
            counterMutations = new ArrayList<CounterMutation>();
        counterMutations.add(counterMutation);
    }

    public void addRowMutation(RowMutation rm)
    {
        if (rowMutations == null)
            rowMutations = new ArrayList<RowMutation>();
        rowMutations.add(rm);        
    }

    public void addCounter(Collection<IMutation> prepareRowMutations)
    {
        if (counterMutations == null)
            counterMutations = new ArrayList<CounterMutation>();
        counterMutations.addAll(prepareRowMutations);
    }

    public void addRowMutation(Collection<IMutation> mutations)
    {
        if (rowMutations == null)
            rowMutations = new ArrayList<RowMutation>();
        rowMutations.addAll(mutations);
    }
}
