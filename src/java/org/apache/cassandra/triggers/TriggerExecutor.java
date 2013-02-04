package org.apache.cassandra.triggers;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cql.QueryProcessor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.FBUtilities;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class TriggerExecutor
{
    public static final TriggerExecutor instance = new TriggerExecutor();

    private final Map<String, ITrigger> cachedTriggers = Maps.newConcurrentMap();
    private final ClassLoader parent = Thread.currentThread().getContextClassLoader();
    private final File triggerDirectory = new File(FBUtilities.cassandraHomeDir(), "triggers");
    private volatile ClassLoader customClassLoader;

    private TriggerExecutor()
    {
        reloadClasses();
    }

    public void reloadClasses()
    {
        customClassLoader = new CustomClassLoader(parent, triggerDirectory);
        cachedTriggers.clear();
    }

    public Collection<RowMutation> execute(Collection<? extends IMutation> updates) throws InvalidRequestException
    {
        boolean hasCounters = false;
        Collection<RowMutation> mutations = null;
        for (IMutation mutation : updates)
        {
            if (mutations == null)
                mutations = execute(mutation);
            else
                mutations.addAll(execute(mutation));

            if (mutation instanceof CounterMutation)
                hasCounters = true;
        }
        if (mutations != null && hasCounters)
            throw new InvalidRequestException("Counter mutations and trigger mutations cannot be applied together atomically.");
        return mutations;
    }

    private Collection<RowMutation> execute(IMutation update) throws InvalidRequestException
    {
        List<RowMutation> tmutations = Lists.newLinkedList();
        for (ColumnFamily cf : update.getColumnFamilies())
        {
            execute(update.key(), cf, tmutations);
            validate(tmutations);
        }
        return tmutations;
    }

    private void validate(Collection<RowMutation> tmutations) throws InvalidRequestException
    {
        for (RowMutation mutation : tmutations)
        {
            QueryProcessor.validateKey(mutation.key());
            for (ColumnFamily tcf : mutation.getColumnFamilies())
                for (ByteBuffer tName : tcf.getColumnNames())
                    QueryProcessor.validateColumn(tcf.metadata(), tName, tcf.getColumn(tName).value());
        }
    }

    private void execute(ByteBuffer key, ColumnFamily columnFamily, Collection<RowMutation> tmutations)
    {
        Set<String> triggerNames = columnFamily.metadata().getTriggerClass();
        if (triggerNames == null)
            return;
        Thread.currentThread().setContextClassLoader(customClassLoader);
        try
        {
            for (String triggerName: triggerNames)
            {
                ITrigger trigger = cachedTriggers.get(triggerName);
                if (trigger == null)
                {
                    trigger = loadTriggerInstance(triggerName);
                    cachedTriggers.put(triggerName, trigger);
                }
                Collection<RowMutation> temp = trigger.augment(key, columnFamily);
                if (temp != null)
                    tmutations.addAll(temp);
            }
        }
        catch (Exception ex)
        {
            throw new RuntimeException(String.format("Exception while creating trigger a trigger on CF with ID: %s", columnFamily.id()), ex);
        }
        finally
        {
            Thread.currentThread().setContextClassLoader(parent);
        }
    }

    private synchronized ITrigger loadTriggerInstance(String triggerName) throws Exception
    {
        // double check.
        if (cachedTriggers.get(triggerName) != null)
            return cachedTriggers.get(triggerName);

        Constructor<? extends ITrigger> costructor = (Constructor<? extends ITrigger>) customClassLoader.loadClass(triggerName).getConstructor(new Class<?>[0]);
        return costructor.newInstance(new Object[0]);
    }
}
