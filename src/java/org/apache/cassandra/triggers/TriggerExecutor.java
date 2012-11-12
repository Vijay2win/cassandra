package org.apache.cassandra.triggers;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.utils.WrappedRunnable;

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
        Runnable runnable = new WrappedRunnable()
        {
            // switch the classes so GC will unload old classes
            protected void runMayThrow() throws ConfigurationException
            {
                reloadClasses();
            }
        };
        ResourceWatcher.watch(triggerDirectory.getAbsolutePath(), runnable, System.currentTimeMillis(), 60 * 1000);
    }

    public void reloadClasses()
    {
        customClassLoader = new CustomClassLoader(parent, triggerDirectory);
        cachedTriggers.clear();
    }

    public Collection<RowMutation> execute(Collection<RowMutation> updates)
    {
        List<RowMutation> mutations = Lists.newArrayList();
        for (RowMutation mutation : updates)
            mutations.addAll(execute(mutation));
        return mutations;
    }

    public Collection<RowMutation> execute(RowMutation update)
    {
        List<RowMutation> mutations = Lists.newArrayList();
        for (ColumnFamily cf : update.getColumnFamilies())
        {
            Collection<RowMutation> mutation = execute(update.key(), cf);
            if (mutation != null)
                mutations.addAll(mutation);
        }
        return mutations;
    }

    public Collection<RowMutation> execute(ByteBuffer key, ColumnFamily columnFamily)
    {
        String name = columnFamily.metadata().getTriggerClass();
        if (name == null)
            return null;
        Thread.currentThread().setContextClassLoader(customClassLoader);
        try
        {
            ITrigger trigger = cachedTriggers.get(columnFamily.id());
            if (trigger == null)
            {
                trigger = createTrigger(name);
                cachedTriggers.put(name, trigger);
            }
            return trigger.augment(key, columnFamily);
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

    private synchronized ITrigger createTrigger(String name) throws Exception
    {
        // double check.
        if (cachedTriggers.get(name) != null)
            return cachedTriggers.get(name);

        Constructor<? extends ITrigger> costructor = (Constructor<? extends ITrigger>) customClassLoader.loadClass(name).getConstructor(new Class<?>[0]);
        return costructor.newInstance(new Object[0]);
    }
}
