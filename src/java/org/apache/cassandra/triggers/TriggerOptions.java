package org.apache.cassandra.triggers;

import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TriggerOptions
{
    public static final String NAME = "name";
    public static final String CLASS_KEY = "class";
    private static final String OPTIONS_KEY = "trigger_options";

    public static List<Map<String, String>> getAllTriggers(String ksName, String cfName)
    {
        String req = "SELECT * FROM system.%s WHERE keyspace_name='%s' AND column_family='%s'";
        UntypedResultSet result = processInternal(String.format(req, SystemTable.SCHEMA_TRIGGERS_CF, ksName, cfName));
        List<Map<String, String>> triggers = new ArrayList<>();
        if (result.isEmpty())
            return triggers;
        for (Row row : result)
            triggers.add(row.getMap(OPTIONS_KEY, UTF8Type.instance, UTF8Type.instance));
        return triggers;
    }

    public static void addColumns(RowMutation rm, String cfName, Map<String, String> map, long modificationTimestamp)
    {
        ColumnFamily cf = rm.addOrGet(SystemTable.SCHEMA_TRIGGERS_CF);
        assert map.get(NAME) != null && map.get(CLASS_KEY) != null;
        ColumnNameBuilder builder = CFMetaData.SchemaTriggerCf.getCfDef().getColumnNameBuilder();
        builder.add(ByteBufferUtil.bytes(cfName)).add(ByteBufferUtil.bytes(map.get(NAME))).add(ByteBufferUtil.bytes(OPTIONS_KEY));
        for (Entry<String, String> entry : map.entrySet())
        {
            ColumnNameBuilder builderCopy = builder.copy();
            builderCopy.add(ByteBufferUtil.bytes(entry.getKey()));
            cf.addColumn(builderCopy.build(), ByteBufferUtil.bytes(entry.getValue()), modificationTimestamp);
        }
    }

    public static void deleteColumns(RowMutation rm, String cfName, Map<String, String> entry, long modificationTimestamp)
    {
        ColumnFamily cf = rm.addOrGet(SystemTable.SCHEMA_TRIGGERS_CF);
        int ldt = (int) (System.currentTimeMillis() / 1000);
        assert entry.get(NAME) != null;
        ColumnNameBuilder builder = CFMetaData.SchemaTriggerCf.getCfDef().getColumnNameBuilder();
        builder.add(ByteBufferUtil.bytes(cfName)).add(ByteBufferUtil.bytes(entry.get(NAME)));
        cf.addAtom(new RangeTombstone(builder.build(), builder.buildAsEndOfRange(), modificationTimestamp, ldt));
    }

    public static void update(CFMetaData cfm, String triggerName, String clazz)
    {
        List<Map<String, String>> existingTriggers = cfm.getTriggers();
        Map<String, String> triggerUnit = new HashMap<>();
        triggerUnit.put(NAME, triggerName);
        triggerUnit.put(CLASS_KEY, clazz);
        assert !existingTriggers.contains(triggerUnit);
        existingTriggers.add(triggerUnit);
        cfm.triggers(existingTriggers);
    }

    public static void remove(CFMetaData cfm, String triggerName)
    {
        List<Map<String, String>> existingTriggers = cfm.getTriggers(); // have a copy of the triggers
        for (Map<String, String> entry : cfm.getTriggers())
            if (entry.get(NAME).equals(triggerName))
                existingTriggers.remove(entry);
        cfm.triggers(existingTriggers);
    }

    public static boolean hasTrigger(CFMetaData cfm, String triggerName)
    {
        List<Map<String, String>> existingTriggers = cfm.getTriggers();
        for (Map<String, String> entry : existingTriggers)
            if (entry.get(NAME).equals(triggerName))
                return true;
        return false;

    }

    public static Collection<String> extractClasses(List<Map<String, String>> triggers)
    {
        List<String> classes = new ArrayList<>();
        if (triggers.isEmpty())
            return null;
        for (Map<String, String> options : triggers)
            classes.add(options.get(CLASS_KEY));
        return classes;
    }
}
