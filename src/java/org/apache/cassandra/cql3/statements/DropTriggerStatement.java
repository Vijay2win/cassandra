package org.apache.cassandra.cql3.statements;

import java.util.Collection;
import java.util.Map;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropTriggerStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(DropTriggerStatement.class);
    private final String triggerName;

    public DropTriggerStatement(CFName name, String triggerName)
    {
        super(name);
        this.triggerName = triggerName;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.DROP);
    }

    @Override
    public void validate(ClientState state) throws RequestValidationException
    {
        ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
        Collection<String> classes = Schema.instance.getCFMetaData(keyspace(), columnFamily()).getTriggerClass();
        if (classes == null)
            throw new RequestValidationException(ExceptionCode.CONFIG_ERROR, "No triggers found") {};
    }

    public void announceMigration() throws InvalidRequestException, ConfigurationException
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).clone();
        Map<String, String> existingTriggers = cfm.getTriggers();
        assert existingTriggers.get(triggerName) != null;
        existingTriggers.remove(triggerName);
        cfm.triggers(existingTriggers);
        logger.info("Dropping trigger with name {}", triggerName);
        MigrationManager.announceColumnFamilyUpdate(cfm, false);
    }

    public ResultMessage.SchemaChange.Change changeType()
    {
        return ResultMessage.SchemaChange.Change.DROPPED;
    }
}
