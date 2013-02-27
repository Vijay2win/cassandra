package org.apache.cassandra.transport;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.net.AsyncResponse;
import org.apache.cassandra.net.IResponseTransformer;
import org.apache.cassandra.transport.Message.Response;
import org.apache.cassandra.utils.SimpleCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncResponse extends AsyncResponse<Object>
{
    protected static final Logger logger = LoggerFactory.getLogger(SyncResponse.class);
    private SimpleCondition condition = new SimpleCondition();
    public UUID tracingId;

    @Override
    public void respond()
    {
        condition.signal();
    }

    public void respond(Response response)
    {
        throw new UnsupportedOperationException();
    }

    public void setTracingId(UUID tracingId)
    {
        this.tracingId = tracingId;
    }

    public List getResponse()
    {
        try
        {
            condition.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        return results;
    }
}
