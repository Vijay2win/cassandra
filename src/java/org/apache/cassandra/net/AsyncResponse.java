package org.apache.cassandra.net;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.transport.Message.Response;

public abstract class AsyncResponse<R>
{
    private int waitBefore = 1;
    protected IResponseTransformer<R> transformer;
    protected List results = new ArrayList();

    public void timeout()
    {
        transformer.timeout();
    }

    public void setTransformer(IResponseTransformer<R> transformer)
    {
        this.transformer = transformer;
    }

    public IResponseTransformer<R> getTransformer()
    {
        return transformer;
    }

    public synchronized void result(Object result)
    {
        results.add(result);
        if (waitBefore == results.size())
            respond();
    }

    public void setWaitBefore(int waitBefore)
    {
        this.waitBefore = waitBefore;
    }

    public abstract void respond();

    public abstract void respond(Response response);

    public abstract void setTracingId(UUID tracingId);

}
