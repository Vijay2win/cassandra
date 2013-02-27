package org.apache.cassandra.net;

import java.util.List;

public interface IResponseTransformer<T>
{
    public void timeout();

    public T transform(List<Object> responses);
}
