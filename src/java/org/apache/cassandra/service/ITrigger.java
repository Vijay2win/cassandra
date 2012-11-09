package org.apache.cassandra.service;

import java.util.Collection;

import org.apache.cassandra.db.RowMutation;

public interface ITrigger
{
    public Collection<RowMutation> augment(RowMutation update);
}
