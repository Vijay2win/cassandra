package org.apache.cassandra.metrics;

import java.net.InetAddress;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.SystemTable;
import org.apache.log4j.lf5.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;

/**
 * Metrics for {@code HintedHandOffManager}.
 */
public class HintedHandoffMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(HintedHandoffMetrics.class);

    public static final String GROUP_NAME = "org.apache.cassandra.metrics";
    public static final String TYPE_NAME = "HintedHandOffManager";

    /** Total number of hint requests */
    private final Meter requests = Metrics.newMeter(new MetricName(GROUP_NAME, TYPE_NAME, "Requests"), "requests", TimeUnit.SECONDS);
    /** Total number of hints which not stored */
    private final LoadingCache<InetAddress, DifferencingMeter> notStored = CacheBuilder.newBuilder().build(new CacheLoader<InetAddress, DifferencingMeter>()
    {
        public DifferencingMeter load(InetAddress address)
        {
            return new DifferencingMeter(address);
        }
    });

    public void incrHintsCount()
    {
        requests.mark();
    }

    public long hintsCount()
    {
        return requests.count();
    }

    public void incrPastWindow(InetAddress address)
    {
        try
        {
            notStored.get(address).mark();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e); // this cannot happen
        }
    }

    public void log()
    {
        for (Entry<InetAddress, DifferencingMeter> entry : notStored.asMap().entrySet())
        {
            long diffrence = entry.getValue().diffrence();
            if (diffrence == 0)
                continue;
            String message = String.format("%s has %s dropped hints, because node is down past configured hint window.", entry.getKey(), diffrence);
            logger.warn(message);
            SystemTable.updateAuditInfo(LogLevel.WARN, "Dropped Hints", message);
        }
    }

    public class DifferencingMeter
    {
        private final Meter meter;
        private long reported = 0;

        public DifferencingMeter(InetAddress address)
        {
            this.meter = Metrics.newMeter(new MetricName(GROUP_NAME, TYPE_NAME, "Hints_not_stored-" + address.toString()), "hints_not_stored", TimeUnit.SECONDS);
        }

        public long diffrence()
        {
            long current = meter.count();
            long diffrence = current - reported;
            this.reported = current;
            return diffrence;
        }

        public long count()
        {
            return meter.count();
        }

        public void mark()
        {
            meter.mark();
        }
    }
}
