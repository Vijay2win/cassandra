package org.apache.cassandra.concurrent;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.MultiThreadedLowContentionClaimStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorExecutor extends AbstractExecutorService implements ExecutorService
{
    private Disruptor<RunnableContainer> disruptor;
    private final RingBuffer<RunnableContainer> ringBuffer;
    private final EventFactory<RunnableContainer> factory = new EventFactory<RunnableContainer>()
    {
        public RunnableContainer newInstance()
        {
            return new RunnableContainer(null);
        }
    };
    private volatile boolean isShutdown = false;

    public DisruptorExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory)
    {
        this.disruptor = new Disruptor<RunnableContainer>(factory, Executors.newCachedThreadPool(), new MultiThreadedLowContentionClaimStrategy(
                maximumPoolSize * 2), new YieldingWaitStrategy());
        EventHandler<RunnableContainer>[] handlers = new EventHandler[maximumPoolSize];
        for (int i = 0; i < handlers.length; i++)
        {
            handlers[i] = new EventHandler<RunnableContainer>()
            {
                public void onEvent(final RunnableContainer event, final long sequence, final boolean endOfBatch) throws Exception
                {
                    event.run();
                }
            };
        }
        disruptor.handleEventsWith(handlers);
        this.ringBuffer = disruptor.start();
    }

    public static final class RunnableContainer implements Runnable
    {
        private volatile Runnable runnable;

        public RunnableContainer(Runnable runnable)
        {
            this.runnable = runnable;
        }

        @Override
        public void run()
        {
            runnable.run();
        }

        public void setRunnable(Runnable runnable)
        {
            this.runnable = runnable;
        }
    }

    @Override
    public void execute(Runnable command)
    {
        long sequence = ringBuffer.next();
        RunnableContainer event = ringBuffer.get(sequence);
        event.setRunnable(command);
        ringBuffer.publish(sequence);
    }

    @Override
    public void shutdown()
    {
        isShutdown = true;
        disruptor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        isShutdown = true;
        disruptor.shutdown();
        return Lists.<Runnable>newArrayList();
    }

    @Override
    public boolean isShutdown()
    {
        return isShutdown;
    }

    @Override
    public boolean isTerminated()
    {
        return isShutdown;
    }

    
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        isShutdown = true;
        disruptor.shutdown();
        return true;
    }
}
