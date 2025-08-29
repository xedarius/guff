package com.rsc;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class ChunkingMultiWorkerScheduler
{

    public interface Chunkable
    {
        /**
         * @return true if finished; false to yield and resume later
         */
        boolean runChunk(long timeSliceMs);
    }

    private static final class ProducerState
    {
        final AtomicBoolean enqueued = new AtomicBoolean(false);
        final Deque<Chunkable> queue = new ArrayDeque<>();
    }

    private final ConcurrentMap<String, ProducerState> producers = new ConcurrentHashMap<>();
    private final BlockingQueue<String> activeQueue = new LinkedBlockingQueue<>();
    private final ExecutorService workers;
    private final long timeSliceMs;
    private final BiConsumer<String, Throwable> errorHandler;

    public ChunkingMultiWorkerScheduler(int workerCount,
                                        long timeSliceMs,
                                        BiConsumer<String, Throwable> errorHandler)
    {
        this.workers = Executors.newFixedThreadPool(workerCount);
        this.timeSliceMs = timeSliceMs;
        this.errorHandler = errorHandler;
        for (int i = 0; i < workerCount; i++)
        {
            workers.submit(this::workerLoop);
        }
    }

    public void submit(String producerId, Chunkable task)
    {
        ProducerState ps = producers.computeIfAbsent(producerId, id -> new ProducerState());
        synchronized (ps)
        {
            ps.queue.addLast(task);
        }
        if (ps.enqueued.compareAndSet(false, true))
        {
            activeQueue.offer(producerId);
        }
    }

    private void workerLoop()
    {
        try
        {
            while (!Thread.currentThread().isInterrupted())
            {
                String pid = activeQueue.take();
                ProducerState ps = producers.get(pid);
                if (ps == null)
                {
                    continue;
                }
                dispatchFromProducer(pid, ps);
            }
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private void dispatchFromProducer(String pid, ProducerState ps)
    {
        Chunkable task;
        synchronized (ps)
        {
            task = ps.queue.pollFirst();
            if (task == null)
            {
                ps.enqueued.set(false);
                return;
            }
        }
        boolean finished = false;
        try
        {
            finished = task.runChunk(timeSliceMs);
        } catch (Throwable t)
        {
            errorHandler.accept(pid, t);
            finished = true;
        }
        synchronized (ps)
        {
            if (!finished)
            {
                ps.queue.addFirst(task); // unfinished: back to head
            }
        }
        // re‑activate if more work remains
        if (!ps.queue.isEmpty() && ps.enqueued.compareAndSet(false, true))
        {
            activeQueue.offer(pid);
        }
        else
        {
            ps.enqueued.set(false);
        }
    }

    public void shutdown()
    {
        workers.shutdownNow();
    }
}
