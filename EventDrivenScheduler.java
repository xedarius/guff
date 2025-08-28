package com.rsc;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventDrivenScheduler
{
    private final Map<String, ProducerState> producers = new ConcurrentHashMap<>();
    private final BlockingQueue<String> activeProducers = new LinkedBlockingQueue<>();
    private final Set<String> activeSet = ConcurrentHashMap.newKeySet();

    private final ExecutorService workers;
    private final long timeSliceMs;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public EventDrivenScheduler(int workerThreads, long timeSliceMs)
    {
        this.workers = Executors.newFixedThreadPool(workerThreads);
        this.timeSliceMs = timeSliceMs;
    }

    private static class QueuedTask
    {
        final Object key;
        final Chunkable task;

        QueuedTask(Object key, Chunkable task)
        {
            this.key = key;
            this.task = task;
        }
    }

    private static class ProducerState
    {
        final Deque<QueuedTask> queue = new ArrayDeque<>();
        boolean inFlight = false;
        Object inFlightKey = null;
    }

    public interface Chunkable
    {
        /**
         * @return true if finished, false if needs more chunks
         */
        boolean runChunk(long timeSliceMs);
    }

    public boolean submit(String producerId, Object key, Chunkable task)
    {
        ProducerState ps = producers.computeIfAbsent(producerId, k -> new ProducerState());
        synchronized (ps)
        {
            if (ps.queue.stream().anyMatch(qt -> qt.key.equals(key)) ||
                    (ps.inFlight && ps.inFlightKey.equals(key)))
            {
                // Drop duplicate work for same key
                return false;
            }
            ps.queue.addLast(new QueuedTask(key, task));
        }
        if (activeSet.add(producerId))
        {
            activeProducers.offer(producerId);
        }
        return true;
    }

    public void start()
    {
        if (running.compareAndSet(false, true))
        {
            Thread dispatcher = new Thread(this::runLoop, "dispatcher");
            dispatcher.start();
        }
    }

    public void stop()
    {
        running.set(false);
        workers.shutdown();
    }

    private void runLoop()
    {
        try
        {
            while (running.get())
            {
                String pid = activeProducers.take(); // blocks until active producer
                ProducerState ps = producers.get(pid);
                if (ps == null)
                {
                    activeSet.remove(pid);
                    continue;
                }
                boolean stillHasWork = dispatchFromProducer(pid, ps);
                if (stillHasWork)
                {
                    activeProducers.offer(pid);
                }
                else
                {
                    activeSet.remove(pid);
                }
            }
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private boolean dispatchFromProducer(String pid, ProducerState ps)
    {
        QueuedTask taskToRun;
        synchronized (ps)
        {
            if (ps.inFlight || ps.queue.isEmpty())
            {
                return !ps.queue.isEmpty();
            }
            taskToRun = ps.queue.pollFirst();
            ps.inFlight = true;
            ps.inFlightKey = taskToRun.key;
        }

        workers.submit(() -> {
            boolean finished = taskToRun.task.runChunk(timeSliceMs);
            synchronized (ps)
            {
                if (!finished)
                {
                    // Requeue unfinished task at head
                    ps.queue.addFirst(taskToRun);
                }
                ps.inFlight = false;
                ps.inFlightKey = null;
            }
            if (!ps.queue.isEmpty())
            {
                if (activeSet.add(pid))
                {
                    activeProducers.offer(pid);
                }
            }
        });
        return false;
    }
}
