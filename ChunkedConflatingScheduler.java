package com.rsc;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Chunked, per-producer FIFO scheduler with single in-flight per producer and latest-only conflation per key.
 * <p>
 * - Producers are independent; there is no global producer order.
 * - Within a producer:
 * * FIFO for queued items
 * * At most one task in-flight
 * * If the in-flight task yields (not finished), it is requeued at the head
 * * Conflation:
 * - If a key is queued: replace its payload (latest-wins) without adding another entry
 * - If a key is in-flight: stage the latest and apply it on the next scheduling point
 */
public final class ChunkedConflatingScheduler implements AutoCloseable
{

    // ---------- Public API types ----------

    /**
     * A unit of work that supports time-sliced execution.
     * Return true when the task is fully complete; false to yield and be rescheduled.
     */
    public interface Chunkable
    {
        boolean runChunk(long timeSliceMs) throws Exception;
    }

    // ---------- Configuration ----------

    private final long timeSliceMs;
    private final int capacityPerProducer; // max queued entries per producer (unique keys), not counting staged

    // ---------- Internal state ----------

    private static final class QueuedTask
    {
        final Object key;
        Chunkable task;

        QueuedTask(Object key, Chunkable task)
        {
            this.key = Objects.requireNonNull(key, "key");
            this.task = Objects.requireNonNull(task, "task");
        }
    }

    private static final class ProducerState
    {
        final Deque<QueuedTask> queue = new ArrayDeque<>();
        final Map<Object, QueuedTask> pendingByKey = new HashMap<>();   // keys currently in queue
        final Map<Object, Chunkable> stagedByKey = new HashMap<>();     // latest for keys currently in-flight

        boolean inFlight = false;
        Object inFlightKey = null;
    }

    private final Map<String, ProducerState> producers = new ConcurrentHashMap<>();
    private final Semaphore workSignal = new Semaphore(0);
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final ExecutorService workers;
    private Thread dispatcher;

    // ---------- Construction ----------

    public ChunkedConflatingScheduler(int workerThreads, long timeSliceMs, int capacityPerProducer)
    {
        if (workerThreads <= 0)
        {
            throw new IllegalArgumentException("workerThreads must be > 0");
        }
        if (timeSliceMs <= 0)
        {
            throw new IllegalArgumentException("timeSliceMs must be > 0");
        }
        if (capacityPerProducer <= 0)
        {
            throw new IllegalArgumentException("capacityPerProducer must be > 0");
        }

        this.timeSliceMs = timeSliceMs;
        this.capacityPerProducer = capacityPerProducer;

        this.workers = new ThreadPoolExecutor(
                workerThreads, workerThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                r -> {
                    Thread t = new Thread(r, "CCS-worker");
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy() // backpressure if queue fills
        );
    }

    // ---------- Lifecycle ----------

    public void start()
    {
        if (!running.compareAndSet(false, true))
        {
            return;
        }

        dispatcher = new Thread(this::dispatchLoop, "CCS-dispatcher");
        dispatcher.setDaemon(true);
        dispatcher.start();
    }

    @Override
    public void close()
    {
        shutdownGracefully(10, TimeUnit.SECONDS);
    }

    public void shutdownGracefully(long timeout, TimeUnit unit)
    {
        if (!running.compareAndSet(true, false))
        {
            // already stopped or not started
        }
        // Unblock dispatcher if waiting
        workSignal.release(1024);

        // Stop dispatcher
        if (dispatcher != null)
        {
            try
            {
                dispatcher.join(unit.toMillis(timeout));
            } catch (InterruptedException ie)
            {
                Thread.currentThread().interrupt();
            }
        }

        // Stop workers
        workers.shutdownNow();
        try
        {
            workers.awaitTermination(timeout, unit);
        } catch (InterruptedException ie)
        {
            Thread.currentThread().interrupt();
        }
    }

    // ---------- Submission ----------

    /**
     * Submit or conflate a task for a given producer and key.
     * Latest-wins semantics: if the key is queued, its payload is replaced; if in-flight, the latest is staged.
     *
     * @return true if accepted (enqueued or conflated or staged), false if dropped due to capacity.
     */
    public boolean submit(String producerId, Object key, Chunkable task)
    {
        Objects.requireNonNull(producerId, "producerId");
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(task, "task");

        ProducerState ps = producers.computeIfAbsent(producerId, k -> new ProducerState());

        boolean accepted;
        synchronized (ps)
        {
            // If the same key is in-flight, stage the latest (replace any prior staged for this key)
            if (ps.inFlight && Objects.equals(ps.inFlightKey, key))
            {
                ps.stagedByKey.put(key, task);
                accepted = true;
            }
            // If the key is already queued, replace its payload (conflate)
            else if (ps.pendingByKey.containsKey(key))
            {
                QueuedTask qt = ps.pendingByKey.get(key);
                qt.task = task; // latest-wins swap
                accepted = true;
            }
            // Otherwise, attempt to enqueue if capacity allows
            else if (ps.queue.size() < capacityPerProducer)
            {
                QueuedTask qt = new QueuedTask(key, task);
                ps.queue.addLast(qt);
                ps.pendingByKey.put(key, qt);
                accepted = true;
            }
            else
            {
                // Capacity full and no existing entry for this key to conflate; drop
                accepted = false;
            }
        }

        if (accepted)
        {
            workSignal.release();
        }
        return accepted;
    }

    /**
     * Optionally remove a producer when it is idle (no in-flight, no queue, no staged).
     * Returns true if removed; false if still busy or not present.
     */
    public boolean removeProducerIfIdle(String producerId)
    {
        ProducerState ps = producers.get(producerId);
        if (ps == null)
        {
            return false;
        }
        synchronized (ps)
        {
            if (!ps.inFlight && ps.queue.isEmpty() && ps.stagedByKey.isEmpty())
            {
                producers.remove(producerId, ps);
                return true;
            }
        }
        return false;
    }

    // ---------- Dispatcher ----------

    private void dispatchLoop()
    {
        while (running.get())
        {
            try
            {
                workSignal.acquire();
            } catch (InterruptedException ie)
            {
                if (!running.get())
                {
                    break;
                }
                Thread.currentThread().interrupt();
            }
            if (!running.get())
            {
                break;
            }
            dispatchOne();
        }
    }

    /**
     * Scan producers and dispatch the first eligible task.
     * Returns true if something was dispatched, false otherwise.
     */
    private boolean dispatchOne()
    {
        for (Map.Entry<String, ProducerState> entry : producers.entrySet())
        {
            ProducerState ps = entry.getValue();

            final QueuedTask taskToRun;
            synchronized (ps)
            {
                if (ps.inFlight || ps.queue.isEmpty())
                {
                    continue;
                }
                taskToRun = ps.queue.pollFirst();
                ps.pendingByKey.remove(taskToRun.key);
                ps.inFlight = true;
                ps.inFlightKey = taskToRun.key;
            }

            workers.submit(() -> runChunkAndReschedule(ps, taskToRun));
            return true;
        }
        return false;
    }

    private void runChunkAndReschedule(ProducerState ps, QueuedTask taskToRun)
    {
        boolean finished = true;
        try
        {
            finished = taskToRun.task.runChunk(timeSliceMs);
        } catch (Throwable t)
        {
            // Swallow to keep the scheduler healthy; consider a handler hook if you want custom logging
            finished = true; // treat as finished to avoid endless retries
        }

        synchronized (ps)
        {
            if (!finished)
            {
                // Apply staged latest (if any) before requeueing at the head
                Chunkable staged = ps.stagedByKey.remove(taskToRun.key);
                if (staged != null)
                {
                    taskToRun.task = staged;
                }

                ps.queue.addFirst(taskToRun);
                ps.pendingByKey.put(taskToRun.key, taskToRun);
            }
            else
            {
                // Completed: if there is a staged latest, enqueue it as a fresh item (capacity permitting)
                Chunkable staged = ps.stagedByKey.remove(taskToRun.key);
                if (staged != null && ps.queue.size() < capacityPerProducer)
                {
                    QueuedTask qt = new QueuedTask(taskToRun.key, staged);
                    ps.queue.addLast(qt);
                    ps.pendingByKey.put(qt.key, qt);
                }
            }
            ps.inFlight = false;
            ps.inFlightKey = null;
        }

        // Signal potential more work (either requeued same producer or other producers may have items)
        workSignal.release();
    }

    // ---------- Introspection (optional) ----------

    public int queuedSize(String producerId)
    {
        ProducerState ps = producers.get(producerId);
        if (ps == null)
        {
            return 0;
        }
        synchronized (ps)
        {
            return ps.queue.size();
        }
    }

    public boolean isIdle()
    {
        for (ProducerState ps : producers.values())
        {
            synchronized (ps)
            {
                if (ps.inFlight || !ps.queue.isEmpty() || !ps.stagedByKey.isEmpty())
                {
                    return false;
                }
            }
        }
        return true;
    }
}
