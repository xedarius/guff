package com.rsc;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

/**
 * Multi-worker fair scheduler:
 * - Per-producer FIFO with ordering preserved per producer.
 * - Producers are activated at most once via an enqueued flag.
 * - N workers drain an active producer queue for parallel throughput.
 * - Batched dispatch amortizes queue hops/context switches.
 */
public final class FairTaskScheduler implements AutoCloseable {

    // Tunables
    private final int numWorkers;
    private final int batchSize;
    private final long idleParkNanos;

    // Core state
    private final ConcurrentHashMap<String, ProducerState> producers = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> activeQueue = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean draining = new AtomicBoolean(false);
    private final Thread[] workers;

    // Error handling
    private final BiConsumer<String, Throwable> errorHandler;

    // Metrics
    private final LongAdder tasksProcessed = new LongAdder();
    private final LongAdder activations = new LongAdder();
    private final LongAdder reactivations = new LongAdder();
    private final LongAdder emptyPolls = new LongAdder();

    public FairTaskScheduler(int numWorkers, int batchSize, long idleParkNanos,
                             BiConsumer<String, Throwable> errorHandler) {
        if (numWorkers <= 0) throw new IllegalArgumentException("numWorkers must be > 0");
        if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be > 0");
        if (idleParkNanos < 0) throw new IllegalArgumentException("idleParkNanos must be >= 0");
        this.numWorkers = numWorkers;
        this.batchSize = batchSize;
        this.idleParkNanos = idleParkNanos;
        this.errorHandler = errorHandler != null ? errorHandler : (pid, t) -> t.printStackTrace();
        this.workers = new Thread[numWorkers];
        for (int i = 0; i < numWorkers; i++) {
            int idx = i;
            workers[i] = new Thread(this::runLoop, "FairScheduler-Worker-" + idx);
            workers[i].setDaemon(true);
        }
    }

    public FairTaskScheduler(int numWorkers) {
        this(numWorkers, 4, TimeUnit.MICROSECONDS.toNanos(50), null);
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            for (Thread t : workers) t.start();
        }
    }

    /**
     * Stop the scheduler.
     * @param drain If true, finish processing queued tasks before returning from awaitTermination.
     */
    public void stop(boolean drain) {
        draining.set(drain);
        running.set(false);
        // Unpark workers quickly
        for (Thread t : workers) {
            if (t != null) t.interrupt();
        }
    }

    @Override
    public void close() {
        stop(false);
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        for (Thread t : workers) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) return false;
            t.join(TimeUnit.NANOSECONDS.toMillis(remaining));
        }
        return true;
    }

    public void submit(String producerId, Runnable task) {
        if (producerId == null || task == null) throw new NullPointerException();
        if (!running.get() && !draining.get()) {
            throw new RejectedExecutionException("Scheduler is not running");
        }
        ProducerState ps = producers.computeIfAbsent(producerId, id -> new ProducerState());
        ps.queue.offer(task);
        markActive(producerId, ps);
    }

    public Metrics metrics() {
        long pending = producers.values().stream().mapToLong(ps -> ps.queue.size()).sum();
        return new Metrics(tasksProcessed.sum(), activations.sum(), reactivations.sum(),
                emptyPolls.sum(), pending, activeQueue.size(), producers.size());
    }

    public int pendingFor(String producerId) {
        ProducerState ps = producers.get(producerId);
        return ps == null ? 0 : ps.queue.size();
    }

    // -------- internal --------

    private void markActive(String pid, ProducerState ps) {
        if (ps.enqueued.compareAndSet(false, true)) {
            activeQueue.offer(pid);
            activations.increment();
        }
    }

    private void runLoop() {
        final boolean supportDrain = true;
        while (true) {
            String pid = activeQueue.poll();

            if (pid == null) {
                emptyPolls.increment();
                if (!running.get()) {
                    if (!supportDrain || !draining.get()) break;
                    // Drain mode: exit only when no pending work remains
                    if (allQueuesEmpty()) break;
                }
                // Light idle strategy
                if (idleParkNanos > 0) {
                    try {
                        LockSupport.parkNanos(idleParkNanos);
                    } catch (Throwable ignored) {
                        // interrupted: loop will re-check conditions
                    }
                }
                continue;
            }

            ProducerState ps = producers.get(pid);
            if (ps == null) {
                // Producer may have been evicted; skip
                continue;
            }

            int processed = 0;
            while (processed < batchSize) {
                Runnable task = ps.queue.poll();
                if (task == null) break;
                try {
                    task.run();
                } catch (Throwable t) {
                    errorHandler.accept(pid, t);
                } finally {
                    tasksProcessed.increment();
                }
                processed++;
            }

            // Hand-off protocol: allow others to reactivate if new work arrives.
            ps.enqueued.set(false);
            if (!ps.queue.isEmpty() && ps.enqueued.compareAndSet(false, true)) {
                activeQueue.offer(pid);
                reactivations.increment();
            }
        }
    }

    private boolean allQueuesEmpty() {
        if (!activeQueue.isEmpty()) return false;
        for (ProducerState ps : producers.values()) {
            if (!ps.queue.isEmpty() || ps.enqueued.get()) return false;
        }
        return true;
    }

    private static final class ProducerState {
        final AtomicBoolean enqueued = new AtomicBoolean(false);
        final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();
    }

    // Simple immutable snapshot
    public static final class Metrics {
        public final long tasksProcessed;
        public final long activations;
        public final long reactivations;
        public final long emptyPolls;
        public final long pendingTasks;
        public final int activeQueueSize;
        public final int producersRegistered;

        Metrics(long tasksProcessed, long activations, long reactivations, long emptyPolls,
                long pendingTasks, int activeQueueSize, int producersRegistered) {
            this.tasksProcessed = tasksProcessed;
            this.activations = activations;
            this.reactivations = reactivations;
            this.emptyPolls = emptyPolls;
            this.pendingTasks = pendingTasks;
            this.activeQueueSize = activeQueueSize;
            this.producersRegistered = producersRegistered;
        }

        @Override public String toString() {
            return "Metrics{" +
                    "tasksProcessed=" + tasksProcessed +
                    ", activations=" + activations +
                    ", reactivations=" + reactivations +
                    ", emptyPolls=" + emptyPolls +
                    ", pendingTasks=" + pendingTasks +
                    ", activeQueueSize=" + activeQueueSize +
                    ", producersRegistered=" + producersRegistered +
                    '}';
        }
    }
}
