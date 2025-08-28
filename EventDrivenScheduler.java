import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class EventDrivenScheduler {

    // One state per producer
    private static class ProducerState {
        final Deque<Chunkable> queue = new ArrayDeque<>();
        boolean inFlight = false;
    }

    // Task contract: do up to timeSliceMs work; return true when fully done.
    public interface Chunkable {
        boolean runChunk(long timeSliceMs);
    }

    private final Map<String, ProducerState> producers = new ConcurrentHashMap<>();

    // Active producers that (may) have runnable work. We keep a set to avoid duplicates.
    private final BlockingQueue<String> activeProducers = new LinkedBlockingQueue<>();
    private final Set<String> activeSet = ConcurrentHashMap.newKeySet();

    private final ExecutorService workers;
    private final long timeSliceMs;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread dispatcherThread;

    public EventDrivenScheduler(int workerThreads, long timeSliceMs) {
        this.workers = Executors.newFixedThreadPool(workerThreads);
        this.timeSliceMs = timeSliceMs;
    }

    // Submit a task to a producer (FIFO). No conflation, no key logic.
    public void submit(String producerId, Chunkable task) {
        ProducerState ps = producers.computeIfAbsent(producerId, k -> new ProducerState());
        synchronized (ps) {
            ps.queue.addLast(task);
        }
        // Mark producer active if not already queued for dispatch
        ensureActive(producerId);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        dispatcherThread = new Thread(this::runLoop, "event-dispatcher");
        dispatcherThread.setDaemon(true);
        dispatcherThread.start();
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        workers.shutdown();
        if (dispatcherThread != null) {
            dispatcherThread.interrupt();
        }
    }

    private void ensureActive(String producerId) {
        if (activeSet.add(producerId)) {
            activeProducers.offer(producerId);
        }
    }

    private void runLoop() {
        try {
            while (running.get()) {
                String pid = activeProducers.take();   // blocks until some producer is active
                // Critical: remove from active set immediately upon dequeue.
                // This allows completion or new submissions to re-enqueue if needed.
                activeSet.remove(pid);

                ProducerState ps = producers.get(pid);
                if (ps == null) continue;

                dispatchOneFromProducer(pid, ps);
                // Do not re-offer here. Only submission or completion re-enqueues.
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    private void dispatchOneFromProducer(String pid, ProducerState ps) {
        Chunkable taskToRun = null;
        synchronized (ps) {
            if (ps.inFlight || ps.queue.isEmpty()) {
                return; // Either busy or no work; completion/submission will re-enqueue as needed
            }
            taskToRun = ps.queue.pollFirst();
            ps.inFlight = true;
        }

        final Chunkable run = taskToRun;
        workers.submit(() -> {
            boolean finished = false;
            try {
                finished = run.runChunk(timeSliceMs);
            } catch (Throwable t) {
                // Treat exceptions as task finished; you could add logging/metrics here.
                finished = true;
            }

            boolean hasMore;
            synchronized (ps) {
                if (!finished) {
                    // Place incomplete task back at the head (chunking semantics)
                    ps.queue.addFirst(run);
                }
                ps.inFlight = false;
                hasMore = !ps.queue.isEmpty();
            }

            // If producer still has work, re-enqueue it (idempotent thanks to activeSet)
            if (hasMore) {
                ensureActive(pid);
            }
        });
    }
}
