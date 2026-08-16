package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Custom thread pool with configurable queues, balancing, rejection policy,
 * and detailed logging.
 */
public class CustomThreadPoolExecutor implements CustomExecutor {

    private static final Logger LOG = Logger.getLogger(CustomThreadPoolExecutor.class.getName());

    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;

    private final List<BlockingQueue<Runnable>> workerQueues;
    private final List<Worker> workers;
    private final ReentrantLock mainLock = new ReentrantLock();

    private final ThreadFactory threadFactory;

    private final RejectedExecutionHandler rejectedHandler;

    private volatile boolean isShutdown = false;
    private volatile boolean isTerminated = false;
    private final AtomicInteger workerCount = new AtomicInteger(0);
    private final AtomicInteger taskCounter = new AtomicInteger(0);

    private int rrIndex = 0;

    public CustomThreadPoolExecutor(int corePoolSize, int maxPoolSize,
                                    long keepAliveTime, TimeUnit timeUnit,
                                    int queueSize, int minSpareThreads,
                                    ThreadFactory threadFactory,
                                    RejectedExecutionHandler rejectedHandler) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = threadFactory;
        this.rejectedHandler = rejectedHandler;

        this.workerQueues = new ArrayList<>();
        this.workers = new ArrayList<>();

        // Initialise core threads
        for (int i = 0; i < corePoolSize; i++) {
            addWorker(true);
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException();
        if (isShutdown) {
            rejectTask(command, "Executor is shut down");
            return;
        }

        int currentWorkerCount = workerCount.get();
        int activeWorkers = (int) workers.stream().filter(w -> w.isActive()).count();
        int spare = activeWorkers - getQueueSizes().stream().mapToInt(Integer::intValue).sum();

        if (spare < minSpareThreads && currentWorkerCount < maxPoolSize) {
            addWorker(false);
        }

        boolean offered = offerTaskToQueue(command);
        if (!offered) {
            rejectedHandler.rejectedExecution(command, this);
            return;
        }

        wakeUpWorkers();
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) throw new NullPointerException();
        FutureTask<T> future = new FutureTask<>(callable);
        execute(future);
        return future;
    }

    @Override
    public void shutdown() {
        mainLock.lock();
        try {
            if (isShutdown) return;
            isShutdown = true;
            LOG.info("[Pool] Shutdown initiated.");
            for (Worker w : workers) {
                if (!w.isActive()) {
                    w.interrupt();
                }
            }
        } finally {
            mainLock.unlock();
        }
    }

    @Override
    public void shutdownNow() {
        mainLock.lock();
        try {
            if (isShutdown && isTerminated) return;
            isShutdown = true;
            LOG.info("[Pool] ShutdownNow initiated.");
            for (Worker w : workers) {
                w.interrupt();
                w.workerQueue.clear();
            }
            isTerminated = true;
        } finally {
            mainLock.unlock();
        }
    }

    private void addWorker(boolean isCore) {
        mainLock.lock();
        try {
            if (isShutdown) return;
            if (workerCount.get() >= maxPoolSize) return;
            if (isCore && workerCount.get() >= corePoolSize) return;

            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
            Worker worker = new Worker(queue);
            Thread t = threadFactory.newThread(worker);
            worker.setThread(t);
            workerQueues.add(queue);
            workers.add(worker);
            workerCount.incrementAndGet();
            t.start();
            LOG.info(String.format("[Pool] Worker added: %s (total: %d)", t.getName(), workerCount.get()));
        } finally {
            mainLock.unlock();
        }
    }

    private boolean offerTaskToQueue(Runnable task) {
        mainLock.lock();
        try {
            if (workerQueues.isEmpty()) return false;

            int idx = rrIndex % workerQueues.size();
            rrIndex++;
            BlockingQueue<Runnable> q = workerQueues.get(idx);
            boolean offered = q.offer(task);
            if (offered) {
                LOG.info(String.format("[Pool] Task #%d accepted into queue %d: %s",
                        taskCounter.incrementAndGet(), idx, task));
                return true;
            }
            return false;
        } finally {
            mainLock.unlock();
        }
    }

    private void wakeUpWorkers() {
        mainLock.lock();
        try {
            for (Worker w : workers) {
                if (!w.isActive()) {
                    w.interrupt();
                    break;
                }
            }
        } finally {
            mainLock.unlock();
        }
    }

    private List<Integer> getQueueSizes() {
        List<Integer> sizes = new ArrayList<>();
        for (BlockingQueue<Runnable> q : workerQueues) {
            sizes.add(q.size());
        }
        return sizes;
    }

    private void rejectTask(Runnable task, String reason) {
        LOG.warning(String.format("[Rejected] Task %s rejected: %s", task, reason));
    }

    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> workerQueue;
        private Thread thread;
        private volatile boolean isActive = false;

        Worker(BlockingQueue<Runnable> queue) {
            this.workerQueue = queue;
        }

        void setThread(Thread t) { this.thread = t; }
        void interrupt() { if (thread != null) thread.interrupt(); }
        boolean isActive() { return isActive; }

        @Override
        public void run() {
            try {
                while (!isShutdown || !workerQueue.isEmpty()) {
                    Runnable task = null;
                    try {
                        isActive = true;
                        task = workerQueue.poll(keepAliveTime, timeUnit);
                        if (task == null) {
                            LOG.info(String.format("[Worker] %s idle timeout, stopping.", thread.getName()));
                            break;
                        }
                        LOG.info(String.format("[Worker] %s executes %s", thread.getName(), task));
                        task.run();
                        LOG.info(String.format("[Worker] %s finished %s", thread.getName(), task));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } finally {
                        isActive = false;
                    }
                }
            } finally {
                // Worker termination
                mainLock.lock();
                try {
                    workerCount.decrementAndGet();
                    workerQueues.remove(workerQueue);
                    workers.remove(this);
                    LOG.info(String.format("[Worker] %s terminated. Remaining: %d",
                            thread.getName(), workerCount.get()));

                    if (!isShutdown && workerCount.get() < corePoolSize) {
                        addWorker(true);
                    }
                } finally {
                    mainLock.unlock();
                }
            }
        }
    }


    @FunctionalInterface
    public interface RejectedExecutionHandler {
        void rejectedExecution(Runnable r, CustomThreadPoolExecutor executor);
    }

    public static class CallerRunsPolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, CustomThreadPoolExecutor executor) {
            LOG.info(String.format("[Rejected] Task %s runs in caller thread", r));
            r.run();
        }
    }
}