import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;

public class CustomThreadPool implements CustomExecutor {
    private static final Logger logger = Logger.getLogger(CustomThreadPool.class.getName());

    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;
    private final RejectedExecutionHandler rejectedHandler;
    private final ThreadFactory threadFactory;

    private final List<BlockingQueue<Runnable>> taskQueues;
    private final List<Worker> workers;
    private final AtomicInteger workerIndex = new AtomicInteger(0);
    private final AtomicInteger taskCounter = new AtomicInteger(0);
    private final ReentrantLock lock = new ReentrantLock();

    private volatile boolean isShutdown = false;
    private volatile boolean isShutdownNow = false;

    // Алгоритм балансировки
    private enum BalancingStrategy { ROUND_ROBIN, LEAST_LOADED }
    private final BalancingStrategy strategy;

    public CustomThreadPool(Builder builder) {
        this.corePoolSize = builder.corePoolSize;
        this.maxPoolSize = builder.maxPoolSize;
        this.keepAliveTime = builder.keepAliveTime;
        this.timeUnit = builder.timeUnit;
        this.queueSize = builder.queueSize;
        this.minSpareThreads = builder.minSpareThreads;
        this.rejectedHandler = builder.rejectedHandler;
        this.threadFactory = builder.threadFactory != null ? builder.threadFactory : new DefaultThreadFactory();
        this.strategy = builder.strategy;

        this.taskQueues = new ArrayList<>(maxPoolSize);
        this.workers = new ArrayList<>(maxPoolSize);

        // Инициализация очередей
        for (int i = 0; i < maxPoolSize; i++) {
            taskQueues.add(new LinkedBlockingQueue<>(queueSize));
        }

        // Создание core потоков
        for (int i = 0; i < corePoolSize; i++) {
            Worker worker = createWorker();
            workers.add(worker);
            worker.start();
        }

        logger.info(String.format("[Pool] Initialized with corePoolSize=%d, maxPoolSize=%d, queueSize=%d, keepAliveTime=%d%s",
                corePoolSize, maxPoolSize, queueSize, keepAliveTime, timeUnit.toString()));
    }

    private Worker createWorker() {
        Thread thread = threadFactory.newThread(() -> {});
        Worker worker = new Worker(thread);
        return worker;
    }

    @Override
    public void execute(Runnable command) {
        if (isShutdown || isShutdownNow) {
            logger.warning("[Pool] Task rejected - pool is shutdown");
            throw new RejectedExecutionException("Pool is shutdown");
        }

        if (command == null) {
            throw new NullPointerException("Command cannot be null");
        }

        int taskId = taskCounter.incrementAndGet();
        logger.info(String.format("[Pool] Task #%d accepted: %s", taskId, command.toString()));

        // Выбор очереди согласно стратегии балансировки
        BlockingQueue<Runnable> targetQueue = selectQueue();

        boolean offered = targetQueue.offer(command);

        if (!offered) {
            // Очередь переполнена - применяем политику отказа
            handleRejected(command, taskId);
            return;
        }

        // Проверка необходимости создания новых потоков
        checkAndCreateThreads();
    }

    private BlockingQueue<Runnable> selectQueue() {
        if (strategy == BalancingStrategy.ROUND_ROBIN) {
            int index = workerIndex.getAndIncrement() % workers.size();
            return taskQueues.get(index);
        } else { // LEAST_LOADED
            int minSize = Integer.MAX_VALUE;
            int selectedIndex = 0;
            for (int i = 0; i < taskQueues.size(); i++) {
                int size = taskQueues.get(i).size();
                if (size < minSize) {
                    minSize = size;
                    selectedIndex = i;
                }
            }
            return taskQueues.get(selectedIndex);
        }
    }

    private void checkAndCreateThreads() {
        lock.lock();
        try {
            if (isShutdown || isShutdownNow) return;

            int activeThreads = (int) workers.stream().filter(w -> w.isActive()).count();
            int totalThreads = workers.size();
            int idleThreads = totalThreads - activeThreads;

            // Создание потоков при недостатке резервных
            if (idleThreads < minSpareThreads && totalThreads < maxPoolSize) {
                int toCreate = Math.min(minSpareThreads - idleThreads, maxPoolSize - totalThreads);
                for (int i = 0; i < toCreate; i++) {
                    Worker worker = createWorker();
                    workers.add(worker);
                    worker.start();
                    logger.info(String.format("[Pool] Created additional worker, total threads: %d", workers.size()));
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void handleRejected(Runnable command, int taskId) {
        if (rejectedHandler != null) {
            rejectedHandler.rejectedExecution(command, this);
        } else {
            logger.warning(String.format("[Rejected] Task #%d was rejected due to overload!", taskId));
            throw new RejectedExecutionException("Task rejected due to overload");
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> futureTask = new FutureTask<>(callable);
        execute(futureTask);
        return futureTask;
    }

    @Override
    public void shutdown() {
        logger.info("[Pool] Shutdown initiated");
        isShutdown = true;

        // Прерывание idle потоков
        for (Worker worker : workers) {
            if (!worker.isActive()) {
                worker.interrupt();
            }
        }
    }

    @Override
    public void shutdownNow() {
        logger.warning("[Pool] ShutdownNow initiated - immediate stop");
        isShutdownNow = true;
        isShutdown = true;

        for (Worker worker : workers) {
            worker.interrupt();
            // Очистка очередей
            for (BlockingQueue<Runnable> queue : taskQueues) {
                queue.clear();
            }
        }
    }

    // Внутренний класс Worker
    private class Worker extends Thread {
        private final Thread thread;
        private volatile boolean isActive = false;
        private long lastTaskTime = System.currentTimeMillis();
        private final BlockingQueue<Runnable> queue;

        public Worker(Thread thread) {
            this.thread = thread;
            this.queue = taskQueues.get(workers.size());
            this.setName(thread.getName());
        }

        @Override
        public void run() {
            logger.info(String.format("[Worker] %s started", thread.getName()));

            while (!isShutdownNow && !isShutdown) {
                try {
                    isActive = false;

                    // Проверка на завершение idle потоков
                    if (shouldTerminate()) {
                        logger.info(String.format("[Worker] %s idle timeout, stopping.", thread.getName()));
                        break;
                    }

                    // Получение задачи с таймаутом
                    Runnable task = queue.poll(keepAliveTime, timeUnit);

                    if (task == null) {
                        continue;
                    }

                    isActive = true;
                    lastTaskTime = System.currentTimeMillis();

                    // Выполнение задачи
                    logger.info(String.format("[Worker] %s executes %s", thread.getName(), task.toString()));
                    task.run();
                    logger.info(String.format("[Worker] %s completed task", thread.getName()));

                } catch (InterruptedException e) {
                    if (isShutdownNow || isShutdown) {
                        logger.info(String.format("[Worker] %s interrupted due to shutdown", thread.getName()));
                        break;
                    }
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    logger.log(Level.SEVERE, String.format("[Worker] %s error: %s", thread.getName(), e.getMessage()), e);
                }
            }

            logger.info(String.format("[Worker] %s terminated.", thread.getName()));
        }

        private boolean shouldTerminate() {
            if (workers.size() <= corePoolSize) {
                return false;
            }

            long idleTime = System.currentTimeMillis() - lastTaskTime;
            return idleTime > timeUnit.toMillis(keepAliveTime);
        }

        public boolean isActive() {
            return isActive || !queue.isEmpty();
        }

        public void interrupt() {
            thread.interrupt();
        }
    }

    // Builder для настройки пула
    public static class Builder {
        private int corePoolSize = 2;
        private int maxPoolSize = 4;
        private long keepAliveTime = 5;
        private TimeUnit timeUnit = TimeUnit.SECONDS;
        private int queueSize = 10;
        private int minSpareThreads = 1;
        private RejectedExecutionHandler rejectedHandler = null;
        private ThreadFactory threadFactory = null;
        private BalancingStrategy strategy = BalancingStrategy.ROUND_ROBIN;

        public Builder corePoolSize(int corePoolSize) {
            this.corePoolSize = corePoolSize;
            return this;
        }

        public Builder maxPoolSize(int maxPoolSize) {
            this.maxPoolSize = maxPoolSize;
            return this;
        }

        public Builder keepAliveTime(long keepAliveTime, TimeUnit timeUnit) {
            this.keepAliveTime = keepAliveTime;
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder queueSize(int queueSize) {
            this.queueSize = queueSize;
            return this;
        }

        public Builder minSpareThreads(int minSpareThreads) {
            this.minSpareThreads = minSpareThreads;
            return this;
        }

        public Builder rejectedHandler(RejectedExecutionHandler handler) {
            this.rejectedHandler = handler;
            return this;
        }

        public Builder threadFactory(ThreadFactory factory) {
            this.threadFactory = factory;
            return this;
        }

        public Builder useRoundRobin() {
            this.strategy = BalancingStrategy.ROUND_ROBIN;
            return this;
        }

        public Builder useLeastLoaded() {
            this.strategy = BalancingStrategy.LEAST_LOADED;
            return this;
        }

        public CustomThreadPool build() {
            return new CustomThreadPool(this);
        }
    }
}