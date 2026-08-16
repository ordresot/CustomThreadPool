package org.example.otherThreadPoolsTests;

import java.util.concurrent.*;
import java.util.logging.*;
import java.util.concurrent.atomic.AtomicInteger;

public class JucThreadPoolTest {
    public static void main(String[] args) throws Exception {
        configureLogging();

        ThreadFactory tf = new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            private final String namePrefix = "JUC-Pool";

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, namePrefix + "-worker-" + threadNumber.getAndIncrement());
                t.setDaemon(false);
                System.out.printf("[ThreadFactory] Creating new thread: %s%n", t.getName());
                return t;
            }
        };

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                8,
                16,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(32),
                tf,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        System.out.println("[JUC-Pool] Starting task submission...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            final int taskId = i;
            executor.execute(() -> {
                String threadName = Thread.currentThread().getName();
                try {
                    System.out.printf("[JUC-Pool] [Task %d] Started on %s%n", taskId, threadName);
                    Thread.sleep(1000 + (long)(Math.random() * 2000));
                    System.out.printf("[JUC-Pool] [Task %d] Finished on %s%n", taskId, threadName);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.printf("[JUC-Pool] [Task %d] Interrupted on %s%n", taskId, threadName);
                }
            });
            Thread.sleep(200);
        }

        System.out.println("[JUC-Pool] All tasks submitted. Waiting for completion...");
        Thread.sleep(10000);

        long endTime = System.currentTimeMillis();
        System.out.printf("[JUC-Pool] All tasks completed in %d ms%n", (endTime - startTime));

        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            System.out.println("[JUC-Pool] Force shutdown...");
            executor.shutdownNow();
        }

        System.out.println("=== Thread Pool Test Finished ===");
    }

    private static void configureLogging() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.WARNING);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        root.addHandler(handler);
    }
}