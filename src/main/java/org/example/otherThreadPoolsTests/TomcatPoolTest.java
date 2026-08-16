package org.example.otherThreadPoolsTests;

import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.apache.tomcat.util.threads.TaskQueue;
import org.apache.tomcat.util.threads.TaskThreadFactory;

import java.util.concurrent.TimeUnit;
import java.util.logging.*;

public class TomcatPoolTest {
    public static void main(String[] args) throws Exception {
        configureLogging();

        TaskQueue taskQueue = new TaskQueue(32);

        TaskThreadFactory threadFactory = new TaskThreadFactory("Tomcat-Pool-", true, 1);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                8,
                16,
                30,
                TimeUnit.SECONDS,
                taskQueue,
                threadFactory
        );

        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        taskQueue.setParent(executor);

        System.out.println("[Tomcat-Pool] Starting task submission...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            final int taskId = i;
            executor.execute(() -> {
                String threadName = Thread.currentThread().getName();
                try {
                    System.out.printf("[Tomcat-Pool] [Task %d] Started on %s%n", taskId, threadName);
                    Thread.sleep(1000 + (long)(Math.random() * 2000));
                    System.out.printf("[Tomcat-Pool] [Task %d] Finished on %s%n", taskId, threadName);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.printf("[Tomcat-Pool] [Task %d] Interrupted on %s%n", taskId, threadName);
                }
            });
            Thread.sleep(200);
        }

        System.out.println("[Tomcat-Pool] All tasks submitted. Waiting for completion...");
        Thread.sleep(10000);

        long endTime = System.currentTimeMillis();
        System.out.printf("[Tomcat-Pool] All tasks completed in %d ms%n", (endTime - startTime));

        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            System.out.println("[Tomcat-Pool] Force shutdown...");
            executor.shutdownNow();
        }

        System.out.println("=== Tomcat Pool Test Finished ===");
    }

    private static void configureLogging() {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.WARNING);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        root.addHandler(handler);
    }
}