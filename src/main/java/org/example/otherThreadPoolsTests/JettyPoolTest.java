package org.example;

import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

public class JettyPoolTest {
    public static void main(String[] args) throws Exception {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        root.addHandler(handler);

        System.out.println("=== Jetty ThreadPool Demo ===");

        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(32);
        QueuedThreadPool executor = new QueuedThreadPool(
                16,
                8,
                30000,
                queue
        );

        executor.setName("Jetty-Pool");

        executor.start();

        System.out.println("=== Starting task submission ===");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            final int taskId = i;
            executor.execute(() -> {
                try {
                    String name = Thread.currentThread().getName();
                    System.out.printf("[Task %d] Started on %s%n", taskId, name);
                    Thread.sleep(1000 + (long)(Math.random() * 2000));
                    System.out.printf("[Task %d] Finished on %s%n", taskId, name);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.out.printf("[Task %d] Interrupted%n", taskId);
                }
            });
            Thread.sleep(100);
        }

        System.out.println("=== All tasks submitted. Waiting... ===");
        Thread.sleep(10000);

        long endTime = System.currentTimeMillis();
        System.out.printf("=== All tasks completed in %d ms ===%n", (endTime - startTime));

        System.out.println("=== Initiating shutdown ===");
        executor.stop();

        System.out.println("=== Jetty Pool Test Finished ===");
    }
}