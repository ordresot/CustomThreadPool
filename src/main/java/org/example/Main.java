package org.example;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
import java.util.concurrent.*;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Main {
    public static void main(String[] args) throws Exception {
        Logger root = Logger.getLogger("");
        root.setLevel(Level.INFO);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        root.addHandler(handler);

        ThreadFactory factory = new CustomThreadFactory("CustomPool");

        CustomThreadPoolExecutor executor = new CustomThreadPoolExecutor(
                8,
                16,
                30,
                TimeUnit.SECONDS,
                32,
                1,
                factory,
                new CustomThreadPoolExecutor.CallerRunsPolicy()
        );

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
                }
            });
            Thread.sleep(200);
        }

        System.out.println("=== All tasks submitted. Waiting... ===");
        Thread.sleep(8000);

        long endTime = System.currentTimeMillis();
        System.out.printf("=== All tasks completed in %d ms ===%n", (endTime - startTime));

        System.out.println("=== Initiating shutdown ===");
        executor.shutdown();

        boolean terminated = false;
        for (int i = 0; i < 10 && !terminated; i++) {
            Thread.sleep(1000);
            if (i == 9) {
                System.out.println("Forcing shutdownNow");
                executor.shutdownNow();
            }
        }

        System.out.println("=== Demo finished ===");
    }
}