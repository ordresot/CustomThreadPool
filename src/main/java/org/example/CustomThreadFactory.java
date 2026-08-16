package org.example;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class CustomThreadFactory implements ThreadFactory {
    private static final Logger LOG = Logger.getLogger(CustomThreadFactory.class.getName());
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    public CustomThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, namePrefix + "-" + threadNumber.getAndIncrement());
        t.setDaemon(false);
        LOG.info(String.format("[ThreadFactory] Creating new thread: %s", t.getName()));
        t.setUncaughtExceptionHandler((thread, throwable) ->
                LOG.severe(String.format("[ThreadFactory] %s crashed: %s", thread.getName(), throwable.getMessage()))
        );
        return t;
    }
}