package io.cantor.http;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;

class WatchedThreadFactory implements ThreadFactory {

    private static final AtomicInteger SEQUENCE = new AtomicInteger(0);
    private static final Object YUMMY = new Object();
    private static final ThreadGroup THREAD_GROUP = new ThreadGroup("core");
    private static final Map<WatchedThread, Object> THREADS = new WeakHashMap<>();

    @Getter(AccessLevel.PACKAGE)
    private final int seq;

    @Getter(AccessLevel.PACKAGE)
    private final String name;

    @Getter(AccessLevel.PACKAGE)
    private final AtomicInteger threadCount = new AtomicInteger(0);

    @Getter(AccessLevel.PACKAGE)
    private final ThreadPattern pattern;

    @Getter(AccessLevel.PACKAGE)
    private final long maxDuration;

    WatchedThreadFactory(@NonNull String name, @NonNull ThreadPattern pattern, long maxDuration) {
        seq = SEQUENCE.getAndIncrement();
        this.name = name;
        this.pattern = pattern;
        this.maxDuration = maxDuration;
    }

    private static synchronized void addToMap(WatchedThread thread) {
        THREADS.put(thread, YUMMY);
    }

    public Thread newThread(@NonNull Runnable r) {
        WatchedThread t = new WatchedThread(THREAD_GROUP, r, this);
        addToMap(t);

        t.setDaemon(false);
        return t;
    }
}
