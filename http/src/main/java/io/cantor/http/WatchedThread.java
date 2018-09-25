package io.cantor.http;

import io.netty.util.concurrent.FastThreadLocalThread;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@ToString(callSuper = true)
@Getter
class WatchedThread extends FastThreadLocalThread {

    private long started;

    private long maxDuration;

    @Getter
    private final ThreadPattern pattern;

    @Setter
    private AffinityScheduler scheduler;

    WatchedThread(@NonNull ThreadGroup threadGroup, @NonNull Runnable target, @NonNull WatchedThreadFactory factory) {
        super(threadGroup,
              target,
              String.format("%s:%s:%s", factory.name(), factory.seq(), factory.threadCount().getAndIncrement()));
        this.maxDuration = factory.maxDuration();
        this.pattern = factory.pattern();
    }

    final void watch() {
        started = System.nanoTime();
    }

    final long stopWatch() {
        long elapse = System.nanoTime() - started;
        started = 0;
        return elapse;
    }

    boolean isTimeout() {
        return 0 < stopWatch() - maxDuration;
    }
}
