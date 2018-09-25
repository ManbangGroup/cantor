package io.cantor.http;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import lombok.Builder;
import lombok.Getter;


public class AffinityScheduler {

    @Getter
    private final ClassLoader contextLoader;

    private final EventLoop acceptor;

    private final EventLoop worker;

    private final EventLoop executor;

    @Builder
    private AffinityScheduler(ClassLoader contextLoader,
                              EventLoop acceptor,
                              EventLoop worker,
                              EventLoopGroup executors) {
        this.contextLoader = contextLoader;
        this.acceptor = acceptor;
        this.worker = worker;
        executor = executors.next();
    }

    public void io(final Runnable command) {
        Threads.verify(WatchedThread.class);
        worker.execute(command);
    }

    protected void io4test(final Runnable cmd) {
        worker.execute(cmd);
    }

    public void compute(final Runnable command) {
        Threads.verify(WatchedThread.class);
        executor.submit(command);
    }

    protected void compute4test(final Runnable command) {
        executor.submit(command);
    }

    public ScheduledFuture<?> schedule(final Runnable command, long delay, TimeUnit unit) {
        return executor.schedule(command, delay, unit);
    }

    public <V> ScheduledFuture<V> schedule(final Callable<V> callable, long delay, TimeUnit unit) {
        return executor.schedule(callable, delay, unit);
    }

    public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command,
                                                  long initialDelay,
                                                  long period,
                                                  TimeUnit unit) {
        return executor.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command,
                                                     long initialDelay,
                                                     long delay,
                                                     TimeUnit unit) {
        return executor.scheduleAtFixedRate(command, initialDelay, delay, unit);
    }
}
