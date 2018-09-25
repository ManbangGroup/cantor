package io.cantor.http;

import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import io.netty.util.internal.PlatformDependent;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class ComputeEventLoop extends SingleThreadEventLoop {

    ComputeEventLoop(EventLoopGroup parent, Executor executor, int maxPendingTasks) {
        super(parent, executor, true, maxPendingTasks, RejectedExecutionHandlers.reject());
    }

    private void sleepDream() throws Exception{
        TimeUnit.MICROSECONDS.sleep(10);
    }

    @Override
    protected void run() {
        for (; ; ) {
            try {
                if (!runAllTasks())
                    sleepDream();
            } catch (Throwable t) {
                handleLoopException(t);
            }
            // Always handle shutdown even if the loop processing threw an exception.
            try {
                if (isShuttingDown()) {
                    if (confirmShutdown()) {
                        return;
                    }
                }
            } catch (Throwable t) {
                handleLoopException(t);
            }
        }
    }

    private static void handleLoopException(Throwable t) {
        if (log.isWarnEnabled())
            log.warn("Unexpected exception in the event loop.", t);

        // Prevent possible consecutive immediate failures that lead to
        // excessive CPU consumption.
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            // Ignore.
//        }
    }

    @Override
    protected Queue<Runnable> newTaskQueue(int maxPendingTasks) {
        // This event loop never calls takeTask()
        return PlatformDependent.newMpscQueue(maxPendingTasks);
    }
}
