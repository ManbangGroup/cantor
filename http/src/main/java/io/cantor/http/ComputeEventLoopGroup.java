package io.cantor.http;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

import io.netty.channel.EventLoop;
import io.netty.channel.MultithreadEventLoopGroup;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ComputeEventLoopGroup extends MultithreadEventLoopGroup {

    ComputeEventLoopGroup(int nThreads, ThreadFactory threadFactory, int maxPendingTasks) {
        super(nThreads, threadFactory, maxPendingTasks);
    }

    @Override
    protected EventLoop newChild(Executor executor, Object... args) throws Exception {
        int maxPendingTasks = Integer.MAX_VALUE;
        if (null != args)
            if (null != args[0])
                try {
                    maxPendingTasks = Integer.valueOf(args[0].toString());
                } catch (Exception e) {
                    if (log.isWarnEnabled())
                        log.warn("executor.queue.size has not been configured", e);
                }

        return new ComputeEventLoop(this, executor, maxPendingTasks);
    }
}
