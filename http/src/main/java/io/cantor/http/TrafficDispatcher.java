package io.cantor.http;

import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.Getter;

public class TrafficDispatcher {

    @Getter
    private final EventLoopGroup workers;

    @Getter
    private final EventLoopGroup acceptors;

    @Getter
    private final EventLoopGroup executors;

    TrafficDispatcher() {
        acceptors = new NioEventLoopGroup(1,
                                          new WatchedThreadFactory(String.format("%s-%s",
                                                                                   "traffic",
                                                                                   "accept"),
                                                                     ThreadPattern.IO_LOOP,
                                                                     5000));

        NioEventLoopGroup.class.cast(acceptors).setIoRatio(100);

        workers = new NioEventLoopGroup(8,
                                        new WatchedThreadFactory(String.format("%s-%s",
                                                                               "traffic",
                                                                               "io"),
                                                                 ThreadPattern.IO_LOOP,
                                                                 5000));
        NioEventLoopGroup.class.cast(workers).setIoRatio(50);

        executors = new ComputeEventLoopGroup(16,
                                              new WatchedThreadFactory(String.format("%s-%s",
                                                                                     "traffic",
                                                                                     "exec"),
                                                                       ThreadPattern.IO_LOOP,
                                                                       5000),
                                              64);
    }

    public AffinityScheduler assign() {
        return assign(workers.next());
    }

    public AffinityScheduler assign(EventLoop ioWorker) {
        return AffinityScheduler.builder()
                                .contextLoader(Threads.contextLoader())
                                .acceptor(acceptors.next())
                                .worker(ioWorker)
                                .executors(executors)
                                .build();
    }
}
