package io.cantor.http;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Bootstraps {

    private static boolean USE_EPOLL = System.getProperty("os.name").toLowerCase().equals("linux");

    public static ServerBootstrap serverBootstrap(EventLoopGroup acceptors,
                                                  EventLoopGroup workers) {

        ServerBootstrap bootstrap = new ServerBootstrap().group(acceptors, workers)
                                                         .childOption(ChannelOption.TCP_NODELAY,
                                                                      true)
                                                         .childOption(ChannelOption.ALLOCATOR,
                                                                      PooledDirectByteBufAllocator.INSTANCE)
                                                         .childOption(ChannelOption.SO_KEEPALIVE,
                                                                      false)
                                                         .option(ChannelOption.SO_REUSEADDR,
                                                                 true);

        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                         new WriteBufferWaterMark(32 * 1024, 128 * 1024));

        return bootstrap;
    }
}
