package io.cantor.http;

import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpServer {

    private final HttpServerHandler handler;

    private final ServerBootstrap bootstrap;

    private final ChannelGroup channelGroup;

    private final AtomicBoolean listen = new AtomicBoolean(false);

    public HttpServer(@NonNull EventLoopGroup acceptors,
                      @NonNull EventLoopGroup workers,
                      @NonNull HttpServerHandler handler) {
        this.handler = handler;
        this.channelGroup = new DefaultChannelGroup("netserver-channels",
                                                    GlobalEventExecutor.INSTANCE);
        this.bootstrap = Bootstraps.serverBootstrap(acceptors, workers);
    }

    public synchronized void bind(@NonNull String host, int port, Callback<Channel> callback) {
        if (!listen.compareAndSet(false, true))
            throw new IllegalStateException("already listening");

        pipeline();

        try {
            ChannelFuture channelFuture = bootstrap.bind(host, port);
            channelFuture.addListener(new NetServerListener(channelFuture.channel(),
                                                            callback,
                                                            channelGroup,
                                                            channelFuture,
                                                            host,
                                                            port));
        } catch (Throwable t) {
            listen.set(false);

            if (log.isErrorEnabled())
                log.error(String.format("net server failed to listen on %s:%s", host, port), t);

            Callbacks.fail(callback, t);
        }
    }

    private void pipeline() {
        bootstrap.childHandler(new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();

                codec(pipeline);

                pipeline.addLast("handler", handler);
            }
        });
    }

    private static class NetServerListener implements ChannelFutureListener {

        private Channel ch;
        private Callback<Channel> callback;
        private ChannelGroup channelGroup;
        private String host;
        private int port;
        private ChannelFuture channelFuture;

        NetServerListener(Channel ch,
                          Callback<Channel> callback,
                          ChannelGroup chgp,
                          ChannelFuture channelFuture,
                          String host,
                          int port) {
            this.ch = ch;
            this.callback = callback;
            this.channelGroup = chgp;
            this.host = host;
            this.port = port;
            this.channelFuture = channelFuture;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {

            if (future.isSuccess()) {
                //                Channel ch = channelFuture.channel();

                if (log.isInfoEnabled())
                    log.info("net server is listening on {}:{}, local address: {}",
                             host,
                             port,
                             ch.localAddress());

                channelGroup.add(ch);

                Callbacks.complete(callback, ch);
            } else {
                Throwable t = channelFuture.cause();
                if (log.isErrorEnabled())
                    log.error(String.format("net server failed to listen on %s:%s", host, port), t);

                Callbacks.fail(callback, t);
            }
        }
    }

    protected void codec(ChannelPipeline pipeline) {
        HttpRequestDecoder httpDecoder = new HttpRequestDecoder(4096,
                                                                8192,
                                                                8192,
                                                                false);
        pipeline.addLast("decoder", httpDecoder)
                .addLast("encoder", new BoundHttpResponseEncoder())
                .addLast("aggregator", new HttpObjectAggregator(1048576));
    }
}
