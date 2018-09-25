package io.cantor.http;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Server extends HttpServerHandler {
    private TrafficDispatcher dispatcher;
    private HttpServer server;
    private Application application;

    public Server(@NonNull Application application) {
        this.application = application;
        dispatcher = new TrafficDispatcher();
        server = new HttpServer(dispatcher.acceptors(), dispatcher.workers(), this);
    }

    public void startup(int port) {
        AtomicBoolean isSuccessful = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(1);
        server.bind("0.0.0.0", port, new Callback<Channel>() {
            @Override
            public void onComplete(Channel result) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                isSuccessful.set(false);
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("unexpected error", e);
        }
        if (!isSuccessful.get()) {
            throw new IllegalStateException("failed to start server");
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, FullHttpRequest request) {
        HandlerRequest handlerRequest = new HandlerRequest(request, ctx.channel());
        ReferenceCountUtil.release(request);

        if (request.decoderResult().isFailure()) {
            ctx.writeAndFlush(HttpResponses.badRequest(handlerRequest));
            return;
        }

        AffinityScheduler schd = dispatcher.assign(ctx.channel().eventLoop());

        try {
            schd.compute(() -> {

                Channel channel = ctx.channel();

                TriConsumer<FullHttpResponse, Runnable, Consumer<Throwable>> writer =
                        (resp, completer, catcher) -> {
                            GenericFutureListener<Future<? super Void>> completeListener = (future) -> {
                                if (future.isSuccess()) {
                                    if (completer != null)
                                        completer.run();
                                } else {
                                    Throwable cause = future.cause();
                                    if (log.isErrorEnabled()) {
                                        log.error("flush outbound data error", cause);
                                    }
                                    if (catcher != null) {
                                        catcher.accept(cause);
                                    }
                                }
                            };

                            if (channel.eventLoop().inEventLoop()) {
                                ctx.writeAndFlush(resp).addListener(completeListener);
                            } else {
                                channel.eventLoop().execute(
                                        () -> {
                                            ctx.writeAndFlush(resp).addListener(completeListener);
                                        });
                            }
                        };

                HandlerResponse handlerResponse = new HandlerResponse(handlerRequest,
                                                                      schd,
                                                                      writer);

                try {
                    application.handle(schd, handlerRequest, handlerResponse);
                } catch (Exception e) {
                    if (log.isErrorEnabled())
                        log.error("handle request failed", e);

                    schd.io(() -> {
                        ctx.writeAndFlush(HttpResponses.internalServerError(handlerRequest));
                    });
                }
            });
        } catch (final RejectedExecutionException e) {
            if (log.isErrorEnabled())
                log.error("execution queue rejected", e);

            ctx.writeAndFlush(HttpResponses.internalServerError(handlerRequest));
        }
    }
}
