package io.cantor.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;

@Slf4j
@ChannelHandler.Sharable
public abstract class HttpServerHandler extends ChannelDuplexHandler {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (FullHttpRequest.class.isAssignableFrom(msg.getClass())) {
            FullHttpRequest req = FullHttpRequest.class.cast(msg);
            DecoderResult result = req.decoderResult();

            if (result.isFailure()) {
                if (log.isWarnEnabled())
                    log.warn("http decoder failure", result.cause());
                ReferenceCountUtil.release(msg);
                ctx.writeAndFlush(HttpResponses.badRequest());
                ctx.channel().close();
                return;
            }

            if (HttpUtil.is100ContinueExpected(req))
                ctx.writeAndFlush(new DefaultFullHttpResponse(req.protocolVersion(), CONTINUE));

            FullHttpRequest safeReq = new DefaultFullHttpRequest(req.protocolVersion(),
                                                                 req.method(),
                                                                 req.uri(),
//                                                                 Buffers.safeByteBuf(req.content(), ctx.alloc()),
                                                                 req.content(),
                                                                 req.headers(),
                                                                 req.trailingHeaders());
            channelRead(ctx, safeReq);
        } else
            ctx.fireChannelRead(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Channel channel = ctx.channel();
        if (log.isErrorEnabled())
            log.error(String.format("unexpected error (%s)", channel != null ? channel.toString() : ""), cause);
        ctx.flush();
    }

    public abstract void channelRead(ChannelHandlerContext ctx, FullHttpRequest request);
}
