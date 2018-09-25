package io.cantor.http;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseEncoder;

class BoundHttpResponseEncoder extends HttpResponseEncoder {

    private ChannelHandlerContext context;

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out)
            throws Exception {
        super.encode(context, msg, out);
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        context = PooledDirectByteBufAllocator.forceDirectAllocator(ctx);
        super.handlerAdded(context);
    }
}
