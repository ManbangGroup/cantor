package io.cantor.http;

import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HttpResponses {

    public static FullHttpResponse badRequest(HandlerRequest request) {
        return error(request, HttpResponseStatus.BAD_REQUEST);
    }

    public static FullHttpResponse badRequest() {
        FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST);
        String content = HttpResponseStatus.BAD_REQUEST.toString();
        resp.headers().set(HttpHeaderNames.CONTENT_LENGTH.toString(), content.length());
        ByteBufUtil.writeUtf8(resp.content(), content);
        return resp;
    }

    public static FullHttpResponse internalServerError(HandlerRequest request) {
        return error(request, HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }

    public static FullHttpResponse unauthorized(HandlerRequest request) {
        return error(request, HttpResponseStatus.UNAUTHORIZED);
    }

    public static FullHttpResponse forbidden(HandlerRequest request) {
        return error(request, HttpResponseStatus.FORBIDDEN);
    }

    private static FullHttpResponse error(HandlerRequest request, HttpResponseStatus status) {
        FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.valueOf(request.version()),
                                                            status);
        String content = status.toString();
        resp.headers().set(HttpHeaderNames.CONTENT_LENGTH.toString(), content.length());
        ByteBufUtil.writeUtf8(resp.content(), content);
        return resp;
    }
}
