package io.cantor.http;

import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import io.netty.buffer.ByteBufOutputStream;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

public class HandlerResponse {

    private static final byte[] EMPTY_BODY = "".getBytes();

    private AtomicBoolean ok = new AtomicBoolean(true);

    private FullHttpResponse delegator;
    private HttpStatus status;
    private HttpHeaders headers;
    private Consumer<Throwable> catcher;
    private Runnable completer;
    private TriConsumer<FullHttpResponse, Runnable, Consumer<Throwable>> writer;
    private AffinityScheduler scheduler;
    private String httpVer;

    HandlerResponse(HandlerRequest request,
                    AffinityScheduler scheduler,
                    TriConsumer<FullHttpResponse, Runnable, Consumer<Throwable>> writer) {
        this.httpVer = request.version();
        this.scheduler = scheduler;
        this.writer = writer;
    }

    private void checkAndInit() {
        if (ok.compareAndSet(true, false)) {
            delegator = new DefaultFullHttpResponse(HttpVersion.valueOf(httpVer),
                                                    HttpResponseStatus.OK);
            status = new HttpStatus(delegator);
            headers = new HttpHeaders(delegator.headers());
        }
    }
    public HandlerResponse header(String name, String value) {
        headers().set(name, value);
        return this;
    }

    public HandlerResponse statusCode(int code) {
        status().code(code);
        return this;
    }

    public HttpStatus status() {
        checkAndInit();
        return status;
    }

    public HttpHeaders headers() {
        checkAndInit();
        return headers;
    }

    public HandlerResponse write(byte[] bytes) {
        checkAndInit();
        delegator.content().writeBytes(bytes);
        return this;
    }

    public OutputStream stream() {
        checkAndInit();
        return new ByteBufOutputStream(delegator.content());
    }

    public void catcher(Consumer<Throwable> catcher) {
        this.catcher = catcher;
    }

    public void completer(Runnable completer) {
        this.completer = completer;
        complete();
    }

    public void complete() {
        checkAndInit();
        if (!headers().contains(HttpHeaderNames.CONTENT_LENGTH.toString())) {
            int length = delegator().content().writerIndex();
            headers().set(HttpHeaderNames.CONTENT_LENGTH.toString(), length);
        }

        scheduler.io(() -> {
            writer.accept(delegator, completer, catcher);
        });
    }

    public void ok() {
        complete(HttpResponseStatus.OK.code(), EMPTY_BODY);
    }

    public void ok(byte[] body) {
        complete(HttpResponseStatus.OK.code(), body);
    }

    public void badRequest() {
        complete(HttpResponseStatus.BAD_REQUEST.code(),
                 HttpResponseStatus.BAD_REQUEST.toString().getBytes());
    }

    public void badRequest(byte[] msg) {
        complete(HttpResponseStatus.BAD_REQUEST.code(), msg);
    }

    public void notFound() {
        complete(HttpResponseStatus.NOT_FOUND.code(),
                 HttpResponseStatus.NOT_FOUND.toString().getBytes());
    }

    public void notFound(byte[] msg) {
        complete(HttpResponseStatus.NOT_FOUND.code(), msg);
    }

    public void internalServerError() {
        complete(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                 HttpResponseStatus.INTERNAL_SERVER_ERROR.toString().getBytes());
    }

    public void internalServerError(byte[] msg) {
        complete(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), msg);
    }

    public void forbidden() {
        complete(HttpResponseStatus.FORBIDDEN.code(),
                 HttpResponseStatus.FORBIDDEN.toString().getBytes());
    }

    public void forbidden(byte[] msg) {
        complete(HttpResponseStatus.FORBIDDEN.code(), msg);
    }

    public void unauthorized() {
        complete(HttpResponseStatus.UNAUTHORIZED.code(),
                 HttpResponseStatus.UNAUTHORIZED.toString().getBytes());
    }

    public void unauthorized(byte[] msg) {
        complete(HttpResponseStatus.UNAUTHORIZED.code(), msg);
    }

    private void complete(int code, byte[] body) {
        status().code(code);
        write(body);
        complete();
    }

    private FullHttpResponse delegator() {
        checkAndInit();
        return delegator;
    }

}
