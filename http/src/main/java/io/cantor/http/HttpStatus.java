package io.cantor.http;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;

public class HttpStatus {
    private FullHttpResponse delegator;

    HttpStatus(FullHttpResponse delegator) {
        this.delegator = delegator;
    }

    public int code() {
        return delegator.status().code();
    }

    public void code(int code) {
        delegator.setStatus(HttpResponseStatus.valueOf(code));
    }
}
