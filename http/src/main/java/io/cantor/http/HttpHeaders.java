package io.cantor.http;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;

public class HttpHeaders {
    public static final String CONTENT_TYPE = HttpHeaderNames.CONTENT_TYPE.toString();
    public static final String APPLICATION_JSON = HttpHeaderValues.APPLICATION_JSON.toString();
    public static final String APPLICATION_X_WWW_FORM_URLENCODED =
            HttpHeaderValues.APPLICATION_X_WWW_FORM_URLENCODED.toString();

    private io.netty.handler.codec.http.HttpHeaders headers;

    public HttpHeaders(io.netty.handler.codec.http.HttpHeaders headers) {
        this.headers = headers;
    }

    public void set(String name, Object value) {
        headers.set(name, value);
    }

    public boolean contains(String name) {
        return headers.contains(name);
    }

    public List<String> getAll(String name) {
        return headers.getAll(name);
    }

    public Set<String> names() {
        return headers.names();
    }

    public String get(String name) {
        return headers.get(name);
    }

    public Iterator<Map.Entry<String, String>> iterator() {
        return headers.iteratorAsString();
    }
}
