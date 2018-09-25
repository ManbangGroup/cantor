package io.cantor.http;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBufUtil;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;

public class HandlerRequest {

    private static final int EMPTY = 0;

    private String uri;

    private HttpVersion version;

    private String method;

    private HttpHeaders headers;

    private byte[] content;

    private Map<String, String> pathVariables = new HashMap<>();

    private Map<String, String> query;

    private String remoteHostAddress;

    private int remotePort;

    HandlerRequest(FullHttpRequest request, Channel channel) {
        this.uri = request.uri();
        mergeURISlash();
        SocketAddress socketAddress = channel.remoteAddress();
        if (InetSocketAddress.class.isAssignableFrom(socketAddress.getClass())) {
            InetSocketAddress inetSocketAddress = InetSocketAddress.class.cast(socketAddress);
            if (inetSocketAddress.getAddress() != null) {
                remoteHostAddress = inetSocketAddress.getAddress().getHostAddress();
            } else {
                remoteHostAddress = inetSocketAddress.getHostString();
            }
            remotePort = inetSocketAddress.getPort();
        }
        this.version = request.protocolVersion();
        this.method = request.method().name();
        this.content = ByteBufUtil.getBytes(request.content());
        this.headers = new HttpHeaders(request.headers());
        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.uri(), true);
        this.query = new HashMap<>();
        queryStringDecoder.parameters().forEach((k, valueList) -> {
            if (null != valueList && valueList.size() > EMPTY)
                this.query.put(k, valueList.get(0));
        });
    }

    public String uri() {
        return uri;
    }

    public String version() {
        return version.text();
    }

    public String method() {
        return method;
    }

    public HttpHeaders headers() {
        return headers;
    }

    public InputStream stream() {
        return new ByteArrayInputStream(content);
    }

    public void pathVariables(String key, String value) {
        pathVariables.put(key, value);
    }

    public String pathVariables(String key) {
        return pathVariables.get(key);
    }

    public Map<String, String> queries() {
        return this.query;
    }

    public byte[] body() {
        return content;
    }

    public <T> T body(Class<T> valueType) throws IOException {
        return Codecs.json().readValue(content, valueType);
    }

    public <T> T body(TypeRef<T> typeRef) throws IOException {
        return Codecs.json().readValue(content, JacksonTypeRefs.typeRef(typeRef));
    }

    public String remoteHostAddress() {
        return remoteHostAddress;
    }

    public int remotePort() {
        return remotePort;
    }

    private void mergeURISlash() {
        uri = uri.replaceAll("//+", "/");
    }
}
