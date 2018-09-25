package io.cantor.sdk;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 *
 */
@Slf4j
public class MockHttpInterceptor implements Interceptor {
    public final long EPOCH = 1514736000L;
    private static AtomicInteger i = new AtomicInteger(0);
    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        Long category = Long.valueOf(request.url().queryParameter("cate"));
        String range = request.url().queryParameter("range");
        long timestamp = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - EPOCH;
        log.info("mock timestamp [{}]", timestamp);
        long id = category << Deserializer.CATEGORY_LEFT
                  | 2L << Deserializer.INSTANCE_LEFT
                  | timestamp << Deserializer.TIMESTAMP_LEFT
                  | i.getAndIncrement() * 1000 + 1;
        ResponseBody responseBody = ResponseBody.create(
                MediaType.parse("application/json; charset=utf-8"),
                String.format("{\"id\":\"%s\",\"range\":\"%s\"}", id, range));
        Response mockResponse = new Response.Builder()
                .request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("")
                .body(responseBody)
                .build();

        return mockResponse;
    }
}
