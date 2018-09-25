package io.cantor.sdk;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;

import static io.cantor.sdk.RequestBuilder.builder;

@Slf4j
public class ServiceCaller {

    public static final int DEFAULT_TIMEOUT = 15;
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final OkHttpClient client;
    private final String host;
    private final int port;

    ServiceCaller(String host, int port) {
        this(host, port, new OkHttpClient());
    }

    ServiceCaller(String host, int port, OkHttpClient client) {
        this.host = host;
        this.port = port;
        this.client = client;
    }

    public Range call(RequestBuilder.SequenceRequest sequenceRequest) {
        Long timeout = sequenceRequest.timeout();
        HttpUrl.Builder urlBuilder = new HttpUrl.Builder()
                .scheme("http")
                .host(host)
                .port(port)
                .addPathSegment(sequenceRequest.path());
        sequenceRequest.queries().forEach(urlBuilder::addQueryParameter);
        okhttp3.Request request = new okhttp3.Request.Builder()
                .url(urlBuilder.build())
                .get()
                .build();

        CountDownLatch latch = new CountDownLatch(1);

        CopyOnWriteArrayList<SequenceRespBody> syncHolder
                = new CopyOnWriteArrayList<>();
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                if (log.isErrorEnabled()) {
                    log.error("failed to get id from server");
                }
                latch.countDown();
            }

            @Override
            public void onResponse(Call call, okhttp3.Response response) {
                try {
                    String responseString = new String(response.body().bytes(), Charset.forName("utf-8"));
                    if (log.isInfoEnabled()) {
                        log.info("batch get ids from server [{}]", responseString);
                    }
                    ServiceCaller.SequenceRespBody respBody = OBJECT_MAPPER.readValue(responseString,
                                                                                      ServiceCaller.SequenceRespBody.class);
                    syncHolder.add(respBody);
                } catch (Exception e) {
                    log.error("failed to parse response");
                } finally {
                    latch.countDown();
                }
            }
        });
        try {
            boolean inTime = latch.await(timeout != null ? timeout : DEFAULT_TIMEOUT,
                                        TimeUnit.SECONDS);
            if (!inTime && log.isDebugEnabled()) {
                log.debug("request is timed out ({} seconds)", timeout != null ? timeout : DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                throw new TimeoutException();
            }
            if (!syncHolder.isEmpty()) {
                return syncHolder.get(0).sequenceRange();
            } else {
                log.debug("no response data found");
                throw new SequenceException("no response data found");
            }
        } catch (Exception e) {
            log.debug("unexpected error", e);
            throw new SequenceException(e);
        }
    }

    public RequestBuilder newRequestBuilder() {
        return builder();
    }

    @Getter
    public static class SequenceRespBody {
        @JsonProperty("start")
        private String start;
        @JsonProperty("range")
        private String range;

        public Range sequenceRange() {
            return new Range(Long.valueOf(start), Long.valueOf(range));
        }
    }
}
