package io.cantor.sdk;

import java.util.HashMap;
import java.util.Map;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RequestBuilder {

    private long category;
    private long range;
    private Long timeout;

    public static RequestBuilder builder() {
        return new RequestBuilder();
    }

    public RequestBuilder category(long category) {
        this.category = category;
        return this;
    }

    public RequestBuilder range(long range) {
        this.range = range;
        return this;
    }

    public RequestBuilder timeout(Long timeout) {
        this.timeout = timeout;
        return this;
    }

    public SequenceRequest build() {
        return new SequenceRequest(category, range, timeout);
    }


    @Getter
    public class SequenceRequest {
        private long category;
        private Long timeout;
        private Map<String, String> queries = new HashMap<>();

        private SequenceRequest(long category, long range, Long timeout) {
            this.category = category;
            this.timeout = timeout;
            queries.put("cate", Long.valueOf(category).toString());
            queries.put("range", Long.valueOf(range).toString());
            queries.put("mode", "0");
        }

        public Map<String, String> headers() {
            return null;
        }

        public Map<String, String> queries() {
            return queries;
        }

        public String path() {
            return "id";
        }
    }
}