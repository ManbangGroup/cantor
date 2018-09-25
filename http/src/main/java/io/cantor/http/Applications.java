package io.cantor.http;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static io.cantor.http.RequestMappingRegistry.HttpMethodKey.DELETE;
import static io.cantor.http.RequestMappingRegistry.HttpMethodKey.GET;
import static io.cantor.http.RequestMappingRegistry.HttpMethodKey.POST;
import static io.cantor.http.RequestMappingRegistry.HttpMethodKey.PUT;


@Slf4j
public class Applications {

    public static SimpleApplicationBuilder builder() {
        return new SimpleApplicationBuilder();
    }

    public static class SimpleApplicationBuilder {

        private SimpleApplicationBuilder() {
        }

        private RequestMappingRegistry<AffinityScheduler, HandlerRequest, HandlerResponse> mappingRegistry
                = new RequestMappingRegistry<>();


        public SimpleApplicationBuilder method(@NonNull String path,
                                               @NonNull RequestMappingRegistry.HttpMethodKey method,
                                               AppRequestResponseHandler<AffinityScheduler, HandlerRequest, HandlerResponse> handler) {
            mappingRegistry.registry(method, path, handler);
            return this;
        }

        public SimpleApplicationBuilder put(String path,
                                            AppRequestResponseHandler<AffinityScheduler, HandlerRequest, HandlerResponse> handler) {
            mappingRegistry.registry(PUT, path, handler);
            return this;
        }

        public SimpleApplicationBuilder get(String path,
                                            AppRequestResponseHandler<AffinityScheduler, HandlerRequest, HandlerResponse> handler) {
            mappingRegistry.registry(GET, path, handler);
            return this;
        }

        public SimpleApplicationBuilder post(String path,
                                             AppRequestResponseHandler<AffinityScheduler, HandlerRequest, HandlerResponse> handler) {
            mappingRegistry.registry(POST, path, handler);
            return this;
        }

        public SimpleApplicationBuilder delete(String path,
                                               AppRequestResponseHandler<AffinityScheduler, HandlerRequest, HandlerResponse> handler) {
            mappingRegistry.registry(DELETE, path, handler);
            return this;
        }

        public Application build() {
            return new NativeApplication(mappingRegistry);
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    public static class NativeApplication implements Application {

        private RequestMappingRegistry<AffinityScheduler, HandlerRequest, HandlerResponse> mappingRegistry;

        @Override
        public void handle(AffinityScheduler scheduler, HandlerRequest handlerRequest, HandlerResponse handlerResponse) throws Exception {
            RequestMappingRegistry.PathMatcher pathMatcher = mappingRegistry.find(handlerRequest);

            if (pathMatcher.matched()) {
                pathMatcher.serverHandler().handle(scheduler, handlerRequest, handlerResponse);
            } else {
                handlerResponse.headers().set("content-length", 0);
                int code = 404;
                if (pathMatcher.methodUnmatched()) {
                    code = 405;
                }
                if(pathMatcher.error()) {
                    code = 500;
                }
                handlerResponse.status().code(code);
                handlerResponse.complete();
            }
        }
    }
}
