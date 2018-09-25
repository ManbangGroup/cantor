package io.cantor.http;


@FunctionalInterface
public interface Application {

    void handle(AffinityScheduler scheduler, HandlerRequest handlerRequest,
                HandlerResponse handlerResponse) throws Exception;

}
