package io.cantor.http;

@FunctionalInterface
public interface AppRequestResponseHandler<S extends AffinityScheduler, I, O> {
    void handle(S scheduler, I inBound, O outBound);
}
