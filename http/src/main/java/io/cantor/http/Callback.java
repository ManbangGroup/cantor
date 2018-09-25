package io.cantor.http;

/**
 * A callback for accepting the results of a computation.
 */
public interface Callback<V> {

    /**
     * Invoked with the result of computation when it is completed.
     */
    void onComplete(V result);

    /**
     * Invoked when a computation fails or is canceled.
     */
    void onFailure(Throwable t);
}
