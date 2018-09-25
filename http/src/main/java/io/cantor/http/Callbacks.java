package io.cantor.http;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Callbacks {

    public static <V> boolean complete(Callback<V> callback, V result) {
        if (null == callback)
            return false;

        callback.onComplete(result);
        return true;
    }

    public static <V> boolean fail(Callback<V> callback, Throwable t) {
        if (null == callback)
            return false;

        callback.onFailure(t);
        return true;
    }

    public static abstract class Completion<V> implements Callback<V> {

        @Override
        public void onFailure(Throwable t) {
        }
    }

    public static abstract class Failure<V> implements Callback<V> {

        @Override
        public void onComplete(V result) {
        }
    }
}
