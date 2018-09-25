package io.cantor.http;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Threads {

    public static Thread current() {
        return Thread.currentThread();
    }

    public static <T extends Thread> T current(Class<T> clazz) {
        Thread t = current();
        return clazz.isAssignableFrom(t.getClass()) ? clazz.cast(t) : null;
    }

    public static <T extends Thread> T verify(Class<T> clazz) {
        T t = current(clazz);
        if (null == t)
            throw new IllegalStateException("Current thread is not an instance of: "
                                            + clazz.getName());
        return t;
    }

    public static ClassLoader contextLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    public static void contextLoader(ClassLoader classLoader) {
        Thread.currentThread().setContextClassLoader(classLoader);
    }
}
