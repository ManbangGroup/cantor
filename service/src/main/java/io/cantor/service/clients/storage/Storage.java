package io.cantor.service.clients.storage;

import java.util.List;
import java.util.Optional;

public interface Storage {

    int ILLEGAL_INSTANCE = -1;
    int DEFAULT_HEARTBEAT_SECONDS = 30;

    Optional<Long> incrementAndGet(long category, long ts, long range);

    void close();

    boolean available();

    long syncTime(long localTime);

    List<Long> timeMeta();

    void deregister();

    String type();

    long descriptor();

    /**
     * To check and register in the instance box of storage.
     * If register successfully, it returns an {@link Integer} value as 1.
     * If it's failed, it returns the {@value ILLEGAL_INSTANCE}.
     * @return instance box index
     */
    int checkAndRegister(int maxInstances);

    boolean heartbeat(int instanceNumber, int ttl);
}
