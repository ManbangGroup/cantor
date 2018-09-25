package io.cantor.service.clients;

import org.apache.commons.math3.util.Pair;

import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LocalIdGenerator {

    public static final long LOCAL_CATE = 2L;

    private static ConcurrentHashMap<Long, SnowFlake> idSpace = new ConcurrentHashMap<>();

    public Pair<Long, Long> getFromLocal(long category, long range, long locatorId) {
        Parser.Deserializer deserializer = next(category, range, locatorId);

        return new Pair<>(deserializer.timestamp(), deserializer.sequence());
    }

    private Parser.Deserializer next(long category, long range, long locatorId) {

        long[] parts = idSpace.computeIfAbsent(category,
                (k) -> new SnowFlake(locatorId, category, LOCAL_CATE)).next(range);
        return SnowFlake.deserializer(parts[0]);
    }

}
