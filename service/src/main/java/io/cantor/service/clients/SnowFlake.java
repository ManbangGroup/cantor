package io.cantor.service.clients;

import java.util.concurrent.TimeUnit;

class SnowFlake {

    private static final long BEGINNING = 1L;

    private long sequence = BEGINNING;
    private long lastEpochSeconds = -1L;
    private long instanceId;
    private long category;
    private long descriptor;

    SnowFlake(long category, long instanceId, long desc) {
        this.category = category & Parser.CURRENT_SCHEMA.categoryMusk();
        this.instanceId = instanceId & Parser.CURRENT_SCHEMA.instanceMusk();
        this.descriptor = desc;
    }

    synchronized long[] next(long batch) {
        if (batch > Parser.CURRENT_SCHEMA.sequenceMusk() + 1) {
            throw new IllegalStateException("illegal batch");
        }
        long current = epochSeconds(System.currentTimeMillis());
        if (current < lastEpochSeconds) {
            throw new IllegalStateException("clock moved backwards");
        }

        if (current > lastEpochSeconds) {
            sequence = BEGINNING;
        }

        if ((sequence + batch) > Parser.CURRENT_SCHEMA.sequenceMusk()) {
            current = forceToNextMillis();
            if ((sequence + batch) > (Parser.CURRENT_SCHEMA.sequenceMusk() + 1)) {
                sequence = BEGINNING;
            }
        }

        lastEpochSeconds = current;

        long id = descriptor << Parser.CURRENT_SCHEMA.descriptorLeft()
                | category << Parser.CURRENT_SCHEMA.categoryCodeLeft()
                | instanceId << Parser.CURRENT_SCHEMA.instanceCodeLeft()
                | current << Parser.CURRENT_SCHEMA.timestampLeft()
                | sequence;

        sequence = (sequence + batch) & Parser.CURRENT_SCHEMA.sequenceMusk();

        return new long[]{id, current};
    }

    private long forceToNextMillis() {
        long stamp = epochSeconds(System.currentTimeMillis());
        while (stamp <= lastEpochSeconds) {
            stamp = epochSeconds(System.currentTimeMillis());
        }
        return stamp;
    }

    private long epochSeconds(long millis) {
        long epochMillis = millis - Parser.START_EPOCH;
        return TimeUnit.MILLISECONDS.toSeconds(epochMillis);
    }

    static Parser.Deserializer deserializer(long id) {
        return new Parser.Deserializer(id);
    }

}
