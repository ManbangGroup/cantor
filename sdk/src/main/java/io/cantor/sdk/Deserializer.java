package io.cantor.sdk;

import lombok.Getter;

/**
 *
 */
@Getter
public class Deserializer {

    /**
     * SDK generation descriptor
     */
    static final long DESCRIPTOR = 3L;

    // 64 bit
    static final long VERSION_BIT = 2L;
    static final long VERSION_MUSK = ~(-1L << VERSION_BIT);

    static final long DESCRIPTOR_BIT = 2L;
    static final long DESCRIPTOR_MUSK = ~(-1L << DESCRIPTOR_BIT);

    static final long CATEGORY_BIT = 7L;
    static final long CATEGORY_MUSK = ~(-1L << CATEGORY_BIT);

    static final long INSTANCE_BIT = 3L;
    static final long INSTANCE_MUSK = ~(-1L << INSTANCE_BIT);

    static final long TIMESTAMP_BIT = 28L;
    static final long TIMESTAMP_MUSK = ~(-1L << TIMESTAMP_BIT);

    static final long SEQUENCE_BIT = 21L;
    static final long SEQUENCE_MUSK = ~(-1L << SEQUENCE_BIT);


    // shift
    static final long TIMESTAMP_LEFT = SEQUENCE_BIT;
    static final long INSTANCE_LEFT = TIMESTAMP_BIT + TIMESTAMP_LEFT;
    static final long CATEGORY_LEFT = INSTANCE_BIT + INSTANCE_LEFT;
    static final long DESCRIPTOR_LEFT = CATEGORY_BIT + CATEGORY_LEFT;
    static final long VERSION_LEFT = DESCRIPTOR_BIT + DESCRIPTOR_LEFT;

    private long id;
    private long version;
    private long category;
    private long instance;
    private long timestamp;
    private long sequence;
    private long descriptor;


    Deserializer(long id) {
        this.id = id;
        version = (id >> VERSION_LEFT) & VERSION_MUSK;
        descriptor = (id >> DESCRIPTOR_LEFT) & DESCRIPTOR_MUSK;
        category = (id >> CATEGORY_LEFT) & CATEGORY_MUSK;
        instance = (id >> INSTANCE_LEFT) & INSTANCE_MUSK;
        timestamp = (id >> TIMESTAMP_LEFT) & TIMESTAMP_MUSK;
        sequence = id & SEQUENCE_MUSK;
    }
}
