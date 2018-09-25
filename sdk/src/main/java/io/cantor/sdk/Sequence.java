package io.cantor.sdk;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 *
 */
@EqualsAndHashCode
public class Sequence {
    @Getter
    private final long id;
    private final Deserializer deserializer;

    Sequence(long id) {
        this.id = id;
        this.deserializer = new Deserializer(id);
    }

    public long version() {
        return deserializer.version();
    }

    public long descriptor() {
        return deserializer.descriptor();
    }

    public long category() {
        return deserializer.category();
    }

    public long instance() {
        return deserializer.instance();
    }

    public long timestamp() {
        return deserializer.timestamp();
    }

    public long sequence() {
        return deserializer.sequence();
    }

}
