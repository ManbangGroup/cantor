package io.cantor.service.clients;

import com.google.common.collect.ImmutableMap;

import java.math.BigInteger;
import java.util.Map;

import io.cantor.service.schema.Schema;
import io.cantor.service.schema.VersionedSchema;
import lombok.Builder;
import lombok.Getter;

public class Parser {

    public enum Info {
        CATEGORY, TIMESTAMP, SEQUENCE, DESCRIPTOR, INSTANCE
    }

    public static final long START_EPOCH = 1514736000L;
    public static final int WHOLE_ID = 0;
    public static final int RADIX = 1;
    public static final int RADIX_36 = 36;

    public static final VersionedSchema CURRENT_SCHEMA = Schema.VERSION_0;

    private static final long BEGINNING = 0L;

    public static Serializer serialize(long category, long desc, long ts, long seq, long instanceId) {

        Serializer serializer = Serializer.builder()
                                          .category(category)
                                          .descriptor(desc)
                                          .timestamp(ts)
                                          .sequence(seq)
                                          .instance(instanceId)
                                          .build();
        serializer.serialize();

        return serializer;
    }

    public static Map<Info, Number> parseFromNormal(String id) {
        return parseFromParts(Long.valueOf(id));

    }

    public static Map<Info, Number> parserFromRadix(String radixId) {
        BigInteger value = new BigInteger(radixId, RADIX_36);
        return parseFromNormal(value.toString());
    }

    public static Map<Info, Number> parseFromParts(long id) {

        Deserializer deserializer = new Deserializer(id);

        return ImmutableMap.of(Info.CATEGORY, deserializer.categoryCode(), Info.TIMESTAMP,
                deserializer.timestamp(), Info.SEQUENCE, deserializer.sequence(), Info.DESCRIPTOR,
                deserializer.descriptor(), Info.INSTANCE, deserializer.instance());
    }

    public static boolean isValid(long category, long ts, long seq) {
        return validCategory(category) && validSequence(seq) && validTimestamp(ts);
    }

    private static boolean validTimestamp(long timestamp) {
        return BEGINNING < timestamp && timestamp <= CURRENT_SCHEMA.maxTimestamp();
    }

    public static boolean validCategory(long category) {
        return BEGINNING <= category && category <= CURRENT_SCHEMA.maxCategory();
    }

    public static boolean validSequence(long seq) {
        return BEGINNING <= seq && seq <= CURRENT_SCHEMA.maxSequence();
    }

    @Getter
    static class Deserializer {

        private long id;
        private long version;
        private long categoryCode;
        private long instance;
        private long timestamp;
        private long sequence;
        private long descriptor;

        Deserializer(long id) {
            this.id = id;

            version = (id >> Schema.VERSION_LEFT) & Schema.VERSION_MUSK;
            VersionedSchema versionedSchema = Schema.versionedSchema(version);

            descriptor =
                    (id >> versionedSchema.descriptorLeft()) & versionedSchema.descriptorMusk();
            categoryCode =
                    (id >> versionedSchema.categoryCodeLeft()) & versionedSchema.categoryMusk();
            instance = (id >> versionedSchema.instanceCodeLeft()) & versionedSchema.instanceMusk();
            timestamp = (id >> versionedSchema.timestampLeft()) & versionedSchema.timestampMusk();
            sequence = id & versionedSchema.sequenceMusk();
        }
    }

    @Builder
    public static class Serializer {


        @Getter
        private long id;

        // id space
        private long version;
        private long descriptor;
        private long category;
        private long instance;
        private long timestamp;
        private long sequence;

        private void serialize() {
            // high
            id = descriptor << CURRENT_SCHEMA.descriptorLeft() |
                    category << CURRENT_SCHEMA.categoryCodeLeft() |
                    instance << CURRENT_SCHEMA.instanceCodeLeft() |
                    timestamp << CURRENT_SCHEMA.timestampLeft() | sequence;
        }

        private BigInteger toBigInteger() {
            return BigInteger.valueOf(id);
        }

        public String toString(int radix) {
            BigInteger integer = toBigInteger();
            return integer.toString(radix);
        }
    }

}
