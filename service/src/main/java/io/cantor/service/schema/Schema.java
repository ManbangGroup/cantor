package io.cantor.service.schema;

public class Schema {

    public static VersionedSchema VERSION_0 = new V0();

    private static final long VERSION_BIT = 2L;
    public static final long VERSION_MUSK = ~(-1L << VERSION_BIT);
    public static final long VERSION_LEFT = 59L;

    public static VersionedSchema versionedSchema(long version) {
        switch (Long.valueOf(version).intValue()) {
            case V0.VERSION_CODE:
                return VERSION_0;
            default:
                return VERSION_0;
        }
    }

    private static class V0 implements VersionedSchema {
        private static final int VERSION_CODE = 0;

        private static final long DESCRIPTOR_BIT = 2L;
        private static final long CATEGORY_BIT = 7L;
        private static final long INSTANCE_BIT = 3L;
        private static final long TIMESTAMP_BIT = 28L;
        private static final long SEQUENCE_BIT = 21L;

        private static final long MAX_CATEGORY = (1L << CATEGORY_BIT) - 1;
        private static final long MAX_TIMESTAMP = (1L << TIMESTAMP_BIT) - 1;
        private static final long MAX_SEQUENCE = (1L << SEQUENCE_BIT) - 1;

        private static final long DESCRIPTOR_MUSK = ~(-1L << DESCRIPTOR_BIT);
        private static final long CATEGORY_MUSK = ~(-1L << CATEGORY_BIT);
        private static final long INSTANCE_MUSK = ~(-1L << INSTANCE_BIT);
        private static final long TIMESTAMP_MUSK = ~(-1L << TIMESTAMP_BIT);
        private static final long SEQUENCE_MUSK = ~(-1L << SEQUENCE_BIT);

        private static final long TIMESTAMP_LEFT = SEQUENCE_BIT;
        private static final long INSTANCE_LEFT = TIMESTAMP_BIT + TIMESTAMP_LEFT;
        private static final long CATEGORY_LEFT = INSTANCE_BIT + INSTANCE_LEFT;
        private static final long DESCRIPTOR_LEFT = CATEGORY_BIT + CATEGORY_LEFT;

        private static final long MAX_INSTANCE_COUNT = 1L << INSTANCE_BIT;

        @Override
        public long descriptorMusk() {
            return V0.DESCRIPTOR_MUSK;
        }

        @Override
        public long descriptorLeft() {
            return V0.DESCRIPTOR_LEFT;
        }

        @Override
        public long instanceMusk() {
            return V0.INSTANCE_MUSK;
        }

        @Override
        public long instanceCodeLeft() {
            return V0.INSTANCE_LEFT;
        }

        @Override
        public long timestampMusk() {
            return V0.TIMESTAMP_MUSK;
        }

        @Override
        public long timestampLeft() {
            return V0.TIMESTAMP_LEFT;
        }

        @Override
        public long sequenceMusk() {
            return V0.SEQUENCE_MUSK;
        }

        @Override
        public long categoryMusk() {
            return V0.CATEGORY_MUSK;
        }

        @Override
        public long categoryCodeLeft() {
            return V0.CATEGORY_LEFT;
        }

        @Override
        public long maxTimestamp() {
            return V0.MAX_TIMESTAMP;
        }

        @Override
        public long maxSequence() {
            return V0.MAX_SEQUENCE;
        }

        @Override
        public long maxCategory() {
            return V0.MAX_CATEGORY;
        }

        @Override
        public int maxInstanceCount() {
            return (int) V0.MAX_INSTANCE_COUNT;
        }
    }
}
