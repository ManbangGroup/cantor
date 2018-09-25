package io.cantor.service.schema;

public interface VersionedSchema {

    long descriptorMusk();

    long descriptorLeft();

    long timestampMusk();

    long timestampLeft();

    long sequenceMusk();

    long categoryMusk();

    long categoryCodeLeft();

    long instanceMusk();

    long instanceCodeLeft();

    long maxTimestamp();

    long maxSequence();

    long maxCategory();

    int maxInstanceCount();
}
