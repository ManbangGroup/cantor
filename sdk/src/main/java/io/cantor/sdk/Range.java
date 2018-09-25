package io.cantor.sdk;

public class Range {
    private final long id;
    private final long range;
    private long index = 0;
    private boolean overflow = false;

    public Range(long id, long range) {
        this.id = id;
        this.range = range;
    }

    public Sequence peek() {
        if (overflow) return null;
        return new Sequence(id + index);
    }

    public Sequence getAndIncrement() {
        Sequence result = new Sequence(id + index);
        index += 1;
        if (index == range) {
            overflow = true;
        }
        return result;
    }
}
