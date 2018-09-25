package io.cantor.http;

import lombok.Getter;

enum ThreadPattern {
    POOL(false),
    LOOP(false),
    IO_LOOP(true),
    STRAGGLER(false);

    @Getter
    private final boolean io;

    ThreadPattern(boolean io) {
        this.io = io;
    }
}
