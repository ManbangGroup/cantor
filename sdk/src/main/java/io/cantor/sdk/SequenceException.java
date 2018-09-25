package io.cantor.sdk;

public class SequenceException extends RuntimeException {
    public SequenceException(String message) {
        super(message);
    }

    public SequenceException(Throwable t) {
        super(t);
    }
}
