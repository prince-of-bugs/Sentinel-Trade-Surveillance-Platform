package com.sentinel.ingest;

/**
 * The payload can never be processed: malformed JSON, a missing field, a bad value. Retrying won't
 * help, so the error handler skips retries and sends the record straight to {@code trades.dlt}.
 */
public class InvalidEventException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidEventException(String reason) {
        super(reason);
    }

    public InvalidEventException(String reason, Throwable cause) {
        super(reason, cause);
    }
}
