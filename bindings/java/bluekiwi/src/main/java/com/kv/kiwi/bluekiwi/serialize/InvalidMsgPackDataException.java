package com.kv.kiwi.bluekiwi.serialize;

import java.io.IOException;

/**
 * Thrown when we can't unpack the given bytes to normal data.
 * 
 * @author jon
 */
public class InvalidMsgPackDataException extends IOException {
    public InvalidMsgPackDataException() {
        super();
    }

    public InvalidMsgPackDataException(String message) {
        super(message);
    }

    public InvalidMsgPackDataException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidMsgPackDataException(Throwable cause) {
        super(cause);
    }
}