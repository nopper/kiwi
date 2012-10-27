package com.kv.kiwi.bluekiwi.serialize;


/**
 * Thrown when we try to pack something we can't pack.
 * 
 * @author jon
 */
public class UnpackableItemException extends IllegalArgumentException {
    public UnpackableItemException() {
        super();
    }

    public UnpackableItemException(String message) {
        super(message);
    }

    public UnpackableItemException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnpackableItemException(Throwable cause) {
        super(cause);
    }
}