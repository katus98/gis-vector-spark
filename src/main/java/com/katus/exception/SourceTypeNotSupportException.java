package com.katus.exception;

/**
 * @author SUN Katus
 * @version 1.0, 2021-04-12
 * @since 2.0
 */
public class SourceTypeNotSupportException extends RuntimeException {

    public SourceTypeNotSupportException(String source) {
        super("Not Support this source type! Source: " + source);
    }
}
