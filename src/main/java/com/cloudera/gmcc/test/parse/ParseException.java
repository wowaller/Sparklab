package com.cloudera.gmcc.test.parse;

public class ParseException extends Exception {
    public ParseException(String message, Exception cause) {
        super(message, cause);
    }
}
