// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.api.integration.athenz;

/**
 * @author bjorncs
 */
public class ZtsException extends RuntimeException {

    private final int code;

    public ZtsException(int code, Throwable cause) {
        super(cause.getMessage(), cause);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
