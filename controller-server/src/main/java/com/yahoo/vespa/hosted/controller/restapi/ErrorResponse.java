// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.restapi;

import com.yahoo.slime.Cursor;
import com.yahoo.slime.Slime;
import com.yahoo.vespa.hosted.controller.api.integration.configserver.ConfigServerException;
import com.yahoo.yolean.Exceptions;

import static com.yahoo.jdisc.Response.Status.BAD_REQUEST;
import static com.yahoo.jdisc.Response.Status.CONFLICT;
import static com.yahoo.jdisc.Response.Status.FORBIDDEN;
import static com.yahoo.jdisc.Response.Status.INTERNAL_SERVER_ERROR;
import static com.yahoo.jdisc.Response.Status.METHOD_NOT_ALLOWED;
import static com.yahoo.jdisc.Response.Status.NOT_FOUND;
import static com.yahoo.jdisc.Response.Status.UNAUTHORIZED;

/**
 * A HTTP JSON response containing an error code and a message
 * 
 * @author bratseth
 */
public class ErrorResponse extends SlimeJsonResponse {

    public enum errorCodes {
        NOT_FOUND,
        BAD_REQUEST,
        FORBIDDEN,
        METHOD_NOT_ALLOWED,
        INTERNAL_SERVER_ERROR,
        UNAUTHORIZED
    }

    public ErrorResponse(int statusCode, String errorType, String message) {
        super(statusCode, asSlimeMessage(errorType, message));
    }

    private static Slime asSlimeMessage(String errorType, String message) {
        Slime slime = new Slime();
        Cursor root = slime.setObject();
        root.setString("error-code", errorType);
        root.setString("message", message);
        return slime;
    }
    
    public static ErrorResponse notFoundError(String message) {
        return new ErrorResponse(NOT_FOUND, errorCodes.NOT_FOUND.name(), message);
    }

    public static ErrorResponse internalServerError(String message) {
        return new ErrorResponse(INTERNAL_SERVER_ERROR, errorCodes.INTERNAL_SERVER_ERROR.name(), message);
    }

    public static ErrorResponse badRequest(String message) {
        return new ErrorResponse(BAD_REQUEST, errorCodes.BAD_REQUEST.name(), message);
    }

    public static ErrorResponse forbidden(String message) {
        return new ErrorResponse(FORBIDDEN, errorCodes.FORBIDDEN.name(), message);
    }

    public static ErrorResponse unauthorized(String message) {
        return new ErrorResponse(UNAUTHORIZED, errorCodes.UNAUTHORIZED.name(), message);
    }

    public static ErrorResponse methodNotAllowed(String message) {
        return new ErrorResponse(METHOD_NOT_ALLOWED, errorCodes.METHOD_NOT_ALLOWED.name(), message);
    }

    public static ErrorResponse from(ConfigServerException e) {
        switch (e.getErrorCode()) {
            case NOT_FOUND:
                return new ErrorResponse(NOT_FOUND, e.getErrorCode().name(), Exceptions.toMessageString(e));
            case ACTIVATION_CONFLICT:
                return new ErrorResponse(CONFLICT, e.getErrorCode().name(), Exceptions.toMessageString(e));
            case INTERNAL_SERVER_ERROR:
                return new ErrorResponse(INTERNAL_SERVER_ERROR, e.getErrorCode().name(), Exceptions.toMessageString(e));
            default:
                return new ErrorResponse(BAD_REQUEST, e.getErrorCode().name(), Exceptions.toMessageString(e));
        }
    }

}
