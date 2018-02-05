// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.clustercontroller.utils.staterestapi.server;

import com.yahoo.log.LogLevel;
import com.yahoo.yolean.Exceptions;
import com.yahoo.vespa.clustercontroller.utils.communication.http.HttpRequest;
import com.yahoo.vespa.clustercontroller.utils.communication.http.HttpRequestHandler;
import com.yahoo.vespa.clustercontroller.utils.communication.http.HttpResult;
import com.yahoo.vespa.clustercontroller.utils.communication.http.JsonHttpResult;
import com.yahoo.vespa.clustercontroller.utils.staterestapi.StateRestAPI;
import com.yahoo.vespa.clustercontroller.utils.staterestapi.errors.*;
import com.yahoo.vespa.clustercontroller.utils.staterestapi.requests.SetUnitStateRequest;
import com.yahoo.vespa.clustercontroller.utils.staterestapi.requests.UnitStateRequest;
import com.yahoo.vespa.clustercontroller.utils.staterestapi.response.*;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RestApiHandler implements HttpRequestHandler {

    private final static Logger log = Logger.getLogger(RestApiHandler.class.getName());

    private final StateRestAPI restApi;
    private final JsonWriter jsonWriter;
    private final JsonReader jsonReader = new JsonReader();

    public RestApiHandler(StateRestAPI restApi) {
        this.restApi = restApi;
        this.jsonWriter = new JsonWriter();
    }

    public RestApiHandler setDefaultPathPrefix(String defaultPathPrefix) {
        jsonWriter.setDefaultPathPrefix(defaultPathPrefix);
        return this;
    }

    private static void logRequestException(HttpRequest request, Exception exception, Level level) {
        String exceptionString = Exceptions.toMessageString(exception);
        log.log(level, "Failed to process request with URI path " +
                request.getPath() + ": " + exceptionString);
    }

    @Override
    public HttpResult handleRequest(HttpRequest request) {
        try{
            final String[] unitPath = createUnitPath(request);
            if (request.getHttpOperation().equals(HttpRequest.HttpOp.GET)) {
                final int recursiveLevel = getRecursiveLevel(request);
                UnitResponse data = restApi.getState(new UnitStateRequest() {
                    @Override
                    public int getRecursiveLevels() {
                        return recursiveLevel;
                    }
                    @Override
                    public String[] getUnitPath() {
                        return unitPath;
                    }
                });
                return new JsonHttpResult().setJson(jsonWriter.createJson(data));
            } else {
                final JsonReader.SetRequestData setRequestData = jsonReader.getStateRequestData(request);
                SetResponse setResponse = restApi.setUnitState(new SetUnitStateRequest() {
                    @Override
                    public Map<String, UnitState> getNewState() {
                        return setRequestData.stateMap;
                    }
                    @Override
                    public String[] getUnitPath() {
                        return unitPath;
                    }
                    @Override
                    public Condition getCondition() { return setRequestData.condition; }
                    @Override
                    public ResponseWait getResponseWait() { return setRequestData.responseWait; }
                });
                return new JsonHttpResult().setJson(jsonWriter.createJson(setResponse));
            }
        } catch (OtherMasterException exception) {
            logRequestException(request, exception, LogLevel.DEBUG);
            JsonHttpResult result = new JsonHttpResult();
            result.setHttpCode(307, "Temporary Redirect");
            result.addHeader("Location", getMasterLocationUrl(request, exception.getHost(), exception.getPort()));
            result.setJson(jsonWriter.createErrorJson(exception.getMessage()));
            return result;
        } catch (UnknownMasterException exception) {
            logRequestException(request, exception, Level.WARNING);
            JsonHttpResult result = new JsonHttpResult();
            result.setHttpCode(503, "Service Unavailable");
            result.setJson(jsonWriter.createErrorJson(exception.getMessage()));
            return result;
        } catch (DeadlineExceededException exception) {
            logRequestException(request, exception, Level.WARNING);
            JsonHttpResult result = new JsonHttpResult();
            result.setHttpCode(504, "Gateway Timeout");
            result.setJson(jsonWriter.createErrorJson(exception.getMessage()));
            return result;
        } catch (StateRestApiException exception) {
            logRequestException(request, exception, Level.WARNING);
            JsonHttpResult result = new JsonHttpResult();
            result.setHttpCode(500, "Failed to process request");
            if (exception.getStatus() != null) result.setHttpCode(result.getHttpReturnCode(), exception.getStatus());
            if (exception.getCode() != null) result.setHttpCode(exception.getCode(), result.getHttpReturnCodeDescription());
            result.setJson(jsonWriter.createErrorJson(exception.getMessage()));
            return result;
        } catch (Exception exception) {
            logRequestException(request, exception, LogLevel.ERROR);
            JsonHttpResult result = new JsonHttpResult();
            result.setHttpCode(500, "Failed to process request");
            result.setJson(jsonWriter.createErrorJson(exception.getClass().getName() + ": " + exception.getMessage()));
            return result;
        }
    }

    private String[] createUnitPath(HttpRequest request) {
        List<String> path = Arrays.asList(request.getPath().split("/"));
        return path.subList(3, path.size()).toArray(new String[0]);
    }

    private int getRecursiveLevel(HttpRequest request) throws StateRestApiException {
        String val = request.getOption("recursive", "false");
        if (val.toLowerCase().equals("false")) { return 0; }
        if (val.toLowerCase().equals("true")) { return Integer.MAX_VALUE; }
        int level;
        try{
            level = Integer.parseInt(val);
            if (level < 0) throw new NumberFormatException();
        } catch (NumberFormatException e) {
            throw new InvalidOptionValueException(
                    "recursive", val, "Recursive option must be true, false, 0 or a positive integer");
        }
        return level;
    }

    private String getMasterLocationUrl(HttpRequest request, String host, int port) {
        StringBuilder sb = new StringBuilder();
        sb.append("http://").append(host).append(':').append(port)
          .append(request.getPath());
        if (!request.getUrlOptions().isEmpty()) {
            boolean first = true;
            for (HttpRequest.KeyValuePair kvp : request.getUrlOptions()) {
                sb.append(first ? '?' : '&');
                first = false;
                sb.append(httpEscape(kvp.getKey())).append('=').append(httpEscape(kvp.getValue()));
            }
        }
        return sb.toString();
    }

    private static class Escape {
        public final String pattern;
        public final String replaceWith;

        public Escape(String pat, String repl) {
            this.pattern = pat;
            this.replaceWith = repl;
        }
    }
    private static List<Escape> escapes = new ArrayList<>();
    static {
        escapes.add(new Escape("%", "%25"));
        escapes.add(new Escape(" ", "%20"));
        escapes.add(new Escape("\\?", "%3F"));
        escapes.add(new Escape("=", "%3D"));
        escapes.add(new Escape("\\&", "%26"));
    }

    private static String httpEscape(String value) {
        for(Escape e : escapes) {
            value = value.replaceAll(e.pattern, e.replaceWith);
        }
        return value;
    }

}
