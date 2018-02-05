// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.jdisc.http.server.jetty;

import com.yahoo.container.logging.AccessLogEntry;
import com.yahoo.jdisc.Metric.Context;
import com.yahoo.jdisc.References;
import com.yahoo.jdisc.ResourceReference;
import com.yahoo.jdisc.Response;
import com.yahoo.jdisc.handler.BindingNotFoundException;
import com.yahoo.jdisc.handler.ContentChannel;
import com.yahoo.jdisc.handler.OverloadException;
import com.yahoo.jdisc.handler.RequestHandler;
import com.yahoo.jdisc.http.HttpHeaders;
import com.yahoo.jdisc.http.HttpRequest;
import org.eclipse.jetty.io.EofException;
import org.eclipse.jetty.server.HttpConnection;

import javax.servlet.AsyncContext;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.yahoo.jdisc.http.HttpHeaders.Values.APPLICATION_X_WWW_FORM_URLENCODED;
import static com.yahoo.jdisc.http.core.HttpServletRequestUtils.getConnection;
import static com.yahoo.jdisc.http.server.jetty.Exceptions.throwUnchecked;

/**
 * @author Simon Thoresen Hult
 * @author bjorncs
 */
class HttpRequestDispatch {

    private static final Logger log = Logger.getLogger(HttpRequestDispatch.class.getName());

    private final static String CHARSET_ANNOTATION = ";charset=";

    private final JDiscContext jDiscContext;
    private final AsyncContext async;
    private final HttpServletRequest servletRequest;

    private final ServletResponseController servletResponseController;
    private final RequestHandler requestHandler;
    private final MetricReporter metricReporter;

    public HttpRequestDispatch(JDiscContext jDiscContext,
                               AccessLogEntry accessLogEntry,
                               Context metricContext,
                               HttpServletRequest servletRequest,
                               HttpServletResponse servletResponse) throws IOException {
        this.jDiscContext = jDiscContext;

        requestHandler = newRequestHandler(jDiscContext, accessLogEntry, servletRequest);

        this.metricReporter = new MetricReporter(jDiscContext.metric, metricContext,
                ((org.eclipse.jetty.server.Request) servletRequest).getTimeStamp());
        this.servletRequest = servletRequest;
        honourMaxKeepAliveRequests();
        this.servletResponseController = new ServletResponseController(
                servletRequest,
                servletResponse,
                jDiscContext.janitor,
                metricReporter,
                jDiscContext.developerMode());

        this.async = servletRequest.startAsync();
        async.setTimeout(0);
    }

    public void dispatch() throws IOException {
        ServletRequestReader servletRequestReader;
        try {
            servletRequestReader = handleRequest();
        } catch (Throwable throwable) {
            servletResponseController.trySendError(throwable);
            servletResponseController.finishedFuture().whenComplete((result, exception) ->
                    completeRequestCallback.accept(null, throwable));
            return;
        }

        try {
            onError(servletRequestReader.finishedFuture,
                    servletResponseController::trySendError);

            onError(servletResponseController.finishedFuture(),
                    servletRequestReader::onError);

            CompletableFuture.allOf(servletRequestReader.finishedFuture, servletResponseController.finishedFuture())
                    .whenComplete(completeRequestCallback);
        } catch (Throwable throwable) {
            log.log(Level.WARNING, "Failed registering finished listeners.", throwable);
        }
    }

    private void honourMaxKeepAliveRequests() {
        if (jDiscContext.serverConfig.maxKeepAliveRequests() > 0) {
            HttpConnection connection = getConnection(servletRequest);
            if (connection.getMessagesIn() >= jDiscContext.serverConfig.maxKeepAliveRequests()) {
                connection.getGenerator().setPersistent(false);
            }
        }
    }

    private BiConsumer<Void, Throwable> completeRequestCallback;
    {
        AtomicBoolean completeRequestCalled = new AtomicBoolean(false);
        HttpRequestDispatch parent = this; //used to avoid binding uninitialized variables

        completeRequestCallback = (result, error) -> {
            boolean alreadyCalled = completeRequestCalled.getAndSet(true);
            if (alreadyCalled) {
                AssertionError e = new AssertionError("completeRequest called more than once");
                log.log(Level.WARNING, "Assertion failed.", e);
                throw e;
            }

            boolean reportedError = false;

            if (error != null) {
                if (error instanceof CompletionException && error.getCause() instanceof EofException) {
                    log.log(Level.FINE,
                            error,
                            () -> "Network connection was unexpectedly terminated: " + parent.servletRequest.getRequestURI());
                } else if (!(error instanceof OverloadException || error instanceof BindingNotFoundException)) {
                    log.log(Level.WARNING, "Request failed: " + parent.servletRequest.getRequestURI(), error);
                }
                reportedError = true;
                parent.metricReporter.failedResponse();
            } else {
                parent.metricReporter.successfulResponse();
            }

            try {
                parent.async.complete();
                log.finest(() -> "Request completed successfully: " + parent.servletRequest.getRequestURI());
            } catch (Throwable throwable) {
                Level level = reportedError ? Level.FINE: Level.WARNING;
                log.log(level, "async.complete failed", throwable);
            }
        };
    }

    @SuppressWarnings("try")
    private ServletRequestReader handleRequest() throws IOException {
        HttpRequest jdiscRequest = HttpRequestFactory.newJDiscRequest(jDiscContext.container, servletRequest);
        ContentChannel requestContentChannel;

        try (ResourceReference ref = References.fromResource(jdiscRequest)) {
            HttpRequestFactory.copyHeaders(servletRequest, jdiscRequest);
            requestContentChannel = requestHandler.handleRequest(jdiscRequest, servletResponseController.responseHandler);
        }

        ServletInputStream servletInputStream = servletRequest.getInputStream();

        ServletRequestReader servletRequestReader =
                new ServletRequestReader(
                        servletInputStream,
                        requestContentChannel,
                        jDiscContext.janitor,
                        metricReporter);

        servletInputStream.setReadListener(servletRequestReader);
        return servletRequestReader;
    }

    private static void onError(CompletableFuture<?> future, Consumer<Throwable> errorHandler) {
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                errorHandler.accept(exception);
            }
        });
    }

    ContentChannel handleRequestFilterResponse(Response response) {
        try {
            servletRequest.getInputStream().close();
            ContentChannel responseContentChannel = servletResponseController.responseHandler.handleResponse(response);
            servletResponseController.finishedFuture().whenComplete(completeRequestCallback);
            return responseContentChannel;
        } catch (IOException e) {
            throw throwUnchecked(e);
        }
    }


    private static RequestHandler newRequestHandler(JDiscContext context,
                                                    AccessLogEntry accessLogEntry,
                                                    HttpServletRequest servletRequest) {
        RequestHandler requestHandler = wrapHandlerIfFormPost(
                new FilteringRequestHandler(context.requestFilters, context.responseFilters),
                servletRequest, context.serverConfig.removeRawPostBodyForWwwUrlEncodedPost());

        return new AccessLoggingRequestHandler(requestHandler, accessLogEntry);
    }

    private static RequestHandler wrapHandlerIfFormPost(RequestHandler requestHandler,
                                                        HttpServletRequest servletRequest,
                                                        boolean removeBodyForFormPost) {
        if (!servletRequest.getMethod().equals("POST")) {
            return requestHandler;
        }
        String contentType = servletRequest.getHeader(HttpHeaders.Names.CONTENT_TYPE);
        if (contentType == null) {
            return requestHandler;
        }
        if (!contentType.startsWith(APPLICATION_X_WWW_FORM_URLENCODED)) {
            return requestHandler;
        }
        return new FormPostRequestHandler(requestHandler, getCharsetName(contentType), removeBodyForFormPost);
    }

    private static String getCharsetName(String contentType) {
        if (!contentType.startsWith(CHARSET_ANNOTATION, APPLICATION_X_WWW_FORM_URLENCODED.length())) {
            return StandardCharsets.UTF_8.name();
        }
        return contentType.substring(APPLICATION_X_WWW_FORM_URLENCODED.length() + CHARSET_ANNOTATION.length());
    }
}
