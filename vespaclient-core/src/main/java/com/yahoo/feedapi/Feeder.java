// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.feedapi;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.stream.XMLStreamException;

import com.yahoo.document.DocumentTypeManager;
import com.yahoo.vespaxmlparser.FeedReader;
import com.yahoo.vespaxmlparser.VespaXMLFeedReader;

/**
 * Base class for unpacking document operation streams and pushing to feed
 * access points.
 *
 * @author Thomas Gundersen
 * @author steinar
 */
public abstract class Feeder {

    protected final InputStream stream;
    protected final DocumentTypeManager docMan;
    protected List<String> errors = new LinkedList<String>();
    protected boolean doAbort = true;
    protected boolean createIfNonExistent = false;
    protected AtomicBoolean documentError = new AtomicBoolean(false);
    protected final VespaFeedSender sender;
    private final ExecutorService orderedExecutor = Executors.newSingleThreadExecutor();
    private final int MAX_ERRORS = 10;

    protected Feeder(DocumentTypeManager docMan, VespaFeedSender sender, InputStream stream) {
        this.docMan = docMan;
        this.sender = sender;
        this.stream = stream;
    }

    public void setAbortOnDocumentError(boolean doAbort) {
        this.doAbort = doAbort;
    }

    public void setCreateIfNonExistent(boolean value) {
        this.createIfNonExistent = value;
    }

    public void addException(Exception e) {
        String message;
        if (e.getMessage() != null) {
            message = e.getMessage().replaceAll("\"", "'");
        } else {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            message = "(no message) " + sw.toString();
        }

        addError("ERROR: " + message);
    }

    private void addError(String error) {
        if (errors.size() < MAX_ERRORS) {
            errors.add(error);
        } else if (errors.size() == MAX_ERRORS) {
            errors.add("Reached maximum limit of errors (" + MAX_ERRORS + "). Not collecting any more.");
        }
    }

    protected abstract FeedReader createReader() throws Exception;

    public List<String> parse() {
        FeedReader reader = null;

        try {
            reader = createReader();
        } catch (Exception e) {
            addError("ERROR: " + e.getClass().toString() + ": " + e.getMessage().replaceAll("\"", "'"));
            return errors;
        }

        while (!sender.isAborted() && !documentError.get()) {
            try {
                Future<VespaXMLFeedReader.Operation> op = reader.read();
                orderedExecutor.execute(() -> send(op));
            } catch (XMLStreamException e) {
                addException(e);
                break;
            } catch (Exception e) {
                addException(e);
                if (doAbort) {
                    break;
                }
            }
        }
        orderedExecutor.shutdown();
        try {
            orderedExecutor.awaitTermination(-1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }

        return errors;
    }

    void send(Future<VespaXMLFeedReader.Operation> future) {
        try {
            VespaXMLFeedReader.Operation op = future.get();
            if (createIfNonExistent && op.getDocumentUpdate() != null) {
                op.getDocumentUpdate().setCreateIfNonExistent(true);
            }

            // Done feeding.
            if (op.getType() == VespaXMLFeedReader.OperationType.INVALID) {
                documentError.set(true);
            } else {
                sender.sendOperation(op);
            }
        } catch (InterruptedException e) {
        } catch (ExecutionException e) {
        }
    }

}
