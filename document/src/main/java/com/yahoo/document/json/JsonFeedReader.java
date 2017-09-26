// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.document.json;

import java.io.InputStream;
import java.util.concurrent.Future;

import com.fasterxml.jackson.core.JsonFactory;
import com.yahoo.document.DocumentOperation;
import com.yahoo.document.DocumentPut;
import com.yahoo.document.DocumentRemove;
import com.yahoo.document.DocumentTypeManager;
import com.yahoo.document.DocumentUpdate;
import com.yahoo.vespaxmlparser.FeedReader;
import com.yahoo.vespaxmlparser.VespaXMLFeedReader.Operation;


/**
 * Facade between JsonReader and the FeedReader API.
 *
 * <p>
 * The feed reader will take ownership of the input stream and close it when the
 * last parseable document has been read.
 *
 * @author steinar
 */
public class JsonFeedReader implements FeedReader {
    private final JsonReader reader;
    private static final JsonFactory jsonFactory = new JsonFactory().disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES);

    public JsonFeedReader(InputStream stream, DocumentTypeManager docMan) {
        reader = new JsonReader(docMan, stream, jsonFactory);
    }

    @Override
    public Future<Operation> readOne() throws Exception {
        return reader.nextFuture();
    }

}
