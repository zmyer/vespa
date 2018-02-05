// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.clustercontroller.apps.clustercontroller;

import com.google.inject.Inject;
import com.yahoo.vespa.clustercontroller.apputil.communication.http.JDiscHttpRequestHandler;

public class StatusHandler extends JDiscHttpRequestHandler {

    private final com.yahoo.vespa.clustercontroller.core.status.StatusHandler statusHandler;

    @Inject
    public StatusHandler(ClusterController fc, JDiscHttpRequestHandler.Context ctx) {
        this(new com.yahoo.vespa.clustercontroller.core.status.StatusHandler(fc), ctx);
    }

    private StatusHandler(com.yahoo.vespa.clustercontroller.core.status.StatusHandler handler,
                          JDiscHttpRequestHandler.Context ctx)
    {
        super(handler, ctx);
        this.statusHandler = handler;
    }

}
