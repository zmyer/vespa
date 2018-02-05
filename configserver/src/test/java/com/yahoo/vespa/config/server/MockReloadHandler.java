// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.config.server;

import com.yahoo.config.provision.ApplicationId;
import com.yahoo.vespa.config.server.application.ApplicationSet;

import java.util.Set;

/**
 * @author lulf
 * @since 5.1.24
 */
public class MockReloadHandler implements ReloadHandler {

    public ApplicationSet current = null;
    public volatile ApplicationId lastRemoved = null;

    @Override
    public void reloadConfig(ApplicationSet application) {
        this.current = application;
    }

    @Override
    public void removeApplication(ApplicationId applicationId) {
        lastRemoved = applicationId;
    }

    @Override
    public void removeApplicationsExcept(Set<ApplicationId> applicationIds) { }

}
