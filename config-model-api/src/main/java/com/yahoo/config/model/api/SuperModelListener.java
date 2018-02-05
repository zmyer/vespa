// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.config.model.api;

import com.yahoo.config.provision.ApplicationId;

/**
 * Interface for those wanting to be notified about changes to the SuperModel.
 */
public interface SuperModelListener {
    /**
     * Application has been activated: Either deployed the first time,
     * internally redeployed, or externally triggered redeploy.
     */
    void applicationActivated(SuperModel superModel, ApplicationInfo application);

    /**
     * Application has been removed.
     */
    void applicationRemoved(SuperModel superModel, ApplicationId id);
}
