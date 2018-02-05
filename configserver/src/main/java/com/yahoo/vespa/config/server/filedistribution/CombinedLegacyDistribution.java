// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.config.server.filedistribution;

import com.yahoo.config.FileReference;
import com.yahoo.config.model.api.FileDistribution;
import com.yahoo.jrt.ErrorCode;
import com.yahoo.jrt.Request;
import com.yahoo.jrt.Spec;
import com.yahoo.jrt.StringArray;
import com.yahoo.jrt.Supervisor;
import com.yahoo.jrt.Target;
import com.yahoo.log.LogLevel;

import java.util.Collection;
import java.util.Set;
import java.util.logging.Logger;

/**
 * @author baldersheim
 */
public class CombinedLegacyDistribution implements FileDistribution {
    private final static Logger log = Logger.getLogger(CombinedLegacyDistribution.class.getName());

    private final Supervisor supervisor;
    private final FileDistribution legacy;
    private final boolean disableFileDistributor;

    CombinedLegacyDistribution(Supervisor supervisor, FileDBHandler legacy, boolean disableFileDistributor) {
        this.supervisor = supervisor;
        this.legacy = legacy;
        this.disableFileDistributor = disableFileDistributor;
    }

    @Override
    public void sendDeployedFiles(String hostName, Set<FileReference> fileReferences) {
        legacy.sendDeployedFiles(hostName, fileReferences);
    }

    @Override
    public void startDownload(String hostName, int port, Set<FileReference> fileReferences) {
        if (disableFileDistributor)
            startDownloadingFileReferences(hostName, port, fileReferences);
    }

    @Override
    public void reloadDeployFileDistributor() {
        legacy.reloadDeployFileDistributor();
    }

    @Override
    public void removeDeploymentsThatHaveDifferentApplicationId(Collection<String> targetHostnames) {
        legacy.removeDeploymentsThatHaveDifferentApplicationId(targetHostnames);
    }

    // Notifies config proxy which file references it should start downloading. It's OK if the call does not succeed,
    // as downloading will then start synchronously when a service requests a file reference instead
    private void startDownloadingFileReferences(String hostName, int port, Set<FileReference> fileReferences) {
        Target target = supervisor.connect(new Spec(hostName, port));
        double timeout = 0.1;
        Request request = new Request("filedistribution.setFileReferencesToDownload");
        request.parameters().add(new StringArray(fileReferences.stream().map(FileReference::value).toArray(String[]::new)));
        log.log(LogLevel.DEBUG, "Executing " + request.methodName() + " against " + target.toString());
        target.invokeSync(request, timeout);
        if (request.isError() && request.errorCode() != ErrorCode.CONNECTION) {
            log.log(LogLevel.INFO, request.methodName() + " failed: " + request.errorCode() + " (" + request.errorMessage() + ")");
        }
        target.close();
    }
}
