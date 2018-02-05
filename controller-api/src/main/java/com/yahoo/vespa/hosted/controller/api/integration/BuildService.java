// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.api.integration;

/**
 * @author jvenstad
 */
public interface BuildService {

    /**
     * Enqueue a job defined by "buildJob in an external build system, and return the outcome of the enqueue request.
     * This method should return @false only when a retry is in order, and @true otherwise, e.g., on success, or for
     * invalid jobs.
     */
    boolean trigger(BuildJob buildJob);

    class BuildJob {

        private final long projectId;
        private final String jobName;

        public BuildJob(long projectId, String jobName) {
            this.projectId = projectId;
            this.jobName = jobName;
        }

        public long projectId() { return projectId; }
        public String jobName() { return jobName; }

        @Override
        public String toString() { return jobName + "@" + projectId; }

    }

}
