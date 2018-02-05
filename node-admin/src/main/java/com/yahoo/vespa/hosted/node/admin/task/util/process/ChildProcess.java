// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.node.admin.task.util.process;

import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * @author hakonhall
 */
public interface ChildProcess extends AutoCloseable {
    String commandLine();
    ChildProcess waitForTermination();
    int exitValue();
    ChildProcess throwIfFailed();
    String getUtf8Output();

    /**
     * Only call this if process was spawned under the assumption the program had no side
     * effects (see Command::spawnProgramWithoutSideEffects).  If it is determined later
     * that the program did in fact have side effects (modified system), this method can
     * be used to log that fact. Alternatively, call TaskContext::recordSystemModification
     * directly.
     */
    void logAsModifyingSystemAfterAll(Logger logger);

    @Override
    void close();

    // For testing only
    Path getProcessOutputPath();
}
