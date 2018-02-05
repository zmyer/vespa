// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.application;

import java.util.Objects;
import java.util.Optional;

/**
 * An application package version. This represents an build artifact, identified by a source revision and a build
 * number.
 *
 * @author bratseth
 * @author mpolden
 */
public class ApplicationVersion implements Comparable<ApplicationVersion> {

    /**
     * Used in cases where application version cannot be determined, such as manual deployments (e.g. in dev
     * environment)
     */
    public static final ApplicationVersion unknown = new ApplicationVersion();

    // This never changes and is only used to create a valid semantic version number, as required by application bundles
    private static final String majorVersion = "1.0";

    // TODO: Remove after 2018-03-01
    private final Optional<String> applicationPackageHash;
    private final Optional<SourceRevision> source;
    private final Optional<Long> buildNumber;

    private ApplicationVersion() {
        this.applicationPackageHash = Optional.empty();
        this.source = Optional.empty();
        this.buildNumber = Optional.empty();
    }

    private ApplicationVersion(Optional<String> applicationPackageHash, Optional<SourceRevision> source,
                               Optional<Long> buildNumber) {
        Objects.requireNonNull(applicationPackageHash, "applicationPackageHash cannot be null");
        Objects.requireNonNull(source, "source cannot be null");
        Objects.requireNonNull(buildNumber, "buildNumber cannot be null");
        if (!applicationPackageHash.isPresent() && source.isPresent() != buildNumber.isPresent()) {
            throw new IllegalArgumentException("both buildNumber and source must be set together");
        }
        if (buildNumber.isPresent() && buildNumber.get() <= 0) {
            throw new IllegalArgumentException("buildNumber must be > 0");
        }
        this.applicationPackageHash = applicationPackageHash;
        this.source = source;
        this.buildNumber = buildNumber;
    }

    /** Create an application package revision where there is no information about its source */
    public static ApplicationVersion from(String applicationPackageHash) {
        return new ApplicationVersion(Optional.of(applicationPackageHash), Optional.empty(), Optional.empty());
    }

    /** Create an application package revision with a source */
    public static ApplicationVersion from(String applicationPackageHash, SourceRevision source) {
        return new ApplicationVersion(Optional.of(applicationPackageHash), Optional.of(source), Optional.empty());
    }

    /** Create an application package version from a completed build */
    public static ApplicationVersion from(SourceRevision source, long buildNumber) {
        return new ApplicationVersion(Optional.empty(), Optional.of(source), Optional.of(buildNumber));
    }

    /** Returns an unique identifier for this version */
    public String id() {
        if (applicationPackageHash.isPresent()) {
            return applicationPackageHash.get();
        }
        return source.map(sourceRevision -> String.format("%s.%d-%s", majorVersion, buildNumber.get(),
                                                          abbreviateCommit(source.get().commit())))
                     .orElse("unknown");
    }

    /** Returns the application package hash, if known */
    public Optional<String> applicationPackageHash() { return applicationPackageHash; }

    /**
     * Returns information about the source of this revision, or empty if the source is not know/defined
     * (which is the case for command-line deployment from developers, but never for deployment jobs)
     */
    public Optional<SourceRevision> source() { return source; }

    /** Returns the build number that built this version */
    public Optional<Long> buildNumber() { return buildNumber; }

    /** Returns whether this is unknown */
    public boolean isUnknown() {
        return this.equals(unknown);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplicationVersion that = (ApplicationVersion) o;
        return Objects.equals(applicationPackageHash, that.applicationPackageHash) &&
               Objects.equals(source, that.source) &&
               Objects.equals(buildNumber, that.buildNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(applicationPackageHash, source, buildNumber);
    }

    @Override
    public String toString() {
        return "Application package version: " + id() + source.map(s -> ", " + s.toString()).orElse("");
    }

    /** Abbreviate given commit hash to 9 characters */
    private static String abbreviateCommit(String hash) {
        return hash.length() <= 9 ? hash : hash.substring(0, 9);
    }

    @Override
    public int compareTo(ApplicationVersion o) {
        if (!buildNumber().isPresent() || !o.buildNumber().isPresent()) {
            return 0; // No sorting for application package hash
        }
        return buildNumber().get().compareTo(o.buildNumber().get());
    }
}
