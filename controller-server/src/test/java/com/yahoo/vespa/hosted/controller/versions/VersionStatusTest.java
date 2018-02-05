// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.versions;

import com.google.common.collect.ImmutableSet;
import com.yahoo.component.Version;
import com.yahoo.component.Vtag;
import com.yahoo.config.provision.Environment;
import com.yahoo.vespa.hosted.controller.Application;
import com.yahoo.vespa.hosted.controller.ApplicationController;
import com.yahoo.vespa.hosted.controller.Controller;
import com.yahoo.vespa.hosted.controller.ControllerTester;
import com.yahoo.vespa.hosted.controller.api.identifiers.TenantId;
import com.yahoo.vespa.hosted.controller.application.ApplicationPackage;
import com.yahoo.vespa.hosted.controller.deployment.ApplicationPackageBuilder;
import com.yahoo.vespa.hosted.controller.deployment.DeploymentTester;
import com.yahoo.vespa.hosted.controller.versions.VespaVersion.Confidence;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;

import static com.yahoo.vespa.hosted.controller.application.DeploymentJobs.JobType.component;
import static com.yahoo.vespa.hosted.controller.application.DeploymentJobs.JobType.productionUsEast3;
import static com.yahoo.vespa.hosted.controller.application.DeploymentJobs.JobType.productionUsWest1;
import static com.yahoo.vespa.hosted.controller.application.DeploymentJobs.JobType.stagingTest;
import static com.yahoo.vespa.hosted.controller.application.DeploymentJobs.JobType.systemTest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test computing of version status
 * 
 * @author bratseth
 */
public class VersionStatusTest {
    
    @Test
    public void testEmptyVersionStatus() {
        VersionStatus status = VersionStatus.empty();
        assertFalse(status.systemVersion().isPresent());
        assertTrue(status.versions().isEmpty());
    }

    @Test
    public void testSystemVersionIsControllerVersionIfConfigserversAreNewer() {
        ControllerTester tester = new ControllerTester();
        Version largerThanCurrent = new Version(Vtag.currentVersion.getMajor() + 1);
        tester.configServer().setDefaultVersion(largerThanCurrent);
        VersionStatus versionStatus = VersionStatus.compute(tester.controller());
        assertEquals(Vtag.currentVersion, versionStatus.systemVersion().get().versionNumber());
    }

    @Test
    public void testSystemVersionIsVersionOfOldestConfigServer() throws URISyntaxException {
        ControllerTester tester = new ControllerTester();
        Version oldest = new Version(5);
        tester.configServer().versions().put(new URI("https://cfg.prod.corp-us-east-1.test:4443"), oldest);
        VersionStatus versionStatus = VersionStatus.compute(tester.controller());
        assertEquals(oldest, versionStatus.systemVersion().get().versionNumber());
    }

    @Test
    public void testVersionStatusAfterApplicationUpdates() {
        DeploymentTester tester = new DeploymentTester();
        ApplicationPackage applicationPackage = new ApplicationPackageBuilder()
                .upgradePolicy("default")
                .environment(Environment.prod)
                .region("us-west-1")
                .region("us-east-3")
                .build();

        Version version1 = new Version("5.1");
        Version version2 = new Version("5.2");
        tester.upgradeSystem(version1);

        // Setup applications
        Application app1 = tester.createAndDeploy("app1", 11, applicationPackage);
        Application app2 = tester.createAndDeploy("app2", 22, applicationPackage);
        Application app3 = tester.createAndDeploy("app3", 33, applicationPackage);

        // version2 is released
        tester.upgradeSystem(version2);

        // - app1 is in production on version1, but then fails in system test on version2
        tester.completeUpgradeWithError(app1, version2, applicationPackage, systemTest);
        // - app2 is partially in production on version1 and partially on version2
        tester.completeUpgradeWithError(app2, version2, applicationPackage, productionUsEast3);
        // - app3 is in production on version1, but then fails in staging test on version2
        tester.completeUpgradeWithError(app3, version2, applicationPackage, stagingTest);

        tester.updateVersionStatus();
        List<VespaVersion> versions = tester.controller().versionStatus().versions();
        assertEquals("The two versions above exist", 2, versions.size());

        System.err.println(tester.controller().applications().deploymentTrigger().jobTimeoutLimit());

        VespaVersion v1 = versions.get(0);
        assertEquals(version1, v1.versionNumber());
        assertEquals("No applications are failing on version1.", ImmutableSet.of(), v1.statistics().failing());
        assertEquals("All applications have at least one active production deployment on version 1.", ImmutableSet.of(app1.id(), app2.id(), app3.id()), v1.statistics().production());
        assertEquals("No applications have active deployment jobs on version1.", ImmutableSet.of(), v1.statistics().deploying());

        VespaVersion v2 = versions.get(1);
        assertEquals(version2, v2.versionNumber());
        assertEquals("All applications have failed on version2 in at least one zone.", ImmutableSet.of(app1.id(), app2.id(), app3.id()), v2.statistics().failing());
        assertEquals("Only app2 has successfully deployed to production on version2.", ImmutableSet.of(app2.id()), v2.statistics().production());
        // Should test the below, but can't easily be done with current test framework. This test passes in DeploymentApiTest.
        // assertEquals("All applications are being retried on version2.", ImmutableSet.of(app1.id(), app2.id(), app3.id()), v2.statistics().deploying());
    }
    
    @Test
    public void testVersionConfidence() {
        DeploymentTester tester = new DeploymentTester();

        Version version0 = new Version("5.0");
        tester.upgradeSystem(version0);

        // Setup applications - all running on version0
        Application canary0 = tester.createAndDeploy("canary0", 1, "canary");
        Application canary1 = tester.createAndDeploy("canary1", 2, "canary");
        Application canary2 = tester.createAndDeploy("canary2", 3, "canary");
        Application default0 = tester.createAndDeploy("default0", 4, "default");
        Application default1 = tester.createAndDeploy("default1", 5, "default");
        Application default2 = tester.createAndDeploy("default2", 6, "default");
        Application default3 = tester.createAndDeploy("default3", 7, "default");
        Application default4 = tester.createAndDeploy("default4", 8, "default");
        Application default5 = tester.createAndDeploy("default5", 9, "default");
        Application default6 = tester.createAndDeploy("default6", 10, "default");
        Application default7 = tester.createAndDeploy("default7", 11, "default");
        Application default8 = tester.createAndDeploy("default8", 12, "default");
        Application default9 = tester.createAndDeploy("default9", 13, "default");
        Application conservative0 = tester.createAndDeploy("conservative1", 14, "conservative");

        // Applications that do not affect confidence calculation:

        // Application without deployment
        Application ignored0 = tester.createApplication("ignored0", "tenant1", 1000, 1000L);

        // Pull request builds
        tester.controllerTester().createApplication(new TenantId("tenant1"),
                                                    "ignored1",
                                                    "43", 1000);

        assertEquals("All applications running on this version: High",
                     Confidence.high, confidence(tester.controller(), version0));

        // New version is released
        Version version1 = new Version("5.1");
        tester.upgradeSystem(version1);

        // Canaries upgrade to new versions and fail
        tester.completeUpgrade(canary0, version1, "canary");
        tester.completeUpgradeWithError(canary1, version1, "canary", productionUsWest1);
        tester.updateVersionStatus();
        assertEquals("One canary failed: Broken",
                     Confidence.broken, confidence(tester.controller(), version1));

        // Finish running jobs
        tester.deployAndNotify(canary2, DeploymentTester.applicationPackage("canary"), false, systemTest);
        tester.clock().advance(Duration.ofHours(1));
        tester.deployAndNotify(canary1, DeploymentTester.applicationPackage("canary"), false, productionUsWest1);
        tester.deployAndNotify(canary2, DeploymentTester.applicationPackage("canary"), false, systemTest);

        // New version is released
        Version version2 = new Version("5.2");
        tester.upgradeSystem(version2);
        assertEquals("Confidence defaults to low for version with no applications",
                     Confidence.low, confidence(tester.controller(), version2));

        // All canaries upgrade successfully
        tester.completeUpgrade(canary0, version2, "canary");
        tester.completeUpgrade(canary1, version2, "canary");

        assertEquals("Confidence for remains unchanged for version1: Broken",
                     Confidence.broken, confidence(tester.controller(), version1));
        assertEquals("Nothing has failed but not all canaries have upgraded: Low",
                     Confidence.low, confidence(tester.controller(), version2));

        // Remaining canary upgrades to version2 which raises confidence to normal and more apps upgrade
        tester.completeUpgrade(canary2, version2, "canary");
        tester.upgradeSystem(version2);
        assertEquals("Canaries have upgraded: Normal",
                     Confidence.normal, confidence(tester.controller(), version2));
        tester.completeUpgrade(default0, version2, "default");
        tester.completeUpgrade(default1, version2, "default");
        tester.completeUpgrade(default2, version2, "default");
        tester.completeUpgrade(default3, version2, "default");
        tester.completeUpgrade(default4, version2, "default");
        tester.completeUpgrade(default5, version2, "default");
        tester.completeUpgrade(default6, version2, "default");
        tester.completeUpgrade(default7, version2, "default");
        tester.updateVersionStatus();

        // Remember confidence across restart
        tester.restartController();

        assertEquals("Confidence remains unchanged for version0: High",
                     Confidence.high, confidence(tester.controller(), version0));
        assertEquals("All canaries deployed + < 90% of defaults: Normal",
                     Confidence.normal, confidence(tester.controller(), version2));
        assertTrue("Status for version without applications is removed",
                   tester.controller().versionStatus().versions().stream()
                           .noneMatch(vespaVersion -> vespaVersion.versionNumber().equals(version1)));
        
        // Another default application upgrades, raising confidence to high
        tester.completeUpgrade(default8, version2, "default");
        tester.completeUpgrade(default9, version2, "default");
        tester.updateVersionStatus();

        assertEquals("Confidence remains unchanged for version0: High",
                     Confidence.high, confidence(tester.controller(), version0));
        assertEquals("90% of defaults deployed successfully: High",
                     VespaVersion.Confidence.high, confidence(tester.controller(), version2));

        // A new version is released, all canaries upgrade successfully, but enough "default" apps fail to mark version
        // as broken
        Version version3 = new Version("5.3");
        tester.upgradeSystem(version3);
        tester.completeUpgrade(canary0, version3, "canary");
        tester.completeUpgrade(canary1, version3, "canary");
        tester.completeUpgrade(canary2, version3, "canary");
        tester.upgradeSystem(version3);
        tester.completeUpgradeWithError(default0, version3, "default", stagingTest);
        tester.completeUpgradeWithError(default1, version3, "default", stagingTest);
        tester.completeUpgradeWithError(default2, version3, "default", stagingTest);
        tester.completeUpgradeWithError(default9, version3, "default", stagingTest);
        tester.updateVersionStatus();

        assertEquals("Confidence remains unchanged for version0: High",
                     Confidence.high, confidence(tester.controller(), version0));
        assertEquals("Confidence remains unchanged for version2: High",
                     Confidence.high, confidence(tester.controller(), version2));
        assertEquals("40% of defaults failed: Broken",
                     VespaVersion.Confidence.broken, confidence(tester.controller(), version3));

        // Test version order
        List<VespaVersion> versions = tester.controller().versionStatus().versions();
        assertEquals(3, versions.size());
        assertEquals("5", versions.get(0).versionNumber().toString());
        assertEquals("5.2", versions.get(1).versionNumber().toString());
        assertEquals("5.3", versions.get(2).versionNumber().toString());
    }

    @Test
    public void testIgnoreConfidence() {
        DeploymentTester tester = new DeploymentTester();

        Version version0 = new Version("5.0");
        tester.upgradeSystem(version0);

        // Setup applications - all running on version0
        Application canary0 = tester.createAndDeploy("canary0", 1, "canary");
        Application canary1 = tester.createAndDeploy("canary1", 2, "canary");
        Application default0 = tester.createAndDeploy("default0", 3, "default");
        Application default1 = tester.createAndDeploy("default1", 4, "default");
        Application default2 = tester.createAndDeploy("default2", 5, "default");
        Application default3 = tester.createAndDeploy("default3", 6, "default");
        Application default4 = tester.createAndDeploy("default4", 7, "default");

        // New version is released
        Version version1 = new Version("5.1");
        tester.upgradeSystem(version1);

        // All canaries upgrade successfully, 1 default apps ok, 3 default apps fail
        tester.completeUpgrade(canary0, version1, "canary");
        tester.completeUpgrade(canary1, version1, "canary");
        tester.upgradeSystem(version1);
        tester.completeUpgrade(default0, version1, "default");
        tester.completeUpgradeWithError(default1, version1, "default", stagingTest);
        tester.completeUpgradeWithError(default2, version1, "default", stagingTest);
        tester.completeUpgradeWithError(default3, version1, "default", stagingTest);
        tester.completeUpgradeWithError(default4, version1, "default", stagingTest);
        tester.updateVersionStatus();
        assertEquals("Canaries have upgraded, 1 of 4 default apps failing: Broken",
                     Confidence.broken, confidence(tester.controller(), version1));

        // Same as above, but ignore confidence calculations, will force normal confidence
        tester.controllerTester().curator().writeIgnoreConfidence(true);
        tester.updateVersionStatus();
        assertEquals("Canaries have upgraded, 1 of 4 default apps failing, but confidence ignored: Low",
                     Confidence.normal, confidence(tester.controller(), version1));
        tester.controllerTester().curator().writeIgnoreConfidence(false);
    }

    @Test
    public void testComputeIgnoresVersionWithUnknownGitMetadata() {
        ControllerTester tester = new ControllerTester();
        ApplicationController applications = tester.controller().applications();

        tester.gitHub()
                .mockAny(false)
                .knownTag(Vtag.currentVersion.toFullString(), "foo") // controller
                .knownTag("6.1.0", "bar"); // config server

        Version versionWithUnknownTag = new Version("6.1.2");

        Application app = tester.createAndDeploy("tenant1", "domain1","application1", Environment.test, 11);
        tester.clock().advance(Duration.ofMillis(1));
        applications.notifyJobCompletion(DeploymentTester.jobReport(app, component, true));
        applications.notifyJobCompletion(DeploymentTester.jobReport(app, systemTest, true));

        List<VespaVersion> vespaVersions = VersionStatus.compute(tester.controller()).versions();

        assertEquals(2, vespaVersions.size()); // controller and config server
        assertTrue("Version referencing unknown tag is skipped", 
                   vespaVersions.stream().noneMatch(v -> v.versionNumber().equals(versionWithUnknownTag)));
    }

    private Confidence confidence(Controller controller, Version version) {
        return controller.versionStatus().versions().stream()
                .filter(v -> v.statistics().version().equals(version))
                .findFirst()
                .map(VespaVersion::confidence)
                .orElseThrow(() -> new IllegalArgumentException("Expected to find version: " + version));
    }

}
