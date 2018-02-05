// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.restapi;

import com.yahoo.application.container.JDisc;
import com.yahoo.application.container.handler.Request;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.vespa.athenz.api.AthenzDomain;
import com.yahoo.vespa.athenz.utils.AthenzIdentities;
import com.yahoo.vespa.hosted.controller.Application;
import com.yahoo.vespa.hosted.controller.Controller;
import com.yahoo.vespa.hosted.controller.TestIdentities;
import com.yahoo.vespa.hosted.controller.api.Tenant;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.DeployOptions;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.GitRevision;
import com.yahoo.vespa.hosted.controller.api.application.v4.model.ScrewdriverBuildJob;
import com.yahoo.vespa.hosted.controller.api.identifiers.GitBranch;
import com.yahoo.vespa.hosted.controller.api.identifiers.GitCommit;
import com.yahoo.vespa.hosted.controller.api.identifiers.GitRepository;
import com.yahoo.vespa.hosted.controller.api.identifiers.Property;
import com.yahoo.vespa.hosted.controller.api.identifiers.PropertyId;
import com.yahoo.vespa.hosted.controller.api.identifiers.ScrewdriverId;
import com.yahoo.vespa.hosted.controller.api.identifiers.TenantId;
import com.yahoo.vespa.hosted.controller.api.integration.athenz.ApplicationAction;
import com.yahoo.vespa.hosted.controller.api.integration.athenz.HostedAthenzIdentities;
import com.yahoo.vespa.hosted.controller.api.integration.zone.ZoneId;
import com.yahoo.vespa.hosted.controller.application.ApplicationPackage;
import com.yahoo.vespa.hosted.controller.application.DeploymentJobs;
import com.yahoo.vespa.hosted.controller.athenz.mock.AthenzClientFactoryMock;
import com.yahoo.vespa.hosted.controller.athenz.mock.AthenzDbMock;
import com.yahoo.vespa.hosted.controller.maintenance.JobControl;
import com.yahoo.vespa.hosted.controller.maintenance.Upgrader;
import com.yahoo.vespa.hosted.controller.persistence.CuratorDb;
import com.yahoo.vespa.hosted.controller.persistence.MockCuratorDb;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

/**
 * Provides testing of controller functionality accessed through the container
 * 
 * @author bratseth
 */
public class ContainerControllerTester {

    private final ContainerTester containerTester;
    private final Upgrader upgrader;

    public ContainerControllerTester(JDisc container, String responseFilePath) {
        containerTester = new ContainerTester(container, responseFilePath);
        CuratorDb curatorDb = new MockCuratorDb();
        curatorDb.writeUpgradesPerMinute(100);
        upgrader = new Upgrader(controller(), Duration.ofDays(1), new JobControl(curatorDb), curatorDb);
    }

    public Controller controller() { return containerTester.controller(); }

    public Upgrader upgrader() { return upgrader; }

    /** Returns the wrapped generic container tester */
    public ContainerTester containerTester() { return containerTester; }

    public Application createApplication() {
        return createApplication("domain1","tenant1",
                                 "application1");
    }

    public Application createApplication(String athensDomain, String tenant, String application) {
        AthenzDomain domain1 = addTenantAthenzDomain(athensDomain, "mytenant");
        controller().tenants().addTenant(Tenant.createAthensTenant(new TenantId(tenant), domain1,
                                                                 new Property("property1"),
                                                                 Optional.of(new PropertyId("1234"))),
                                       Optional.of(TestIdentities.userNToken));
        ApplicationId app = ApplicationId.from(tenant, application, "default");
        return controller().applications().createApplication(app, Optional.of(TestIdentities.userNToken));
    }

    public Application deploy(Application application, ApplicationPackage applicationPackage, ZoneId zone, long projectId) {
        ScrewdriverId app1ScrewdriverId = new ScrewdriverId(String.valueOf(projectId));
        GitRevision app1RevisionId = new GitRevision(new GitRepository("repo"), new GitBranch("master"), new GitCommit("commit1"));
        controller().applications().deployApplication(application.id(),
                                                    zone,
                                                    Optional.of(applicationPackage),
                                                    new DeployOptions(Optional.of(new ScrewdriverBuildJob(app1ScrewdriverId, app1RevisionId)), Optional.empty(), false, false));
        return application;
    }

    public void notifyJobCompletion(ApplicationId applicationId, long projectId, boolean success, DeploymentJobs.JobType job) {
        try {
            Thread.sleep(1);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        controller().applications().notifyJobCompletion(new DeploymentJobs.JobReport(applicationId, job, projectId,
                                                                                     42,
                                                                                     Optional.empty(),
                                                                                     success ? Optional.empty() : Optional.of(DeploymentJobs.JobError.unknown)
        ));
    }

    public AthenzDomain addTenantAthenzDomain(String domainName, String userName) {
        AthenzClientFactoryMock mock = (AthenzClientFactoryMock) containerTester.container().components()
                .getComponent(AthenzClientFactoryMock.class.getName());
        AthenzDomain athensDomain = new AthenzDomain(domainName);
        AthenzDbMock.Domain domain = new AthenzDbMock.Domain(athensDomain);
        domain.markAsVespaTenant();
        domain.admin(AthenzIdentities.from(new AthenzDomain("domain"), userName));
        mock.getSetup().addDomain(domain);
        return athensDomain;
    }

    // ---- Delegators:
    
    public void assertResponse(Request request, File expectedResponse) throws IOException {
        containerTester.assertResponse(request, expectedResponse);
    }

    public void assertResponse(Request request, String expectedResponse, int expectedStatusCode) throws IOException {
        containerTester.assertResponse(request, expectedResponse, expectedStatusCode);
    }

    /*
     * Authorize action on tenantDomain/application for a given screwdriverId
     */
    public void authorize(AthenzDomain tenantDomain, ScrewdriverId screwdriverId, ApplicationAction action, Application application) {
        AthenzClientFactoryMock mock = (AthenzClientFactoryMock) containerTester.container().components()
                .getComponent(AthenzClientFactoryMock.class.getName());

        mock.getSetup()
                .domains.get(tenantDomain)
                .applications.get(new com.yahoo.vespa.hosted.controller.api.identifiers.ApplicationId(application.id().application().value()))
                .addRoleMember(action, HostedAthenzIdentities.from(screwdriverId));
    }

}
