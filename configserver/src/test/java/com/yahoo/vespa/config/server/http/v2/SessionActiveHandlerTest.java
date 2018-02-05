// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.config.server.http.v2;

import com.yahoo.config.application.api.ApplicationMetaData;
import com.yahoo.config.application.api.ApplicationPackage;
import com.yahoo.config.model.NullConfigModelRegistry;
import com.yahoo.config.model.application.provider.BaseDeployLogger;
import com.yahoo.config.model.application.provider.DeployData;
import com.yahoo.config.model.application.provider.FilesApplicationPackage;
import com.yahoo.config.model.application.provider.MockFileRegistry;
import com.yahoo.config.provision.ApplicationId;
import com.yahoo.config.provision.ApplicationName;
import com.yahoo.config.provision.HostSpec;
import com.yahoo.config.provision.InstanceName;
import com.yahoo.config.provision.AllocatedHosts;
import com.yahoo.config.provision.TenantName;
import com.yahoo.config.provision.Zone;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.container.logging.AccessLog;
import com.yahoo.jdisc.Response;
import com.yahoo.jdisc.http.HttpRequest;
import com.yahoo.slime.JsonFormat;
import com.yahoo.vespa.config.server.ApplicationRepository;
import com.yahoo.vespa.config.server.SuperModelGenerationCounter;
import com.yahoo.vespa.config.server.TestComponentRegistry;
import com.yahoo.vespa.config.server.application.MemoryTenantApplications;
import com.yahoo.vespa.config.server.application.TenantApplications;
import com.yahoo.vespa.config.server.deploy.TenantFileSystemDirs;
import com.yahoo.vespa.config.server.deploy.ZooKeeperClient;
import com.yahoo.vespa.config.server.host.HostRegistry;
import com.yahoo.vespa.config.server.http.HandlerTest;
import com.yahoo.vespa.config.server.http.HttpErrorResponse;
import com.yahoo.vespa.config.server.http.SessionHandler;
import com.yahoo.vespa.config.server.http.SessionHandlerTest;
import com.yahoo.vespa.config.server.modelfactory.ModelFactoryRegistry;
import com.yahoo.vespa.config.server.session.LocalSession;
import com.yahoo.vespa.config.server.session.LocalSessionRepo;
import com.yahoo.vespa.config.server.session.MockSessionZKClient;
import com.yahoo.vespa.config.server.session.RemoteSession;
import com.yahoo.vespa.config.server.session.RemoteSessionRepo;
import com.yahoo.vespa.config.server.session.Session;
import com.yahoo.vespa.config.server.session.SessionContext;
import com.yahoo.vespa.config.server.session.SessionFactory;
import com.yahoo.vespa.config.server.session.SessionTest;
import com.yahoo.vespa.config.server.session.SessionZooKeeperClient;
import com.yahoo.vespa.config.server.tenant.Tenants;
import com.yahoo.vespa.config.server.zookeeper.ConfigCurator;
import com.yahoo.vespa.curator.Curator;
import com.yahoo.vespa.curator.mock.MockCurator;
import com.yahoo.vespa.model.VespaModelFactory;
import org.hamcrest.core.Is;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.Optional;

import static com.yahoo.jdisc.Response.Status.BAD_REQUEST;
import static com.yahoo.jdisc.Response.Status.CONFLICT;
import static com.yahoo.jdisc.Response.Status.INTERNAL_SERVER_ERROR;
import static com.yahoo.jdisc.Response.Status.METHOD_NOT_ALLOWED;
import static com.yahoo.jdisc.Response.Status.NOT_FOUND;
import static com.yahoo.jdisc.Response.Status.OK;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.*;

public class SessionActiveHandlerTest extends SessionHandlerTest {

    private static final File testApp = new File("src/test/apps/app");
    private static final String appName = "default";
    private static final TenantName tenant = TenantName.from("activatetest");
    private static final String activatedMessage = " for tenant '" + tenant + "' activated.";

    private ConfigCurator configCurator;
    private Curator curator;
    private RemoteSessionRepo remoteSessionRepo;
    private LocalSessionRepo localRepo;
    private TenantApplications applicationRepo;
    private MockProvisioner hostProvisioner;
    private VespaModelFactory modelFactory;
    private TestComponentRegistry componentRegistry;

    @Before
    public void setup() throws Exception {
        remoteSessionRepo = new RemoteSessionRepo(tenant);
        applicationRepo = new MemoryTenantApplications();
        curator = new MockCurator();
        configCurator = ConfigCurator.create(curator);
        localRepo = new LocalSessionRepo(Clock.systemUTC());
        pathPrefix = "/application/v2/tenant/" + tenant + "/session/";
        hostProvisioner = new MockProvisioner();
        modelFactory = new VespaModelFactory(new NullConfigModelRegistry());
        componentRegistry = new TestComponentRegistry.Builder()
                .curator(curator)
                .configCurator(configCurator)
                .modelFactoryRegistry(new ModelFactoryRegistry(Collections.singletonList(modelFactory)))
                .build();
    }

    @Test
    public void testThatPreviousSessionIsDeactivated() throws Exception {
        Clock clock = Clock.systemUTC();
        RemoteSession firstSession = activateAndAssertOK(90l, 0l, clock);
        activateAndAssertOK(91l, 90l, clock);
        assertThat(firstSession.getStatus(), Is.is(Session.Status.DEACTIVATE));
    }

    @Test
    public void testForceActivationWithActivationInBetween() throws Exception {
        Clock clock = Clock.systemUTC();
        activateAndAssertOK(90l, 0l, clock);
        activateAndAssertOK(92l, 89l, "?force=true", clock);
    }

    @Test
    public void testUnknownSession() throws Exception {
        HttpResponse response = createHandler().handle(SessionHandlerTest.createTestRequest(pathPrefix, HttpRequest.Method.PUT, Cmd.ACTIVE, 9999L, "?timeout=1.0"));
        assertEquals(response.getStatus(), 404);
    }

    @Test
    public void testActivationWithBarrierTimeout() throws Exception {
        // Needed so we can test that previous active session is still active after a failed activation
        activateAndAssertOK(90l, 0l, Clock.systemUTC());
        ((MockCurator) curator).timeoutBarrierOnEnter(true);
        ActivateRequest activateRequest = new ActivateRequest(91l, 90l, "", Clock.systemUTC()).invoke();
        HttpResponse actResponse = activateRequest.getActResponse();
        assertThat(actResponse.getStatus(), Is.is(INTERNAL_SERVER_ERROR));
    }

    @Test
    public void testActivationOfSessionThatDoesNotExistAsLocalSession() throws Exception {
        ActivateRequest activateRequest = new ActivateRequest(90l, 0l, "", Clock.systemUTC()).invoke(false);
        HttpResponse actResponse = activateRequest.getActResponse();
        assertThat(actResponse.getStatus(), Is.is(NOT_FOUND));
        String message = getRenderedString(actResponse);
        assertThat(message, Is.is("{\"error-code\":\"NOT_FOUND\",\"message\":\"Session 90 was not found\"}"));
    }

    @Test
    public void require_that_session_created_from_active_that_is_no_longer_active_cannot_be_activated() throws Exception {
        Clock clock = Clock.systemUTC();

        long sessionId = 1;
        activateAndAssertOK(1, 0, clock);
        sessionId++;
        activateAndAssertOK(sessionId, 1, clock);

        sessionId++;
        ActivateRequest activateRequest = new ActivateRequest(sessionId, 1, "", Clock.systemUTC()).invoke();
        HttpResponse actResponse = activateRequest.getActResponse();
        String message = getRenderedString(actResponse);
        assertThat(message, actResponse.getStatus(), Is.is(CONFLICT));
        assertThat(message,
                   containsString("Cannot activate session 3 because the currently active session (2) has changed since session 3 was created (was 1 at creation time)"));
    }

    @Test
    public void testAlreadyActivatedSession() throws Exception {
        activateAndAssertOK(1, 0, Clock.systemUTC());
        HttpResponse response = createHandler().handle(SessionHandlerTest.createTestRequest(pathPrefix, HttpRequest.Method.PUT, Cmd.ACTIVE, 1l));
        String message = getRenderedString(response);
        assertThat(message, response.getStatus(), Is.is(BAD_REQUEST));
        assertThat(message, containsString("Session 1 is already active"));
    }

    @Test
    public void testActivation() throws Exception {
        activateAndAssertOK(1, 0, Clock.systemUTC());
    }

    @Test
    public void testActivationWithActivationInBetween() throws Exception {
        Clock clock = Clock.systemUTC();
        activateAndAssertOK(90l, 0l, clock);
        activateAndAssertError(92l, 89l, clock,
                               Response.Status.CONFLICT, HttpErrorResponse.errorCodes.ACTIVATION_CONFLICT,
                               "tenant:" + tenant + " app:default:default Cannot activate session 92 because the currently active session (90) has changed since session 92 was created (was 89 at creation time)");
    }

    @Test
    public void testActivationOfUnpreparedSession() throws Exception {
        Clock clock = Clock.systemUTC();
        // Needed so we can test that previous active session is still active after a failed activation
        RemoteSession firstSession = activateAndAssertOK(90l, 0l, clock);
        long sessionId = 91L;
        ActivateRequest activateRequest = new ActivateRequest(sessionId, 0l, Session.Status.NEW, "", clock).invoke();
        HttpResponse actResponse = activateRequest.getActResponse();
        RemoteSession session = activateRequest.getSession();
        assertThat(actResponse.getStatus(), is(Response.Status.BAD_REQUEST));
        assertThat(getRenderedString(actResponse), is("{\"error-code\":\"BAD_REQUEST\",\"message\":\"tenant:"+tenant+" app:default:default Session " + sessionId + " is not prepared\"}"));
        assertThat(session.getStatus(), is(not(Session.Status.ACTIVATE)));
        assertThat(firstSession.getStatus(), is(Session.Status.ACTIVATE));
    }

    @Test
    public void require_that_handler_gives_error_for_unsupported_methods() throws Exception {
        testUnsupportedMethod(SessionHandlerTest.createTestRequest(pathPrefix, HttpRequest.Method.POST, Cmd.PREPARED, 1L));
        testUnsupportedMethod(SessionHandlerTest.createTestRequest(pathPrefix, HttpRequest.Method.DELETE, Cmd.PREPARED, 1L));
        testUnsupportedMethod(SessionHandlerTest.createTestRequest(pathPrefix, HttpRequest.Method.GET, Cmd.PREPARED, 1L));
    }

    @Test
    @Ignore
    public void require_that_handler_gives_error_when_provisioner_activated_fails() throws Exception {
        hostProvisioner = new FailingMockProvisioner();
        hostProvisioner.activated = false;
        activateAndAssertError(1, 0, Clock.systemUTC(), BAD_REQUEST, HttpErrorResponse.errorCodes.BAD_REQUEST, "Cannot activate application");
        assertFalse(hostProvisioner.activated);
    }

    private RemoteSession createRemoteSession(long sessionId, Session.Status status, SessionZooKeeperClient zkClient, Clock clock) throws IOException {
        zkClient.writeStatus(status);
        ZooKeeperClient zkC = new ZooKeeperClient(configCurator, new BaseDeployLogger(), false, Tenants.getSessionsPath(tenant).append(String.valueOf(sessionId)));
        zkC.write(Collections.singletonMap(modelFactory.getVersion(), new MockFileRegistry()));
        zkC.write(AllocatedHosts.withHosts(Collections.emptySet()));
        RemoteSession session = new RemoteSession(TenantName.from("default"), sessionId, componentRegistry, zkClient, clock);
        remoteSessionRepo.addSession(session);
        return session;
    }

    private LocalSessionRepo addLocalSession(long sessionId, DeployData deployData, SessionZooKeeperClient zkc) {
        writeApplicationId(zkc, deployData.getApplicationName());
        TenantFileSystemDirs tenantFileSystemDirs = TenantFileSystemDirs.createTestDirs(tenant);
        ApplicationPackage app = FilesApplicationPackage.fromFileWithDeployData(testApp, deployData);
        localRepo.addSession(new LocalSession(tenant, sessionId, new SessionTest.MockSessionPreparer(),
                new SessionContext(app, zkc, new File(tenantFileSystemDirs.sessionsPath(), String.valueOf(sessionId)),
                        applicationRepo, new HostRegistry<>(), new SuperModelGenerationCounter(curator))));
        return localRepo;
    }

    private ActivateRequest activateAndAssertOKPut(long sessionId, long previousSessionId, String subPath, Clock clock) throws Exception {
        ActivateRequest activateRequest = new ActivateRequest(sessionId, previousSessionId, subPath, clock);
        activateRequest.invoke();
        HttpResponse actResponse = activateRequest.getActResponse();
        String message = getRenderedString(actResponse);
        assertThat(message, actResponse.getStatus(), Is.is(OK));
        assertActivationMessageOK(activateRequest, message);
        RemoteSession session = activateRequest.getSession();
        assertThat(session.getStatus(), Is.is(Session.Status.ACTIVATE));
        return activateRequest;
    }

    private void activateAndAssertErrorPut(long sessionId, long previousSessionId, Clock clock,
                                                      int statusCode, HttpErrorResponse.errorCodes errorCode, String expectedError) throws Exception {
        ActivateRequest activateRequest = new ActivateRequest(sessionId, previousSessionId, "", clock);
        activateRequest.invoke();
        HttpResponse actResponse = activateRequest.getActResponse();
        RemoteSession session = activateRequest.getSession();
        assertThat(actResponse.getStatus(), Is.is(statusCode));
        String message = getRenderedString(actResponse);
        assertThat(message, Is.is("{\"error-code\":\"" + errorCode.name() + "\",\"message\":\"" + expectedError + "\"}"));
        assertThat(session.getStatus(), Is.is(Session.Status.PREPARE));
    }

    private void testUnsupportedMethod(com.yahoo.container.jdisc.HttpRequest request) throws Exception {
        HttpResponse response = createHandler().handle(request);
        HandlerTest.assertHttpStatusCodeErrorCodeAndMessage(response, METHOD_NOT_ALLOWED,
                                                            HttpErrorResponse.errorCodes.METHOD_NOT_ALLOWED,
                                                            "Method '" + request.getMethod().name() + "' is not supported");
    }

    protected class ActivateRequest {

        private long sessionId;
        private RemoteSession session;
        private SessionHandler handler;
        private HttpResponse actResponse;
        private Session.Status initialStatus;
        private DeployData deployData;
        private ApplicationMetaData metaData;
        private String subPath;
        private Clock clock;

        ActivateRequest(long sessionId, long previousSessionId, String subPath, Clock clock) {
            this(sessionId, previousSessionId, Session.Status.PREPARE, subPath, clock);
        }

        ActivateRequest(long sessionId, long previousSessionId, Session.Status initialStatus, String subPath, Clock clock) {
            this.sessionId = sessionId;
            this.initialStatus = initialStatus;
            this.deployData = new DeployData("foo", "bar", appName, 0l, sessionId, previousSessionId);
            this.subPath = subPath;
            this.clock = clock;
        }

        public RemoteSession getSession() {
            return session;
        }

        public SessionHandler getHandler() {
            return handler;
        }

        HttpResponse getActResponse() {
            return actResponse;
        }

        public long getSessionId() {
            return sessionId;
        }

        ApplicationMetaData getMetaData() {
            return metaData;
        }

        ActivateRequest invoke() throws Exception {
            return invoke(true);
        }

        ActivateRequest invoke(boolean createLocalSession) throws Exception {
            SessionZooKeeperClient zkClient = new MockSessionZKClient(curator, tenant, sessionId,
                                                                      Optional.of(AllocatedHosts.withHosts(Collections.singleton(new HostSpec("bar", Collections.emptyList())))));
            session = createRemoteSession(sessionId, initialStatus, zkClient, clock);
            if (createLocalSession) {
                LocalSessionRepo repo = addLocalSession(sessionId, deployData, zkClient);
                metaData = repo.getSession(sessionId).getMetaData();
            }
            handler = createHandler();
            actResponse = handler.handle(SessionHandlerTest.createTestRequest(pathPrefix, HttpRequest.Method.PUT, Cmd.ACTIVE, sessionId, subPath));
            return this;
        }
    }

    private RemoteSession activateAndAssertOK(long sessionId, long previousSessionId, Clock clock) throws Exception {
        ActivateRequest activateRequest = activateAndAssertOKPut(sessionId, previousSessionId, "", clock);
        return activateRequest.getSession();
    }

    private Session activateAndAssertOK(long sessionId, long previousSessionId, String subPath, Clock clock) throws Exception {
        ActivateRequest activateRequest = activateAndAssertOKPut(sessionId, previousSessionId, subPath, clock);
        return activateRequest.getSession();
    }
    
    private void assertActivationMessageOK(ActivateRequest activateRequest, String message) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        new JsonFormat(true).encode(byteArrayOutputStream, activateRequest.getMetaData().getSlime());
        assertThat(message, containsString("\"tenant\":\"" + tenant + "\",\"message\":\"Session " + activateRequest.getSessionId() + activatedMessage));
        assertThat(message, containsString("/application/v2/tenant/" + tenant +
                "/application/" + appName +
                "/environment/" + "prod" +
                "/region/" + "default" +
                "/instance/" + "default"));
        assertTrue(hostProvisioner.activated);
        assertThat(hostProvisioner.lastHosts.size(), is(1));
    }

    private void activateAndAssertError(long sessionId, long previousSessionId, Clock clock, int statusCode, HttpErrorResponse.errorCodes errorCode, String expectedError) throws Exception {
        hostProvisioner.activated = false;
        activateAndAssertErrorPut(sessionId, previousSessionId, clock, statusCode, errorCode, expectedError);
        assertFalse(hostProvisioner.activated);
    }

    private void writeApplicationId(SessionZooKeeperClient zkc, String applicationName) {
        ApplicationId id = ApplicationId.from(tenant, ApplicationName.from(applicationName), InstanceName.defaultName());
        zkc.writeApplicationId(id);
    }

    private SessionHandler createHandler() throws Exception {
        final SessionFactory sessionFactory = new MockSessionFactory();
        TestTenantBuilder testTenantBuilder = new TestTenantBuilder();
        testTenantBuilder.createTenant(tenant)
                .withSessionFactory(sessionFactory)
                .withLocalSessionRepo(localRepo)
                .withRemoteSessionRepo(remoteSessionRepo)
                .withApplicationRepo(applicationRepo)
                .build();
        return new SessionActiveHandler(
                SessionActiveHandler.testOnlyContext(),
                new ApplicationRepository(testTenantBuilder.createTenants(),
                                          hostProvisioner,
                                          Clock.systemUTC()),
                testTenantBuilder.createTenants(),
                Zone.defaultZone());
    }

}
