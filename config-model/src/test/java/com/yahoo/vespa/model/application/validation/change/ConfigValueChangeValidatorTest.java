// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.model.application.validation.change;

import com.yahoo.test.AnotherrestartConfig;
import com.yahoo.config.ConfigInstance;
import com.yahoo.test.RestartConfig;
import com.yahoo.test.SimpletypesConfig;
import com.yahoo.config.model.api.ConfigChangeAction;
import com.yahoo.config.model.producer.AbstractConfigProducer;
import com.yahoo.config.model.producer.AbstractConfigProducerRoot;
import com.yahoo.config.model.test.MockRoot;
import com.yahoo.vespa.model.AbstractService;
import com.yahoo.vespa.model.Host;
import com.yahoo.vespa.model.HostResource;
import com.yahoo.vespa.model.VespaModel;
import com.yahoo.vespa.model.application.validation.RestartConfigs;
import com.yahoo.config.application.api.ValidationOverrides;
import com.yahoo.vespa.model.test.utils.DeployLoggerStub;
import com.yahoo.vespa.model.test.utils.VespaModelCreatorWithMockPkg;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Testing the validator on both a stub model and a real-life Vespa model.
 *
 * @author bjorncs
 */
public class ConfigValueChangeValidatorTest {

    private DeployLoggerStub logger;

    @Before
    public void resetLogger() {
        logger = new DeployLoggerStub();
    }

    /**
     * NOTE: This test method has the following assumptions about the {@link com.yahoo.vespa.model.VespaModel}:
     *    1) verbosegc and heapsize in qr-start.def is marked with restart.
     *    2) {@link com.yahoo.vespa.model.container.Container} class has QrStartConfig listed in its
     *       {@link com.yahoo.vespa.model.application.validation.RestartConfigs} attribute.
     *    3) That the config ids for the container services have a specific value.
     *
     *    This test will to a certain degree ensure that the annotations in the VespaModel is correctly applied.
     */
    @Test
    public void requireThatValidatorHandlesVespaModel() {
        List<ConfigChangeAction> changes = getConfigChanges(
                createVespaModel(createQrStartConfigSegment(true, 2096)),
                createVespaModel(createQrStartConfigSegment(false, 2096))
        );
        assertEquals(3, changes.size());
        assertComponentsEquals(changes, "default/container.0", 0);
        assertComponentsEquals(changes, "admin/cluster-controllers/0", 1);
        assertComponentsEquals(changes, "docproc/cluster.basicsearch.indexing/0", 2);
    }

    @Test
    public void requireThatDocumentTypesCanBeAddedWithoutNeedForRestart() {
        List<ConfigChangeAction> changes = getConfigChanges(
                createVespaModel("", Arrays.asList("foo")),
                createVespaModel("", Arrays.asList("foo", "bar")));
        assertEquals(0, changes.size());
    }

    @Test
    public void requireThatValidatorDetectsConfigChangeFromService() {
        MockRoot oldRoot = createRootWithChildren(new SimpleConfigProducer("p", 0)
                .withChildren(new ServiceWithAnnotation("s1", 1), new ServiceWithAnnotation("s2", 2)));
        MockRoot newRoot = createRootWithChildren(new SimpleConfigProducer("p", 0)
                .withChildren(new ServiceWithAnnotation("s1", 3), new ServiceWithAnnotation("s2", 4)));
        List<ConfigChangeAction> changes = getConfigChanges(oldRoot, newRoot);
        assertEquals(2, changes.size());
        assertComponentsEquals(changes, "p/s1", 0);
        assertComponentsEquals(changes, "p/s2", 1);
        assertEquals("anotherrestart.anothervalue has changed from 1 to 3", changes.get(0).getMessage());
        assertEquals("anotherrestart.anothervalue has changed from 2 to 4", changes.get(1).getMessage());
        assertEmptyLog();
    }

    @Test
    public void requireThatValidatorDetectsConfigChangeFromParentProducer() {
        MockRoot oldRoot = createRootWithChildren(new SimpleConfigProducer("p", 1)
                .withChildren(new ServiceWithAnnotation("s1", 0), new ServiceWithAnnotation("s2", 0)));
        MockRoot newRoot = createRootWithChildren(new SimpleConfigProducer("p", 2)
                .withChildren(new ServiceWithAnnotation("s1", 0), new ServiceWithAnnotation("s2", 0)));
        List<ConfigChangeAction> changes = getConfigChanges(oldRoot, newRoot);
        assertEquals(2, changes.size());
        assertComponentsEquals(changes, "p/s1", 0);
        assertComponentsEquals(changes, "p/s2", 1);
        assertEmptyLog();
    }

    @Test
    public void requireThatValidatorHandlesModelsWithDifferentTopology() {
        MockRoot oldRoot = createRootWithChildren(
                new SimpleConfigProducer("p1", 0).withChildren(new ServiceWithAnnotation("s1", 1)),
                new SimpleConfigProducer("p2", 0).withChildren(new ServiceWithAnnotation("s2", 1)));
        MockRoot newRoot = createRootWithChildren(
                new ServiceWithAnnotation("s1", 2),
                new ServiceWithAnnotation("s2", 2),
                new ServiceWithAnnotation("s3", 2)
        );

        List<ConfigChangeAction> changes = getConfigChanges(oldRoot, newRoot);
        assertTrue(changes.isEmpty());
        assertEmptyLog();
    }

    @Test(expected = IllegalStateException.class)
    public void requireThatAnnotationDoesNotHaveEmtpyConfigList() {
        MockRoot root = createRootWithChildren(new EmptyConfigListAnnotationService(""));
        getConfigChanges(root, root);
    }

    @Test(expected = IllegalStateException.class)
    public void requireThatConfigHasRestartMethods() {
        MockRoot root = createRootWithChildren(new ConfigWithMissingMethodsAnnotatedService(""));
        getConfigChanges(root, root);
    }

    @Test
    public void requireThatServicesAnnotatedWithNonRestartConfigProduceWarningInLog() {
        MockRoot root = createRootWithChildren(new NonRestartConfigAnnotatedService(""));
        getConfigChanges(root, root);
        assertEquals(1, logger.entries.size());
    }

    @Test
    public void requireThatConfigsFromAnnotatedSuperClassesAreDetected() {
        MockRoot oldRoot = createRootWithChildren(new SimpleConfigProducer("p", 1).withChildren(
                new ChildServiceWithAnnotation("child1", 0),
                new ChildServiceWithoutAnnotation("child2", 0)));
        MockRoot newRoot = createRootWithChildren(new SimpleConfigProducer("p", 2).withChildren(
                new ChildServiceWithAnnotation("child1", 0),
                new ChildServiceWithoutAnnotation("child2", 0)));
        List<ConfigChangeAction> changes = getConfigChanges(oldRoot, newRoot);
        assertEquals(2, changes.size());
        assertComponentsEquals(changes, "p/child1", 0);
        assertComponentsEquals(changes, "p/child2", 1);
        assertEmptyLog();
    }

    private List<ConfigChangeAction> getConfigChanges(VespaModel currentModel, VespaModel nextModel) {
        ConfigValueChangeValidator validator = new ConfigValueChangeValidator(logger);
        return validator.validate(currentModel, nextModel, ValidationOverrides.empty, Instant.now());
    }

    private List<ConfigChangeAction> getConfigChanges(AbstractConfigProducerRoot currentModel,
                                                      AbstractConfigProducerRoot nextModel) {
        ConfigValueChangeValidator validator = new ConfigValueChangeValidator(logger);
        return validator.findConfigChangesFromModels(currentModel, nextModel).collect(Collectors.toList());
    }

    private static void assertComponentsEquals(List<ConfigChangeAction> changes, String name, int index) {
        assertEquals(name, changes.get(index).getServices().get(0).getConfigId());
    }

    private void assertEmptyLog() {
        assertTrue(logger.entries.isEmpty());
    }

    private static VespaModel createVespaModel(String configSegment) {
        return createVespaModel(configSegment, Arrays.asList("music"));
    }

    private static VespaModel createVespaModel(String configSegment, List<String> docTypes) {
        // Note that the configSegment is here located on root.
        return new VespaModelCreatorWithMockPkg(
                null,
                "<services version='1.0'>\n" +
                        configSegment +
                        "    <admin version='2.0'>\n" +
                        "      <adminserver hostalias='node1'/>\n" +
                        "    </admin>\n" +
                        "    <jdisc id='default' version='1.0'>\n" +
                        "       <search/>\n" +
                        "       <nodes>\n" +
                        "           <node hostalias='node1'/>\n" +
                        "       </nodes>\n" +
                        "   </jdisc>\n" +
                        "   <content id='basicsearch' version='1.0'>\n" +
                        "       <redundancy>1</redundancy>\n" +
                        createDocumentsSegment(docTypes) + "\n" +
                        "       <group>\n" +
                        "           <node hostalias='node1' distribution-key='0'/>\n" +
                        "       </group>\n" +
                        "       <engine>\n" +
                        "           <proton>\n" +
                        "               <searchable-copies>1</searchable-copies>\n" +
                        "           </proton>\n" +
                        "       </engine>\n" +
                        "   </content>\n" +
                        "</services>",
                createSearchDefinitions(docTypes)
        ).create();
    }

    private static String createDocumentsSegment(List<String> docTypes) {
        return "<documents>\n" +
                docTypes.stream()
                        .map(type -> "<document type='" + type + "' mode='index'/>")
                        .collect(Collectors.joining("\n")) +
                "</documents>";
    }

    private static List<String> createSearchDefinitions(List<String> docTypes) {
        return docTypes.stream()
                .map(type -> "search " + type + " { document " + type + " { } }")
                .collect(Collectors.toList());
    }

    private static String createQrStartConfigSegment(boolean verboseGc, int heapsize) {
        return "<config name='search.config.qr-start'>\n" +
                "    <jvm>\n" +
                "        <verbosegc>" + verboseGc + "</verbosegc>\n" +
                "        <heapsize>" + heapsize + "</heapsize>\n" +
                "    </jvm>" +
                "</config>\n";
    }

    private static MockRoot createRootWithChildren(AbstractConfigProducer<?>... children) {
        MockRoot root = new MockRoot();
        Arrays.asList(children).forEach(root::addChild);
        root.freezeModelTopology();
        return root;
    }

    private static class NonRestartConfig extends ConfigInstance {}

    private static abstract class ServiceStub extends AbstractService {
        public ServiceStub(String name) {
            super(name);
            setHostResource(new HostResource(new Host(null, "localhost")));
        }

        @Override
        public int getPortCount() {
            return 0;
        }
    }

    private static class SimpleConfigProducer extends AbstractConfigProducer<AbstractConfigProducer<?>>
            implements RestartConfig.Producer {
        public final int value;

        public SimpleConfigProducer(String name, int value) {
            super(name);
            this.value = value;
        }

        @Override
        public void getConfig(RestartConfig.Builder builder) {
            builder.value(value);
        }

        public SimpleConfigProducer withChildren(AbstractConfigProducer<?>... producer) {
            Arrays.asList(producer).forEach(this::addChild);
            return this;
        }
    }


    @RestartConfigs({RestartConfig.class, AnotherrestartConfig.class})
    private static class ServiceWithAnnotation extends ServiceStub implements AnotherrestartConfig.Producer {
        public final int anotherValue;

        public ServiceWithAnnotation(String name, int anotherValue) {
            super(name);
            this.anotherValue = anotherValue;
        }

        @Override
        public void getConfig(AnotherrestartConfig.Builder builder) {
            builder.anothervalue(anotherValue);
        }
    }

    @RestartConfigs(AnotherrestartConfig.class)
    private static class ChildServiceWithAnnotation extends ServiceWithAnnotation {
        public ChildServiceWithAnnotation(String name, int anotherValue) {
            super(name, anotherValue);
        }
    }

    private static class ChildServiceWithoutAnnotation extends ServiceWithAnnotation {
        public ChildServiceWithoutAnnotation(String name, int anotherValue) {
            super(name, anotherValue);
        }
    }

    @RestartConfigs(SimpletypesConfig.class)
    private static class NonRestartConfigAnnotatedService extends ServiceStub {
        public NonRestartConfigAnnotatedService(String name) {
            super(name);
        }
    }

    @RestartConfigs(NonRestartConfig.class)
    private static class ConfigWithMissingMethodsAnnotatedService extends ServiceStub {
        public ConfigWithMissingMethodsAnnotatedService(String name) {
            super(name);
        }
    }

    @RestartConfigs
    private static class EmptyConfigListAnnotationService extends ServiceStub {
        public EmptyConfigListAnnotationService(String name) {
            super(name);
        }
    }
}

