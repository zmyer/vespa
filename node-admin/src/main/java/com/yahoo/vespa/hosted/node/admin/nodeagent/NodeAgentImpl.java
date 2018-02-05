// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.node.admin.nodeagent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.yahoo.concurrent.ThreadFactoryFactory;
import com.yahoo.vespa.hosted.dockerapi.Container;
import com.yahoo.vespa.hosted.dockerapi.ContainerName;
import com.yahoo.vespa.hosted.dockerapi.ContainerResources;
import com.yahoo.vespa.hosted.dockerapi.Docker;
import com.yahoo.vespa.hosted.dockerapi.DockerException;
import com.yahoo.vespa.hosted.dockerapi.DockerExecTimeoutException;
import com.yahoo.vespa.hosted.dockerapi.DockerImage;
import com.yahoo.vespa.hosted.dockerapi.ProcessResult;
import com.yahoo.vespa.hosted.dockerapi.metrics.DimensionMetrics;
import com.yahoo.vespa.hosted.dockerapi.metrics.Dimensions;
import com.yahoo.vespa.hosted.dockerapi.metrics.MetricReceiverWrapper;
import com.yahoo.vespa.hosted.node.admin.ContainerNodeSpec;
import com.yahoo.vespa.hosted.node.admin.docker.DockerOperations;
import com.yahoo.vespa.hosted.node.admin.maintenance.StorageMaintainer;
import com.yahoo.vespa.hosted.node.admin.noderepository.NodeRepository;
import com.yahoo.vespa.hosted.node.admin.orchestrator.Orchestrator;
import com.yahoo.vespa.hosted.node.admin.orchestrator.OrchestratorException;
import com.yahoo.vespa.hosted.node.admin.util.Environment;
import com.yahoo.vespa.hosted.node.admin.util.PrefixLogger;
import com.yahoo.vespa.hosted.provision.Node;

import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.yahoo.vespa.hosted.node.admin.nodeagent.NodeAgentImpl.ContainerState.ABSENT;
import static com.yahoo.vespa.hosted.node.admin.nodeagent.NodeAgentImpl.ContainerState.UNKNOWN;

/**
 * @author dybis
 * @author bakksjo
 */
public class NodeAgentImpl implements NodeAgent {
    // This is used as a definition of 1 GB when comparing flavor specs in node-repo
    private static final long BYTES_IN_GB = 1_000_000_000L;


    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private boolean isFrozen = true;
    private boolean wantFrozen = false;
    private boolean workToDoNow = true;
    private boolean expectNodeNotInNodeRepo = false;

    private final Object monitor = new Object();

    private final PrefixLogger logger;
    private DockerImage imageBeingDownloaded = null;

    private final ContainerName containerName;
    private final String hostname;
    private final NodeRepository nodeRepository;
    private final Orchestrator orchestrator;
    private final DockerOperations dockerOperations;
    private final StorageMaintainer storageMaintainer;
    private final Runnable aclMaintainer;
    private final Environment environment;
    private final Clock clock;
    private final Duration timeBetweenEachConverge;

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final LinkedList<String> debugMessages = new LinkedList<>();

    private int numberOfUnhandledException = 0;
    private Instant lastConverge;

    private final Thread loopThread;

    private final ScheduledExecutorService filebeatRestarter =
            Executors.newScheduledThreadPool(1, ThreadFactoryFactory.getDaemonThreadFactory("filebeatrestarter"));
    private Consumer<String> serviceRestarter;
    private Future<?> currentFilebeatRestarter;

    private boolean resumeScriptRun = false;

    /**
     * ABSENT means container is definitely absent - A container that was absent will not suddenly appear without
     * NodeAgent explicitly starting it.
     * Otherwise we can't be certain. A container that was running a minute ago may no longer be running without
     * NodeAgent doing anything (container could have crashed). Therefore we always have to ask docker daemon
     * to get updated state of the container.
     */
    enum ContainerState {
        ABSENT,
        UNKNOWN
    }

    private ContainerState containerState = UNKNOWN;

    // The attributes of the last successful node repo attribute update for this node. Used to avoid redundant calls.
    private NodeAttributes lastAttributesSet = null;
    private ContainerNodeSpec lastNodeSpec = null;
    private CpuUsageReporter lastCpuMetric = new CpuUsageReporter();

    public NodeAgentImpl(
            final String hostName,
            final NodeRepository nodeRepository,
            final Orchestrator orchestrator,
            final DockerOperations dockerOperations,
            final StorageMaintainer storageMaintainer,
            final Runnable aclMaintainer,
            final Environment environment,
            final Clock clock,
            final Duration timeBetweenEachConverge) {
        this.containerName = ContainerName.fromHostname(hostName);
        this.logger = PrefixLogger.getNodeAgentLogger(NodeAgentImpl.class, containerName);
        this.hostname = hostName;
        this.nodeRepository = nodeRepository;
        this.orchestrator = orchestrator;
        this.dockerOperations = dockerOperations;
        this.storageMaintainer = storageMaintainer;
        this.aclMaintainer = aclMaintainer;
        this.environment = environment;
        this.clock = clock;
        this.timeBetweenEachConverge = timeBetweenEachConverge;
        this.lastConverge = clock.instant();

        this.loopThread = new Thread(() -> {
            while (!terminated.get()) tick();
        });
        this.loopThread.setName("tick-" + hostname);
    }

    @Override
    public boolean setFrozen(boolean frozen) {
        synchronized (monitor) {
            if (wantFrozen != frozen) {
                wantFrozen = frozen;
                addDebugMessage(wantFrozen ? "Freezing" : "Unfreezing");
                signalWorkToBeDone();
            }

            return isFrozen == frozen;
        }
    }

    private void addDebugMessage(String message) {
        synchronized (debugMessages) {
            while (debugMessages.size() > 1000) {
                debugMessages.pop();
            }

            logger.debug(message);
            debugMessages.add("[" + sdf.format(new Date()) + "] " + message);
        }
    }

    @Override
    public Map<String, Object> debugInfo() {
        Map<String, Object> debug = new LinkedHashMap<>();
        debug.put("Hostname", hostname);
        debug.put("isFrozen", isFrozen);
        debug.put("wantFrozen", wantFrozen);
        debug.put("terminated", terminated);
        debug.put("workToDoNow", workToDoNow);
        synchronized (debugMessages) {
            debug.put("History", new LinkedList<>(debugMessages));
        }
        debug.put("Node repo state", lastNodeSpec.nodeState.name());
        return debug;
    }

    @Override
    public void start() {
        String message = "Starting with interval " + timeBetweenEachConverge.toMillis() + " ms";
        logger.info(message);
        addDebugMessage(message);

        loopThread.start();

        serviceRestarter = service -> {
            try {
                ProcessResult processResult = dockerOperations.executeCommandInContainerAsRoot(
                        containerName, "service", service, "restart");

                if (!processResult.isSuccess()) {
                    logger.error("Failed to restart service " + service + ": " + processResult);
                }
            } catch (Exception e) {
                logger.error("Failed to restart service " + service, e);
            }
        };
    }

    @Override
    public void stop() {
        addDebugMessage("Stopping");
        filebeatRestarter.shutdown();
        if (!terminated.compareAndSet(false, true)) {
            throw new RuntimeException("Can not re-stop a node agent.");
        }
        signalWorkToBeDone();

        do {
            try {
                loopThread.join();
                filebeatRestarter.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for converge thread and filebeatRestarter scheduler to shutdown");
            }
        } while (loopThread.isAlive() || !filebeatRestarter.isTerminated());

        logger.info("Stopped");
    }

    private void runLocalResumeScriptIfNeeded() {
        if (! resumeScriptRun) {
            addDebugMessage("Starting optional node program resume command");
            dockerOperations.resumeNode(containerName);
            resumeScriptRun = true;
        }
    }

    private void updateNodeRepoWithCurrentAttributes(final ContainerNodeSpec nodeSpec) {
        final NodeAttributes nodeAttributes = new NodeAttributes()
                .withRestartGeneration(nodeSpec.wantedRestartGeneration.orElse(null))
                // update reboot gen with wanted gen if set, we ignore reboot for Docker nodes but
                // want the two to be equal in node repo
                .withRebootGeneration(nodeSpec.wantedRebootGeneration.orElse(0L))
                .withDockerImage(nodeSpec.wantedDockerImage.filter(node -> containerState != ABSENT).orElse(new DockerImage("")))
                .withVespaVersion(nodeSpec.wantedVespaVersion.filter(node -> containerState != ABSENT).orElse(""));

        publishStateToNodeRepoIfChanged(nodeAttributes);
    }

    private void publishStateToNodeRepoIfChanged(NodeAttributes currentAttributes) {
        // TODO: We should only update if the new current values do not match the node repo's current values
        if (!currentAttributes.equals(lastAttributesSet)) {
            logger.info("Publishing new set of attributes to node repo: "
                    + lastAttributesSet + " -> " + currentAttributes);
            addDebugMessage("Publishing new set of attributes to node repo: {" +
                    lastAttributesSet + "} -> {" + currentAttributes + "}");
            nodeRepository.updateNodeAttributes(hostname, currentAttributes);
            lastAttributesSet = currentAttributes;
        }
    }

    private void startContainer(ContainerNodeSpec nodeSpec) {
        aclMaintainer.run();
        dockerOperations.startContainer(containerName, nodeSpec);
        lastCpuMetric = new CpuUsageReporter();

        currentFilebeatRestarter = filebeatRestarter.scheduleWithFixedDelay(() -> serviceRestarter.accept("filebeat"), 1, 1, TimeUnit.DAYS);
        storageMaintainer.writeMetricsConfig(containerName, nodeSpec);
        storageMaintainer.writeFilebeatConfig(containerName, nodeSpec);

        resumeScriptRun = false;
        containerState = UNKNOWN;
        logger.info("Container successfully started, new containerState is " + containerState);
    }

    private Optional<Container> removeContainerIfNeededUpdateContainerState(ContainerNodeSpec nodeSpec, Optional<Container> existingContainer) {
        return existingContainer
                .flatMap(container -> removeContainerIfNeeded(nodeSpec, container))
                .map(container -> {
                        shouldRestartServices(nodeSpec).ifPresent(restartReason -> {
                            logger.info("Will restart services for container " + container + ": " + restartReason);
                            restartServices(nodeSpec, container);
                        });
                        return container;
                });
    }

    private Optional<String> shouldRestartServices(ContainerNodeSpec nodeSpec) {
        if (!nodeSpec.wantedRestartGeneration.isPresent()) return Optional.empty();

        if (!nodeSpec.currentRestartGeneration.isPresent() ||
                nodeSpec.currentRestartGeneration.get() < nodeSpec.wantedRestartGeneration.get()) {
            return Optional.of("Restart requested - wanted restart generation has been bumped: "
                    + nodeSpec.currentRestartGeneration.get() + " -> " + nodeSpec.wantedRestartGeneration.get());
        }
        return Optional.empty();
    }

    private void restartServices(ContainerNodeSpec nodeSpec, Container existingContainer) {
        if (existingContainer.state.isRunning() && nodeSpec.nodeState == Node.State.active) {
            ContainerName containerName = existingContainer.name;
            logger.info("Restarting services for " + containerName);
            // Since we are restarting the services we need to suspend the node.
            orchestratorSuspendNode();
            dockerOperations.restartVespaOnNode(containerName);
        }
    }

    @Override
    public void stopServices() {
        logger.info("Stopping services for " + containerName);
        dockerOperations.trySuspendNode(containerName);
        dockerOperations.stopServicesOnNode(containerName);
    }

    private Optional<String> shouldRemoveContainer(ContainerNodeSpec nodeSpec, Container existingContainer) {
        final Node.State nodeState = nodeSpec.nodeState;
        if (nodeState == Node.State.dirty || nodeState == Node.State.provisioned) {
            return Optional.of("Node in state " + nodeState + ", container should no longer be running");
        }
        if (nodeSpec.wantedDockerImage.isPresent() && !nodeSpec.wantedDockerImage.get().equals(existingContainer.image)) {
            return Optional.of("The node is supposed to run a new Docker image: "
                    + existingContainer + " -> " + nodeSpec.wantedDockerImage.get());
        }
        if (!existingContainer.state.isRunning()) {
            return Optional.of("Container no longer running");
        }

        ContainerResources wantedContainerResources = ContainerResources.from(
                nodeSpec.minCpuCores, nodeSpec.minMainMemoryAvailableGb);
        if (!wantedContainerResources.equals(existingContainer.resources)) {
            return Optional.of("Container should be running with different resource allocation, wanted: " +
                    wantedContainerResources + ", actual: " + existingContainer.resources);
        }
        return Optional.empty();
    }

    private Optional<Container> removeContainerIfNeeded(ContainerNodeSpec nodeSpec, Container existingContainer) {
        Optional<String> removeReason = shouldRemoveContainer(nodeSpec, existingContainer);
        if (removeReason.isPresent()) {
            logger.info("Will remove container " + existingContainer + ": " + removeReason.get());

            if (existingContainer.state.isRunning()) {
                if (nodeSpec.nodeState == Node.State.active) {
                    orchestratorSuspendNode();
                }

                try {
                    stopServices();
                } catch (Exception e) {
                    logger.info("Failed stopping services, ignoring", e);
                }
            }
            if (currentFilebeatRestarter != null) currentFilebeatRestarter.cancel(true);
            dockerOperations.removeContainer(existingContainer, nodeSpec);
            containerState = ABSENT;
            logger.info("Container successfully removed, new containerState is " + containerState);
            return Optional.empty();
        }
        return Optional.of(existingContainer);
    }


    private void scheduleDownLoadIfNeeded(ContainerNodeSpec nodeSpec) {
        if (nodeSpec.currentDockerImage.equals(nodeSpec.wantedDockerImage)) return;

        if (dockerOperations.pullImageAsyncIfNeeded(nodeSpec.wantedDockerImage.get())) {
            imageBeingDownloaded = nodeSpec.wantedDockerImage.get();
        } else if (imageBeingDownloaded != null) { // Image was downloading, but now it's ready
            imageBeingDownloaded = null;
        }
    }

    private void signalWorkToBeDone() {
        synchronized (monitor) {
            if (!workToDoNow) {
                workToDoNow = true;
                addDebugMessage("Signaling work to be done");
                monitor.notifyAll();
            }
        }
    }

    void tick() {
        boolean isFrozenCopy;
        synchronized (monitor) {
            while (!workToDoNow) {
                long remainder = timeBetweenEachConverge
                        .minus(Duration.between(lastConverge, clock.instant()))
                        .toMillis();
                if (remainder > 0) {
                    try {
                        monitor.wait(remainder);
                    } catch (InterruptedException e) {
                        logger.error("Interrupted, but ignoring this: " + hostname);
                    }
                } else break;
            }
            lastConverge = clock.instant();
            workToDoNow = false;

            if (isFrozen != wantFrozen) {
                isFrozen = wantFrozen;
                logger.info("Updated NodeAgent's frozen state, new value: isFrozen: " + isFrozen);
            }
            isFrozenCopy = isFrozen;
        }

        if (isFrozenCopy) {
            addDebugMessage("tick: isFrozen");
        } else {
            try {
                converge();
            } catch (OrchestratorException e) {
                logger.info(e.getMessage());
                addDebugMessage(e.getMessage());
            } catch (DockerException e) {
                // When a new version of node-admin app is released, there is a brief period of time when both
                // new and old version run together. If one of them stats/stops/deletes the container it manages,
                // the other's assumption of containerState may become incorrect. It'll then start making invalid
                // requests, for example to start a container that is already running, the containerState should
                // therefore be reset if we get an exception from docker.
                numberOfUnhandledException++;
                containerState = UNKNOWN;
                logger.error("Caught a DockerException, resetting containerState to " + containerState, e);
            } catch (Exception e) {
                numberOfUnhandledException++;
                logger.error("Unhandled exception, ignoring.", e);
                addDebugMessage(e.getMessage());
            } catch (Throwable t) {
                logger.error("Unhandled throwable, taking down system.", t);
                System.exit(234);
            }
        }
    }

    // Public for testing
    void converge() {
        final Optional<ContainerNodeSpec> nodeSpecOptional = nodeRepository.getContainerNodeSpec(hostname);

        // We just removed the node from node repo, so this is expected until NodeAdmin stop this NodeAgent
        if (!nodeSpecOptional.isPresent() && expectNodeNotInNodeRepo) return;

        final ContainerNodeSpec nodeSpec = nodeSpecOptional.orElseThrow(() ->
                new IllegalStateException(String.format("Node '%s' missing from node repository.", hostname)));
        expectNodeNotInNodeRepo = false;


        Optional<Container> container = getContainer();
        if (!nodeSpec.equals(lastNodeSpec)) {
            addDebugMessage("Loading new node spec: " + nodeSpec.toString());
            lastNodeSpec = nodeSpec;

            // Every time the node spec changes, we should clear the metrics for this container as the dimensions
            // will change and we will be reporting duplicate metrics.
            // TODO: Should be retried if writing fails
            if (container.isPresent()) {
                storageMaintainer.writeMetricsConfig(containerName, nodeSpec);
            }
        }

        switch (nodeSpec.nodeState) {
            case ready:
            case reserved:
            case parked:
            case failed:
                removeContainerIfNeededUpdateContainerState(nodeSpec, container);
                updateNodeRepoWithCurrentAttributes(nodeSpec);
                break;
            case active:
                storageMaintainer.handleCoreDumpsForContainer(containerName, nodeSpec, false);

                storageMaintainer.getDiskUsageFor(containerName)
                        .map(diskUsage -> (double) diskUsage / BYTES_IN_GB / nodeSpec.minDiskAvailableGb)
                        .filter(diskUtil -> diskUtil >= 0.8)
                        .ifPresent(diskUtil -> storageMaintainer.removeOldFilesFromNode(containerName));

                scheduleDownLoadIfNeeded(nodeSpec);
                if (isDownloadingImage()) {
                    addDebugMessage("Waiting for image to download " + imageBeingDownloaded.asString());
                    return;
                }
                container = removeContainerIfNeededUpdateContainerState(nodeSpec, container);
                if (! container.isPresent()) {
                    storageMaintainer.handleCoreDumpsForContainer(containerName, nodeSpec, false);
                    startContainer(nodeSpec);
                }

                runLocalResumeScriptIfNeeded();
                // Because it's more important to stop a bad release from rolling out in prod,
                // we put the resume call last. So if we fail after updating the node repo attributes
                // but before resume, the app may go through the tenant pipeline but will halt in prod.
                //
                // Note that this problem exists only because there are 2 different mechanisms
                // that should really be parts of a single mechanism:
                //  - The content of node repo is used to determine whether a new Vespa+application
                //    has been successfully rolled out.
                //  - Slobrok and internal orchestrator state is used to determine whether
                //    to allow upgrade (suspend).
                updateNodeRepoWithCurrentAttributes(nodeSpec);
                logger.info("Call resume against Orchestrator");
                orchestrator.resume(hostname);
                break;
            case inactive:
                removeContainerIfNeededUpdateContainerState(nodeSpec, container);
                updateNodeRepoWithCurrentAttributes(nodeSpec);
                break;
            case provisioned:
                nodeRepository.markAsDirty(hostname);
                break;
            case dirty:
                removeContainerIfNeededUpdateContainerState(nodeSpec, container);
                logger.info("State is " + nodeSpec.nodeState + ", will delete application storage and mark node as ready");
                storageMaintainer.cleanupNodeStorage(containerName, nodeSpec);
                updateNodeRepoWithCurrentAttributes(nodeSpec);
                nodeRepository.markNodeAvailableForNewAllocation(hostname);
                expectNodeNotInNodeRepo = true;
                break;
            default:
                throw new RuntimeException("UNKNOWN STATE " + nodeSpec.nodeState.name());
        }
    }

    @SuppressWarnings("unchecked")
    public void updateContainerNodeMetrics() {
        final ContainerNodeSpec nodeSpec = lastNodeSpec;
        if (nodeSpec == null || containerState == ABSENT) return;

        Optional<Docker.ContainerStats> containerStats = dockerOperations.getContainerStats(containerName);
        if (!containerStats.isPresent()) return;

        Dimensions.Builder dimensionsBuilder = new Dimensions.Builder()
                .add("host", hostname)
                .add("role", "tenants")
                .add("state", nodeSpec.nodeState.toString())
                .add("parentHostname", environment.getParentHostHostname());
        Dimensions dimensions = dimensionsBuilder.build();

        Docker.ContainerStats stats = containerStats.get();
        final String APP = MetricReceiverWrapper.APPLICATION_NODE;
        final int totalNumCpuCores = ((List<Number>) ((Map) stats.getCpuStats().get("cpu_usage")).get("percpu_usage")).size();
        final long cpuContainerKernelTime = ((Number) ((Map) stats.getCpuStats().get("cpu_usage")).get("usage_in_kernelmode")).longValue();
        final long cpuContainerTotalTime = ((Number) ((Map) stats.getCpuStats().get("cpu_usage")).get("total_usage")).longValue();
        final long cpuSystemTotalTime = ((Number) stats.getCpuStats().get("system_cpu_usage")).longValue();
        final long memoryTotalBytes = ((Number) stats.getMemoryStats().get("limit")).longValue();
        final long memoryTotalBytesUsage = ((Number) stats.getMemoryStats().get("usage")).longValue();
        final long memoryTotalBytesCache = ((Number) ((Map) stats.getMemoryStats().get("stats")).get("cache")).longValue();
        final long diskTotalBytes = (long) (nodeSpec.minDiskAvailableGb * BYTES_IN_GB);
        final Optional<Long> diskTotalBytesUsed = storageMaintainer.getDiskUsageFor(containerName);

        lastCpuMetric.updateCpuDeltas(cpuSystemTotalTime, cpuContainerTotalTime, cpuContainerKernelTime);

        // Ratio of CPU cores allocated to this container to total number of CPU cores on this host
        final double allocatedCpuRatio = nodeSpec.minCpuCores / totalNumCpuCores;
        double cpuUsageRatioOfAllocated = lastCpuMetric.getCpuUsageRatio() / allocatedCpuRatio;
        double cpuKernelUsageRatioOfAllocated = lastCpuMetric.getCpuKernelUsageRatio() / allocatedCpuRatio;

        long memoryTotalBytesUsed = memoryTotalBytesUsage - memoryTotalBytesCache;
        double memoryUsageRatio = (double) memoryTotalBytesUsed / memoryTotalBytes;
        Optional<Double> diskUsageRatio = diskTotalBytesUsed.map(used -> (double) used / diskTotalBytes);

        List<DimensionMetrics> metrics = new ArrayList<>();
        DimensionMetrics.Builder systemMetricsBuilder = new DimensionMetrics.Builder(APP, dimensions)
                .withMetric("mem.limit", memoryTotalBytes)
                .withMetric("mem.used", memoryTotalBytesUsed)
                .withMetric("mem.util", 100 * memoryUsageRatio)
                .withMetric("cpu.util", 100 * cpuUsageRatioOfAllocated)
                .withMetric("cpu.sys.util", 100 * cpuKernelUsageRatioOfAllocated)
                .withMetric("disk.limit", diskTotalBytes);

        diskTotalBytesUsed.ifPresent(diskUsed -> systemMetricsBuilder.withMetric("disk.used", diskUsed));
        diskUsageRatio.ifPresent(diskRatio -> systemMetricsBuilder.withMetric("disk.util", 100 * diskRatio));
        metrics.add(systemMetricsBuilder.build());

        stats.getNetworks().forEach((interfaceName, interfaceStats) -> {
            Dimensions netDims = dimensionsBuilder.add("interface", interfaceName).build();
            Map<String, Number> infStats = (Map<String, Number>) interfaceStats;
            DimensionMetrics networkMetrics = new DimensionMetrics.Builder(APP, netDims)
                    .withMetric("net.in.bytes", infStats.get("rx_bytes").longValue())
                    .withMetric("net.in.errors", infStats.get("rx_errors").longValue())
                    .withMetric("net.in.dropped", infStats.get("rx_dropped").longValue())
                    .withMetric("net.out.bytes", infStats.get("tx_bytes").longValue())
                    .withMetric("net.out.errors", infStats.get("tx_errors").longValue())
                    .withMetric("net.out.dropped", infStats.get("tx_dropped").longValue())
                    .build();
            metrics.add(networkMetrics);
        });

        pushMetricsToContainer(metrics);
    }

    private void pushMetricsToContainer(List<DimensionMetrics> metrics) {
        StringBuilder params = new StringBuilder();
        try {
            for (DimensionMetrics dimensionMetrics : metrics) {
                params.append(dimensionMetrics.toSecretAgentReport());
            }
            String wrappedMetrics = "s:" + params.toString();

            // Push metrics to the metrics proxy in each container - give it maximum 1 seconds to complete
            String[] command = {"vespa-rpc-invoke",  "-t", "2",  "tcp/localhost:19091",  "setExtraMetrics", wrappedMetrics};
            dockerOperations.executeCommandInContainerAsRoot(containerName, 5L, command);
        } catch (DockerExecTimeoutException | JsonProcessingException  e) {
            logger.warning("Unable to push metrics to container: " + containerName, e);
        }
    }

    private Optional<Container> getContainer() {
        if (containerState == ABSENT) return Optional.empty();
        Optional<Container> container = dockerOperations.getContainer(containerName);
        if (! container.isPresent()) containerState = ABSENT;
        return container;
    }

    @Override
    public String getHostname() {
        return hostname;
    }

    @Override
    public boolean isDownloadingImage() {
        return imageBeingDownloaded != null;
    }

    @Override
    public int getAndResetNumberOfUnhandledExceptions() {
        int temp = numberOfUnhandledException;
        numberOfUnhandledException = 0;
        return temp;
    }

    class CpuUsageReporter {
        private long containerKernelUsage = 0;
        private long totalContainerUsage = 0;
        private long totalSystemUsage = 0;

        private long deltaContainerKernelUsage;
        private long deltaContainerUsage;
        private long deltaSystemUsage;

        private void updateCpuDeltas(long totalSystemUsage, long totalContainerUsage, long containerKernelUsage) {
            deltaSystemUsage = this.totalSystemUsage == 0 ? 0 : (totalSystemUsage - this.totalSystemUsage);
            deltaContainerUsage = totalContainerUsage - this.totalContainerUsage;
            deltaContainerKernelUsage = containerKernelUsage - this.containerKernelUsage;

            this.totalSystemUsage = totalSystemUsage;
            this.totalContainerUsage = totalContainerUsage;
            this.containerKernelUsage = containerKernelUsage;
        }

        /**
         * Returns the CPU usage ratio for the docker container that this NodeAgent is managing
         * in the time between the last two times updateCpuDeltas() was called. This is calculated
         * by dividing the CPU time used by the container with the CPU time used by the entire system.
         */
        double getCpuUsageRatio() {
            return deltaSystemUsage == 0 ? Double.NaN : (double) deltaContainerUsage / deltaSystemUsage;
        }

        double getCpuKernelUsageRatio() {
            return deltaSystemUsage == 0 ? Double.NaN : (double) deltaContainerKernelUsage / deltaSystemUsage;
        }
    }

    // TODO: Also skip orchestration if we're downgrading in test/staging
    // How to implement:
    //  - test/staging: We need to figure out whether we're in test/staging, zone is available in Environment
    //  - downgrading: Impossible to know unless we look at the hosted version, which is
    //    not available in the docker image (nor its name). Not sure how to solve this. Should
    //    the node repo return the hosted version or a downgrade bit in addition to
    //    wanted docker image etc?
    // Should the tenant pipeline instead use BCP tool to upgrade faster!?
    //
    // More generally, the node repo response should contain sufficient info on what the docker image is,
    // to allow the node admin to make decisions that depend on the docker image. Or, each docker image
    // needs to contain routines for drain and suspend. For many images, these can just be dummy routines.
    private void orchestratorSuspendNode() {
        logger.info("Ask Orchestrator for permission to suspend node " + hostname);
        orchestrator.suspend(hostname);
    }
}
