/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.dynoyarn.common.ClusterInfo;
import com.linkedin.dynoyarn.common.Constants;
import com.linkedin.dynoyarn.common.ContainerRequest;
import com.linkedin.dynoyarn.common.DynoYARNConfigurationKeys;
import com.linkedin.dynoyarn.common.Utils;
import com.linkedin.dynoyarn.workload.preprocessor.AppSpecPreprocessor;
import com.linkedin.dynoyarn.workload.simulation.AppSpec;
import com.linkedin.dynoyarn.workload.simulation.ParserUtils;
import com.linkedin.dynoyarn.workload.simulation.SimulatedAppSubmitter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


/**
 * WorkloadApplicationMaster is responsible for requesting containers in which
 * the simulated AMs run. Each requested container runs a client (as part of
 * {@link SimulatedAppSubmitter} which submits an application to the fake RM.
 */
public class WorkloadApplicationMaster {
  enum Status {
    RUNNING,
    FAILED,
    SUCCEEDED
  }

  public static final Log LOG = LogFactory.getLog(WorkloadApplicationMaster.class);

  private Configuration dyarnConf = new Configuration();
  private AMRMClientAsync amRMClient;
  private NMClientAsync nmClientAsync;
  private NMCallbackHandler containerListener;

  private ByteBuffer allTokens;
  private FileSystem fs;
  private Path appResourcesPath;

  // Count of failed nm nodes
  private int numCompletedContainers;
  private int numFailedContainers;
  private AtomicInteger numAllocatedContainers = new AtomicInteger();
  private AtomicInteger numRequestedContainers = new AtomicInteger();

  /**
   * DynoYARN Arguments
   */
  private String clusterSpecLocation;
  private List<AppSpec> appSpecs;
  private int numAppsLaunched;
  // User-configured epoch time to start the simulation at. (Any apps in the workload spec
  // with a submit time before this will be ignored.)
  private long workloadStartTime;
  private int appsPerContainer;
  private long simulationStartTime;
  private ScheduledExecutorService resourceRequestor = Executors.newScheduledThreadPool(10);
  private ClusterInfo clusterInfo;
  private List<AppSpecPreprocessor> appSpecPreprocessors;

  private int numUnallocatedContainers;

  private WorkloadApplicationMaster() {
  }

  public boolean init(String[] args) throws Exception {
    Options opts = new Options();
    opts.addOption("cluster_spec_location", true, "Location on HDFS of cluster spec.");

    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (ParseException e) {
      LOG.error("Got exception while parsing options", e);
      return false;
    }

    String confFile = new Path(System.getenv(Constants.DYARN_CONF_NAME)).getName();
    if (confFile != null) {
      dyarnConf.addResource(new Path(confFile));
    }
    fs = FileSystem.get(dyarnConf);
    appResourcesPath = Utils.constructAppResourcesPath(fs, Utils.getApplicationId().toString());
    clusterSpecLocation = cliParser.getOptionValue("cluster_spec_location");
    Utils.poll(() -> {
      InputStream inputStream = null;
      try {
        inputStream = fs.open(new Path(clusterSpecLocation));
        String out = IOUtils.toString(inputStream);
        clusterInfo = new org.codehaus.jackson.map.ObjectMapper().readValue(out, ClusterInfo.class);
        return true;
      } catch (Exception e) {
        LOG.info("Not able to get file: " + clusterSpecLocation);
        return false;
      } finally {
        if (inputStream != null) {
          inputStream.close();
        }
      }
    }, 5, 3000);
    Configuration fakeRMConf = new Configuration(dyarnConf);
    String rmEndpoint = clusterInfo.getRmHost() + ":" + clusterInfo.getRmPort();
    fakeRMConf.set(YarnConfiguration.RM_ADDRESS, rmEndpoint);
    fakeRMConf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS);
    appSpecPreprocessors = dyarnConf.<AppSpecPreprocessor>getInstances(
        DynoYARNConfigurationKeys.WORKLOAD_APP_SPEC_PREPROCESSOR_CLASSES, AppSpecPreprocessor.class);
    for (AppSpecPreprocessor preprocessor : appSpecPreprocessors) {
      preprocessor.init(fakeRMConf);
      LOG.info("Initialized app spec preprocessor: " + preprocessor.getClass().getCanonicalName());
    }
    appsPerContainer = dyarnConf.getInt(DynoYARNConfigurationKeys.WORKLOAD_APPS_PER_CONTAINER,
        DynoYARNConfigurationKeys.DEFAULT_WORKLOAD_APPS_PER_CONTAINER);
    return true;
  }

  private boolean prepare() {
    LOG.info("Preparing application master..");
    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(dyarnConf);
    nmClientAsync.start();

    // Init AMRMClient
    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(dyarnConf);
    amRMClient.start();

    for (AppSpecPreprocessor preprocessor : appSpecPreprocessors) {
      preprocessor.start();
      LOG.info("Prepared app spec preprocessor: " + preprocessor.getClass().getCanonicalName());
    }

    try {
      String appMasterHostname = NetUtils.getHostname();
      amRMClient.registerApplicationMaster(appMasterHostname, -1, "");
      allTokens = Utils.setupAndGetContainerCredentials();
      parseAppSpecs();
    } catch (Exception e) {
      LOG.error("Exception while preparing AM", e);
      return false;
    }
    return true;
  }

  public static void main(String[] args) {
    boolean result = false;
    try {
      WorkloadApplicationMaster appMaster = new WorkloadApplicationMaster();
      LOG.info("Initializing WorkloadApplicationMaster");
      boolean doRun = appMaster.init(args);
      if (!doRun) {
        System.exit(0);
      }
      if (!appMaster.prepare()) {
        System.exit(-1);
      }
      result = appMaster.run();
    } catch (Throwable t) {
      LOG.fatal("Error running ApplicationMaster", t);
      System.exit(1);
    }
    if (result) {
      LOG.info("Application Master completed successfully. exiting");
      System.exit(0);
    } else {
      LOG.info("Application Master failed. exiting");
      System.exit(2);
    }
  }

  public boolean run() throws InterruptedException {
    simulationStartTime = System.currentTimeMillis();
    int numContainers = (appSpecs.size() + appsPerContainer - 1) / appsPerContainer;
    int appSubmitterMemory = dyarnConf.getInt(DynoYARNConfigurationKeys.SIMULATED_APP_SUBMITTER_MEMORY,
        DynoYARNConfigurationKeys.DEFAULT_SIMULATED_APP_SUBMITTER_MEMORY);
    int appSubmitterVcores = dyarnConf.getInt(DynoYARNConfigurationKeys.SIMULATED_APP_SUBMITTER_VCORES,
        DynoYARNConfigurationKeys.DEFAULT_SIMULATED_APP_SUBMITTER_VCORES);
    for (int i = 0; i < numContainers; i++) {
      int startIdx = i * appsPerContainer;
      long batchSubmissionDelay = appSpecs.get(startIdx).getSubmitTime() - workloadStartTime;
      for (int j = startIdx; j < startIdx + appsPerContainer; j++) {
        if (j >= appSpecs.size()) {
          break;
        }
        // Requesting containers via ScheduledExecutorService is an approximation (AMRMClient will often give
        // more containers than requested, meaning a fake app will get its corresponding container before it should be
        // submitted to the fake RM). Adjust the app's submit time here so that in this scenario, app submission on the
        // SimulatedAppSubmitter side can delay app submission to the time actually specified in the workload spec.
        AppSpec appSpec = appSpecs.get(j);
        long simulationSubmitDelay = appSpec.getSubmitTime() - workloadStartTime;
        appSpec.setSubmitTime(simulationStartTime + simulationSubmitDelay);
      }
      resourceRequestor.schedule(new Runnable() {
        @Override
        public void run() {
          Utils.scheduleTask(amRMClient, new ContainerRequest(1, appSubmitterMemory, appSubmitterVcores, 0));
          numRequestedContainers.incrementAndGet();
        }
      }, batchSubmissionDelay, TimeUnit.MILLISECONDS);
    }
    Status status = monitor();
    while (status == Status.RUNNING) {
      Thread.sleep(1000);
      status = monitor();
    }
    try {
      amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "SUCCEEDED!", null);
    } catch (Exception ex) {
      LOG.error("Failed to unregister application", ex);
    }
    nmClientAsync.stop();
    amRMClient.waitForServiceToStop(5000);
    amRMClient.stop();
    return status == Status.SUCCEEDED;
  }

  private Status monitor() {
    int unallocatedContainers = numRequestedContainers.get() - numAllocatedContainers.get();
    if (unallocatedContainers != this.numUnallocatedContainers) {
      LOG.info("Number of containers requested but not allocated: " + unallocatedContainers);
      this.numUnallocatedContainers = unallocatedContainers;
    }
    if (numCompletedContainers == appSpecs.size()) {
      if (numFailedContainers == 0) {
        return Status.SUCCEEDED;
      } else {
        return Status.FAILED;
      }
    }
    return Status.RUNNING;
  }

  private void parseAppSpecs() throws IOException {
    // Truncate apps that are submitted earlier than the user-provided workloadStartTime
    appSpecs = ParserUtils.parseWorkloadFile(Constants.WORKLOAD_SPEC_NAME);
    Collections.sort(appSpecs);
    workloadStartTime = dyarnConf.getLong(DynoYARNConfigurationKeys.WORKLOAD_START_TIME,
        appSpecs.get(0).getSubmitTime());
    AppSpec dummySpec = new AppSpec();
    dummySpec.setSubmitTime(workloadStartTime);
    dummySpec.setAppId("");
    // TODO: this is wrong if the workload spec contains multiple apps with exactly workloadStartTime
    // (Collections.binarySearch does not define which one it will find)
    int earliestAppIdx = Collections.binarySearch(appSpecs, dummySpec);
    if (earliestAppIdx >= 0) {
      appSpecs = appSpecs.subList(earliestAppIdx, appSpecs.size());
    } else {
      appSpecs = appSpecs.subList(-earliestAppIdx - 1, appSpecs.size());
    }
    LOG.info("Got workload start time: " + workloadStartTime);
  }

  /**
   * Node manager call back handler
   */
  static class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {
    @Override
    public void onContainerStopped(ContainerId containerId) {
      LOG.info("Succeeded to stop container " + containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
      LOG.info("Container Status: id =" + containerId + ", status =" + containerStatus);
    }

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
      LOG.info("Successfully started container " + containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to start container " + containerId, t);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of container " + containerId, t);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop container " + containerId, t);
    }

    @Override
    public void onContainerResourceIncreased(ContainerId containerId, Resource resource) { }

    @Override
    public void onContainerResourceUpdated(ContainerId containerId, Resource resource) { }

    @Override
    public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) { }

    @Override
    public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) { }
  }

  private NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler();
  }

  private class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Completed containers: " + completedContainers.size());
      for (ContainerStatus status : completedContainers) {
        boolean containerFailed = status.getExitStatus() != 0;
        numFailedContainers += (containerFailed ? 1 : 0);
        if (containerFailed) {
          LOG.info("Container " + status.getContainerId() + " failed with exit status " + status.getExitStatus()
              + " and diagnostics " + status.getDiagnostics());
        }
      }
      numCompletedContainers += completedContainers.size();
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      LOG.info("Allocated: " + containers.size() + " containers.");
      numAllocatedContainers.addAndGet(containers.size());
      for (Container container : containers) {
        // Need to explicitly remove container requests from remoteRequestsTable in AMRMClient, otherwise
        // resources get double-requested (YARN-1902)
        amRMClient.removeContainerRequest(Utils.setupContainerRequest(new ContainerRequest(1,
            container.getResource().getMemory(), container.getResource().getVirtualCores(),
            container.getPriority().getPriority())));
        LOG.info("Launching a task in container"
            + ", containerId = " + container.getId()
            + ", containerNode = " + container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
            + ", resourceRequest = " + container.getResource());
        new ContainerLauncher(container).run();
      }
    }

    @Override
    public void onContainersUpdated(List<UpdatedContainer> containers) {
    }

    @Override
    public void onShutdownRequest() { }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error("Error " + throwable, throwable);
      amRMClient.stop();
    }
  }

  /**
   * The command to prepare inside containers.
   */
  private class ContainerLauncher implements Runnable {
    Container container;

    ContainerLauncher(Container container) {
      this.container = container;
    }

    /**
     * Set up container's launch command and start the container.
     */
    public void run() {
      int toIndex = Math.min(numAppsLaunched + appsPerContainer, appSpecs.size());
      int containerAppsSpecsSize = toIndex - numAppsLaunched;
      AppSpec[] containerAppSpecs = new AppSpec[containerAppsSpecsSize];
      appSpecs.subList(numAppsLaunched, toIndex).toArray(containerAppSpecs);
      for (AppSpec appSpec : containerAppSpecs) {
        LOG.info("Submitting " + appSpec.getAppId() + " at time " + (appSpec.getSubmitTime() - simulationStartTime));
        for (AppSpecPreprocessor preprocessor : appSpecPreprocessors) {
          preprocessor.processAppSpec(appSpec);
        }
      }
      numAppsLaunched += containerAppsSpecsSize;
      // The command to run inside the container.
      List<String> commands = new ArrayList<>();
      List<CharSequence> arguments = new ArrayList<>(5);
      arguments.add("$JAVA_HOME/bin/java");
      arguments.add("-XX:CICompilerCount=2");
      arguments.add("-XX:ParallelGCThreads=2");
      arguments.add(SimulatedAppSubmitter.class.getName());
      arguments.add("-cluster_spec_location " + clusterSpecLocation);
      arguments.add("-multiplier " + dyarnConf.getFloat(DynoYARNConfigurationKeys.WORKLOAD_MULTIPLIER,
          DynoYARNConfigurationKeys.DEFAULT_WORKLOAD_MULTIPLIER));
      if (dyarnConf.getBoolean(
          DynoYARNConfigurationKeys.WORKLOAD_SUBMITTER_EXIT_UPON_SUBMISSION,
          DynoYARNConfigurationKeys.DEFAULT_WORKLOAD_SUBMITTER_EXIT_UPON_SUBMISSION)) {
        arguments.add("-exit_upon_submission");
      }
      arguments.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      arguments.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
      StringBuilder command = new StringBuilder();
      for (CharSequence str : arguments) {
        command.append(str).append(" ");
      }
      commands.add(command.toString());

      ByteBuffer tokens;
      tokens = allTokens.duplicate();
      // Set logs to be readable by everyone.
      Map<ApplicationAccessType, String> acls = new HashMap<>(2);
      acls.put(ApplicationAccessType.VIEW_APP, "*");
      acls.put(ApplicationAccessType.MODIFY_APP, " ");
      Map<String, String> containerShellEnv = new ConcurrentHashMap<>();
      StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
          .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
      for (String c : dyarnConf.getStrings(
          YarnConfiguration.YARN_APPLICATION_CLASSPATH,
          YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
        classPathEnv.append(c.trim());
      }
      containerShellEnv.put("CLASSPATH", classPathEnv.toString());
      containerShellEnv.put(Constants.SIMULATED_FATJAR_NAME, System.getenv(Constants.SIMULATED_FATJAR_NAME));
      containerShellEnv.put("HDFS_CLASSPATH", System.getenv("HDFS_CLASSPATH"));
      containerShellEnv.put(Constants.DYARN_CONF_NAME, System.getenv(Constants.DYARN_CONF_NAME));
      containerShellEnv.put("driverAppId", System.getenv("driverAppId"));
      try (PrintWriter out = new PrintWriter(Constants.SPEC_FILE, "UTF-8")) {
        String spec = new ObjectMapper().writeValueAsString(containerAppSpecs)
            .replaceAll("\"", "'");
        out.println(spec);
        out.flush();
        Map<String, LocalResource> containerResources = getContainerResources();
        Utils.localizeLocalResource(dyarnConf, fs, Constants.SPEC_FILE, LocalResourceType.FILE,
            new Path(appResourcesPath + "/dynoyarn-" + container.getId()),
            containerResources);
        containerShellEnv.put(Constants.DYARN_JAR_NAME, System.getenv(Constants.DYARN_JAR_NAME));
        ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(containerResources, containerShellEnv,
            commands, null, tokens, acls);
        nmClientAsync.startContainerAsync(container, ctx);
      } catch (IOException e) {
        LOG.error("Failed to add containerResources", e);
      }
    }
  }

  private Map<String, LocalResource> getContainerResources() throws IOException {
    Map<String, LocalResource> containerResources = new ConcurrentHashMap<>();
    String dyarnJar = System.getenv(Constants.DYARN_JAR_NAME);
    Utils.localizeHDFSResource(fs, dyarnJar, Constants.DYARN_JAR, LocalResourceType.FILE, containerResources);

    String hdfsClasspath = System.getenv("HDFS_CLASSPATH");
    if (hdfsClasspath != null) {
      for (FileStatus status : fs.listStatus(new Path(hdfsClasspath))) {
        Utils.localizeHDFSResource(fs, status.getPath().toString(), status.getPath().getName(), LocalResourceType.FILE,
            containerResources);
      }
    }

    return containerResources;
  }
}
