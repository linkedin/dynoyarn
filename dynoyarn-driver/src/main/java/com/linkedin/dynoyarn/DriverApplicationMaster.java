/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.dynoyarn.common.ClusterInfo;
import com.linkedin.dynoyarn.common.Constants;
import com.linkedin.dynoyarn.common.ContainerRequest;
import com.linkedin.dynoyarn.common.DynoYARNConfigurationKeys;
import com.linkedin.dynoyarn.common.Utils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceManagerAdministrationProtocol;
import org.apache.hadoop.yarn.server.api.protocolrecords.AddToClusterNodeLabelsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.ReplaceLabelsOnNodeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * DriverApplicationMaster is used to spin up a YARN cluster and request containers
 * in which the simulated RM/NMs run.
 */
public class DriverApplicationMaster {
  enum Status {
    RUNNING,
    FAILED,
    SUCCEEDED
  }

  private static final int RM_PRIORITY = 1;
  private static final int NM_PRIORITY = 2;

  private static final String CAPACITY_SCHEDULER_CONF_FILENAME = "capacity-scheduler-final.xml";

  public static final Log LOG = LogFactory.getLog(DriverApplicationMaster.class);

  private Configuration dyarnConf = new Configuration();
  private AMRMClientAsync amRMClient;
  private NMClientAsync nmClientAsync;
  private NMCallbackHandler containerListener;
  private int totalNMs;
  private int numTotalNodeManagerContainers;
  private int nodeManagersPerContainer;
  private boolean assignedLeftovers;

  private AtomicInteger numCompletedNMContainers = new AtomicInteger();
  private AtomicInteger numFailedNMContainers = new AtomicInteger();

  private AtomicInteger numCompletedRMContainers = new AtomicInteger();
  private AtomicInteger numFailedRMContainers = new AtomicInteger();

  private ByteBuffer allTokens;
  private Map<DriverComponent, ContainerRequest> requestMap;
  private Map<ContainerId, DriverComponent> containerIdToComponentMap;
  private final Object rmAllocationLock = new Object();
  private FileSystem fs;
  private Path hdfsStoragePath;
  private ClusterInfo cluster;
  private Path appResourcesPath;
  private Map<String, LocalResource> containerResources;

  @VisibleForTesting
  protected DriverApplicationMaster() {
  }

  public boolean init(String[] args) throws IOException {
    Options opts = new Options();
    opts.addOption("conf", true, "Path to dynoyarn configuration.");

    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (ParseException e) {
      LOG.error("Got exception while parsing options", e);
      return false;
    }
    dyarnConf.addResource(new Path(Constants.DYARN_CONF));

    Path confFile = new Path(cliParser.getOptionValue("conf"));
    if (confFile != null) {
      dyarnConf.addResource(confFile);
    }
    requestMap = parseContainerRequests(dyarnConf);
    containerIdToComponentMap = new HashMap<>();
    totalNMs = dyarnConf.getInt(DynoYARNConfigurationKeys.NUM_NMS, 1);
    nodeManagersPerContainer = dyarnConf.getInt(DynoYARNConfigurationKeys.NMS_PER_CONTAINER, 1);
    numTotalNodeManagerContainers = Utils.getNumNMContainerRequests(dyarnConf);
    assignedLeftovers = (totalNMs % nodeManagersPerContainer == 0);
    fs = FileSystem.get(dyarnConf);
    appResourcesPath = Utils.constructAppResourcesPath(fs, Utils.getApplicationId().toString());
    hdfsStoragePath = new Path(appResourcesPath, Constants.HDFS_STORAGE_FILE);
    return true;
  }

  private boolean prepare() {
    LOG.info("Preparing application master..");
    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(dyarnConf);
    nmClientAsync.start();

    // Init AMRMClient
    AMRMClientAsync.AbstractCallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(dyarnConf);
    amRMClient.start();

    try {
      String appMasterHostname = NetUtils.getHostname();
      amRMClient.registerApplicationMaster(appMasterHostname, -1, "");
      allTokens = Utils.setupAndGetContainerCredentials();
    } catch (Exception e) {
      LOG.error("Exception while preparing AM", e);
      return false;
    }
    return true;
  }

  public static void main(String[] args) {
    boolean result = false;
    try {
      DriverApplicationMaster appMaster = new DriverApplicationMaster();
      LOG.info("Initializing ApplicationMaster");
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

  public boolean run() throws IOException, InterruptedException {
    containerResources = getContainerResources();
    scheduleTasks();
    new NodeLabelsUpdater().start();
    Status status = monitor();
    while (status == Status.RUNNING) {
      Thread.sleep(5000);
      status = monitor();
    }
    try {
      FinalApplicationStatus finalApplicationStatus = (status == Status.SUCCEEDED) ? FinalApplicationStatus.SUCCEEDED
          : FinalApplicationStatus.FAILED;
      amRMClient.unregisterApplicationMaster(finalApplicationStatus,
              "Finished with status " + finalApplicationStatus, null);
    } catch (Exception ex) {
      LOG.error("Failed to unregister application", ex);
    }
    nmClientAsync.stop();
    amRMClient.waitForServiceToStop(5000);
    amRMClient.stop();
    return status == Status.SUCCEEDED;
  }

  /**
   * This thread labels nodes based on {@code dynoyarn.driver.partition.<label>.count}, e.g. if
   * dynoyarn.driver.partition.label1.count set to 100 and dynoyarn.driver.partition.label2.count set to 200,
   * then 100 NMs will be labeled label1 and 200 NMs will be labeled label2.
   */
  class NodeLabelsUpdater extends Thread {
    @VisibleForTesting
    void remoteAddLabels(ResourceManagerAdministrationProtocol rmAdminClient, Map<String, Integer> labelToCounts,
        List<NodeReport> nodeReports) throws IOException, YarnException {
      List<NodeLabel> labels = new ArrayList<>();
      labelToCounts.keySet().forEach(label -> labels.add(NodeLabel.newInstance(label)));
      AddToClusterNodeLabelsRequest addRequest = AddToClusterNodeLabelsRequest.newInstance(labels);
      rmAdminClient.addToClusterNodeLabels(addRequest);
      Map<NodeId, Set<String>> nodeToLabelsMapping = new HashMap<>();
      int idx = 0;
      for (NodeLabel label : labels) {
        for (int i = idx; i < labelToCounts.get(label.getName()) + idx; i++) {
          NodeReport nodeReport = nodeReports.get(i);
          nodeToLabelsMapping.put(nodeReport.getNodeId(), Collections.<String>singleton(label.getName()));
          LOG.debug("Added label " + label + " to node " + nodeReport.getNodeId());
        }
        idx += labelToCounts.get(label.getName());
      }
      ReplaceLabelsOnNodeRequest replaceRequest = ReplaceLabelsOnNodeRequest.newInstance(nodeToLabelsMapping);
      rmAdminClient.replaceLabelsOnNode(replaceRequest);
    }

    @Override
    public void run() {
      synchronized (DriverApplicationMaster.this) {
        try {
          while (cluster == null) {
            DriverApplicationMaster.this.wait();
          }
        } catch (InterruptedException e) {
          LOG.error("DriverApplicationMaster interrupted while waiting for cluster spec.", e);
        }
      }
      try {
        Map<String, Integer> labelToCounts = DynoYARNConfigurationKeys.getConfiguredDriverPartitions(dyarnConf);
        if (labelToCounts.isEmpty()) {
          return;
        }
        int numLabeledNodes = 0;
        for (int count : labelToCounts.values()) {
          numLabeledNodes += count;
        }
        String rmEndpoint = cluster.getRmHost() + ":" + cluster.getRmPort();
        String rmAdminEndpoint = cluster.getRmHost() + ":" + cluster.getRmAdminPort();
        Configuration fakeRMConf = new Configuration(dyarnConf);
        fakeRMConf.set(YarnConfiguration.RM_ADDRESS, rmEndpoint);
        fakeRMConf.set(YarnConfiguration.RM_ADMIN_ADDRESS, rmAdminEndpoint);
        fakeRMConf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS);
        ResourceManagerAdministrationProtocol rmAdminClient =
            ClientRMProxy.createRMProxy(fakeRMConf, ResourceManagerAdministrationProtocol.class);
        YarnClient rmClient = YarnClient.createYarnClient();
        rmClient.init(fakeRMConf);
        rmClient.start();
        List<NodeReport> nodeReports = rmClient.getNodeReports();
        while (nodeReports.size() < numLabeledNodes) {
          Thread.sleep(5000);
          nodeReports = rmClient.getNodeReports();
        }
        remoteAddLabels(rmAdminClient, labelToCounts, nodeReports);
      } catch (IOException | InterruptedException | YarnException e) {
        LOG.error("Failed to update node labels.", e);
      }
    }
  }

  private Status monitor() {
    if (numCompletedNMContainers.get() == numTotalNodeManagerContainers) {
      if (numFailedNMContainers.get() == 0) {
        return Status.SUCCEEDED;
      } else {
        return Status.FAILED;
      }
    }
    if (numCompletedRMContainers.get() == 1) {
      if (numFailedRMContainers.get() == 0) {
        return Status.SUCCEEDED;
      } else {
        return Status.FAILED;
      }
    }
    return Status.RUNNING;
  }

  public static Map<DriverComponent, ContainerRequest> parseContainerRequests(Configuration conf) {
    Map<DriverComponent, ContainerRequest> containerRequests = new HashMap<>();
    String nmMemoryString = conf.get(DynoYARNConfigurationKeys.NM_MEMORY, "2g");
    long nmMemory = Utils.parseMemoryString(nmMemoryString);
    int nmCores = conf.getInt(DynoYARNConfigurationKeys.NM_VCORES, 1);

    String rmMemoryString = conf.get(DynoYARNConfigurationKeys.RM_MEMORY, "4g");
    long rmMemory = Utils.parseMemoryString(rmMemoryString);
    int rmCores = conf.getInt(DynoYARNConfigurationKeys.RM_VCORES, 2);
    containerRequests.put(DriverComponent.NODE_MANAGER,
        new ContainerRequest(Utils.getNumNMContainerRequests(conf), nmMemory, nmCores, NM_PRIORITY));
    containerRequests.put(DriverComponent.RESOURCE_MANAGER,
        new ContainerRequest(1, rmMemory, rmCores, RM_PRIORITY,
            conf.get(DynoYARNConfigurationKeys.RM_NODE_LABEL)));
    return containerRequests;
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
      LOG.error("Failed to start container " + containerId);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to query the status of container " + containerId);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
      LOG.error("Failed to stop container " + containerId);
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
      for (ContainerStatus containerStatus : completedContainers) {
        int exitStatus = containerStatus.getExitStatus();
        ContainerId containerId = containerStatus.getContainerId();
        if (DriverComponent.NODE_MANAGER.equals(containerIdToComponentMap.get(containerId))) {
          numCompletedNMContainers.incrementAndGet();
          if (ContainerExitStatus.SUCCESS != exitStatus) {
            numFailedNMContainers.incrementAndGet();
          }
        } else {
          numCompletedRMContainers.incrementAndGet();
          if (ContainerExitStatus.SUCCESS != exitStatus) {
            numFailedRMContainers.incrementAndGet();
          }
        }
        LOG.info("ContainerID = " + containerStatus.getContainerId()
            + ", state = " + containerStatus.getState()
            + ", exitStatus = " + exitStatus);
        String diagnostics = containerStatus.getDiagnostics();
        if (ContainerExitStatus.SUCCESS != exitStatus) {
          LOG.error(diagnostics);
        } else {
          LOG.info(diagnostics);
        }
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      /*
       * How is cluster information propagated?
       * We first allocate the ResourceManager first and collect the address of the RM.
       * Once this is finished, we start scheduling containers for the node managers and
       * put that inside a RM_ADDRESS OS environment for the node manager containers.
       *
       * For simplicity at the moment, we don't rely on RPC but basing on reading/writing
       * to a HDFS location.
       */
      DriverComponent component = DriverComponent.NODE_MANAGER;
      for (Container container : containers) {
        if (container.getPriority().getPriority() == RM_PRIORITY) {
          synchronized (rmAllocationLock) {
            rmAllocationLock.notifyAll();
            component = DriverComponent.RESOURCE_MANAGER;
          }
        }
      }

      LOG.info("Allocated: " + containers.size() + " containers.");
      for (Container container : containers) {
        LOG.info("Launching a task in container"
            + ", containerId = " + container.getId()
            + ", containerNode = " + container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
            + ", resourceRequest = " + container.getResource());
        containerIdToComponentMap.put(container.getId(), component);
        new ContainerLauncher(container, component).run();
        // If fake RM, register tracking url
        if (container.getPriority().getPriority() == RM_PRIORITY) {
          synchronized (DriverApplicationMaster.this) {
            Utils.poll(() -> {
              FSDataInputStream inputStream = null;
              try {
                inputStream = fs.open(hdfsStoragePath);
                String out = IOUtils.toString(inputStream);
                cluster = new ObjectMapper().readValue(out, ClusterInfo.class);
                String rmHttp = cluster.getRmHost() + ":" + cluster.getRmHttpPort();
                amRMClient.updateTrackingUrl(rmHttp);
                LOG.info("Updated tracking url for fake RM to " + rmHttp);
                return true;
              } catch (Exception e) {
                LOG.info("Not able to get file: " + hdfsStoragePath);
                return false;
              } finally {
                if (inputStream != null) {
                  inputStream.close();
                }
              }
            }, 5, 3000);
            DriverApplicationMaster.this.notify();
          }
        }
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
      LOG.info("Error " + throwable);
      amRMClient.stop();
    }
  }

  /**
   * The command to prepare inside containers.
   */
  private class ContainerLauncher implements Runnable {
    Container container;
    DriverComponent component;

    ContainerLauncher(Container container, DriverComponent component) {
      this.container = container;
      this.component = component;
    }

    /**
     * Set up container's launch command and start the container.
     */
    public void run() {
      // The command to run inside the container.
      List<String> commands = new ArrayList<>();
      StringJoiner arguments = new StringJoiner(" ");
      long memory = component == DriverComponent.NODE_MANAGER
          ? Utils.parseMemoryString(dyarnConf.get(DynoYARNConfigurationKeys.NM_MEMORY, "2g"))
          : Utils.parseMemoryString(dyarnConf.get(DynoYARNConfigurationKeys.RM_MEMORY, "4g"));
      int heapSize = (int) (memory * 0.9f);
      arguments.add("$JAVA_HOME/bin/java");
      arguments.add("-Xmx" + heapSize + "m");
      arguments.add(Mesher.class.getName());
      arguments.add("--task_command");
      // Wrap full command in escaped double quotes (escaped because
      // ContainerLaunch's setup will quote the entire "java Mesher" command).
      arguments.add("\\\"./" + Constants.DYARN_START_SCRIPT);
      arguments.add(DriverApplicationMaster.constructUserConfOverrides(new Path(Constants.DYNOYARN_SITE_XML)) + "\\\"");
      arguments.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      arguments.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
      commands.add(arguments.toString());

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
      containerShellEnv.put(Constants.COMPONENT_NAME, component.toString());
      containerShellEnv.put(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV,
          System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV));
      containerShellEnv.put("JAVA_HEAP_MAX", "-Xmx" + ((int) (heapSize * 0.9)) + "m");
      containerShellEnv.put(Constants.HDFS_STORAGE_PATH, hdfsStoragePath.toString());
      containerShellEnv.put(Constants.LOG_DIR, ApplicationConstants.LOG_DIR_EXPANSION_VAR);
      if (component == DriverComponent.NODE_MANAGER) {
        int numNMs = assignedLeftovers ? nodeManagersPerContainer : (totalNMs % nodeManagersPerContainer);
        assignedLeftovers = true;
        containerShellEnv.put(Constants.NM_COUNT, String.valueOf(numNMs));
      }
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(containerResources, containerShellEnv,
          commands, null, tokens, acls);
      nmClientAsync.startContainerAsync(container, ctx);
    }
  }

  @VisibleForTesting
  protected static String constructUserConfOverrides(Path dynoyarnSite) {
    Configuration userOverrides = new Configuration(false);
    userOverrides.setAllowNullValueProperties(true);
    userOverrides.addResource(dynoyarnSite);
    StringJoiner sj = new StringJoiner(" ");
    for (Map.Entry<String, String> entry : userOverrides) {
      String key = entry.getKey();
      if (userOverrides.onlyKeyExists(key)) {
        sj.add(String.format("-D %s=", key));
      } else {
        // dynoyarn specific variable overrides are set in ./start-component.sh,
        // so leave the variable expansions untouched here. Escape '$' so they are not
        // expanded by shell interpreter.
        sj.add(String.format("-D %s=%s", key, userOverrides.getRaw(key)
            .replaceAll("\\$", Matcher.quoteReplacement("\\\\\\$"))));
      }
    }
    return sj.toString();
  }

  /**
   * Schedule tasks - RM container request and NM container requests.
   */
  private void scheduleTasks() throws InterruptedException {
    // Schedule ResourceManager first.
    ContainerRequest rmRequest = requestMap.get(DriverComponent.RESOURCE_MANAGER);
    LOG.info("Start to schedule Resource Manager..");
    Utils.scheduleTask(amRMClient, rmRequest);
    long clusterDurationMs = dyarnConf.getLong(DynoYARNConfigurationKeys.DRIVER_DURATION_MS,
        DynoYARNConfigurationKeys.DEFAULT_DRIVER_DURATION_MS);
    synchronized (rmAllocationLock) {
      rmAllocationLock.wait(clusterDurationMs);
    }
    LOG.info("Scheduled Resource Manager, moving to schedule node managers!");
    // Continue to schedule nodemanager tasks
    ContainerRequest nmRequest = requestMap.get(DriverComponent.NODE_MANAGER);
    Utils.scheduleTask(amRMClient, nmRequest);
  }

  private Map<String, LocalResource> getContainerResources() throws IOException {
    Map<String, LocalResource> containerResources = new ConcurrentHashMap<>();
    String hadoopBin = System.getenv(Constants.HADOOP_BIN_ZIP_NAME);
    String confPath = System.getenv(Constants.DYARN_CONF_NAME);
    String dyarnJar = System.getenv(Constants.DYARN_JAR_NAME);
    String startScript = System.getenv(Constants.DYARN_START_SCRIPT_NAME);
    String capacitySchedulerXml = System.getenv(Constants.CAPACITY_SCHEDULER_NAME);
    String containerExecutorCfg = System.getenv(Constants.CONTAINER_EXECUTOR_CFG_NAME);
    FileSystem fs = FileSystem.get(dyarnConf);
    Utils.localizeHDFSResource(fs, hadoopBin, Constants.HADOOP_BIN_ZIP, LocalResourceType.ARCHIVE, containerResources);
    Utils.localizeHDFSResource(fs, confPath, Constants.DYARN_CONF, LocalResourceType.FILE, containerResources);
    Utils.localizeHDFSResource(fs, dyarnJar, Constants.DYARN_JAR, LocalResourceType.FILE, containerResources);
    Utils.localizeHDFSResource(fs, startScript, Constants.DYARN_START_SCRIPT, LocalResourceType.FILE, containerResources);
    Utils.localizeHDFSResource(fs, containerExecutorCfg, Constants.CONTAINER_EXECUTOR_CFG, LocalResourceType.FILE, containerResources);

    //Try to get Dyno_Generate jar
    Utils.localizeHDFSResource(fs, System.getenv(Constants.SIMULATED_FATJAR_NAME), Constants.SIMULATED_FATJAR, LocalResourceType.FILE, containerResources);

    String hdfsClasspath = System.getenv("HDFS_CLASSPATH");
    for (FileStatus status : fs.listStatus(new Path(hdfsClasspath))) {
      Utils.localizeHDFSResource(fs, status.getPath().toString(), status.getPath().getName(), LocalResourceType.FILE, containerResources);
    }

    boolean finalConfPath = prepareFinalCapacitySchedulerConf(dyarnConf, new Path(capacitySchedulerXml));
    if (!finalConfPath) {
      Utils.localizeHDFSResource(fs, capacitySchedulerXml, Constants.CAPACITY_SCHEDULER_XML, LocalResourceType.FILE, containerResources);
    } else {
      Utils.localizeLocalResource(dyarnConf, fs, Constants.CAPACITY_SCHEDULER_XML, LocalResourceType.FILE, appResourcesPath, containerResources);
    }
    return containerResources;
  }

  /**
   * Adjusts provided capacity-scheduler.xml based on dynoyarn configurations.
   * @param dynoConf Dynoyarn configurations.
   * @param csPath Path to user-provided capacity-scheduler.xml on HDFS
   * @return Whether new configuration was created or not.
   * @throws IOException
   */
  private boolean prepareFinalCapacitySchedulerConf(Configuration dynoConf, Path csPath) throws IOException {
    float multiplier = dynoConf.getFloat(DynoYARNConfigurationKeys.WORKLOAD_MULTIPLIER,
        DynoYARNConfigurationKeys.DEFAULT_WORKLOAD_MULTIPLIER);
    if (multiplier == DynoYARNConfigurationKeys.DEFAULT_WORKLOAD_MULTIPLIER) {
      // Nothing to adjust.
      return false;
    }
    Configuration finalConf = new Configuration(false);
    finalConf.addResource(fs.open(csPath));
    for (Map.Entry<String, String> entry : finalConf) {
      String key = entry.getKey();
      if (key.endsWith(CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX)) {
        finalConf.setInt(key, (int) (Integer.parseInt(finalConf.get(key)) * multiplier));
      }
    }
    try (FileOutputStream fos = new FileOutputStream(Constants.CAPACITY_SCHEDULER_XML)) {
      finalConf.writeXml(fos);
      fos.flush();
    }
    return true;
  }
}
