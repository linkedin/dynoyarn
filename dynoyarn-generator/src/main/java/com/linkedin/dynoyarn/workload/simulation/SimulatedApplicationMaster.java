/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.simulation;

import com.linkedin.dynoyarn.common.ClusterInfo;
import com.linkedin.dynoyarn.common.Constants;
import com.linkedin.dynoyarn.common.ContainerRequest;
import com.linkedin.dynoyarn.common.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * AM for load testing. This handles the logic for applications submitted to the fake RM.
 * It will do resource negotiation with the fake RM as well as submitting fake containers
 * to the fake NMs (including this simulated application's container runtime/utilization
 * information).
 */
public class SimulatedApplicationMaster {
  enum Status {
    RUNNING,
    FAILED,
    SUCCEEDED
  }

  public static final Log LOG = LogFactory.getLog(SimulatedApplicationMaster.class);

  private Configuration conf = new Configuration();
  private AMRMClientAsync amRMClient;
  private NMClientAsync nmClientAsync;
  private SimulatedApplicationMaster.NMCallbackHandler containerListener;

  private ByteBuffer containerTokens;
  private FileSystem fs;
  private AppSpec appSpec;
  private Map<Integer, ResourceRequestSpec> priorityToSpecMap = new HashMap<>();
  private Map<Integer, List<Long>> priorityToRuntimeMap = new HashMap<>();
  private int numRequestedContainers;
  private int numCompletedContainers;
  private int numFailedContainers;

  /**
   * Container allocation controls. If there are multiple resource requests,
   * we request them in series (i.e. we request batch 1, wait for all of them to finish,
   * then request the next batch). This emulates MapReduce with slowstart 1.0.
   */
  private Lock allocationLock = new ReentrantLock();
  private Condition resourceRequestFinishedCondition = allocationLock.newCondition();

  /**
   * DynoYARN Arguments
   */
  private String rmEndpoint;
  private String amEndpoint;

  private SimulatedApplicationMaster() {
  }

  public boolean init(String[] args) throws IOException {
    Options opts = new Options();
    opts.addOption("cluster_spec_location", true, "Path on HDFS to cluster spec information.");

    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (ParseException e) {
      LOG.error("Got exception while parsing options", e);
      return false;
    }
    conf.addResource(System.getenv(Constants.DYARN_CONF_NAME));
    UserGroupInformation.setConfiguration(conf);
    fs = FileSystem.get(conf);
    String clusterSpecLocation = cliParser.getOptionValue("cluster_spec_location");
    InputStream inputStream = fs.open(new Path(clusterSpecLocation));
    String out = IOUtils.toString(inputStream);
    ClusterInfo cluster = new ObjectMapper().readValue(out, ClusterInfo.class);
    rmEndpoint = cluster.getRmHost() + ":" + cluster.getRmPort();
    amEndpoint = cluster.getRmHost() + ":" + cluster.getRmSchedulerPort();
    conf.set(YarnConfiguration.RM_ADDRESS, rmEndpoint);
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, amEndpoint);
    // Set NM/RM max-wait to respective intervals, so that the underlying ipc.Client only retries once.
    // This way if the test YARN cluster is torn down, the SimulatedApplicationMaster instance does not spin
    // for too long trying to reconnect.
    // Change these values/make them configurable if you happen to be testing NM/RM restart.
    conf.setLong(YarnConfiguration.CLIENT_NM_CONNECT_MAX_WAIT_MS,
        YarnConfiguration.DEFAULT_CLIENT_NM_CONNECT_RETRY_INTERVAL_MS);
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
        YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS);
    // Tune this to limit num concurrent NM launch requests per simulated AM
    conf.setInt(YarnConfiguration.NM_CLIENT_ASYNC_THREAD_POOL_MAX_SIZE, 5);
    LOG.info("Set AM endpoint to " + amEndpoint);

    appSpec = new ObjectMapper().readValue(System.getenv(Constants.APP_SPEC_NAME).replaceAll("'", "\""), AppSpec.class);
    LOG.info("App spec: " + appSpec);
    parseAppSpec();
    return true;
  }

  private void parseAppSpec() {
    for (ResourceRequestSpec spec : appSpec.getResourceRequestSpecs()) {
      priorityToSpecMap.put(spec.getPriority(), spec);
      List<Long> runtimes = new ArrayList<>();
      // Use average runtime, if specified
      // TODO: This can definitely be optimized (no need to add 30k tasks with the same runtimes)
      if (spec.getAverageRuntime() != 0) {
        for (int i = 0; i < spec.getNumInstances(); i++) {
          runtimes.add(spec.getAverageRuntime());
        }
      } else if (spec.getRuntimes() != null && spec.getRuntimes().length != 0) {
        for (long runtime : spec.getRuntimes()) {
          runtimes.add(runtime);
        }
      }
      if (!runtimes.isEmpty()) {
        priorityToRuntimeMap.put(spec.getPriority(), runtimes);
      }
    }
  }

  private boolean prepare() {
    LOG.info("Preparing application master..");
    containerListener = createNMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);
    nmClientAsync.start();

    try {
      String appMasterHostname = NetUtils.getHostname();
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      Credentials credentials = ugi.getCredentials();
      setupContainerCredentials(ugi);
      UserGroupInformation remoteUser = UserGroupInformation
          .createRemoteUser(appSpec.getUser());
      remoteUser.addCredentials(credentials);

      // Init AMRMClient
      AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
      amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
      amRMClient.init(conf);
      amRMClient.start();
      remoteUser.doAs(new PrivilegedAction<Boolean>() {
        @Override
        public Boolean run() {
          try {
            amRMClient.registerApplicationMaster(appMasterHostname, -1, "");
            return true;
          } catch (IOException | YarnException e) {
            throw new RuntimeException("Failed to register AM", e);
          }
        }
      });
    } catch (Exception e) {
      LOG.error("Exception while preparing AM", e);
      return false;
    }
    return true;
  }

  // Set up credentials for the containers.
  private void setupContainerCredentials(UserGroupInformation ugi) throws Exception {
    Credentials creds = ugi.getCredentials();
    Iterator<Token<?>> iter = creds.getAllTokens().iterator();
    // Remove AMRMToken so tasks don't have it
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    DataOutputBuffer dob = new DataOutputBuffer();
    creds.writeTokenStorageToStream(dob);
    containerTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
  }

  public static void main(String[] args) throws InterruptedException {
    boolean result = false;
    try {
      SimulatedApplicationMaster appMaster = new SimulatedApplicationMaster();
      LOG.info("Initializing SimulatedApplicationMaster");
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
    scheduleTasks();
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
    if (amRMClient.isInState(Service.STATE.STOPPED)) {
      LOG.error("AMRMClient was stopped. Stopping SimulatedApplicationMaster.");
      return Status.FAILED;
    }
    if (numCompletedContainers == numRequestedContainers) {
      if (numFailedContainers == 0) {
        return Status.SUCCEEDED;
      } else {
        return Status.FAILED;
      }
    }
    return Status.RUNNING;
  }

  private String serializeUtilizations(ResourceRequestSpec.Utilization[] utils)
      throws Exception {
    return new ObjectMapper().writeValueAsString(utils);
  }

  /**
   * Node manager call back handler
   */
  static class NMCallbackHandler implements NMClientAsync.CallbackHandler {
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
  }

  private NMCallbackHandler createNMCallbackHandler() {
    return new NMCallbackHandler();
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      for (ContainerStatus status : completedContainers) {
        numFailedContainers += (status.getExitStatus() == 0 ? 0 : 1);
        LOG.info("Got completed container: " + status.getContainerId());
      }
      numCompletedContainers += completedContainers.size();
      LOG.info("Num requested: " + numRequestedContainers + ", numCompleted: " + numCompletedContainers);
      if (numRequestedContainers == numCompletedContainers) {
        LOG.info("Requesting next batch of containers...");
        allocationLock.lock();
        try {
          resourceRequestFinishedCondition.signal();
        } finally {
          allocationLock.unlock();
        }
      }
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
      LOG.info("Allocated: " + containers.size() + " containers.");
      for (Container container : containers) {
        LOG.info("Launching a task in container"
            + ", containerId = " + container.getId()
            + ", containerNode = " + container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
            + ", resourceRequest = " + container.getResource());
        new ContainerLauncher(container).run();
      }
    }

    @Override
    public void onShutdownRequest() {
      LOG.error("AMRMClient heartbeat thread stopped. Terminating.");
      allocationLock.lock();
      try {
        amRMClient.stop();
        // Signal task scheduling which will detect that AMRMClient has been stopped,
        // thereby stopping scheduling.
        resourceRequestFinishedCondition.signal();
      } finally {
        allocationLock.unlock();
      }
    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void onError(Throwable throwable) {
      LOG.error("Callback handler thread stopped. Terminating.", throwable);
      allocationLock.lock();
      try {
        amRMClient.stop();
        // Signal task scheduling which will detect that AMRMClient has been stopped,
        // thereby stopping scheduling.
        resourceRequestFinishedCondition.signal();
      } finally {
        allocationLock.unlock();
      }
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
      Iterator<Long> itr = priorityToRuntimeMap.get(container.getPriority().getPriority()).iterator();
      long runtime = itr.next();
      itr.remove();
      List<String> commands = new ArrayList<>();
      StringJoiner arguments = new StringJoiner(" ");
      arguments.add("sleep " + ((double) runtime / 1000)); // sleep command is unused when using FakeContainerLauncher
      arguments.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      arguments.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
      commands.add(arguments.toString());

      ByteBuffer tokens;
      tokens = containerTokens.duplicate();
      // Set logs to be readable by everyone.
      Map<ApplicationAccessType, String> acls = new HashMap<>(2);
      acls.put(ApplicationAccessType.VIEW_APP, "*");
      acls.put(ApplicationAccessType.MODIFY_APP, " ");
      Map<String, String> containerShellEnv = new ConcurrentHashMap<>();
      ResourceRequestSpec spec = priorityToSpecMap.get(container.getPriority().getPriority());
      try {
        containerShellEnv.put(Constants.CONTAINER_UTILIZATION, serializeUtilizations(spec.getUtilizations()));
      } catch (Exception e) {
        throw new RuntimeException("Failed to serialize container utilizations.", e);
      }
      containerShellEnv.put("CONTAINER_RUNTIME_ENV", String.valueOf(runtime));
      Map<String, LocalResource> containerResources = getContainerResources();
      ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(containerResources, containerShellEnv,
          commands, null, tokens, acls);
      nmClientAsync.startContainerAsync(container, ctx);
    }
  }

  /**
   * Schedule tasks based on spec's specified delays and resource requests.
   */
  private void scheduleTasks() throws InterruptedException {
    List<ResourceRequestSpec> requestedResources = appSpec.getResourceRequestSpecs();
    allocationLock.lock();
    try {
      for (ResourceRequestSpec rrSpec : requestedResources) {
        ResourceRequestSpec.RequestedResource rr = rrSpec.getResource();
        if (rrSpec.getNumInstances() != 0) {
          Utils.scheduleTask(amRMClient,
              new ContainerRequest(rrSpec.getNumInstances(), rr.getMemoryMB(), rr.getVcores(), rrSpec.getPriority()));
        } else {
          continue;
        }
        numRequestedContainers += rrSpec.getNumInstances();
        resourceRequestFinishedCondition.await();
        if (amRMClient.isInState(Service.STATE.STOPPED)) {
          LOG.error("AMRMClient was stopped. Stopping task scheduling.");
          return;
        }
      }
    } finally {
      allocationLock.unlock();
    }
  }

  private Map<String, LocalResource> getContainerResources() {
    Map<String, LocalResource> containerResources = new ConcurrentHashMap<>();
    return containerResources;
  }
}
