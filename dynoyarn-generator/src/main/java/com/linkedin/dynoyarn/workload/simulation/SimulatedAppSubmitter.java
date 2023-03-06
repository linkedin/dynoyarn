/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.simulation;

import com.linkedin.dynoyarn.common.ClusterInfo;
import com.linkedin.dynoyarn.common.Constants;
import com.linkedin.dynoyarn.common.Utils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jackson.map.ObjectMapper;

import static com.linkedin.dynoyarn.common.Constants.CORE_SITE_CONF;
import static com.linkedin.dynoyarn.common.Constants.HADOOP_CONF_DIR;
import static com.linkedin.dynoyarn.common.Constants.HDFS_SITE_CONF;
import static com.linkedin.dynoyarn.common.Constants.SPEC_FILE;

/**
 * A client for submitting apps to the simulated RM. Runs within a container in the
 * {@link WorkloadApplicationMaster} application. This client submits an app which
 * starts the {@link SimulatedApplicationMaster}.
 */
public class SimulatedAppSubmitter {

  public static final Log LOG = LogFactory.getLog(SimulatedAppSubmitter.class);

  private Configuration conf = new Configuration();
  private FileSystem fs;
  private String rmEndpoint;
  private String schedulerEndPoint;
  private String clusterSpecLocation;
  private float multiplier;
  private AppSpec[] appSpecs;
  private boolean exitUponSubmission = false;
  private static final String EXIT_UPON_SUBMISSION_CMD_OPTION = "exit_upon_submission";

  public static void main(String[] args) throws Exception {
    SimulatedAppSubmitter appSubmitter = new SimulatedAppSubmitter();
    boolean sanityCheck = appSubmitter.init(args);
    if (!sanityCheck) {
      LOG.fatal("Failed to parse arguments.");
      System.exit(-1);
    }
    int exitCode = appSubmitter.start();
    System.exit(exitCode);
  }

  private boolean init(String[] args) throws IOException {
    fs = FileSystem.get(conf);
    Options opts = new Options();
    opts.addOption("cluster_spec_location", true, "Path on HDFS to cluster spec information.");
    opts.addOption("multiplier", true, "How many times to submit each app.");
    opts.addOption(EXIT_UPON_SUBMISSION_CMD_OPTION, false, "if true, exit as soon as all apps are submitted");
    CommandLine cliParser;
    try {
      cliParser = new GnuParser().parse(opts, args);
    } catch (ParseException e) {
      LOG.error("Got exception while parsing options", e);
      return false;
    }

    clusterSpecLocation = cliParser.getOptionValue("cluster_spec_location");
    multiplier = Float.parseFloat(cliParser.getOptionValue("multiplier", "1.0"));
    exitUponSubmission = cliParser.hasOption(EXIT_UPON_SUBMISSION_CMD_OPTION);
    LOG.info("Exit when all applications are submitted: " + exitUponSubmission);

    InputStream inputStream = fs.open(new Path(clusterSpecLocation));
    String out = IOUtils.toString(inputStream);
    ClusterInfo cluster = new ObjectMapper().readValue(out, ClusterInfo.class);
    rmEndpoint = cluster.getRmHost() + ":" + cluster.getRmPort();
    schedulerEndPoint = cluster.getRmHost() + ":" + cluster.getRmSchedulerPort();
    conf.set(YarnConfiguration.RM_ADDRESS, rmEndpoint);
    conf.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS,
        YarnConfiguration.DEFAULT_RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS);
    LOG.info("Set " + YarnConfiguration.RM_ADDRESS + " to " + rmEndpoint);
    String appSpecsStr = "";
    try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(SPEC_FILE), StandardCharsets.UTF_8))) {
      appSpecsStr = in.readLine();
    }
    if (appSpecsStr == null) {
      LOG.error("Failed to read spec");
      return false;
    }
    appSpecs = new ObjectMapper().readValue(appSpecsStr.replaceAll("'", "\""), AppSpec[].class);
    if (LOG.isDebugEnabled()) {
      for (AppSpec appSpec : appSpecs) {
        LOG.debug("Got app spec " + appSpec);
      }
    }
    if (System.getenv("HADOOP_CONF_DIR") != null) {
      conf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + CORE_SITE_CONF));
      conf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + HDFS_SITE_CONF));
    }
    return true;
  }

  public int start() {
    boolean result = true;
    try {
      run();
    } catch (InterruptedException e) {
      result = false;
    }
    if (result) {
      LOG.info("Applications completed successfully");
      return 0;
    }
    LOG.error("Applications failed to complete successfully");
    return -1;
  }

  private void run() throws InterruptedException {
    LOG.info("Starting SimulatedAppSubmitter.");
    AtomicInteger appSpecCount = new AtomicInteger();
    // Spawn a separate thread for each app. A thread creates a yarn client and submits app.
    // Wait for all threads (i.e. all apps) to finish.
    int numThreads = (int) (appSpecs.length * multiplier);
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            AppSpec appSpec = appSpecs[(int) (appSpecCount.getAndIncrement() / multiplier)];
            UserGroupInformation proxyUser =
                UserGroupInformation.createProxyUser(appSpec.getUser(), UserGroupInformation.getCurrentUser());
            ByteBuffer tokens = Utils.getTokens(conf, null, false);
            ContainerLaunchContext amSpec = createAMContainerSpec(appSpec, tokens);
            // Delay app submission until actual specified time, in case the current container was acquired before
            // specified app submission time.
            long actualSubmissionTime = appSpec.getSubmitTime();
            long currentTime = System.currentTimeMillis();
            if (actualSubmissionTime > currentTime) {
              long sleepTime = actualSubmissionTime - currentTime;
              LOG.debug("Sleeping for " + sleepTime + "ms");
              Thread.sleep(sleepTime);
            }
            LOG.info("Submitting app " + appSpec.getAppId() + " with submit time " + appSpec.getSubmitTime());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Spec: " + appSpec);
            }
            proxyUser.doAs(new PrivilegedAction<Boolean>() {
              @Override
              public Boolean run() {
                try {
                  YarnClient yarnClient = YarnClient.createYarnClient();
                  yarnClient.init(conf);
                  yarnClient.start();
                  YarnClientApplication app = yarnClient.createApplication();
                  ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
                  ApplicationId appId = appContext.getApplicationId();
                  ResourceRequestSpec.RequestedResource amResourceRequest = appSpec.getAmResourceRequest();
                  Resource capability = Resource.newInstance(amResourceRequest.getMemoryMB(), amResourceRequest.getVcores());
                  appContext.setResource(capability);
                  appContext.setAMContainerSpec(amSpec);
                  appContext.setQueue(appSpec.getQueue());
                  appContext.setNodeLabelExpression(appSpec.getNodeLabel());
                  appContext.setApplicationType(appSpec.getAppType());
                  Set<String> appTags = new HashSet<>(appSpec.getApplicationTags());
                  // originalAppId is the appId that actually ran on the real cluster which is being parsed here.
                  // When submitting this app to the fake cluster it will generate a new appId. So when navigating to the
                  // new app on the fake cluster, this tag identifies which real app this fake app corresponds to.
                  appTags.add("originalAppId=" + appSpec.getAppId());
                  // Add abridged app spec to tag for debugging
                  appTags.add(constructAbridgedAppSpec(appSpec));
                  if (appSpec.getNodeLabel() != null && !appSpec.getNodeLabel().trim().isEmpty()) {
                    appTags.add("cluster:" + appSpec.getNodeLabel());
                  }
                  appContext.setApplicationTags(appTags);
                  yarnClient.submitApplication(appContext);

                  return exitUponSubmission ? true : monitorApplication(yarnClient, appId);
                } catch (Exception e) {
                  LOG.warn("Failed to submit simulated job with appId " + appSpec.getAppId());
                  throw new RuntimeException(e);
                }
              }
            });
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      });
      threads[i].start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
  }

  private String constructAbridgedAppSpec(AppSpec appSpec) {
    StringBuilder sb = new StringBuilder("appSpec=");
    for (ResourceRequestSpec rrSpec : appSpec.getResourceRequestSpecs()) {
      sb.append(String.format("time:%dmin,num:%d;", rrSpec.getAverageRuntime() / 1000 / 60, rrSpec.getNumInstances()));
    }
    String tag = sb.toString();
    return tag.substring(0, Math.min(tag.length(), YarnConfiguration.APPLICATION_MAX_TAG_LENGTH));
  }

  private boolean monitorApplication(YarnClient yarnClient, ApplicationId appId)
      throws YarnException, IOException, InterruptedException {

    while (true) {
      // Check app status every 1 second.
      Thread.sleep(1000);

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state || YarnApplicationState.FAILED == state
          || YarnApplicationState.KILLED == state) {
        LOG.info("Application " + appId.getId() + " finished with YarnState=" + state.toString()
            + ", DSFinalStatus=" + dsStatus.toString() + ", breaking monitoring loop.");
        // Set amRpcClient to null so client does not try to connect to it after completion.
        return FinalApplicationStatus.SUCCEEDED == dsStatus;
      }
    }
  }

  private ContainerLaunchContext createAMContainerSpec(AppSpec appSpec, ByteBuffer tokens) throws IOException {
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    FileSystem fs = FileSystem.get(conf);
    Map<String, String> containerEnv = new HashMap<>();

    Map<String, LocalResource> localResources = new HashMap<>();
    //Utils.localizeHDFSResource(fs, System.getenv(Constants.DYARN_JAR_NAME), Constants.DYARN_JAR, LocalResourceType.FILE, localResources);
    //Utils.localizeHDFSResource(fs, System.getenv(Constants.SIMULATED_FATJAR_NAME), Constants.SIMULATED_FATJAR, LocalResourceType.FILE, localResources);
    //Utils.localizeHDFSResource(fs, System.getenv(Constants.DYARN_CONF_NAME), Constants.DYARN_CONF_NAME, LocalResourceType.FILE, localResources);

    //In my case, I never use HDFS_CLASSPATH, therefore, I don't deal with this localizeHDFSResource
    String hdfsClasspath = System.getenv("HDFS_CLASSPATH");
    if (hdfsClasspath != null) {
      for (FileStatus status : fs.listStatus(new Path(hdfsClasspath))) {
        Utils.localizeHDFSResource(fs, status.getPath().toString(), status.getPath().getName(), LocalResourceType.FILE,
            localResources);
      }
    }

    //We need to pass those parameters, since we don't upload the Dyarn_Conf
    containerEnv.put("schedulerEndPoint", schedulerEndPoint);
    containerEnv.put("rmEndPoint", rmEndpoint);
    containerEnv.put(Constants.DYARN_CONF_NAME, new Path(System.getenv(Constants.DYARN_CONF_NAME)).getName());

    //Add simulatedfatjar.jar in the classpath.
    containerEnv.put("CLASSPATH", "./*:$HADOOP_CONF_DIR:$HADOOP_YARN_HOME/share/hadoop/yarn/*:$HADOOP_YARN_HOME/lib/*:/tmp/simulatedfatjar.jar");
    containerEnv.put(Constants.IS_AM, "true");
    containerEnv.put("HDFS_CLASSPATH", System.getenv("HDFS_CLASSPATH"));
    containerEnv.put(Constants.APP_SPEC_NAME, new ObjectMapper().writeValueAsString(appSpec)
        .replaceAll("\"", "'"));

    // Set logs to be readable by everyone. Set app to be modifiable only by app owner.
    Map<ApplicationAccessType, String> acls = new HashMap<>(2);
    acls.put(ApplicationAccessType.VIEW_APP, "*");
    acls.put(ApplicationAccessType.MODIFY_APP, " ");
    amContainer.setApplicationACLs(acls);

    StringJoiner arguments = new StringJoiner(" ");
    arguments.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
    // Managed AM: needs to fit in NM memory
    arguments.add("-Xmx64m");
    arguments.add("-XX:CICompilerCount=2");
    arguments.add("-XX:ParallelGCThreads=2");
    // Add configuration for log dir to retrieve log output from python subprocess in AM
    arguments.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    // Run AM which will submit to fake RM
    arguments.add(SimulatedApplicationMaster.class.getName());
    arguments.add("-cluster_spec_location " + clusterSpecLocation);
    arguments.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar + Constants.AM_STDOUT_FILENAME);
    arguments.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar + Constants.AM_STDERR_FILENAME);

    List<String> commands = new ArrayList<>();
    commands.add(arguments.toString());

    amContainer.setCommands(commands);
    if (tokens != null) {
      amContainer.setTokens(tokens);
    }
    amContainer.setEnvironment(containerEnv);
    amContainer.setLocalResources(localResources);
    return amContainer;
  }
}
