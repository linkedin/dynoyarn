/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.dynoyarn.common.Constants;
import com.linkedin.dynoyarn.common.DynoYARNConfigurationKeys;
import com.linkedin.dynoyarn.common.Utils;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import static com.linkedin.dynoyarn.common.Constants.CORE_SITE_CONF;
import static com.linkedin.dynoyarn.common.Constants.HADOOP_CONF_DIR;
import static com.linkedin.dynoyarn.common.Constants.HDFS_SITE_CONF;


/**
 * WorkloadClient is responsible for generating the simulated workload.
 * It submits a YARN application to the host cluster, which starts a
 * {@link WorkloadApplicationMaster}, which then requests containers in which the
 * simulated AMs run.
 */
public class WorkloadClient implements AutoCloseable {

  private static final Log LOG = LogFactory.getLog(WorkloadClient.class);
  private YarnClient yarnClient;
  private Options opts;
  private Configuration conf;
  private FileSystem fs;
  private String clusterSpecLocation;
  private String workloadSpecLocation;
  private String simulatedFatJarLocation;
  private String confPath;
  private String workloadJarPath;

  private String driverAppId;

  private Path appResourcesPath;

  public static final String DRIVER_APP_ID_ARG = "driver_app_id";
  public static final String WORKLOAD_SPEC_LOCATION_ARG = "workload_spec_location";
  public static final String SIMULATED_FATJAR_ARG = "simulated_fatjar";
  public static final String CONF_ARG = "conf";

  public static void main(String[] args) {
    int exitCode = 0;
    try (WorkloadClient client = new WorkloadClient()) {
      boolean sanityCheck = client.init(args);
      if (!sanityCheck) {
        LOG.fatal("Failed to parse arguments.");
      }
      exitCode = client.start();
    } catch (Exception e) {
      LOG.fatal("Failed to init client.", e);
      System.exit(-1);
    }
    System.exit(exitCode);
  }

  @VisibleForTesting
  public int start() {
    boolean result = true;
    try {
      ApplicationId appId = submitApplication();
      result = Utils.monitorApplication(yarnClient, appId);
    } catch (IOException | InterruptedException | URISyntaxException | YarnException e) {
      LOG.fatal("Failed to run dynoyarn workload client.", e);
    } finally {
      if (appResourcesPath != null) {
        try {
          fs.delete(appResourcesPath, true);
        } catch (IOException e) {
          LOG.error("Failed to cleanup app resources path: " + appResourcesPath.toString(), e);
        }
      }
    }
    if (result) {
      LOG.info("Application completed successfully");
      return 0;
    }
    LOG.error("Application failed to complete successfully");
    return -1;
  }

  public WorkloadClient() {
    this(new Configuration(true));
  }

  public WorkloadClient(Configuration conf) {
    initOptions();
    this.conf = conf;
  }

  public ApplicationId submitApplication() throws IOException, InterruptedException, URISyntaxException, YarnException {
    LOG.info("Starting dynoyarn workload client..");
    // Upload dynoyarn jar.
    workloadJarPath = new File(WorkloadClient.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();

    yarnClient.start();
    YarnClientApplication app = yarnClient.createApplication();
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    int amMemory = conf.getInt(DynoYARNConfigurationKeys.WORKLOAD_AM_MEMORY,
        DynoYARNConfigurationKeys.DEFAULT_WORKLOAD_AM_MEMORY);
    Resource capability = Resource.newInstance(amMemory, 2);
    appContext.setResource(capability);
    ContainerLaunchContext amSpec =
        createAMContainerSpec(appId, Utils.getTokens(conf, yarnClient, true), amMemory);
    appContext.setAMContainerSpec(amSpec);
    appContext.setApplicationType("DYNOWORKLOAD");
    String queue = conf.get(DynoYARNConfigurationKeys.WORKLOAD_QUEUE);
    if (queue != null) {
      appContext.setQueue(queue);
    }
    appContext.setPriority(Priority.newInstance(conf.getInt(DynoYARNConfigurationKeys.WORKLOAD_PRIORITY,
            DynoYARNConfigurationKeys.DEFAULT_WORKLOAD_PRIORITY)));
    String nodeLabelExpression = conf.get(DynoYARNConfigurationKeys.WORKLOAD_NODE_LABEL_EXPRESSION);
    if (nodeLabelExpression != null) {
      appContext.setNodeLabelExpression(nodeLabelExpression);
    }
    LOG.info("Submitting YARN application");
    yarnClient.submitApplication(appContext);
    return appId;
  }

  public boolean init(String[] args) throws IOException, ParseException {
    CommandLine cliParser = new GnuParser().parse(opts, args, true);
    if (args.length == 0) {
      throw new IllegalArgumentException("No args specified for client to initialize");
    }
    //String driverAppId = cliParser.getOptionValue(DRIVER_APP_ID_ARG);
    driverAppId = cliParser.getOptionValue(DRIVER_APP_ID_ARG);
    workloadSpecLocation = cliParser.getOptionValue(WORKLOAD_SPEC_LOCATION_ARG);
    simulatedFatJarLocation = cliParser.getOptionValue(SIMULATED_FATJAR_ARG);
    confPath = cliParser.getOptionValue(CONF_ARG);
    if (confPath != null) {
      conf.addResource(new Path(confPath));
    }
    fs = FileSystem.get(conf);
    clusterSpecLocation = Utils.constructAppResourcesPath(fs, driverAppId) + "/" + Constants.HDFS_STORAGE_FILE;
    createYarnClient();
    return true;
  }

  private void createYarnClient() {
    if (System.getenv("HADOOP_CONF_DIR") != null) {
      conf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + CORE_SITE_CONF));
      conf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + HDFS_SITE_CONF));
    }
    yarnClient = YarnClient.createYarnClient();
    LOG.info(conf);
    yarnClient.init(conf);
  }

  private void initOptions() {
    opts = new Options();
    opts.addOption(DRIVER_APP_ID_ARG, true, "Application id of driver application.");
    opts.addOption(WORKLOAD_SPEC_LOCATION_ARG, true, "Location on local fs of workload spec.");
    opts.addOption(CONF_ARG, true, "Location on local fs of dynoyarn conf.");
    opts.addOption(SIMULATED_FATJAR_ARG, true, "Location on HDFS of fatjar for simulated hadoop bin version.");
  }

  private ContainerLaunchContext createAMContainerSpec(ApplicationId appId, ByteBuffer tokens, int amMemory) throws IOException,
      URISyntaxException {
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    FileSystem fs = FileSystem.get(conf);
    Map<String, String> containerEnv = new HashMap<>();
    appResourcesPath = Utils.constructAppResourcesPath(fs, appId.toString());

    Map<String, LocalResource> localResources = new HashMap<>();
    Path dyarnJar = Utils.localizeLocalResource(conf, fs, workloadJarPath, LocalResourceType.FILE, appResourcesPath, localResources);
    Utils.localizeHDFSResource(fs, workloadSpecLocation, Constants.WORKLOAD_SPEC_NAME, LocalResourceType.FILE, localResources);
    Path remoteConfPath = Utils.localizeLocalResource(conf, fs, confPath, LocalResourceType.FILE, appResourcesPath, localResources);
    File cwd = new File(".");
    File[] files = cwd.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("dynoyarn-generator");
      }
    });
    if (simulatedFatJarLocation != null) {
      // fat jar is on HDFS
      containerEnv.put(Constants.SIMULATED_FATJAR_NAME, simulatedFatJarLocation.toString());
    } else if (files.length == 1) {
      Path simulatedFatJarLocation = Utils.localizeLocalResource(conf, fs, files[0].getPath(),
          LocalResourceType.FILE, appResourcesPath, localResources);
      containerEnv.put(Constants.SIMULATED_FATJAR_NAME, simulatedFatJarLocation.toString());
    } else {
      throw new IllegalArgumentException("Couldn't find dynoyarn-generator* fat jar");
    }
    containerEnv.put(Constants.DYARN_CONF_NAME, remoteConfPath.toUri().getPath());
    containerEnv.put(Constants.DYARN_JAR_NAME, dyarnJar.toString());

    Path libPath = new Path("lib");
    FileSystem localFs = FileSystem.getLocal(conf);
    if (localFs.exists(libPath) && localFs.getFileStatus(libPath).isDirectory()) {
      Path hdfsClasspath = new Path(appResourcesPath, "lib");
      fs.mkdirs(hdfsClasspath);
      for (FileStatus status : localFs.listStatus(libPath)) {
        Utils.localizeLocalResource(conf, fs, status.getPath().toString(), LocalResourceType.FILE, hdfsClasspath,
            localResources);
      }
      containerEnv.put("HDFS_CLASSPATH", hdfsClasspath.toString());
    }

    StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : this.conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    containerEnv.put("CLASSPATH", classPathEnv.toString());
    containerEnv.put("driverAppId", driverAppId);

    // Set logs to be readable by everyone. Set app to be modifiable only by app owner.
    Map<ApplicationAccessType, String> acls = new HashMap<>(2);
    acls.put(ApplicationAccessType.VIEW_APP, "*");
    acls.put(ApplicationAccessType.MODIFY_APP, " ");
    amContainer.setApplicationACLs(acls);

    List<String> arguments = new ArrayList<>(30);
    arguments.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
    // Set Xmx based on am memory size
    arguments.add("-Xmx" + (int) (amMemory * 0.9f) + "m");
    // Add configuration for log dir to retrieve log output from python subprocess in AM
    arguments.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    // Set class name
    arguments.add(WorkloadApplicationMaster.class.getName());
    arguments.add("-cluster_spec_location " + clusterSpecLocation);
    arguments.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar + Constants.AM_STDOUT_FILENAME);
    arguments.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + File.separatorChar + Constants.AM_STDERR_FILENAME);

    StringBuilder command = new StringBuilder();
    for (CharSequence str : arguments) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up Application Master command " + command.toString());
    List<String> commands = new ArrayList<>();
    commands.add(command.toString());

    amContainer.setCommands(commands);
    if (tokens != null) {
      amContainer.setTokens(tokens);
    }
    amContainer.setLocalResources(localResources);
    amContainer.setEnvironment(containerEnv);
    return amContainer;
  }

  public void close() {
  }
}
