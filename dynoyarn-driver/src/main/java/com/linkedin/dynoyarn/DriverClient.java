/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.dynoyarn.common.Constants;
import com.linkedin.dynoyarn.common.DynoYARNConfigurationKeys;
import com.linkedin.dynoyarn.common.Utils;
import java.io.File;
import java.io.FileNotFoundException;
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
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.util.Records;

import static com.linkedin.dynoyarn.common.Constants.CORE_SITE_CONF;
import static com.linkedin.dynoyarn.common.Constants.HADOOP_CONF_DIR;
import static com.linkedin.dynoyarn.common.Constants.HDFS_SITE_CONF;
import static com.linkedin.dynoyarn.common.Constants.HDFS_STORAGE_FILE;


/**
 * DriverClient is responsible for generating the simulated YARN cluster.
 * It submits a YARN application to the host cluster, which starts a
 * {@link DriverApplicationMaster}, which then requests containers in which the
 * simulated RM/NMs run.
 */
public class DriverClient implements AutoCloseable {

  private static final Log LOG = LogFactory.getLog(DriverClient.class);
  private static final String RM_APP_URL_TEMPLATE = "http://%s/cluster/app/%s";
  private static final String START_SCRIPT_LOCATION =
      DriverClient.class.getClassLoader().getResource(Constants.DYARN_START_SCRIPT).toString();
  private static final String CONTAINER_EXECUTOR_CFG =
      DriverClient.class.getClassLoader().getResource(Constants.CONTAINER_EXECUTOR_CFG).toString();
  private static final String DYNOYARN_SITE_XML =
      DriverClient.class.getClassLoader().getResource(Constants.DYNOYARN_SITE_XML).toString();

  public static final String HADOOP_BINARY_PATH_OPT = "hadoop_binary_path";
  public static final String CONF_OPT = "conf";
  public static final String CAPACITY_SCHEDULER_CONF_OPT = "capacity_scheduler_conf";
  public static final String HDFS_CLASSPATH_OPT = "hdfs_classpath";

  private YarnClient yarnClient;
  private Configuration dyarnConf;
  private FileSystem fs;
  private String hadoopBinZipPath;
  private String confPath;
  private String capacitySchedulerConfPath;
  private String dyarnJarPath;

  private Path appResourcesPath;

  public static void main(String[] args) {
    int exitCode = 0;
    try (DriverClient client = new DriverClient()) {
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
      LOG.fatal("Failed to run " + this.getClass().getName(), e);
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

  public DriverClient() {
    this(new Configuration(true));
  }

  public DriverClient(Configuration conf) {
    dyarnConf = conf;
  }

  public ApplicationId submitApplication() throws IOException, InterruptedException, URISyntaxException, YarnException {
    LOG.info("Starting client..");
    // Upload dynoyarn jar.
    // dyarnJarPath = new File(DriverClient.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
    dyarnJarPath = new File(MiniYARNCluster.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();

    yarnClient.start();
    YarnClientApplication app = yarnClient.createApplication();
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    Resource capability = Resource.newInstance(2048, 2);
    appContext.setResource(capability);
    ContainerLaunchContext amSpec = createAMContainerSpec(appId, Utils.getTokens(dyarnConf, yarnClient, true));
    appContext.setAMContainerSpec(amSpec);
    String nodeLabel = dyarnConf.get(DynoYARNConfigurationKeys.APPLICATION_NODE_LABEL);
    if (nodeLabel != null) {
      appContext.setNodeLabelExpression(nodeLabel);
    }
    String queue = dyarnConf.get(DynoYARNConfigurationKeys.DRIVER_QUEUE);
    if (queue != null) {
      appContext.setQueue(queue);
    }
    appContext.setApplicationType("DYNOYARN");
    LOG.info("Cluster info stored at " + appResourcesPath.toUri() + "/" + HDFS_STORAGE_FILE);
    LOG.info("Submitting YARN application");
    yarnClient.submitApplication(appContext);
    ApplicationReport report = yarnClient.getApplicationReport(appId);
    LOG.info("URL to track running application (will proxy to simulated RM once it has started): "
            + report.getTrackingUrl());
    LOG.info("ResourceManager web address for application: "
            + String.format(RM_APP_URL_TEMPLATE,
            dyarnConf.get(YarnConfiguration.RM_WEBAPP_ADDRESS),
            report.getApplicationId()));

    return appId;
  }

  public boolean init(String[] args) throws IOException, ParseException {
    Options opts = new Options();
    opts.addOption(HADOOP_BINARY_PATH_OPT, true, "Path to the Hadoop binary zip.");
    opts.addOption(CONF_OPT, true, "Path to dynoyarn configuration.");
    opts.addOption(CAPACITY_SCHEDULER_CONF_OPT, true, "Path to capacity scheduler configuration.");
    opts.addOption(HDFS_CLASSPATH_OPT, true, "Path on HDFS to jars to be localized.");
    CommandLine cliParser = new GnuParser().parse(opts, args, true);
    if (args.length == 0) {
      throw new IllegalArgumentException("No args specified for client to initialize");
    }
    hadoopBinZipPath = cliParser.getOptionValue(HADOOP_BINARY_PATH_OPT);
    confPath = cliParser.getOptionValue(CONF_OPT);
    capacitySchedulerConfPath =
        cliParser.getOptionValue(CAPACITY_SCHEDULER_CONF_OPT, Constants.CAPACITY_SCHEDULER_XML);
    if (confPath != null) {
      dyarnConf.addResource(new Path(confPath));
    }
    fs = FileSystem.get(dyarnConf);
    createYarnClient();
    return true;
  }

  private void createYarnClient() {
    if (System.getenv("HADOOP_CONF_DIR") != null) {
      dyarnConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + CORE_SITE_CONF));
      dyarnConf.addResource(new Path(System.getenv(HADOOP_CONF_DIR) + File.separatorChar + HDFS_SITE_CONF));
    }
    yarnClient = YarnClient.createYarnClient();
    LOG.info(dyarnConf);
    yarnClient.init(dyarnConf);
  }

  private ContainerLaunchContext createAMContainerSpec(ApplicationId appId, ByteBuffer tokens) throws IOException {
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    Map<String, String> containerEnv = new HashMap<>();
    appResourcesPath = Utils.constructAppResourcesPath(fs, appId.toString());

    containerEnv.put(Constants.HADOOP_BIN_ZIP_NAME, hadoopBinZipPath);

    Map<String, LocalResource> localResources = new HashMap<>();
    Path dyarnJar = Utils.localizeLocalResource(dyarnConf, fs, dyarnJarPath, LocalResourceType.FILE, appResourcesPath, localResources);
    Path conf = Utils.localizeLocalResource(dyarnConf, fs, confPath, LocalResourceType.FILE, appResourcesPath, localResources);
    Path startScript = Utils.localizeLocalResource(dyarnConf, fs, START_SCRIPT_LOCATION, LocalResourceType.FILE, appResourcesPath, localResources);
    Path containerExecutorCfg = Utils.localizeLocalResource(dyarnConf, fs, CONTAINER_EXECUTOR_CFG, LocalResourceType.FILE, appResourcesPath, localResources);
    Utils.localizeLocalResource(dyarnConf, fs, DYNOYARN_SITE_XML, LocalResourceType.FILE, appResourcesPath, localResources);
    containerEnv.put(Constants.DYARN_CONF_NAME, conf.toString());

    //Try to add dynoyarn-generator resource when launch NMs
    File cwd = new File(".");
    File[] files = cwd.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith("dynoyarn-generator");
      }
    });
    if (files.length == 0) {
      throw new FileNotFoundException("Not found dynooyarn-generator-jar");
    }
    Path simulateFatJarLocation = Utils.localizeLocalResource(dyarnConf, fs,
            files[0].getPath(), LocalResourceType.FILE, appResourcesPath, localResources);

    containerEnv.put(Constants.SIMULATED_FATJAR_NAME, simulateFatJarLocation.toString());

    containerEnv.put(Constants.DYARN_JAR_NAME, dyarnJar.toString());
    containerEnv.put(Constants.DYARN_START_SCRIPT_NAME, startScript.toString());
    containerEnv.put(Constants.CAPACITY_SCHEDULER_NAME, capacitySchedulerConfPath);
    containerEnv.put(Constants.CONTAINER_EXECUTOR_CFG_NAME, containerExecutorCfg.toString());
    Path hdfsClasspath = new Path(appResourcesPath, "lib");
    fs.mkdirs(hdfsClasspath);
    FileSystem localFs = FileSystem.getLocal(dyarnConf);
    Path libPath = new Path("lib");
    if (localFs.exists(libPath)) {
      for (FileStatus status : localFs.listStatus(new Path("lib"))) {
        Utils.localizeLocalResource(dyarnConf, fs, status.getPath().toString(), LocalResourceType.FILE, hdfsClasspath,
            localResources);
      }
    }
    containerEnv.put("HDFS_CLASSPATH", hdfsClasspath.toString());

    StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : dyarnConf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    containerEnv.put("CLASSPATH", classPathEnv.toString());

    // Set logs to be readable by everyone. Set app to be modifiable only by app owner.
    Map<ApplicationAccessType, String> acls = new HashMap<>(2);
    acls.put(ApplicationAccessType.VIEW_APP, "*");
    acls.put(ApplicationAccessType.MODIFY_APP, " ");
    amContainer.setApplicationACLs(acls);

    List<String> arguments = new ArrayList<>(30);
    arguments.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
    // Set Xmx based on am memory size
    arguments.add("-Xmx" + "2g");
    // Add configuration for log dir to retrieve log output from python subprocess in AM
    arguments.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "="
        + ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    // Set class name
    arguments.add(DriverApplicationMaster.class.getName());
    arguments.add("-conf " + new Path(confPath).getName());
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
      LOG.info("Adding tokens!");
      amContainer.setTokens(tokens);
    }
    amContainer.setLocalResources(localResources);
    amContainer.setEnvironment(containerEnv);
    return amContainer;
  }

  public YarnClient getYarnClient() {
    return this.yarnClient;
  }

  public void close() {
  }
}
