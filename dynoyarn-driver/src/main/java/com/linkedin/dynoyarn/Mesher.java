/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn;

import azkaban.flow.CommonJobProperties;
import com.linkedin.dynoyarn.common.ClusterInfo;
import com.linkedin.dynoyarn.common.Constants;
import com.linkedin.dynoyarn.common.DynoYARNConfigurationKeys;
import com.linkedin.dynoyarn.common.Utils;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
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
import org.codehaus.jackson.map.ObjectMapper;


/**
 * Entry point to a simulated YARN daemon. This runs in a
 * {@link DriverApplicationMaster}'s container and spawns either an RM
 * or NM. These spawned daemons together form the simulated YARN cluster.
 */
public class Mesher {
  private static final Log LOG = LogFactory.getLog(Mesher.class);
  protected Configuration dyarnConf = new Configuration();
  private String taskCommand;

  private Mesher() {
  }

  public static void main(String[] args) throws ParseException, IOException, InterruptedException {
    LOG.info("TaskExecutor is running..");
    Mesher mesher = new Mesher();
    mesher.init(args);

    // Get container id
    Map<String, String> systemEnv = System.getenv();
    FileSystem fs = FileSystem.get(mesher.dyarnConf);
    Path hdfsStoragePath = new Path(System.getenv(Constants.HDFS_STORAGE_PATH));

    String componentName = systemEnv.get(Constants.COMPONENT_NAME);
    Map<String, String> env = new HashMap<>();
    LOG.info("This is component: " + componentName);
    env.put(Constants.COMPONENT_NAME, componentName);
    env.put(Constants.HADOOP_BIN_ZIP_NAME, Constants.HADOOP_BIN_ZIP);
    env.put(Constants.LOG_DIR, System.getenv(Constants.LOG_DIR));

    // Reserve ports for RM.
    if (componentName.equals(DriverComponent.RESOURCE_MANAGER.toString())) {
      ClusterInfo clusterInfo = new ClusterInfo();

      ServerSocket portSocket = new ServerSocket(0);
      int port = portSocket.getLocalPort();
      env.put(Constants.RM_PORT, String.valueOf(port));
      portSocket.close();
      clusterInfo.setRmPort(port);

      portSocket = new ServerSocket(0);
      port = portSocket.getLocalPort();
      env.put(Constants.RM_SCHEDULER_PORT, String.valueOf(port));
      portSocket.close();
      clusterInfo.setRmSchedulerPort(port);

      portSocket = new ServerSocket(0);
      port = portSocket.getLocalPort();
      env.put(Constants.RM_ADMIN_PORT, String.valueOf(port));
      portSocket.close();
      clusterInfo.setRmAdminPort(port);

      portSocket = new ServerSocket(0);
      port = portSocket.getLocalPort();
      env.put(Constants.RM_HTTP_PORT, String.valueOf(port));
      portSocket.close();
      clusterInfo.setRmHttpPort(port);

      portSocket = new ServerSocket(0);
      port = portSocket.getLocalPort();
      env.put(Constants.RM_TRACKER_PORT, String.valueOf(port));
      portSocket.close();
      clusterInfo.setRmTrackerPort(port);

      String hostAddress = Utils.getCurrentHostName();
      clusterInfo.setRmHost(hostAddress);
      env.put(Constants.RM_HOST, hostAddress);
      LOG.info("Assembled cluster spec: " + clusterInfo + "\nWriting to: " + hdfsStoragePath);
      new ObjectMapper().writeValue(new File(Constants.HDFS_STORAGE_FILE), clusterInfo);
      fs.copyFromLocalFile(new Path(Constants.HDFS_STORAGE_FILE), hdfsStoragePath);
    } else {
      // If it is node manager, poll till the cluster spec file is there.
      Utils.poll(() -> {
        FSDataInputStream inputStream = null;
        try {
          inputStream = fs.open(hdfsStoragePath);
          String out = IOUtils.toString(inputStream);
          ClusterInfo cluster = new ObjectMapper().readValue(out, ClusterInfo.class);
          env.put(Constants.RM_PORT, String.valueOf(cluster.getRmPort()));
          env.put(Constants.RM_HOST, String.valueOf(cluster.getRmHost()));
          env.put(Constants.RM_TRACKER_PORT, String.valueOf(cluster.getRmTrackerPort()));
          env.put(Constants.RM_HTTP_PORT, String.valueOf(cluster.getRmHttpPort()));
          env.put(Constants.RM_SCHEDULER_PORT, String.valueOf(cluster.getRmSchedulerPort()));
          env.put(Constants.RM_ADMIN_PORT, String.valueOf(cluster.getRmAdminPort()));
          LOG.info("Cluster information: \n" + cluster);
          return true;
        } catch (Exception e) {
          LOG.info("Not able to get file: " + hdfsStoragePath);
          return false;
        } finally {
          if (inputStream != null) {
            inputStream.close();
          }
        }
        }, 5, 3000
      );
      env.put(Constants.NM_COUNT, System.getenv(Constants.NM_COUNT));
    }
    long clusterTimeoutMs = mesher.dyarnConf.getLong(DynoYARNConfigurationKeys.DRIVER_DURATION_MS,
        DynoYARNConfigurationKeys.DEFAULT_DRIVER_DURATION_MS);
    int exitCode = -1;
    try {
      exitCode = Utils.executeShell(mesher.taskCommand, clusterTimeoutMs, env);
    } catch (IllegalThreadStateException itse) {
      // Process timed out, and we couldn't fetch exit value. Timeout is expected for RM/NM daemons.
      LOG.info(componentName + " timed out after running for " + clusterTimeoutMs + " ms.");
      exitCode = 0;
    }
    if (componentName.equals(DriverComponent.RESOURCE_MANAGER.toString())) {
      String logOutputPath = mesher.dyarnConf.get(DynoYARNConfigurationKeys.RM_LOG_OUTPUT_PATH);
      if (logOutputPath != null) {
        Path finalPath = new Path(logOutputPath);
        if (mesher.dyarnConf.get(CommonJobProperties.EXEC_ID) != null) {
          finalPath = new Path(finalPath, mesher.dyarnConf.get(CommonJobProperties.EXEC_ID));
        }
        copyRMLogsToHDFS(mesher, finalPath);
      }
    }
    System.exit(exitCode);
  }

  /**
   * Copies RM app summary and GC logs to HDFS.
   * @param mesher Mesher instance.
   * @param hdfsOutputPath Path on HDFS to copy the logs to.
   */
  private static void copyRMLogsToHDFS(Mesher mesher, Path hdfsOutputPath) throws IOException {
    Path localLogPath = new Path(System.getenv("LOG_DIR"));
    FileSystem localFs = FileSystem.getLocal(mesher.dyarnConf);
    FileSystem fs = FileSystem.get(mesher.dyarnConf);
    FileStatus[] rmAppSummaryLogs = localFs.globStatus(new Path(localLogPath, "rm-appsummary*"));
    for (FileStatus rmAppSummaryLog : rmAppSummaryLogs) {
      fs.copyFromLocalFile(rmAppSummaryLog.getPath(), new Path(hdfsOutputPath, rmAppSummaryLog.getPath().getName()));
    }
    FileStatus[] gcLogs = localFs.globStatus(new Path(localLogPath, "gc-rm.log*"));
    for (FileStatus gcLog : gcLogs) {
      fs.copyFromLocalFile(gcLog.getPath(), new Path(hdfsOutputPath, gcLog.getPath().getName()));
    }
  }

  protected boolean init(String[] args) throws ParseException {
    dyarnConf.addResource(new Path(Constants.DYARN_CONF));
    Options opts = new Options();
    opts.addOption("task_command", true, "The task command to run.");
    CommandLine cliParser = new GnuParser().parse(opts, args);
    taskCommand = cliParser.getOptionValue("task_command", "exit 0");
    // Re-escape $ so it is not expanded by Mesher's underlying shell interpreter.
    taskCommand = taskCommand.replaceAll("\\$", Matcher.quoteReplacement("\\$"));
    LOG.info("Task command: " + taskCommand);
    return true;
  }

}
