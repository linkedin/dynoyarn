/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import azkaban.flow.CommonJobProperties;
import com.linkedin.dynoyarn.common.ClusterInfo;
import com.linkedin.dynoyarn.common.Constants;
import com.linkedin.dynoyarn.common.DynoYARNConfigurationKeys;
import com.linkedin.dynoyarn.common.Utils;
import com.linkedin.dynoyarn.workload.WorkloadClient;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


public class DynoYARNAzkabanRunner {
  private static final Logger LOG = Logger.getLogger(DynoYARNAzkabanRunner.class);

  // Azkaban workflow arguments
  private static final String HADOOP_BINARY_PATH_ARG = "hadoop-binary-path";
  private static final String CAPACITY_SCHEDULER_CONF_PATH_ARG = "capacity-scheduler-conf-path";
  private static final String WORKLOAD_SPEC_PATH_ARG = "workload-spec-path";
  private static final String SIMULATED_FATJAR_PATH_ARG = "simulated-fatjar-path";

  private Properties _properties;

  // Azkaban will use reflection to find a constructor of this form when it invokes your job. It
  // must also find a "public void run()" method.
  public DynoYARNAzkabanRunner(String name, Properties properties) {
    _properties = properties;
  }

  private Configuration createConfWithPrefix(String prefix) {
    return createConfWithPrefixes(Collections.singleton(prefix));
  }

  /**
   * Creates a conf file which contains all properties with contains any of the given prefixes.
   * @param prefixes Prefixes to filter properties by.
   * @return Configuration with all properties with the given prefixes.
   * @throws IOException
   */
  private Configuration createConfWithPrefixes(Set<String> prefixes) {
    Set<String> propertyKeys = _properties.stringPropertyNames();
    Configuration dynoyarnConf = new Configuration(false);
    for (String key : propertyKeys) {
      for (String prefix : prefixes) {
        if (key.startsWith(prefix)) {
          String value = _properties.getProperty(key);
          // dynoyarn-site.xml sets variables as $variableName (e.g. $rmHost) which will be replaced in
          // start-component.sh. Setting it as ${variableName} will cause azkaban to interpret it as variable
          // substitution. So we wrap it in braces here.
          dynoyarnConf.set(key, value.replaceAll("\\$([A-Za-z_]+)", "\\${$1}"));
          break;
        }
      }
    }
    return dynoyarnConf;
  }

  private void writeConf(Configuration conf, String confFileName) throws IOException {
    try (FileOutputStream fos = new FileOutputStream(confFileName)) {
      conf.writeXml(fos);
      fos.flush();
    }
  }

  /**
   * Add key and value to command line args.
   * @param key key
   * @param value value
   * @param args List of command line args to add to
   */
  private void addToArgs(String key, String value, List<String> args) {
    args.add(String.format("-%s", key));
    args.add(value);
  }

  private boolean isTerminalState(YarnApplicationState state) {
    return (YarnApplicationState.FINISHED == state || YarnApplicationState.FAILED == state
      || YarnApplicationState.KILLED == state);
  }

  public void run() throws Exception {
    Configuration dynoyarnConf = createConfWithPrefix("dynoyarn.");
    dynoyarnConf.set(CommonJobProperties.EXEC_ID, new Configuration().get(CommonJobProperties.EXEC_ID));
    writeConf(dynoyarnConf, "dynoyarn.xml");
    writeConf(createConfWithPrefixes(new HashSet<>(Arrays.asList("yarn.", "hadoop."))), "dynoyarn-site.xml");

    // Workflow:
    // 1. Launch DriverClient
    // 2. Wait for RUNNING state
    // 3. Fetch cluster.json information for driver
    // 4. Wait for number of NMs to reach requested number
    // 5. Launch WorkloadClient
    // 6. Wait for completion
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    String hadoopBinaryPath = _properties.getProperty(HADOOP_BINARY_PATH_ARG);
    String csConfPath = _properties.getProperty(CAPACITY_SCHEDULER_CONF_PATH_ARG);
    DriverClient driverClient = new DriverClient();
    List<String> driverArgs = new ArrayList<>();
    addToArgs(DriverClient.HADOOP_BINARY_PATH_OPT, hadoopBinaryPath, driverArgs);
    addToArgs(DriverClient.CONF_OPT, "dynoyarn.xml", driverArgs);
    addToArgs(DriverClient.CAPACITY_SCHEDULER_CONF_OPT, csConfPath, driverArgs);
    driverClient.init(driverArgs.toArray(new String[]{}));
    ApplicationId driverAppId = driverClient.submitApplication();
    boolean driverReady = Utils.poll(() -> {
        ApplicationReport report = driverClient.getYarnClient().getApplicationReport(driverAppId);
        if (isTerminalState(report.getYarnApplicationState()) || YarnApplicationState.RUNNING == report.getYarnApplicationState()) {
          return true;
        }
        return false;
    }, 5, 300);
    if (!driverReady) {
      LOG.error("Driver timed out waiting to be RUNNING.");
      throw new RuntimeException("Driver timed out waiting to be RUNNING.");
    }
    ApplicationReport driverReport = driverClient.getYarnClient().getApplicationReport(driverAppId);
    if (isTerminalState(driverReport.getYarnApplicationState())) {
      LOG.error("Driver failed to launch.");
      throw new RuntimeException("Driver failed to launch.");
    }
    Path driverResourcesPath = new Path(Utils.constructAppResourcesPath(fs, driverAppId.toString()),
      Constants.HDFS_STORAGE_FILE);
    int numTotalNMs = Integer.valueOf(_properties.getProperty(DynoYARNConfigurationKeys.NUM_NMS));
    driverReady = Utils.poll(() -> {
      ClusterInfo clusterInfo = null;
      try (FSDataInputStream inputStream = fs.open(driverResourcesPath)) {
        String out = IOUtils.toString(inputStream);
        clusterInfo = new ObjectMapper().readValue(out, ClusterInfo.class);
      } catch (Exception e) {
        LOG.info("Not able to get file: " + driverResourcesPath);
        return false;
      }
      YarnClient rmDriverClient = YarnClient.createYarnClient();
      Configuration driverClientConf = new Configuration();
      driverClientConf.set(YarnConfiguration.RM_ADDRESS, clusterInfo.getRmHost() + ":" + clusterInfo.getRmPort());
      // Set a short timeout to fail fast when RM does not come up.
      driverClientConf.setInt(CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY, 10);
      driverClientConf.setInt(YarnConfiguration.CLIENT_FAILOVER_MAX_ATTEMPTS, 1);
      rmDriverClient.init(driverClientConf);
      rmDriverClient.start();
      YarnClusterMetrics driverMetrics = rmDriverClient.getYarnClusterMetrics();
      int numActiveNMs = driverMetrics.getNumActiveNodeManagers();
      if (numActiveNMs == numTotalNMs) {
        return true;
      }
      return false;
    }, 10, 600);
    if (!driverReady) {
      LOG.error("Driver timed out waiting for " + numTotalNMs + " NodeManagers to be active.");
      throw new RuntimeException("Driver timed out waiting for " + numTotalNMs + " NodeManagers to be active.");
    }

    LOG.info("Driver is up with " + numTotalNMs + " NodeManagers.");
    LOG.info("Starting workload.");
    String workloadSpecPath = _properties.getProperty(WORKLOAD_SPEC_PATH_ARG);
    String simulatedFatJarPath = _properties.getProperty(SIMULATED_FATJAR_PATH_ARG);
    WorkloadClient workloadClient = new WorkloadClient();
    List<String> workloadArgs = new ArrayList<>();
    addToArgs(WorkloadClient.WORKLOAD_SPEC_LOCATION_ARG, workloadSpecPath, workloadArgs);
    addToArgs(WorkloadClient.CONF_ARG, "dynoyarn.xml", workloadArgs);
    addToArgs(WorkloadClient.DRIVER_APP_ID_ARG, driverAppId.toString(), workloadArgs);
    addToArgs(WorkloadClient.SIMULATED_FATJAR_ARG, simulatedFatJarPath, workloadArgs);
    workloadClient.init(workloadArgs.toArray(new String[]{}));
    ApplicationId workloadAppId = workloadClient.submitApplication();
    boolean workloadReady = Utils.poll(() -> {
      ApplicationReport report = driverClient.getYarnClient().getApplicationReport(workloadAppId);
      if (isTerminalState(report.getYarnApplicationState()) || YarnApplicationState.RUNNING == report.getYarnApplicationState()) {
        return true;
      }
      return false;
    }, 5, 300);
    if (!workloadReady) {
      LOG.error("Workload timed out waiting to be RUNNING.");
      throw new RuntimeException("Workload timed out waiting to be RUNNING.");
    }
    ApplicationReport workloadReport = driverClient.getYarnClient().getApplicationReport(workloadAppId);
    if (isTerminalState(workloadReport.getYarnApplicationState())) {
      LOG.error("Workload failed to launch.");
      throw new RuntimeException("Workload failed to launch.");
    }
    boolean success = Utils.monitorApplication(driverClient.getYarnClient(), driverAppId, workloadAppId);
    // Check which application terminated, and kill the other one.
    ApplicationReport report = driverClient.getYarnClient().getApplicationReport(driverAppId);
    if (isTerminalState(report.getYarnApplicationState())) {
      // Driver finished. Terminate workload.
      driverClient.getYarnClient().killApplication(workloadAppId, "Driver " + driverAppId + " terminated.");
      if (report.getFinalApplicationStatus() != FinalApplicationStatus.SUCCEEDED) {
        throw new RuntimeException("Driver failed.");
      }
      return;
    }
    report = driverClient.getYarnClient().getApplicationReport(workloadAppId);
    if (isTerminalState(report.getYarnApplicationState())) {
      // Workload finished. Terminate driver.
      driverClient.getYarnClient().killApplication(driverAppId, "Workload " + workloadAppId + " terminated.");
      if (report.getFinalApplicationStatus() != FinalApplicationStatus.SUCCEEDED) {
        throw new RuntimeException("Workload failed.");
      }
      return;
    }
  }
}
