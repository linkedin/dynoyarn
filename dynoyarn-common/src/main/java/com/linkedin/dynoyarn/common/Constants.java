/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.common;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


public class Constants {
  private Constants() { }
  public static final String CORE_SITE_CONF = YarnConfiguration.CORE_SITE_CONFIGURATION_FILE;
  public static final String HADOOP_CONF_DIR = ApplicationConstants.Environment.HADOOP_CONF_DIR.key();
  public static final String HDFS_SITE_CONF = "hdfs-site.xml";

  public static final String AM_STDOUT_FILENAME = "amstdout.log";
  public static final String AM_STDERR_FILENAME = "amstderr.log";
  public static final String DYARN_FOLDER = ".dyarn";

  public static final String HADOOP_BIN_ZIP = "hadoop.zip";
  public static final String DYARN_CONF = "dyarn.conf";
  public static final String DYARN_JAR = "dyarn.jar";
  public static final String SIMULATED_FATJAR = "simulatedfatjar.jar";
  public static final String DYARN_START_SCRIPT = "start-component.sh";
  public static final String SPEC_FILE = "specfile.txt";
  // Rename this file from capacity-scheduler.xml, otherwise the one installed on the gateway will be localized
  public static final String CAPACITY_SCHEDULER_XML = "dynoyarn-capacity-scheduler.xml";
  public static final String CONTAINER_EXECUTOR_CFG = "dynoyarn-container-executor.cfg";
  public static final String DYNOYARN_SITE_XML = "dynoyarn-site.xml";

  public static final String HADOOP_BIN_ZIP_NAME = "HADOOP_BIN_ZIP_NAME";
  public static final String DYARN_CONF_NAME = "DYARN_CONF_NAME";
  public static final String DYARN_JAR_NAME = "DYARN_JAR_NAME";
  public static final String DYARN_START_SCRIPT_NAME = "DYARN_START_SCRIPT_NAME";
  public static final String CAPACITY_SCHEDULER_NAME = "CAPACITY_SCHEDULER_NAME";
  public static final String CONTAINER_EXECUTOR_CFG_NAME = "CONTAINER_EXECUTOR_CFG";
  public static final String WORKLOAD_SPEC_NAME = "WORKLOAD_SPEC_NAME";
  public static final String APP_SPEC_NAME = "APP_SPEC_NAME";
  public static final String NM_COUNT = "NM_COUNT";
  public static final String SIMULATED_FATJAR_NAME = "SIMULATED_FATJAR";
  public static final String LOG_DIR = "LOG_DIR";

  // Container environment
  public static final String HDFS_STORAGE_PATH = "HDFS_STORAGE_PATH";
  public static final String HDFS_STORAGE_FILE = "cluster.json";
  public static final String COMPONENT_NAME = "COMPONENT_NAME";
  public static final String IS_AM = "IS_AM";
  public static final String CONTAINER_UTILIZATION = "CONTAINER_UTILIZATION";
  public static final String CONTAINER_RUNTIME = "CONTAINER_RUNTIME";

  public static final String RM_HOST = "RM_HOST";
  public static final String RM_HTTP_PORT = "DYARN_RM_HTTP_PORT";
  public static final String RM_SCHEDULER_PORT = "DYARN_RM_SCHEDULER_PORT";
  public static final String RM_PORT = "DYARN_RM_PORT";
  public static final String RM_ADMIN_PORT = "DYARN_RM_ADMIN_PORT";
  public static final String RM_TRACKER_PORT = "DYARN_RM_TRACKER_PORT";
}