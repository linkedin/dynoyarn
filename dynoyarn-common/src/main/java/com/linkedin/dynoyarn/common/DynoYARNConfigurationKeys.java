/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.common;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;


public class DynoYARNConfigurationKeys {
  private DynoYARNConfigurationKeys() {

  }

  public static final String DYARN_PREFIX = "dynoyarn.";
  public static final String OTHER_NAMENODES_TO_ACCESS = DYARN_PREFIX + "other.namenodes";

  public static final String NM_PREFIX = DYARN_PREFIX + "nodemanager.";
  public static final String RM_PREFIX = DYARN_PREFIX + "resourcemanager.";

  public static final String NM_VCORES = NM_PREFIX + "vcores";
  public static final String NM_MEMORY = NM_PREFIX + "memory";
  public static final String NUM_NMS = NM_PREFIX + "instances";
  public static final String NMS_PER_CONTAINER = NM_PREFIX + "instances-per-container";

  public static final String RM_VCORES = RM_PREFIX + "vcores";
  public static final String RM_MEMORY = RM_PREFIX + "memory";
  public static final String RM_NODE_LABEL = RM_PREFIX + "node-label";

  public static final String DRIVER_PREFIX = DYARN_PREFIX + "driver.";

  public static final String RM_LOG_OUTPUT_PATH = DRIVER_PREFIX + "rm-log-output-path";

  public static final String APPLICATION_NODE_LABEL = DRIVER_PREFIX + "node-label";
  public static final String DRIVER_QUEUE = DRIVER_PREFIX + "queue";

  public static final String DRIVER_DURATION_MS = DRIVER_PREFIX + "simulation-duration-ms";
  public static final long DEFAULT_DRIVER_DURATION_MS = 36000000;

  public static final String WORKLOAD_PREFIX = DYARN_PREFIX + "workload.";

  public static final String WORKLOAD_MULTIPLIER = WORKLOAD_PREFIX + "multiplier";
  public static final float DEFAULT_WORKLOAD_MULTIPLIER = 1.0f;

  public static final String WORKLOAD_SUBMITTER_EXIT_UPON_SUBMISSION = WORKLOAD_PREFIX + "app-submitter-exit-upon-submission";
  public static final boolean DEFAULT_WORKLOAD_SUBMITTER_EXIT_UPON_SUBMISSION = true;

  public static final String WORKLOAD_QUEUE = WORKLOAD_PREFIX + "queue";

  public static final String WORKLOAD_AM_MEMORY = WORKLOAD_PREFIX + "am.memory";
  public static final int DEFAULT_WORKLOAD_AM_MEMORY = 4096;

  public static final String WORKLOAD_NODE_LABEL_EXPRESSION = WORKLOAD_PREFIX + "node-label-expression";

  public static final String WORKLOAD_PRIORITY = WORKLOAD_PREFIX + "app-priority";
  public static final int DEFAULT_WORKLOAD_PRIORITY = 0;

  public static final String WORKLOAD_START_TIME = WORKLOAD_PREFIX + "start-time";
  public static final String WORKLOAD_APP_SPEC_PREPROCESSOR_CLASSES = WORKLOAD_PREFIX + "app-spec-preprocessor-classes";

  public static final String WORKLOAD_APPS_PER_CONTAINER = WORKLOAD_PREFIX + "apps-per-container";
  public static final int DEFAULT_WORKLOAD_APPS_PER_CONTAINER = 1;

  public static final String SIMULATED_APP_SUBMITTER_MEMORY = DYARN_PREFIX + "app-submitter.memory";
  public static final int DEFAULT_SIMULATED_APP_SUBMITTER_MEMORY = 4096;

  public static final String SIMULATED_APP_SUBMITTER_VCORES = DYARN_PREFIX + "app-submitter.vcores";
  public static final int DEFAULT_SIMULATED_APP_SUBMITTER_VCORES = 2;

  private static final Pattern DRIVER_PARTITIONS_REGEX =
      Pattern.compile(DRIVER_PREFIX + "partition.([0-9a-zA-Z][0-9a-zA-Z-_]*).count"
          .replaceAll("\\.", "\\\\."));

  /**
   * Returns a map of partitions to number of NMs based on the provided configuration.
   * For example, if the user configures "dynoyarn.driver.partition.x.count = 2" and
   * "dynoyarn.driver.partition.y.count = 3", this function returns a map containing
   * ("x" -> 2, "y" -> 3).
   * @param conf Configuration object
   * @return Map containing partitions to number of NMs in that partition
   */
  public static Map<String, Integer> getConfiguredDriverPartitions(Configuration conf) {
    Map<String, Integer> partitionToCount = new HashMap<>();
    for (Map.Entry<String, String> entry : conf.getValByRegex(DRIVER_PARTITIONS_REGEX.pattern()).entrySet()) {
      Matcher m = DRIVER_PARTITIONS_REGEX.matcher(entry.getKey());
      m.matches();
      partitionToCount.put(m.group(1), Integer.parseInt(entry.getValue()));
    }
    return partitionToCount;
  }

}
