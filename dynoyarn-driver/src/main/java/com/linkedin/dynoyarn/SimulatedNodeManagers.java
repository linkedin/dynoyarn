/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;


/**
 * Starts up a number of NodeManagers within the same JVM.
 */
public class SimulatedNodeManagers extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(SimulatedNodeManagers.class);
  private static final String USAGE = "Usage: com.linkedin.dynoyarn.SimulatedNodeManagers <num-nodemanagers>";

  static void printUsageExit(String err) {
    System.out.println(err);
    System.out.println(USAGE);
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    SimulatedNodeManagers nodemanagers = new SimulatedNodeManagers();
    ToolRunner.run(new YarnConfiguration(), nodemanagers, args);
  }

  public int run(String[] args) {
    if (args.length < 1) {
      printUsageExit("Not enough arguments");
    }

    int numNMs = Integer.parseInt(args[0]);
    LOG.info("Starting NMs with max memory " + Runtime.getRuntime().maxMemory());
    LOG.info("Starting NMs with HADOOP_CONF_DIR " + System.getenv("HADOOP_CONF_DIR"));
    LOG.info("Starting NMs with HADOOP_YARN_HOME " + System.getenv("HADOOP_YARN_HOME"));
    MiniYARNCluster miniYARNCluster = new MiniYARNCluster("DynoYARN", 0, numNMs, 1, 1);
    miniYARNCluster.init(getConf());
    // Don't directly call miniYARNCluster.start(). This will wait for NMs to connect to RM (but we don't have any RMs).
    for (Service nm : miniYARNCluster.getServices()) {
      nm.start();
    }

    return 0;
  }

}
