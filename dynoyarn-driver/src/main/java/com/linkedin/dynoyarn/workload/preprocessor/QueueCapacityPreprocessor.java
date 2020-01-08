/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.preprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.dynoyarn.workload.simulation.AppSpec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;


/**
 * AppSpec preprocessor which fetches configured queue capacities from fake RM,
 * and overrides app spec's queue based on queue capacity distribution (e.g. if capacity scheduler
 * has queues root.a, root.b, root.c, then each app spec has 33% chance of queue being overridden
 * to root.a, root.b, or root.c.
 */
public class QueueCapacityPreprocessor extends AppSpecPreprocessor {

  public static final Log LOG = LogFactory.getLog(QueueCapacityPreprocessor.class);

  private YarnClient rmClient;
  private TreeMap<Double, String> queueDistribution;
  private Random random;

  @Override
  public void init(Configuration conf) {
    rmClient = YarnClient.createYarnClient();
    rmClient.init(conf);
    random = new Random();
  }

  @Override
  public void start() {
    rmClient.start();
    try {
      List<QueueInfo> queueInfos = rmClient.getAllQueues();
      List<QueueInfo> topLevelQueueInfos = rmClient.getRootQueueInfos();
      queueDistribution = new TreeMap<>();
      Map<String, QueueInfo> queueNameToInfo = new HashMap<>();
      for (QueueInfo queueInfo : queueInfos) {
        queueNameToInfo.put(queueInfo.getQueueName(), queueInfo);
      }
      double base = 0.0;
      for (QueueInfo topLevelQueue : topLevelQueueInfos) {
        base = setAbsoluteQueueCapacity(base, 1.0, topLevelQueue.getQueueName(), queueNameToInfo);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Recursively compute absolute capacities of leaf queues. Add accumulated capacities
  // to a TreeMap so we can randomly draw queues based from a distribution according
  // to each queue's absolute capacity. See TestQueueCapacityPreprocessor for an example.
  private double setAbsoluteQueueCapacity(double base, double currentAbsoluteCapacity,
      String queueName, Map<String, QueueInfo> queueInfos) {
    QueueInfo queueInfo = queueInfos.get(queueName);
    currentAbsoluteCapacity *= queueInfo.getCapacity();
    if (queueInfo.getChildQueues().isEmpty()) {
      if (currentAbsoluteCapacity > 1.0E-3) {
        // Only add queues with non-trivial absolute capacity
        queueDistribution.put(base, queueInfo.getQueueName());
        LOG.info("Setting base for " + queueInfo.getQueueName() + " as " + base);
        base += currentAbsoluteCapacity;
      }
      return base;
    } else {
      for (QueueInfo childQueue : queueInfo.getChildQueues()) {
        base = setAbsoluteQueueCapacity(base, currentAbsoluteCapacity, childQueue.getQueueName(), queueInfos);
      }
      return base;
    }
  }

  @Override
  public void processAppSpec(AppSpec appSpec) {
    double rand = random.nextDouble();
    LOG.info("Got rand " + rand);
    Map.Entry<Double, String> entry = queueDistribution.floorEntry(rand);
    LOG.info("Assigning app " + appSpec.getAppId() + " to queue " + entry.getValue());
    appSpec.setQueue(entry.getValue());
  }

  @VisibleForTesting
  protected void setYarnClient(YarnClient yarnClient) {
    this.rmClient = yarnClient;
  }

  @VisibleForTesting
  protected void setRandom(Random random) {
    this.random = random;
  }
}
