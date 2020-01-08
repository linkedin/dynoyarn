/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.simulation;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

import com.linkedin.dynoyarn.workload.simulation.ResourceRequestSpec.Utilization;


/**
 * A DynoYARN specific implementation of {@link ResourceCalculatorProcessTree} for reporting
 * a process's resource utilization. Since this interface only takes a pid, and in a real
 * cluster we would get that pid's utilization through an OS interface, we implement this by
 * writing utilization information for a pid in {@link DynoYARNContainerExecutor} and read it out
 * in this class, so we can report this back to the NM when the NM asks for this pid's utilization.
 */
public class DynoYARNBasedProcessTree extends ResourceCalculatorProcessTree {

  static final Log LOG = LogFactory.getLog(DynoYARNBasedProcessTree.class);

  private static final int MB_TO_BYTES = 1024 * 1024;

  private TreeSet<Utilization> sortedUtils = new TreeSet<>();
  private Utilization currentUtil = new Utilization(0, 0, 0, 0);
  private String pid;
  private long startTime;

  public DynoYARNBasedProcessTree(String pid) {
    // TODO need to report AM utilization too
    super(pid);
    this.pid = pid;
    this.startTime = System.currentTimeMillis();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(pid), StandardCharsets.UTF_8))) {
      Utilization[] utils = new ObjectMapper().readValue(reader.readLine(), Utilization[].class);
      for (Utilization util : utils) {
        sortedUtils.add(util);
      }
    } catch (IOException fnfe) {
      LOG.warn("PID file " + pid + " containing container utilization not found.", fnfe);
    }
  }

  @Override
  public void updateProcessTree() {
    long currentTime = System.currentTimeMillis();
    Utilization ret = new Utilization(currentTime - startTime, 0, 0, 0);
    Utilization candidate = sortedUtils.floor(ret);
    if (candidate != null) {
      currentUtil = candidate;
    }
    LOG.info("Updating process tree " + pid + " with utilization " + candidate + " at time " + (currentTime - startTime));
  }

  @Override
  public long getVirtualMemorySize() {
    // TODO: support reporting virtual memory
    return currentUtil.getMemoryMB() * MB_TO_BYTES;
  }

  @Override
  public long getRssMemorySize() {
    return currentUtil.getMemoryMB() * MB_TO_BYTES;
  }

  @Override
  public float getCpuUsagePercent() {
    return currentUtil.getCpus();
  }

  @Override
  public String getProcessTreeDump() {
    return null;
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    return true;
  }
}
