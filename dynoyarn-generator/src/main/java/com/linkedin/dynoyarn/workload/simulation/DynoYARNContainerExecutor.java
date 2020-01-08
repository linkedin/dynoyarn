/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.simulation;

import com.linkedin.dynoyarn.common.Constants;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerLivenessContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerSignalContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.apache.hadoop.yarn.server.nodemanager.executor.DeletionAsUserContext;


public class DynoYARNContainerExecutor extends DefaultContainerExecutor {

  private static final Log LOG = LogFactory.getLog(DynoYARNContainerExecutor.class);

  private Map<ContainerId, String> cIdToPID = new HashMap<>();
  private Map<ContainerId, Boolean> cIdToExecutor = new HashMap<>();
  // Start with a high number to avoid collisions with AM
  private long lastPID = 123456;

  @Override
  public int launchContainer(ContainerStartContext ctx) throws IOException, ConfigurationException {
    Container container = ctx.getContainer();
    Map<String, String> containerEnv = container.getLaunchContext().getEnvironment();
    ContainerId cId = container.getContainerId();
    cIdToExecutor.put(cId, containerEnv.containsKey(Constants.IS_AM));
    LOG.debug("Starting container " + cId + " with pid " + lastPID + " with utilization "
        + containerEnv.get(Constants.CONTAINER_UTILIZATION));
    if (!containerEnv.containsKey(Constants.IS_AM)) {
      try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(String.valueOf(lastPID)),
          StandardCharsets.UTF_8))) {
        writer.write(containerEnv.get(Constants.CONTAINER_UTILIZATION));
      }
    }
    long pid = lastPID;
    cIdToPID.put(cId, String.valueOf(lastPID++));
    int exitCode = super.launchContainer(ctx);
    cIdToPID.remove(cId);
    cIdToExecutor.remove(cId);
    boolean success = new File(String.valueOf(pid)).delete();
    //TODO: support failed containers
    return exitCode;
  }

  @Override
  public String getProcessId(ContainerId containerId) {
    if (!cIdToExecutor.containsKey(containerId) || cIdToExecutor.get(containerId)) {
      return super.getProcessId(containerId);
    }
    return cIdToPID.get(containerId);
  }

  @Override
  public boolean signalContainer(ContainerSignalContext ctx) throws IOException {
    //TODO: terminate early if signal sent
    return true;
  }

  @Override
  public void deleteAsUser(DeletionAsUserContext ctx) throws IOException, InterruptedException {

  }

  @Override
  public boolean isContainerAlive(ContainerLivenessContext ctx) throws IOException {
    return false;
  }
}
