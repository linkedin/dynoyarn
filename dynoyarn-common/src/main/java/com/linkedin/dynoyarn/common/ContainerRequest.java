/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.common;

/**
 * ContainerRequest encapsulates information needed for DynoYARN to request
 * containers from the host YARN cluster.
 */
public class ContainerRequest {

  public int getNumInstances() {
    return numInstances;
  }

  public long getMemory() {
    return memory;
  }

  public int getVcores() {
    return vCores;
  }

  public int getPriority() {
    return priority;
  }

  public String getNodeLabel() {
    return nodeLabel;
  }

  private int numInstances;
  private long memory;
  private int vCores;
  private int priority;
  private String nodeLabel;

  public ContainerRequest(int numInstances, long memory, int vCores, int priority) {
    this(numInstances, memory, vCores, priority, null);
  }

  public ContainerRequest(int numInstances, long memory, int vCores, int priority, String nodeLabel) {
    this.numInstances = numInstances;
    this.memory = memory;
    this.vCores = vCores;
    this.priority = priority;
    this.nodeLabel = nodeLabel;
  }
}
