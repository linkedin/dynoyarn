/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.common;

/**
 * ClusterInfo contains metadata for the simulated resource manager so that
 * other components can discover and communicate with the resource manager.
 */
public class ClusterInfo {
  private String rmHost;
  private int rmPort;
  private int rmSchedulerPort;
  private int rmAdminPort;
  private int rmTrackerPort;

  public String getRmHost() {
    return rmHost;
  }

  public void setRmHost(String rmHost) {
    this.rmHost = rmHost;
  }

  public int getRmPort() {
    return rmPort;
  }

  public void setRmPort(int rmPort) {
    this.rmPort = rmPort;
  }

  public int getRmSchedulerPort() {
    return rmSchedulerPort;
  }

  public void setRmSchedulerPort(int rmSchedulerPort) {
    this.rmSchedulerPort = rmSchedulerPort;
  }

  public int getRmAdminPort() {
    return rmAdminPort;
  }

  public void setRmAdminPort(int rmAdminPort) {
    this.rmAdminPort = rmAdminPort;
  }

  public int getRmTrackerPort() {
    return rmTrackerPort;
  }

  public void setRmTrackerPort(int rmTrackerPort) {
    this.rmTrackerPort = rmTrackerPort;
  }

  public int getRmHttpPort() {
    return rmHttpPort;
  }

  public void setRmHttpPort(int rmHttpPort) {
    this.rmHttpPort = rmHttpPort;
  }

  private int rmHttpPort;

  @Override
  public String toString() {
    return "RmHost: " + rmHost
        + "\nRmHttpAddress: " + rmHttpPort
        + "\nRmSchedulerPort: " + rmSchedulerPort
        + "\nRmAdminPort: " + rmAdminPort
        + "\nRmTrackerPort: " + rmTrackerPort;
  }
}
