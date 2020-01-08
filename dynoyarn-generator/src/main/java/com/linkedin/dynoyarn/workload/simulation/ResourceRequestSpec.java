/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.simulation;

import java.util.Objects;


/**
 * ResourceRequestSpec contains details about a simulated application's resource requests.
 * See {@link AppSpec} for details.
 */
public class ResourceRequestSpec {
  private long delay;
  private int numInstances;
  private int priority;
  private long[] runtimes;
  private long averageRuntime;
  private RequestedResource resource;
  private Utilization[] utilizations;

  public long getDelay() {
    return this.delay;
  }

  public void setDelay(long delay) {
    this.delay = delay;
  }

  public int getNumInstances() {
    return this.numInstances;
  }

  public void setNumInstances(int numInstances) {
    this.numInstances = numInstances;
  }

  public int getPriority() {
    return this.priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public long[] getRuntimes() {
    return this.runtimes.clone();
  }

  public void setRuntimes(long[] runtimes) {
    this.runtimes = runtimes.clone();
  }

  public long getAverageRuntime() {
    return this.averageRuntime;
  }

  public void setAverageRuntime(long averageRuntime) {
    this.averageRuntime = averageRuntime;
  }


  public RequestedResource getResource() {
    return this.resource;
  }

  public void setResource(RequestedResource resource) {
    this.resource = resource;
  }

  public Utilization[] getUtilizations() {
    return this.utilizations;
  }

  public void setUtilizations(Utilization[] utilizations) {
    this.utilizations = utilizations;
  }

  static class RequestedResource {
    private long memoryMB;
    private int vcores;
    private int gpus;

    public long getMemoryMB() {
      return this.memoryMB;
    }

    public void setMemoryMB(long memoryMB) {
      this.memoryMB = memoryMB;
    }

    public int getVcores() {
      return this.vcores;
    }

    public void setVcores(int vcores) {
      this.vcores = vcores;
    }

    public long getGpus() {
      return this.gpus;
    }

    public void setGpus(int gpus) {
      this.gpus = gpus;
    }

    @Override
    public String toString() {
      return String.format("memoryMB: %d, vcores: %d, gpus: %d", memoryMB, vcores, gpus);
    }
  }

  /**
   * Physical resource utilization of a container. Time represents when this utilization should kick in
   * for this container (relative to the container's start time). We sort utilizations by time so that
   * each container can easily retrieve the latest utilization that is earlier than the current time (i.e. the floor).
   */
  static class Utilization implements Comparable<Utilization> {
    private long time;
    private long memoryMB;
    private int cpus;
    private int gpus;

    public Utilization() {

    }

    public Utilization(long time, long memoryMB, int cpus, int gpus) {
      this.time = time;
      this.memoryMB = memoryMB;
      this.cpus = cpus;
      this.gpus = gpus;
    }

    public long getTime() {
      return this.time;
    }

    public void setTime(long time) {
      this.time = time;
    }

    public long getMemoryMB() {
      return this.memoryMB;
    }

    public void setMemoryMB(long memoryMB) {
      this.memoryMB = memoryMB;
    }

    public long getCpus() {
      return this.cpus;
    }

    public void setCpus(int cpus) {
      this.cpus = cpus;
    }

    public long getGpus() {
      return this.gpus;
    }

    public void setGpus(int gpus) {
      this.gpus = gpus;
    }

    @Override
    public int compareTo(Utilization other) {
      return (int) (this.time - other.time);
    }

    @Override
    public int hashCode() {
      return Objects.hash(time);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Utilization)) {
        return false;
      }
      Utilization other = (Utilization) o;
      return this.getTime() == other.getTime();
    }

    @Override
    public String toString() {
      return String.format("time: %d, memoryMB: %d, cpus: %d, gpus: %d",
          time, memoryMB, cpus, gpus);
    }
  }

  @Override
  public String toString() {
    return String.format("delay: %d, numInstances: %d, averageRuntime: %d, requestedResource: %s", delay, numInstances, averageRuntime, resource.toString());
  }
}
