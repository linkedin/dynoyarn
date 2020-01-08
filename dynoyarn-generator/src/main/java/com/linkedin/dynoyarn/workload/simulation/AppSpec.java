/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.simulation;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;


/**
 * AppSpec contains information about a simulated application
 * (when to submit it, when/what the container requests should be,
 * what their utilizations are, etc.) The entire simulated workload
 * is passed as a user-provided workload-spec.json which contains a
 * line-separated list of app specs.
 * An example app spec:
 *     {
 *       "appId": "app_1234567_0001",
 *       "appType": "MAPREDUCE",
 *       "submitTime": 5000,
 *       "user": "metrics",
 *       "queue": "default",
 *       "nodeLabel": "label1",
 *       "applicationTags": [ "tag1", "tag2" ],
 *       "amResourceRequest": {
 *          "memoryMB": 4096,
 *          "vcores": 2
 *       },
 *       "resourceRequestSpecs": [
 *         {
 *           "delay": 5000,
 *           "priority": 1,
 *           // Either specify runtimes as a list of longs
 *           "runtimes": [5000, 15000, 10000],
 *           // or an average runtime with number of instances
 *           "numInstances": 3,
 *           "averageRuntime": 15000,
 *           // YARN requested resource
 *           "resource": {
 *             "memoryMB": 2048,
 *             "vcores": 2,
 *             "gpus": 1
 *           },
 *           "utilizations": [
 *             // Physical used resource of each container over time.
 *             // Here, each container uses 1.5GB/1 physical core from
 *             // time 0 - 5000ms, and 1GB/2 physical cores past 5000ms.
 *             {
 *               "memoryMB": 1536,
 *               "cpus": 1,
 *               "gpus": 0,
 *               "time": 0
 *             },
 *             {
 *               "memoryMB": 1024,
 *               "cpus": 2,
 *               "gpus": 0,
 *               "time": 5000
 *             }
 *           ]
 *         }
 *       ]
 *     }
 */
public class AppSpec implements Comparable<AppSpec> {
  private String appId;
  private String appType;
  private long submitTime;
  private String user;
  private String queue;
  private String nodeLabel;
  private List<String> applicationTags = new ArrayList<>();
  private ResourceRequestSpec.RequestedResource amResourceRequest;
  private List<ResourceRequestSpec> resourceRequestSpecs = new ArrayList<>();

  public String getAppId() {
    return this.appId;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  public String getAppType() {
    return this.appType;
  }

  public void setAppType(String appType) {
    this.appType = appType;
  }

  public long getSubmitTime() {
    return this.submitTime;
  }

  public void setSubmitTime(long submitTime) {
    this.submitTime = submitTime;
  }

  public String getUser() {
    return this.user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getQueue() {
    return this.queue;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public String getNodeLabel() {
    return this.nodeLabel;
  }

  public void setNodeLabel(String nodeLabel) {
    this.nodeLabel = nodeLabel;
  }

  public List<String> getApplicationTags() {
    return this.applicationTags;
  }

  public void setApplicationTags(List<String> applicationTags) {
    this.applicationTags = applicationTags;
  }

  public ResourceRequestSpec.RequestedResource getAmResourceRequest() {
    return this.amResourceRequest;
  }

  public void setAmResourceRequest(ResourceRequestSpec.RequestedResource resourceRequest) {
    this.amResourceRequest = resourceRequest;
  }

  public List<ResourceRequestSpec> getResourceRequestSpecs() {
    return this.resourceRequestSpecs;
  }

  public void setResourceRequestSpecs(List<ResourceRequestSpec> resourceRequestSpecs) {
    this.resourceRequestSpecs = resourceRequestSpecs;
  }

  @Override
  public String toString() {
    StringJoiner sj = new StringJoiner(",\n");
    for (ResourceRequestSpec rrSpec : resourceRequestSpecs) {
      sj.add(rrSpec.toString());
    }
    return String.format("appId: %s, submitTime: %d, user: %s, queue: %s, amResourceRequest: %s, resourceRequests: %s",
        appId, submitTime, user, queue, amResourceRequest.toString(), sj.toString());
  }

  @Override
  public int compareTo(AppSpec other) {
    if (this.getSubmitTime() < other.getSubmitTime()) {
      return -1;
    } else if (this.getSubmitTime() > other.getSubmitTime()) {
      return 1;
    } else {
      return this.appId.compareTo(other.appId);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(appId, submitTime);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AppSpec)) {
      return false;
    }
    AppSpec other = (AppSpec) o;
    if (this.getSubmitTime() != other.getSubmitTime()) {
      return false;
    }
    if (this.appId == null && other.appId == null) {
      return true;
    }
    if (this.appId == null || other.appId == null) {
      return false;
    }
    return this.appId.equals(other.appId);
  }
}
