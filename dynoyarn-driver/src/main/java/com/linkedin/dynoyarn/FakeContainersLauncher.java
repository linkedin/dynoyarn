/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;


/**
 * A containers launcher implementation which stores "launched" containers
 * in memory, without actually forking new processes (unless the container is
 * an AM).
 */
public class FakeContainersLauncher extends ContainersLauncher {

  private static final Log LOG = LogFactory.getLog(FakeContainersLauncher.class);

  public static final String CONTAINER_RUNTIME_ENV = "CONTAINER_RUNTIME_ENV";
  public static final String IS_AM = "IS_AM";

  private Context context;
  private Dispatcher dispatcher;
  private Clock clock;
  private ScheduledExecutorService ses;
  private ScheduledFuture<?> handler;
  private NodeId nodeId;
  protected TreeSet<FakeContainer> runningContainers;
  protected ContainerCompletionChecker containerCompletionChecker;

  static class FakeContainer implements Comparable<FakeContainer> {
    ContainerId containerId;
    long finishTime;

    FakeContainer(long finishTime) {
      this(null, finishTime);
    }

    FakeContainer(ContainerId containerId, long finishTime) {
      this.containerId = containerId;
      this.finishTime = finishTime;
    }

    public ContainerId getContainerId() {
      return containerId;
    }

    public long getFinishTime() {
      return finishTime;
    }

    @Override
    public int compareTo(FakeContainer other) {
      // Maintain global ordering based on finishTime and containerId,
      // otherwise containers with the same finishTime will get lost in the TreeSet
      // (and the corresponding application will hang since this container never finishes)
      if (this.finishTime == other.finishTime) {
        if (this.containerId == null) {
          return 1;
        } else if (other.containerId == null) {
          return -1;
        } else {
          return this.containerId.compareTo(other.containerId);
        }
      } else {
        return (int) (this.finishTime - other.finishTime);
      }
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof FakeContainer)) {
        return false;
      }
      return this.containerId.equals(((FakeContainer) other).containerId);
    }

    @Override
    public int hashCode() {
      return this.containerId.hashCode();
    }
  }

  public FakeContainersLauncher() {
    super();
  }

  @Override
  public void init(Context nmContext, Dispatcher nmDispatcher,
      ContainerExecutor containerExec, LocalDirsHandlerService nmDirsHandler,
      ContainerManagerImpl nmContainerManager) {
    super.init(nmContext, nmDispatcher, containerExec, nmDirsHandler,
        nmContainerManager);
    this.context = nmContext;
    this.dispatcher = nmDispatcher;
    this.clock = new SystemClock();
    this.runningContainers = new TreeSet<>();
    this.containerCompletionChecker = new ContainerCompletionChecker();
    this.ses = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(getName());
        return t;
      }
    });
  }

  @Override
  protected void serviceStart() {
    long heartbeatInterval = getConfig().getLong(
        YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
        YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    handler = ses.scheduleAtFixedRate(containerCompletionChecker,
        0, heartbeatInterval, TimeUnit.MILLISECONDS);
    nodeId = context.getNodeId();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (handler != null) {
      LOG.info("Stop " + getName());
      handler.cancel(true);
      ses.shutdown();
    }
    super.serviceStop();
  }

  @Override
  public void handle(ContainersLauncherEvent event) {
    Container container = event.getContainer();
    ContainerId containerId = container.getContainerId();
    ContainerLaunchContext launchContext = container.getLaunchContext();
    if (Boolean.parseBoolean(launchContext.getEnvironment().get(IS_AM))) {
      // Launch AMs through normal ContainersLauncher.
      launchContainer(event);
      return;
    }
    switch (event.getType()) {
      case LAUNCH_CONTAINER:
        long containerRuntime = Long.parseLong(launchContext.getEnvironment()
            .get(CONTAINER_RUNTIME_ENV));
        long finishTime = clock.getTime() + containerRuntime;
        synchronized (runningContainers) {
          runningContainers.add(new FakeContainer(containerId, finishTime));
        }
        LOG.info("Launching fake container with id " + containerId
            + ", runtime " + containerRuntime + " and finish time " + finishTime
            + " on node " + nodeId + ", numRunningContainers=" + runningContainers.size()
            + " with head " + runningContainers.first().containerId + " and finish time "
            + runningContainers.first().finishTime);
        dispatcher.getEventHandler().handle(new ContainerEvent(
            containerId, ContainerEventType.CONTAINER_LAUNCHED));
        break;
      case RECOVER_CONTAINER:
      case CLEANUP_CONTAINER:
      default:
        break;
    }
  }

  /**
   * Thread which monitors which containers have passed their elapsed
   * runtime and removes them from memory.
   */
  class ContainerCompletionChecker implements Runnable {
    @Override
    public void run() {
      try {
        synchronized (runningContainers) {
          long currentTime = clock.getTime();
          LOG.info("Checking for completed containers on host " + nodeId
              + ", time " + currentTime);
          Collection<FakeContainer> finishedContainers =
              new TreeSet<>(runningContainers.headSet(new FakeContainer(currentTime)));
          for (FakeContainer finishedContainer : finishedContainers) {
            LOG.info("Finishing container " + finishedContainer.containerId
                + " on host " + nodeId);
            dispatcher.getEventHandler().handle(new ContainerEvent(
                finishedContainer.containerId,
                ContainerEventType.CONTAINER_EXITED_WITH_SUCCESS));
          }
          runningContainers.removeAll(finishedContainers);
          if (!runningContainers.isEmpty()) {
            FakeContainer c = runningContainers.first();
            LOG.info("New head on host " + nodeId + ": " + c.containerId
                + " with finish time " + c.finishTime);
          }
          LOG.info("Finished checking for completed containers on host " + nodeId
              + ", time " + clock.getTime());
        }
      } catch (Exception e) {
        LOG.error("Caught exception while checking for container completion.", e);
      }
    }
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }

  @VisibleForTesting
  public void launchContainer(ContainersLauncherEvent event) {
    super.handle(event);
  }
}
