/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.util.Clock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class TestFakeContainersLauncher {

  FakeContainersLauncher launcher;

  @BeforeMethod
  public void setup() {
    Dispatcher dispatcher = mock(Dispatcher.class);
    EventHandler eventHandler = mock(EventHandler.class);
    doNothing().when(eventHandler).handle(any(ContainerEvent.class));
    when(dispatcher.getEventHandler()).thenReturn(eventHandler);
    launcher = new FakeContainersLauncher() {
      @Override
      public void launchContainer(ContainersLauncherEvent event) {
        // Do nothing.
      }
    };
    launcher.init(null, dispatcher, null, null, null);
  }

  @Test
  public void testLaunchAM() {
    // Launching AM should go through normal container launch path
    Container container = setupContainer(0, 0, 0, 0);
    container.getLaunchContext().getEnvironment().put(FakeContainersLauncher.IS_AM, "true");
    ContainersLauncherEvent event = new ContainersLauncherEvent(container,
        ContainersLauncherEventType.LAUNCH_CONTAINER);
    launcher.handle(event);
    assertTrue(launcher.runningContainers.isEmpty());
  }

  @Test
  public void testLaunchFakeContainers() {
    Clock clock = mock(Clock.class);
    when(clock.getTime()).thenReturn(0L);
    launcher.setClock(clock);

    // container1 finishes at time 5sec
    Container container1 = setupContainer(0, 0, 0, 0);
    container1.getLaunchContext().getEnvironment().put(FakeContainersLauncher.CONTAINER_RUNTIME_ENV, "5000");
    ContainersLauncherEvent event1 = new ContainersLauncherEvent(container1,
        ContainersLauncherEventType.LAUNCH_CONTAINER);
    launcher.handle(event1);

    // container2 finishes at time 10sec
    when(clock.getTime()).thenReturn(7000L);
    Container container2 = setupContainer(0, 0, 0, 1);
    container2.getLaunchContext().getEnvironment().put(FakeContainersLauncher.CONTAINER_RUNTIME_ENV, "3000");
    ContainersLauncherEvent event2 = new ContainersLauncherEvent(container2,
        ContainersLauncherEventType.LAUNCH_CONTAINER);
    launcher.handle(event2);

    // container3 finishes at time 20sec
    when(clock.getTime()).thenReturn(15000L);
    Container container3 = setupContainer(0, 1, 0, 0);
    container3.getLaunchContext().getEnvironment().put(FakeContainersLauncher.CONTAINER_RUNTIME_ENV, "5000");
    ContainersLauncherEvent event3 = new ContainersLauncherEvent(container3,
        ContainersLauncherEventType.LAUNCH_CONTAINER);
    launcher.handle(event3);

    assertEquals(3, launcher.runningContainers.size());

    // Clock set to 15sec. container1 and container2 should be finished
    launcher.containerCompletionChecker.run();
    assertEquals(1, launcher.runningContainers.size());
    assertEquals(container3.getContainerId(), launcher.runningContainers.first().getContainerId());
  }

  @Test
  public void testSameFinishTime() {
    Clock clock = mock(Clock.class);
    when(clock.getTime()).thenReturn(0L);
    launcher.setClock(clock);

    // container1 and container2 finish at time 5sec
    Container container1 = setupContainer(0, 0, 0, 0);
    container1.getLaunchContext().getEnvironment().put(FakeContainersLauncher.CONTAINER_RUNTIME_ENV, "5000");
    ContainersLauncherEvent event1 = new ContainersLauncherEvent(container1,
        ContainersLauncherEventType.LAUNCH_CONTAINER);
    Container container2 = setupContainer(0, 0, 0, 1);
    container2.getLaunchContext().getEnvironment().put(FakeContainersLauncher.CONTAINER_RUNTIME_ENV, "5000");
    ContainersLauncherEvent event2 = new ContainersLauncherEvent(container2,
        ContainersLauncherEventType.LAUNCH_CONTAINER);
    launcher.handle(event1);
    launcher.handle(event2);

    assertEquals(2, launcher.runningContainers.size());
    assertEquals(container1.getContainerId(), launcher.runningContainers.first().getContainerId());
    assertEquals(container2.getContainerId(), launcher.runningContainers.last().getContainerId());

    // Clock set to 10sec. container1 and container2 should be finished
    when(clock.getTime()).thenReturn(10000L);
    launcher.containerCompletionChecker.run();
    assertEquals(0, launcher.runningContainers.size());
  }

  private Container setupContainer(long clusterTimestamp, int appId, int appAttemptId, long containerId) {
    Container container = mock(Container.class);
    ContainerLaunchContext launchContext = mock(ContainerLaunchContext.class);
    when(container.getLaunchContext()).thenReturn(launchContext);
    Map<String, String> launchContextEnv = new HashMap<>();
    when(launchContext.getEnvironment()).thenReturn(launchContextEnv);
    ApplicationId app = ApplicationId.newInstance(clusterTimestamp, appId);
    ApplicationAttemptId appAttempt = ApplicationAttemptId.newInstance(app, appAttemptId);
    when(container.getContainerId()).thenReturn(ContainerId.newContainerId(appAttempt, containerId));
    return container;
  }
}
