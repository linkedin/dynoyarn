/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.preprocessor;

import com.linkedin.dynoyarn.workload.simulation.AppSpec;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class TestQueueCapacityPreprocessor {

  QueueCapacityPreprocessor preprocessor;

  @BeforeMethod
  public void setup() throws Exception {
    YarnClient rmClient = setupYarnClient();
    preprocessor = new QueueCapacityPreprocessor();
    preprocessor.setYarnClient(rmClient);
    preprocessor.start();
  }

  // Sets up yarn client and its queue configurations.
  private YarnClient setupYarnClient() throws Exception {
    YarnClient rmClient = mock(YarnClient.class);
    doNothing().when(rmClient).start();
    // - root.a       40% capacity
    //   - root.a.a1  30% capacity
    //   - root.a.a2  70% capacity
    // - root.b       60% capacity
    QueueInfo a1 = mockQueueInfo("a1", Collections.emptyList(), 0.3f);
    QueueInfo a2 = mockQueueInfo("a2", Collections.emptyList(), 0.7f);
    QueueInfo a = mockQueueInfo("a", Arrays.asList(a1, a2), 0.4f);
    QueueInfo b = mockQueueInfo("b", Collections.emptyList(), 0.6f);
    when(rmClient.getAllQueues()).thenReturn(Arrays.asList(a1, a2, a, b));
    when(rmClient.getRootQueueInfos()).thenReturn(Arrays.asList(a, b));
    return rmClient;
  }

  private QueueInfo mockQueueInfo(String queueName, List<QueueInfo> childQueues,
      float queueCapacity) {
    QueueInfo queueInfo = mock(QueueInfo.class);
    when(queueInfo.getQueueName()).thenReturn(queueName);
    when(queueInfo.getChildQueues()).thenReturn(childQueues);
    when(queueInfo.getCapacity()).thenReturn(queueCapacity);
    return queueInfo;
  }

  @Test
  public void testQueueOverride() {
    Random random = mock(Random.class);
    preprocessor.setRandom(random);
    // root.a.a1 should have base 0 (any double between 0 and 0.12 should be routed to a1)
    verifyQueue(random, 0.0, "a1");
    verifyQueue(random, 0.1, "a1");
    // root.a.a2 should have base 0.4 * 0.3 = 0.12 (any double between 0.12 and 0.4 should be routed to a2)
    verifyQueue(random, 0.13, "a2");
    verifyQueue(random, 0.39, "a2");
    // root.b should have base 0.4 * 0.3 + 0.4 * 0.7 = 0.4 (any double at least 0.4 should be routed to b)
    verifyQueue(random, 0.5, "b");
    verifyQueue(random, 1.0, "b");
  }

  private void verifyQueue(Random random, double rand, String expectedQueueName) {
    AppSpec appSpec = new AppSpec();
    when(random.nextDouble()).thenReturn(rand);
    preprocessor.processAppSpec(appSpec);
    assertEquals(appSpec.getQueue(), expectedQueueName);
  }
}
