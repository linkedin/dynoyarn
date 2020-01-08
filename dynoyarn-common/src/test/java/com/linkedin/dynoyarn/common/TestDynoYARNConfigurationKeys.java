/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.common;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TestDynoYARNConfigurationKeys {

  private Configuration conf;

  @BeforeMethod
  public void setup() {
    conf = new Configuration();
  }

  @Test
  public void testDriverPartitions() {
    conf.setInt(DynoYARNConfigurationKeys.DRIVER_PREFIX + "partition.label1.count", 100);
    conf.setInt(DynoYARNConfigurationKeys.DRIVER_PREFIX + "partition.label2.count", 200);
    Map<String, Integer> partitionCounts = DynoYARNConfigurationKeys.getConfiguredDriverPartitions(conf);
    assertEquals(partitionCounts.size(), 2);
    assertEquals(partitionCounts.get("label1").intValue(), 100);
    assertEquals(partitionCounts.get("label2").intValue(), 200);
  }
}
