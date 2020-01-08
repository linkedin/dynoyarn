/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.simulation;

import java.io.IOException;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class TestParserUtils {

  @Test
  public void testParseSimpleWorkload() throws IOException {
    List<AppSpec> appSpecs = ParserUtils.parseWorkloadFile(getClass().getClassLoader().getResource("simple-workload-spec.json").getFile());
    assertEquals(2, appSpecs.size());
    AppSpec appSpec1 = appSpecs.get(0);
    assertEquals("application_123_0", appSpec1.getAppId());
    assertEquals(0, appSpec1.getSubmitTime());
    assertEquals(4096, appSpec1.getAmResourceRequest().getMemoryMB());
    assertEquals(2, appSpec1.getAmResourceRequest().getVcores());
    assertEquals(1, appSpec1.getApplicationTags().size());
    assertEquals("testTag1", appSpec1.getApplicationTags().get(0));
    assertEquals(1, appSpec1.getResourceRequestSpecs().size());
    ResourceRequestSpec rrs1 = appSpec1.getResourceRequestSpecs().get(0);
    assertEquals(5000, rrs1.getAverageRuntime());
    assertEquals(400, rrs1.getNumInstances());
    assertEquals(1, rrs1.getPriority());
    assertEquals(2048, rrs1.getResource().getMemoryMB());
    assertEquals(1, rrs1.getResource().getVcores());
    assertEquals("test1", appSpec1.getUser());

    AppSpec appSpec2 = appSpecs.get(1);
    assertEquals("application_123_1", appSpec2.getAppId());
    assertEquals(10000, appSpec2.getSubmitTime());
    assertEquals(16384, appSpec2.getAmResourceRequest().getMemoryMB());
    assertEquals(2, appSpec2.getAmResourceRequest().getVcores());
    assertEquals(1, appSpec2.getApplicationTags().size());
    assertEquals("testTag2", appSpec2.getApplicationTags().get(0));
    assertEquals("default", appSpec2.getQueue());
    assertEquals("MAPREDUCE", appSpec2.getAppType());
    assertEquals("label", appSpec2.getNodeLabel());
    assertEquals(2, appSpec2.getResourceRequestSpecs().size());
    ResourceRequestSpec rrs2 = appSpec2.getResourceRequestSpecs().get(0);
    assertEquals(0, rrs2.getAverageRuntime());
    assertEquals(0, rrs2.getNumInstances());
    assertEquals(20, rrs2.getPriority());
    assertEquals(8192, rrs2.getResource().getMemoryMB());
    assertEquals(1, rrs2.getResource().getVcores());
    rrs2 = appSpec2.getResourceRequestSpecs().get(1);
    assertEquals(15000, rrs2.getAverageRuntime());
    assertEquals(17, rrs2.getNumInstances());
    assertEquals(10, rrs2.getPriority());
    assertEquals(2048, rrs2.getResource().getMemoryMB());
    assertEquals(1, rrs2.getResource().getVcores());
    assertEquals("test2", appSpec2.getUser());
  }
}
