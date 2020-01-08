/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn;

import com.linkedin.dynoyarn.common.Constants;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


public class TestDriverApplicationMaster {

  @BeforeMethod
  public void setup() {

  }

  @Test
  public void testConstructUserConfOverrides() throws Exception {
    Path path = new Path(getClass().getClassLoader().getResource(Constants.DYNOYARN_SITE_XML).toURI());
    String args = DriverApplicationMaster.constructUserConfOverrides(path);
    String[] argList = args.split(" ");
    Set<String> argSet = new HashSet<>(Arrays.asList(argList[1], argList[3], argList[5]));
    assertTrue(argSet.contains("name=value"));
    assertTrue(argSet.contains("nameWithEmptyValue="));
    assertTrue(argSet.contains("nameWithVariableValue=\\\\\\$variable"));
  }
}
