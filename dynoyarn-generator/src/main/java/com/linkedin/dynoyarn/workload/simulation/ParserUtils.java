/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.simulation;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.map.ObjectMapper;


public final class ParserUtils {

  public static final Log LOG = LogFactory.getLog(ParserUtils.class);

  private ParserUtils() { }

  public static List<AppSpec> parseWorkloadFile(String filename) throws IOException {
    LOG.info("Parsing workload file (this may take a while): " + filename);
    List<AppSpec> appSpecs = new ArrayList<>();
    try (
        FileInputStream inputStream = new FileInputStream(filename);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    ) {
      String appSpecStr;
      while ((appSpecStr = br.readLine()) != null) {
        AppSpec appSpec = new ObjectMapper().readValue(appSpecStr, AppSpec.class);
        appSpecs.add(appSpec);
      }
    }
    LOG.info("Done parsing workload file");
    return appSpecs;
  }
}
