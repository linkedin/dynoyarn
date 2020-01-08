/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.workload.preprocessor;

import com.linkedin.dynoyarn.workload.simulation.AppSpec;
import org.apache.hadoop.conf.Configuration;


/**
 * Interface for preprocessing an app spec before it is sent to
 * {@link com.linkedin.dynoyarn.workload.simulation.SimulatedAppSubmitter}.
 */
public abstract class AppSpecPreprocessor {

  public abstract void init(Configuration conf);

  public abstract void start();

  public abstract void processAppSpec(AppSpec appSpec);
}
