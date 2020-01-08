/**
 * Copyright 2020 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.dynoyarn.common;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.util.YarnClientUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;


public class Utils {
  private static final Log LOG = LogFactory.getLog(Utils.class);

  private Utils() { }

  /**
   * Computes the number of host containers needed, based on user's configured
   * number of simulated NMs and NMs per container.
   * @param conf Configuration object
   * @return Number of containers required
   */
  public static int getNumNMContainerRequests(Configuration conf) {
    int totalNMs = conf.getInt(DynoYARNConfigurationKeys.NUM_NMS, 1);
    int nodeManagersPerContainer = conf.getInt(DynoYARNConfigurationKeys.NMS_PER_CONTAINER, 1);
    return (totalNMs + nodeManagersPerContainer - 1) / nodeManagersPerContainer;
  }

  /**
   * Parses memory string into MB.
   * @param memory Memory as string, e.g. '2048m' or '2g'
   * @return Memory in MB
   */
  public static long parseMemoryString(String memory) {
    memory = memory.toLowerCase();
    int m = memory.indexOf('m');
    int g = memory.indexOf('g');
    if (-1 != m) {
      return Long.parseLong(memory.substring(0, m));
    }
    if (-1 != g) {
      return Long.parseLong(memory.substring(0, g)) * 1024;
    }
    throw new IllegalArgumentException("Unsupported memory string: " + memory
        + ", only 'm' and 'g' suffix supported e.g. '2048m' or '2g'.");
  }

  /**
   * Execute a shell command.
   * @param taskCommand the shell command to execute
   * @param timeout the timeout to stop running the shell command
   * @param env the environment for this shell command
   * @return the exit code of the shell command
   * @throws IOException
   * @throws InterruptedException
   */
  public static int executeShell(String taskCommand, long timeout, Map<String, String> env) throws IOException, InterruptedException {
    LOG.info("Executing command: " + taskCommand);
    String executablePath = taskCommand.trim().split(" ")[0];
    File executable = new File(executablePath);
    if (!executable.canExecute()) {
      executable.setExecutable(true);
    }

    ProcessBuilder taskProcessBuilder = new ProcessBuilder("bash", "-c", taskCommand);
    taskProcessBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
    taskProcessBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    if (env != null) {
      taskProcessBuilder.environment().putAll(env);
    }
    Process taskProcess = taskProcessBuilder.start();
    if (timeout > 0) {
      taskProcess.waitFor(timeout, TimeUnit.MILLISECONDS);
    } else {
      taskProcess.waitFor();
    }
    return taskProcess.exitValue();
  }

  /**
   * Poll a callable till it returns true or time out
   * @param func a function that returns a boolean
   * @param intervalSec the interval we poll (in seconds).
   * @param timeoutSec the timeout we will stop polling (in seconds).
   * @return if the func returned true before timing out.
   * @throws IllegalArgumentException if {@code interval} or {@code timeout} is negative
   */
  public static boolean poll(Callable<Boolean> func, int intervalSec, int timeoutSec) {
    Preconditions.checkArgument(intervalSec >= 0, "Interval must be non-negative.");
    Preconditions.checkArgument(timeoutSec >= 0, "Timeout must be non-negative.");

    int remainingTime = timeoutSec;
    try {
      while (timeoutSec == 0 || remainingTime >= 0) {
        if (func.call()) {
          LOG.debug("Poll function finished within " + timeoutSec + " seconds");
          return true;
        }
        Thread.sleep(intervalSec * 1000);
        remainingTime -= intervalSec;
      }
    } catch (Exception e) {
      LOG.error("Polled function threw exception.", e);
    }
    LOG.debug("Function didn't return true within " + timeoutSec + " seconds.");
    return false;
  }

  public static String getCurrentHostName() {
    return System.getenv(ApplicationConstants.Environment.NM_HOST.name());
  }

  /**
   * Uploads resource from local filesystem to HDFS, and adds it to list of resources to be localized.
   * @param conf
   * @param fs
   * @param localSrcPath Path on local filesystem to resource
   * @param resourceType
   * @param appResourcesPath Path on HDFS where resource will be uploaded
   * @param localResources List to add this resource to be localized
   * @return Path on HDFS to the uploaded file
   * @throws IOException
   */
  public static Path localizeLocalResource(Configuration conf, FileSystem fs, String localSrcPath, LocalResourceType resourceType,
      Path appResourcesPath, Map<String, LocalResource> localResources) throws IOException {
    URI srcURI;
    try {
      srcURI = new URI(localSrcPath);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    File srcFile = new File(srcURI.getSchemeSpecificPart());
    Path dst = new Path(appResourcesPath, srcFile.getName());

    try (OutputStream outputStream = fs.create(dst, true)) {
      if ("jar".equals(srcURI.getScheme())) {
        try (InputStream inputStream = new URL(localSrcPath).openStream()) {
          IOUtils.copyBytes(inputStream, outputStream, conf);
        }
      } else {
        try (InputStream inputStream = new FileInputStream(srcFile)) {
          IOUtils.copyBytes(inputStream, outputStream, conf);
        }
      }
    }
    fs.setPermission(dst, new FsPermission((short) 0770));
    FileStatus scFileStatus = fs.getFileStatus(dst);
    LocalResource scRsrc =
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(dst.toUri()),
            resourceType, LocalResourceVisibility.PRIVATE,
            scFileStatus.getLen(), scFileStatus.getModificationTime());
    localResources.put(srcFile.getName(), scRsrc);
    return dst;
  }

  /**
   * Monitor provided applications. Return when one of them completes.
   * @param yarnClient YARN client for querying the RM for application status.
   * @param appIds Application IDs to monitor.
   * @return whether the first application to complete was successful or not.
   * @throws YarnException
   * @throws IOException
   * @throws InterruptedException
   */
  public static boolean monitorApplication(YarnClient yarnClient, ApplicationId... appIds)
      throws YarnException, IOException, InterruptedException {
    while (true) {
      // Check app status every 1 second.
      Thread.sleep(1000);

      for (ApplicationId appId : appIds) {
        // Get application report for the appId we are interested in
        ApplicationReport report = yarnClient.getApplicationReport(appId);

        YarnApplicationState state = report.getYarnApplicationState();
        FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();

        if (YarnApplicationState.FINISHED == state || YarnApplicationState.FAILED == state
          || YarnApplicationState.KILLED == state) {
          LOG.info("Application " + appId.getId() + " finished with YarnState=" + state.toString()
            + ", DSFinalStatus=" + dsStatus.toString() + ", breaking monitoring loop.");
          return FinalApplicationStatus.SUCCEEDED == dsStatus;
        }
      }
    }
  }

  /**
   * Converts a {@link com.linkedin.dynoyarn.common.ContaineRequest} object to a
   * {@link org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest} object.
   * @param request {@link com.linkedin.dynoyarn.common.ContaineRequest} object
   * @return {@link org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest} object
   */
  public static AMRMClient.ContainerRequest setupContainerRequest(ContainerRequest request) {
    Resource capability = Resource.newInstance((int) request.getMemory(), request.getVcores());
    AMRMClient.ContainerRequest containerRequest = AMRMClient.ContainerRequest.newBuilder()
        .capability(capability)
        .priority(Priority.newInstance(request.getPriority()))
        .nodeLabelsExpression(request.getNodeLabel())
        .build();
    LOG.info("Requested container ask: " + containerRequest.toString());
    return containerRequest;
  }

  /**
   * Tell the provided {@link org.apache.hadoop.yarn.client.api.AMRMClient} to allocate containers.
   * @param amRMClient {@link org.apache.hadoop.yarn.client.api.AMRMClient} object.
   * @param request {@link com.linkedin.dynoyarn.common.ContaineRequest} object.
   */
  public static void scheduleTask(AMRMClientAsync amRMClient, ContainerRequest request) {
    AMRMClient.ContainerRequest containerAsk = setupContainerRequest(request);
    for (int i = 0; i < request.getNumInstances(); i++) {
      amRMClient.addContainerRequest(containerAsk);
    }
  }

  /**
   * Fetches required tokens, including RM/NN tokens.
   * @param conf Configuration object used to locate RM/NNs to fetch tokens from
   * @param yarnClient YarnClient invoked to fetch RM delegation token, if fetchYarnTokens is true
   * @param fetchYarnTokens Whether to fetch RM tokens
   * @return ByteBuffer containing tokens
   * @throws IOException
   * @throws URISyntaxException
   * @throws YarnException
   */
  public static ByteBuffer getTokens(Configuration conf, YarnClient yarnClient, boolean fetchYarnTokens)
      throws IOException, URISyntaxException, YarnException {
    Credentials cred = new Credentials();
    String fileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (fileLocation != null) {
      cred = Credentials.readTokenStorageFile(new File(fileLocation), conf);
    } else {
      // Tokens have not been pre-written. We need to grab the tokens ourselves.
      String tokenRenewer = YarnClientUtils.getRmPrincipal(conf);
      if (fetchYarnTokens) {
        final Token<?> rmToken = ConverterUtils.convertFromYarn(yarnClient.getRMDelegationToken(new Text(tokenRenewer)),
            conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_PORT));
        cred.addToken(rmToken.getService(), rmToken);
      }
      FileSystem fs = FileSystem.get(conf);
      final Token<?>[] fsToken = fs.addDelegationTokens(tokenRenewer, cred);
      if (fsToken == null) {
        throw new RuntimeException("Failed to get FS delegation token for default FS.");
      }
      String[] otherNamenodes = conf.getStrings(DynoYARNConfigurationKeys.OTHER_NAMENODES_TO_ACCESS);
      if (otherNamenodes != null) {
        for (String nnUri : otherNamenodes) {
          LOG.info("Adding token for other nodes.");
          String namenodeUri = nnUri.trim();
          FileSystem otherFS = FileSystem.get(new URI(namenodeUri), conf);
          final Token<?>[] otherFSToken = otherFS.addDelegationTokens(tokenRenewer, cred);
          if (otherFSToken == null) {
            throw new RuntimeException("Failed to get FS delegation token for configured "
                + "other namenode: " + namenodeUri);
          }
        }
      }
    }
    LOG.debug("Successfully fetched tokens.");
    DataOutputBuffer buffer = new DataOutputBuffer();
    cred.writeTokenStorageToStream(buffer);
    return ByteBuffer.wrap(buffer.getData(), 0, buffer.getLength());
  }

  /**
   * Return tokens to the AM, needed for launching non-AM containers.
   * @return Tokens for non-AM containers
   * @throws IOException
   */
  public static ByteBuffer setupAndGetContainerCredentials() throws IOException {
    Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
    DataOutputBuffer dob = new DataOutputBuffer();
    credentials.writeTokenStorageToStream(dob);
    Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
    while (iter.hasNext()) {
      Token<?> token = iter.next();
      LOG.info(token);
      if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
        iter.remove();
      }
    }
    ByteBuffer allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    String submitterUserName = System.getenv(ApplicationConstants.Environment.USER.name());
    UserGroupInformation submitterUgi = UserGroupInformation.createRemoteUser(submitterUserName);
    submitterUgi.addCredentials(credentials);
    return allTokens;
  }

  /**
   * Add a file on HDFS to list of resources to be localized.
   * @param fs
   * @param hdfsSrcPath Path on HDFS to resource to be localized
   * @param dstName Name of localized file that will be in container's local filesystem
   * @param resourceType
   * @param localResources List of resources to add this resource to
   * @throws IOException
   */
  public static void localizeHDFSResource(FileSystem fs, String hdfsSrcPath, String dstName, LocalResourceType resourceType,
      Map<String, LocalResource> localResources) throws IOException {
    Path src = new Path(hdfsSrcPath);
    FileStatus scFileStatus = fs.getFileStatus(src);
    LocalResource scRsrc =
        LocalResource.newInstance(
            ConverterUtils.getYarnUrlFromURI(src.toUri()),
            resourceType, LocalResourceVisibility.PRIVATE,
            scFileStatus.getLen(), scFileStatus.getModificationTime());
    localResources.put(dstName, scRsrc);
  }

  /**
   * Construct the path on HDFS for the provided application.
   * @param fs FileSystem object
   * @param appId ApplicationId used to construct the path
   * @return HDFS path used for this application's resources
   */
  public static Path constructAppResourcesPath(FileSystem fs, String appId) {
    return new Path(fs.getHomeDirectory(), Constants.DYARN_FOLDER + Path.SEPARATOR + appId);
  }

  /**
   * Returns appId of current process, based on CONTAINER_ID environment variable set on container launch.
   * @return Application ID
   */
  public static ApplicationId getApplicationId() {
    return ContainerId.fromString(System.getenv(ApplicationConstants.Environment.CONTAINER_ID.toString()))
        .getApplicationAttemptId().getApplicationId();
  }
}
