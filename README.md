# DynoYARN
DynoYARN is a tool to spin up on-demand YARN clusters and run simulated YARN workloads for scale testing.
It can simulate 10,000 node YARN cluster performance on a 100 node Hadoop cluster.

DynoYARN was created to address the following:
1. Evaluate YARN features and Hadoop version upgrades on resource manager performance
2. Forecast resource manager performance on large YARN clusters

DynoYARN consists of a "driver" application and "workload" application. The driver is responsible for spinning up
the simulated YARN cluster. The driver assumes the resource manager uses capacity scheduler.
The workload is responsible for replaying a trace on the simulated cluster in real-time.

The driver and workload can be configured to spin up a cluster and replay workloads of arbitrary size, meaning
DynoYARN can simulate a wide range of scenarios, from replaying previous production performance issues, to
predicting resource manager performance of future clusters and workloads.

Both the driver and workload are implemented as YARN applications, so you need a functional Hadoop cluster
to run the simulation.

## Build
To build DynoYARN jars needed to run the simulation, run `./gradlew build` from the root directory.
The required jars are in `dynoyarn-driver/build/libs/dynoyarn-driver-*-all.jar` and
`dynoyarn-generator/build/libs/dynoyarn-generator-*-all.jar`.

## Run

DynoYARN simulations can be run through command line by manually running the driver and workload applications,
or by running it through Azkaban (which packages these applications into a single Azkaban job).

### Command Line

#### Prerequisites

On a machine with Hadoop access, add the following into a directory:
1. `dynoyarn-driver-*-all.jar` jar
2. `dynoyarn-generator-*-all.jar` jar
3. Create a `dynoyarn-site.xml` file. This contains properties which will be added to the simulated cluster daemons
   (resource manager and node managers). A base config is provided [here](dynoyarn-site.xml).
4. Create a `dynoyarn.xml` file. This contains properties which will be used for the simulation itself (e.g. number
   of node managers to spin up, resource capability of each node manager, etc). A base config is provided [here](dynoyarn.xml).

Next, you need a workload trace to replay (see [Workload Spec Format](#workload-spec-format)) for more info.
An example workload trace is provided [here](workload-example.json). Copy the workload trace to be replayed to HDFS:

    hdfs dfs -copyFromLocal workload-example.json /tmp/workload-example.json

It's useful to run the simulated resource manager on the same node across each simulation. Furthermore, we want
to ensure the resource manager is running in an isolated environment to accurately reproduce resource manager behavior.
To do this, configure `dynoyarn.resourcemanager.node-label` in `dynoyarn.xml` to `dyno` (or any label name you choose),
pick a node in your cluster where you want the simulated resource manager to run (e.g. `hostname:8041`), then run
`yarn rmadmin -addToClusterNodeLabels dyno; yarn rmadmin -replaceLabelsOnNode hostname:8041=dyno` so that the
simulated resource manager will run on `hostname:8041` for each simulation.

#### Running the simulation

1. To run the driver application, run from the directory:

    ```
    CLASSPATH=$(${HADOOP_HDFS_HOME}/bin/hadoop classpath --glob):./:./* java com.linkedin.dynoyarn.DriverClient -hadoop_binary_path /hdfs/path/to/hadoop.tarball.tar.gz -conf dynoyarn.xml -capacity_scheduler_conf /hdfs/path/to/capacity-scheduler.xml
    ```

  where the `hadoop_binary_path` argument contains the Hadoop binary and conf which the driver components (RM and NMs) will use (you can use the
  same tarball that you would use when configuring `mapreduce.application.framework.path` for MapReduce jobs),
  and the `capacity_scheduler_conf` argument contains the capacity scheduler configuration which the driver's RM will use.

  The driver application lifetime is controlled by `dynoyarn.driver.simulation-duration-ms`, after which the
  application (and simulated cluster) will terminate, and RM app summary and GC logs will be uploaded to HDFS
  (to `dynoyarn.driver.rm-log-output-path`).

2. To run the workload application, run from the directory:

    ```
    CLASSPATH=$(${HADOOP_HDFS_HOME}/bin/hadoop classpath --glob):./:./* java com.linkedin.dynoyarn.workload.WorkloadClient -workload_spec_location /tmp/workload-example.json -conf dynoyarn.xml -driver_app_id application_1615840027285_57002
    ```

  where `workload_spec_location` is the location on HDFS containing the trace to rerun,
  and `driver_app_id` is the YARN app id for the driver app submitted previously.

### Azkaban

To run a DynoYARN simulation via Azkaban, run `./gradlew build` from the root directory, and upload the resulting
zip to Azkaban at `dynoyarn-azkaban/build/distributions/dynoyarn-azkaban-*`.

## Workload Spec Format
The workload trace is in json format, one app per line. An example app:

    {
      "amResourceRequest": {
        "memoryMB": 2048,
        "vcores": 1
      },
      "appId": "application_1605737660848_3450869",
      "appType": "MAPREDUCE",
      "queue": "default",
      "user": "user2",
      "submitTime": 1607151674623,
      "resourceRequestSpecs": [
        {
          "runtimes": [13262, 41329],
          "numInstances": 2,
          "resource": {
            "memoryMB": 4096,
            "vcores": 1
          },
          "priority": 20
        },
        {
          "runtimes": [13292],
          "numInstances": 1,
          "resource": {
            "memoryMB": 8192,
            "vcores": 2
          },
          "priority": 10
        }
      ]
    }

This was taken from a `MAPREDUCE` app that ran on a production cluster, which ran with id `application_1605737660848_3450869` and
was submitted at `1607151674623`. When replaying this app, it will be submitted as user `user2`, to queue `default`. The AM will
run in a `<2GB, 1 vcore>` container; it will first request two `<4GB, 1 vcore>` containers with priority `20` that run for
about 13 and 41 seconds, respectively. Once both containers finish, the app will request a single `<8GB, 2 vcore>` container
with priority `10` that runs for about 13 seconds.

The apps in the trace are submitted to the simulated cluster in relative real-time; in the [example](workload-example.json),
the first app was submitted at `1607151674543` and marks the start of the simulation; the second app was submitted at `1607151674623`, and will be
submitted `1607151674623 - 1607151674543 = 80` milliseconds after the first app.

To generate a trace, you can combine production RM app summary logs with audit logs containing information
on when containers (e.g. mappers/reducers for MapReduce, or executors for Spark) for each application were requested.