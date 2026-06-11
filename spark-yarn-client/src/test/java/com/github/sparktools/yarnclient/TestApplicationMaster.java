package com.github.sparktools.yarnclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;

/**
 * Minimal YARN ApplicationMaster used in integration tests instead of Spark's
 * {@code org.apache.spark.deploy.yarn.ApplicationMaster}.
 *
 * <p>It simply registers with the ResourceManager, immediately reports SUCCESS,
 * and exits — verifying that our {@link SparkYarnSubmitter} correctly constructs
 * the ApplicationSubmissionContext, localises resources, and sets up the
 * container environment.
 *
 * <p>Arguments (class, jar, properties-file) passed by {@link SparkYarnSubmitter}
 * are accepted but ignored, because no actual Spark driver is needed here.
 *
 * <p>This class is compiled into {@code target/test-classes/} and made available
 * to YARN containers via the test JVM classpath (see the integration test setup).
 */
public class TestApplicationMaster {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();
        try {
            // Register with the RM — required before any heartbeat
            rmClient.registerApplicationMaster("", 0, "");
            // Report success immediately; no containers are requested
            rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "ok", "");
        } finally {
            rmClient.stop();
        }
    }
}
