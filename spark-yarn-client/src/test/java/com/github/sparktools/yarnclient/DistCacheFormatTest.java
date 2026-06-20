package com.github.sparktools.yarnclient;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for distributed cache property format — no Docker or HDFS needed.
 */
class DistCacheFormatTest {

    @Test
    void buildDistCacheProperties_emptyEntries() throws Exception {
        SparkYarnSubmitter submitter = createSubmitter();
        Properties props = submitter.buildDistCacheProperties(Collections.emptyList());
        assertTrue(props.isEmpty());
    }

    @Test
    void buildDistCacheProperties_singleAppJar() throws Exception {
        SparkYarnSubmitter submitter = createSubmitter();
        URI uri = new URI("hdfs", "namenode", "/staging/__app__.jar", null, null);

        List<SparkYarnSubmitter.DistCacheEntry> entries = Collections.singletonList(
                new SparkYarnSubmitter.DistCacheEntry(
                        uri, "__app__.jar", 1024L, 1000000L,
                        LocalResourceType.FILE));

        Properties props = submitter.buildDistCacheProperties(entries);

        String filenames = props.getProperty("spark.yarn.cache.filenames");
        assertNotNull(filenames, "filenames must be set");
        assertTrue(filenames.contains("#__app__.jar"),
                "URI must have link name as fragment: " + filenames);
        assertFalse(filenames.contains(","),
                "single entry must not have comma: " + filenames);

        assertEquals("1024", props.getProperty("spark.yarn.cache.sizes"));
        assertEquals("1000000", props.getProperty("spark.yarn.cache.timestamps"));
        assertEquals("PRIVATE", props.getProperty("spark.yarn.cache.visibilities"));
        assertEquals("FILE", props.getProperty("spark.yarn.cache.types"));
    }

    @Test
    void buildDistCacheProperties_multipleEntries_commaSeparated() throws Exception {
        SparkYarnSubmitter submitter = createSubmitter();

        URI appUri = new URI("hdfs", "nn", "/staging/__app__.jar", null, null);
        URI dataUri = new URI("hdfs", "nn", "/staging/data.csv", null, null);
        URI archUri = new URI("hdfs", "nn", "/staging/libs.zip", null, null);

        List<SparkYarnSubmitter.DistCacheEntry> entries = Arrays.asList(
                new SparkYarnSubmitter.DistCacheEntry(
                        appUri, "__app__.jar", 1024, 100L, LocalResourceType.FILE),
                new SparkYarnSubmitter.DistCacheEntry(
                        dataUri, "data.csv", 2048, 200L, LocalResourceType.FILE),
                new SparkYarnSubmitter.DistCacheEntry(
                        archUri, "__spark_libs__", 4096, 300L, LocalResourceType.ARCHIVE)
        );

        Properties props = submitter.buildDistCacheProperties(entries);

        String filenames = props.getProperty("spark.yarn.cache.filenames");
        assertNotNull(filenames);
        String[] fParts = filenames.split(",");
        assertEquals(3, fParts.length, "must have 3 comma-separated entries: " + filenames);
        assertTrue(fParts[0].contains("#__app__.jar"), "first entry: " + fParts[0]);
        assertTrue(fParts[1].contains("#data.csv"), "second entry: " + fParts[1]);
        assertTrue(fParts[2].contains("#__spark_libs__"), "third entry: " + fParts[2]);

        assertEquals("1024,2048,4096", props.getProperty("spark.yarn.cache.sizes"));
        assertEquals("100,200,300", props.getProperty("spark.yarn.cache.timestamps"));
        assertEquals("PRIVATE,PRIVATE,PRIVATE", props.getProperty("spark.yarn.cache.visibilities"));
        assertEquals("FILE,FILE,ARCHIVE", props.getProperty("spark.yarn.cache.types"));
    }

    @Test
    void buildDistCacheProperties_noIndexedKeys() throws Exception {
        SparkYarnSubmitter submitter = createSubmitter();

        URI uri = new URI("hdfs", "nn", "/staging/file.jar", null, null);
        List<SparkYarnSubmitter.DistCacheEntry> entries = Collections.singletonList(
                new SparkYarnSubmitter.DistCacheEntry(
                        uri, "file.jar", 100, 200L, LocalResourceType.FILE));

        Properties props = submitter.buildDistCacheProperties(entries);

        assertNull(props.getProperty("spark.yarn.cache.filenames.0"),
                "must NOT use indexed format");
        assertNull(props.getProperty("spark.yarn.cache.types.0"),
                "must NOT use indexed format");
        assertNull(props.getProperty("spark.yarn.cache.size"),
                "must NOT use old 'size' key");
    }

    private static SparkYarnSubmitter createSubmitter() {
        SparkYarnConfig config = SparkYarnConfig.builder().build();
        return new SparkYarnSubmitter(config, null, null);
    }
}
