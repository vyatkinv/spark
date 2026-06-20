package com.github.sparktools.yarnclient;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Validates that a fat JAR built by maven-shade-plugin contains all required
 * runtime classes. Catches a common problem where Spark jars installed into
 * the local Maven repo via {@code maven-install-plugin} (from a downloaded
 * distribution tarball) lack transitive dependency metadata — causing shade
 * to silently omit critical libraries like spark-network, scala-library,
 * log4j, jackson, etc.
 *
 * <p>Disabled by default. Activate by setting {@code FAT_JAR_URI}:
 * <pre>
 * mvn test -pl spark-yarn-client \
 *     -Dtest=FatJarContentsTest \
 *     -DFAT_JAR_URI=file:///path/to/my-app-all.jar
 * </pre>
 *
 * <p>Accepts any URI scheme: {@code file:}, {@code hdfs:}, {@code http:},
 * or a bare local path as fallback.
 */
class FatJarContentsTest {

    private static Set<String> jarEntries;

    @BeforeAll
    static void loadJarEntries() throws Exception {
        String raw = System.getProperty("FAT_JAR_URI", System.getenv("FAT_JAR_URI"));
        assumeTrue(raw != null && !raw.isEmpty(),
                "Skipped: FAT_JAR_URI not set");

        URI uri;
        if (raw.contains("://") || raw.startsWith("file:")) {
            uri = new URI(raw);
        } else {
            uri = new java.io.File(raw).toURI();
        }

        jarEntries = new HashSet<>();

        String scheme = uri.getScheme();
        if (scheme == null || scheme.equals("file")) {
            try (JarFile jf = new JarFile(new java.io.File(uri))) {
                Enumeration<JarEntry> e = jf.entries();
                while (e.hasMoreElements()) {
                    jarEntries.add(e.nextElement().getName());
                }
            }
        } else {
            try (InputStream is = uri.toURL().openStream();
                 JarInputStream jis = new JarInputStream(is)) {
                ZipEntry ze;
                while ((ze = jis.getNextEntry()) != null) {
                    jarEntries.add(ze.getName());
                }
            }
        }

        System.out.println("Fat JAR: " + uri + " (" + jarEntries.size() + " entries)");
    }

    // ── Spark core ──────────────────────────────────────────────────────────

    @Test void sparkContext()      { assertClass("org.apache.spark.SparkContext"); }
    @Test void sparkConf()         { assertClass("org.apache.spark.SparkConf"); }
    @Test void sparkEnv()          { assertClass("org.apache.spark.SparkEnv"); }
    @Test void rdd()               { assertClass("org.apache.spark.rdd.RDD"); }
    @Test void taskContext()       { assertClass("org.apache.spark.TaskContext"); }
    @Test void serializerManager() { assertClass("org.apache.spark.serializer.SerializerManager"); }

    // ── Spark YARN ──────────────────────────────────────────────────────────

    @Test void applicationMaster() { assertClass("org.apache.spark.deploy.yarn.ApplicationMaster"); }
    @Test void executorBackend()   { assertClass("org.apache.spark.executor.YarnCoarseGrainedExecutorBackend"); }
    @Test void yarnAllocator()     { assertClass("org.apache.spark.deploy.yarn.YarnAllocator"); }

    // ── Spark network (often missing with manual installs) ──────────────────

    @Test void transportContext()  { assertClass("org.apache.spark.network.TransportContext"); }
    @Test void transportServer()   { assertClass("org.apache.spark.network.server.TransportServer"); }
    @Test void shuffleClient()     { assertClass("org.apache.spark.network.shuffle.ExternalBlockStoreClient"); }

    // ── Spark unsafe / common-utils ─────────────────────────────────────────

    @Test void platform()          { assertClass("org.apache.spark.unsafe.Platform"); }
    @Test void sparkCommonUtils()  { assertClass("org.apache.spark.util.SparkClassUtils"); }

    // ── Scala runtime ───────────────────────────────────────────────────────

    @Test void scalaLibrary()      { assertClass("scala.Predef"); }
    @Test void scalaReflect()      { assertClass("scala.reflect.ClassTag"); }
    @Test void scalaCollection()   { assertClass("scala.collection.immutable.List"); }

    // ── Logging (log4j2) ────────────────────────────────────────────────────

    @Test void log4jApi()          { assertClass("org.apache.logging.log4j.LogManager"); }
    @Test void log4jCore()         { assertClass("org.apache.logging.log4j.core.LoggerContext"); }
    @Test void slf4jApi()          { assertClass("org.slf4j.LoggerFactory"); }

    // ── Serialization (kryo / chill) ────────────────────────────────────────

    @Test void kryo()              { assertClass("com.esotericsoftware.kryo.Kryo"); }
    @Test void chill()             { assertClass("com.twitter.chill.KryoBase"); }

    // ── Jackson ─────────────────────────────────────────────────────────────

    @Test void jacksonDatabind()   { assertClass("com.fasterxml.jackson.databind.ObjectMapper"); }
    @Test void jacksonCore()       { assertClass("com.fasterxml.jackson.core.JsonFactory"); }
    @Test void jacksonScala()      { assertClass("com.fasterxml.jackson.module.scala.DefaultScalaModule"); }

    // ── Netty ───────────────────────────────────────────────────────────────

    @Test void nettyTransport()    { assertClass("io.netty.channel.Channel"); }
    @Test void nettyBuffer()       { assertClass("io.netty.buffer.ByteBuf"); }

    // ── Hadoop client ───────────────────────────────────────────────────────

    @Test void hadoopConfiguration() { assertClass("org.apache.hadoop.conf.Configuration"); }
    @Test void hadoopFileSystem()    { assertClass("org.apache.hadoop.fs.FileSystem"); }
    @Test void hadoopYarnClient()    { assertClass("org.apache.hadoop.yarn.client.api.YarnClient"); }

    // ── Commonly dropped transitive deps ────────────────────────────────────

    @Test void json4s()            { assertClass("org.json4s.jackson.JsonMethods$"); }
    @Test void jerseyServer()      { assertClass("org.glassfish.jersey.server.ResourceConfig"); }
    @Test void commonsCollections4() { assertClass("org.apache.commons.collections4.MapUtils"); }
    @Test void metricsCore()       { assertClass("com.codahale.metrics.MetricRegistry"); }
    @Test void avro()              { assertClass("org.apache.avro.Schema"); }

    // ── SPI / META-INF/services (shade must merge, not drop) ────────────────

    @Test
    void spiFileSystemProviders() {
        assertTrue(jarEntries.contains("META-INF/services/org.apache.hadoop.fs.FileSystem"),
                "META-INF/services/org.apache.hadoop.fs.FileSystem must be present "
                + "(ServicesResourceTransformer must merge SPI files)");
    }

    // ── Helper ──────────────────────────────────────────────────────────────

    private void assertClass(String fqcn) {
        String path = fqcn.replace('.', '/') + ".class";
        assertTrue(jarEntries.contains(path),
                "Fat JAR must contain " + fqcn + " (looked for " + path + ")");
    }
}
