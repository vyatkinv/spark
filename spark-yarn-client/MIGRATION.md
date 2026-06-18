# Migration from SparkLauncher to SparkYarnClient

## Why migrate

| | `SparkLauncher` | `SparkYarnClient` |
|---|---|---|
| Requires `spark-submit` on host | yes | **no** |
| Requires Spark installation on host | yes | **no** |
| Submission mechanism | spawns `spark-submit` process | direct YARN API call |
| Status monitoring | poll external process | in-process YARN client |
| Dependency footprint | `spark-launcher` + full Spark distribution | `spark-yarn-client` + Hadoop client JARs |
| Fat JAR mode (no Spark jars on HDFS) | not supported | **supported** |

---

## Step 1: Dependencies

### Submitter application (the service that submits Spark jobs)

Replace `spark-launcher` with `spark-yarn-client`. Hadoop client JARs are needed
for HDFS and YARN communication.

**Maven (pom.xml of the submitter):**

```xml
<!-- Remove this -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-launcher_2.12</artifactId>
</dependency>

<!-- Add this -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-yarn-client_2.12</artifactId>
    <version>3.5.9-SNAPSHOT</version>
</dependency>
```

`spark-yarn-client` transitively pulls `hadoop-common`, `hadoop-hdfs-client`, and
`hadoop-yarn-client`. If your project already has Hadoop JARs at a compatible version,
exclude duplicates as needed.

### Spark application JAR (the job that runs on YARN)

Two modes are supported — pick one:

#### Mode A: Spark jars pre-staged on HDFS (recommended for production)

Your application JAR should be a **thin fat JAR** — your code plus your own
dependencies, but **not** Spark/Hadoop (they come from HDFS):

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.5.1</version>
    <executions>
        <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
                <transformers>
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                        <mainClass>com.example.MySparkApp</mainClass>
                    </transformer>
                    <!-- Merge META-INF/services for Hadoop FileSystem providers etc. -->
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                </transformers>
                <filters>
                    <filter>
                        <artifact>*:*</artifact>
                        <excludes>
                            <exclude>META-INF/*.SF</exclude>
                            <exclude>META-INF/*.DSA</exclude>
                            <exclude>META-INF/*.RSA</exclude>
                        </excludes>
                    </filter>
                </filters>
            </configuration>
        </execution>
    </executions>
</plugin>
```

Mark Spark and Hadoop as `provided` so they don't bloat the JAR:

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.12</artifactId>
    <version>3.5.1</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.5.1</version>
    <scope>provided</scope>
</dependency>
```

One-time HDFS setup:

```bash
hdfs dfs -mkdir -p /spark/3.5.1/jars
hdfs dfs -put $SPARK_HOME/jars/*.jar /spark/3.5.1/jars/
```

#### Mode B: Fat JAR with Spark included (no HDFS setup needed)

Your application JAR includes **everything** — your code, Spark, Scala, and all
transitive dependencies. No Spark jars need to be on HDFS. Larger upload per
submission (~200-300 MB), but zero cluster-side setup.

Change Spark dependencies from `provided` to `compile`:

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-core_2.12</artifactId>
    <version>3.5.1</version>
    <!-- scope is compile (default) — included in fat JAR -->
</dependency>
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.5.1</version>
</dependency>
<!-- Required for YARN cluster mode — contains ApplicationMaster -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-yarn_2.12</artifactId>
    <version>3.5.1</version>
</dependency>
```

The shade plugin config is the same as Mode A. The resulting JAR will be larger
but fully self-contained.

**Gradle (shadow plugin) equivalent:**

```groovy
plugins {
    id 'com.github.johnrengelman.shadow' version '8.1.1'
}

dependencies {
    // For fat JAR mode — use 'implementation' (included in shadow JAR)
    // For thin mode — use 'compileOnly' (excluded from shadow JAR)
    implementation 'org.apache.spark:spark-core_2.12:3.5.1'
    implementation 'org.apache.spark:spark-sql_2.12:3.5.1'
    implementation 'org.apache.spark:spark-yarn_2.12:3.5.1'
}

shadowJar {
    mergeServiceFiles()
    exclude 'META-INF/*.SF', 'META-INF/*.DSA', 'META-INF/*.RSA'
}
```

---

## Step 2: Code migration

### Before (SparkLauncher)

```java
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.launcher.SparkAppHandle;

SparkAppHandle handle = new SparkLauncher()
    .setSparkHome("/opt/spark")
    .setMaster("yarn")
    .setDeployMode("cluster")
    .setAppResource("/path/to/my-app.jar")
    .setMainClass("com.example.MySparkApp")
    .setAppName("MyJob")
    .setConf("spark.executor.instances", "4")
    .setConf("spark.executor.memory", "2g")
    .setConf("spark.executor.cores", "2")
    .setConf("spark.driver.memory", "1g")
    .setConf("spark.yarn.queue", "production")
    .setConf("spark.yarn.jars", "hdfs:///spark/jars/*.jar")
    .addJar("hdfs:///libs/extra-lib.jar")
    .addFile("/path/to/config.properties")
    .addAppArgs("--input", "hdfs:///data/input", "--output", "hdfs:///data/output")
    .startApplication();

// Poll until done
while (handle.getState() != SparkAppHandle.State.FINISHED
        && handle.getState() != SparkAppHandle.State.FAILED
        && handle.getState() != SparkAppHandle.State.KILLED) {
    Thread.sleep(5000);
}

System.out.println("Final state: " + handle.getState());
handle.stop();
```

### After (SparkYarnClient) — Mode A: Spark jars on HDFS

```java
import com.github.sparktools.yarnclient.*;

// Cluster config — reuse across submissions
SparkYarnConfig config = SparkYarnConfig.builder()
    .sparkConf("spark.yarn.jars", "hdfs:///spark/jars/*.jar")
    .build();

// Per-job config
SparkJobConfig job = SparkJobConfig.builder()
    .appName("MyJob")
    .mainClass("com.example.MySparkApp")
    .localJarPath("/path/to/my-app.jar")
    .numExecutors(4)
    .executorMemory("2g")
    .executorCores(2)
    .driverMemory("1g")
    .queue("production")
    .addJar("hdfs:///libs/extra-lib.jar")
    .addFile("/path/to/config.properties")
    .addArg("--input").addArg("hdfs:///data/input")
    .addArg("--output").addArg("hdfs:///data/output")
    .build();

try (SparkYarnClient client = new SparkYarnClient(config)) {
    SubmittedApplication app = client.submit(job);

    ApplicationInfo result = app.waitForTermination(30 * 60_000L);
    System.out.println("Final state: " + result.getState());

    if (!result.isSucceeded()) {
        System.err.println("Failed: " + result.getDiagnostics());
    }
}
```

### After (SparkYarnClient) — Mode B: Fat JAR, zero config

```java
SparkYarnConfig config = SparkYarnConfig.builder().build();

SparkJobConfig job = SparkJobConfig.builder()
    .appName("MyJob")
    .mainClass("com.example.MySparkApp")
    .localJarPath("/path/to/my-app-fat.jar")  // contains Spark + your code
    .numExecutors(4)
    .executorMemory("2g")
    .addArg("--input").addArg("hdfs:///data/input")
    .addArg("--output").addArg("hdfs:///data/output")
    .build();

try (SparkYarnClient client = new SparkYarnClient(config)) {
    SubmittedApplication app = client.submit(job);
    ApplicationInfo result = app.waitForTermination(30 * 60_000L);
}
```

No `spark.yarn.jars`, no `spark.yarn.archive`, no `hdfsUri`, no `sparkHome`.
Everything is resolved automatically from `HADOOP_CONF_DIR` and the fat JAR.

---

## API mapping

| SparkLauncher | SparkYarnClient | Notes |
|---|---|---|
| `setSparkHome(path)` | *not needed* | No Spark installation required |
| `setMaster("yarn")` | *implicit* | Always YARN |
| `setDeployMode("cluster")` | `job.deployMode("cluster")` | Default is `cluster` |
| `setAppResource(jar)` | `job.localJarPath(jar)` | Uploaded to HDFS automatically |
| `setMainClass(cls)` | `job.mainClass(cls)` | |
| `setAppName(name)` | `job.appName(name)` | |
| `addAppArgs(args...)` | `job.addArg(arg)` | One arg per call |
| `addJar(jar)` | `job.addJar(jar)` | Distributed to executors |
| `addFile(file)` | `job.addFile(file)` | Distributed to executors |
| `setConf(key, val)` | `config.sparkConf(k, v)` or `job.sparkConf(k, v)` | Cluster-level vs per-job |
| `setConf("spark.executor.instances", "4")` | `job.numExecutors(4)` | Typed API |
| `setConf("spark.executor.memory", "2g")` | `job.executorMemory("2g")` | Typed API |
| `setConf("spark.executor.cores", "2")` | `job.executorCores(2)` | Typed API |
| `setConf("spark.driver.memory", "1g")` | `job.driverMemory("1g")` | Typed API |
| `setConf("spark.driver.cores", "1")` | `job.driverCores(1)` | Typed API |
| `setConf("spark.yarn.queue", "q")` | `job.queue("q")` | Typed API |
| `startApplication()` | `client.submit(job)` | Returns `SubmittedApplication` |
| `handle.getState()` | `app.getStatus().getState()` | Returns `YarnApplicationState` |
| `handle.stop()` | `app.kill()` | |
| `handle.kill()` | `app.kill()` | |

---

## Monitoring migration

### Before

```java
SparkAppHandle handle = launcher.startApplication(new SparkAppHandle.Listener() {
    @Override
    public void stateChanged(SparkAppHandle h) {
        System.out.println("State: " + h.getState());
    }
    @Override
    public void infoChanged(SparkAppHandle h) {}
});
```

### After

```java
SubmittedApplication app = client.submit(job);

// Option 1: blocking wait
ApplicationInfo result = app.waitForTermination(timeout);

// Option 2: polling loop
while (true) {
    ApplicationInfo info = app.getStatus();
    System.out.printf("State: %s  Progress: %.0f%%%n",
        info.getState(), info.getProgress() * 100);
    if (info.isFinished()) break;
    Thread.sleep(5000);
}

// Option 3: query by ID from a different process
ApplicationInfo info = client.getApplicationStatus("application_1700000000_0042");
```

---

## Kerberos migration

### Before

```java
new SparkLauncher()
    .setSparkHome("/opt/spark")
    .setConf("spark.kerberos.principal", "svc@REALM")
    .setConf("spark.kerberos.keytab", "/etc/keytabs/svc.keytab")
    // ...
```

### After

```java
SparkYarnConfig config = SparkYarnConfig.builder()
    .kerberos("svc@REALM", "/etc/keytabs/svc.keytab")
    .build();
```

The client handles login, delegation token acquisition (HDFS + YARN RM),
keytab distribution to the AM container, and property forwarding automatically.

---

## Environment requirements

| Requirement | SparkLauncher | SparkYarnClient |
|---|---|---|
| Spark installed on submitter | **yes** (`SPARK_HOME`) | no |
| `spark-submit` on `PATH` | **yes** | no |
| Hadoop config (`HADOOP_CONF_DIR`) | optional | optional (but recommended) |
| Network to HDFS NameNode | via `spark-submit` | **yes** (direct) |
| Network to YARN ResourceManager | via `spark-submit` | **yes** (direct) |
| Spark jars on HDFS | **yes** (always) | no (fat JAR mode) |

---

## Checklist

- [ ] Replace `spark-launcher` dependency with `spark-yarn-client`
- [ ] Decide on Mode A (Spark on HDFS) or Mode B (fat JAR)
- [ ] If Mode B: add `spark-core`, `spark-sql`, `spark-yarn` as `compile` deps in the Spark app
- [ ] If Mode B: configure `maven-shade-plugin` with `ServicesResourceTransformer`
- [ ] Rewrite `SparkLauncher` calls to `SparkYarnConfig` + `SparkJobConfig` + `SparkYarnClient`
- [ ] Replace `SparkAppHandle` polling with `SubmittedApplication.waitForTermination()` or polling loop
- [ ] If using Kerberos: replace `setConf("spark.kerberos.*")` with `.kerberos(principal, keytab)`
- [ ] Remove `SPARK_HOME` / Spark installation from submitter host
- [ ] Set `HADOOP_CONF_DIR` on the submitter host (or pass `hadoopConf()` programmatically)
- [ ] Test submission and verify YARN UI shows the application
