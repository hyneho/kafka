# Apache Kafka

<a href="https://kafka.apache.org/">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/kafka-logo-readme-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="docs/images/kafka-logo-readme-light.svg">
  <img alt="Kafka Logo" src="docs/images/kafka-logo-readme-light.svg" width="80"> 
</picture>
</a>
<br><br>

* [Building Kafka](#building-kafka)
  * [Prerequisites](#prerequisites)
  * [Build steps](#build-steps)
    * [Build a jar and run it](#build-a-jar-and-run-it)
    * [Build source jar](#build-source-jar)
    * [Build aggregated javadoc](#build-aggregated-javadoc)
    * [Build javadoc and scaladoc](#build-javadoc-and-scaladoc)
* [Testing Kafka](#testing-kafka)
  * [Run unit/integration tests](#run-unitintegration-tests)
  * [Force re-running tests without code change](#force-re-running-tests-without-code-change)
  * [Running a particular unit/integration test](#running-a-particular-unitintegration-test)
  * [Repeatedly running a particular unit/integration test with specific times by setting N](#repeatedly-running-a-particular-unitintegration-test-with-specific-times-by-setting-n)
  * [Running a particular test method within a unit/integration test](#running-a-particular-test-method-within-a-unitintegration-test)
  * [Running a particular unit/integration test with log4j output](#running-a-particular-unitintegration-test-with-log4j-output)
  * [Specifying test retries](#specifying-test-retries)
* [Running a Kafka broker in KRaft mode](#running-a-kafka-broker-in-kraft-mode)
  * [Using compiled files](#using-compiled-files)
  * [Using docker image](#using-docker-image)
* [Test Coverage](#test-coverage)
  * [Generating test coverage reports](#generating-test-coverage-reports)
* [Other Actions](#other-actions)
  * [Building a binary release gzipped tarball](#building-a-binary-release-gzipped-tar-ball)
  * [Building auto generated messages](#building-auto-generated-messages)
  * [Cleaning the build](#cleaning-the-build)
  * [Running a task for a specific project](#running-a-task-for-a-specific-project)
  * [Listing all gradle tasks](#listing-all-gradle-tasks)
  * [Building IDE project](#building-ide-project)
  * [Publishing the streams quickstart archetype artifact to maven](#publishing-the-streams-quickstart-archetype-artifact-to-maven)
  * [Installing specific projects to the local Maven repository](#installing-specific-projects-to-the-local-maven-repository)
  * [Building the test jar](#building-the-test-jar)
* [Running code quality checks](#running-code-quality-checks)
  * [Checkstyle](#checkstyle)
  * [Spotless](#spotless)
  * [Spotbugs](#spotbugs)
* [JMH microbenchmarks](#jmh-microbenchmarks)
* [Dependency Analysis](#dependency-analysis)
  * [Determining if any dependencies could be updated](#determining-if-any-dependencies-could-be-updated)
* [Common build options](#common-build-options)
* [Running system tests](#running-system-tests)
* [Running in Vagrant](#running-in-vagrant)
* [Contributing Kafka](#contributing-kafka)
  
# Building Kafka <a name="building-kafka"></a>

[**Apache Kafka**](https://kafka.apache.org) is an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.

## Prerequisites <a name="prerequisites"></a>
* You need to have [Java 8+](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.
* KRaft or ZooKeeper
* Apache Kafka is built and tested using Java versions 17 and 23. However, to ensure maximum compatibility, the Kafka client and streams modules are compiled to work with Java 11 and later, while the broker and tools require Java 17 or later.
* Scala 2.13 is the only supported version in Apache Kafka.

## Build steps <a name="build-steps"></a>
### Build a jar and run it <a name="build-a-jar-and-run-it"></a>
    ./gradlew jar
* Follow instructions in [**Kafka Quickstart**](https://kafka.apache.org/quickstart)

### Build source jar <a name="build-source-jar"></a>
    ./gradlew srcJar
### Build aggregated javadoc <a name="build-aggregated-javadoc"></a>
    ./gradlew aggregatedJavadoc
### Build javadoc and scaladoc <a name="build-javadoc-and-scaladoc"></a>
    ./gradlew javadoc
    ./gradlew javadocJar # builds a javadoc jar for each module
    ./gradlew scaladoc
    ./gradlew scaladocJar # builds a scaladoc jar for each module
    ./gradlew docsJar # builds both (if applicable) javadoc and scaladoc jars for each module

# Testing Kafka <a name="testing-kafka"></a>
## Run unit/integration tests <a name="run-unitintegration-tests"></a>
    ./gradlew test  # runs both unit and integration tests
    ./gradlew unitTest
    ./gradlew integrationTest
    ./gradlew quarantinedTest  # runs the quarantined tests

## Force re-running tests without code change <a name="force-re-running-tests-without-code-change"></a>
    ./gradlew test --rerun-tasks
    ./gradlew unitTest --rerun-tasks
    ./gradlew integrationTest --rerun-tasks

## Running a particular unit/integration test <a name="running-a-particular-unitintegration-test"></a>
    ./gradlew clients:test --tests RequestResponseTest

## Repeatedly running a particular unit/integration test with specific times by setting N <a name="repeatedly-running-a-particular-unitintegration-test-with-specific-times-by-setting-n"></a>
    N=500; I=0; while [ $I -lt $N ] && ./gradlew clients:test --tests RequestResponseTest --rerun --fail-fast; do (( I=$I+1 )); echo "Completed run: $I"; sleep 1; done

## Running a particular test method within a unit/integration test <a name="running-a-particular-test-method-within-a-unitintegration-test"></a>
    ./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic
    ./gradlew clients:test --tests org.apache.kafka.clients.MetadataTest.testTimeToNextUpdate

## Running a particular unit/integration test with log4j output <a name="running-a-particular-unitintegration-test-with-log4j-output"></a>
By default, there will be only small number of logs output while testing. You can adjust it by changing the `log4j.properties` file in the module's `src/test/resources` directory.

For example, if you want to see more logs for clients project tests, you can modify [the line](https://github.com/apache/kafka/blob/trunk/clients/src/test/resources/log4j.properties#L21) in `clients/src/test/resources/log4j.properties` 
to `log4j.logger.org.apache.kafka=INFO` and then run:
    
    ./gradlew cleanTest clients:test --tests NetworkClientTest   

And you should see `INFO` level logs in the file under the `clients/build/test-results/test` directory.

## Specifying test retries <a name="specifying-test-retries"></a>
Retries are disabled by default, but you can set maxTestRetryFailures and maxTestRetries to enable retries.

The following example declares -PmaxTestRetries=1 and -PmaxTestRetryFailures=3 to enable a failed test to be retried once, with a total retry limit of 3.

    ./gradlew test -PmaxTestRetries=1 -PmaxTestRetryFailures=3

The quarantinedTest task also has no retries by default, but you can set maxQuarantineTestRetries and maxQuarantineTestRetryFailures to enable retries, similar to the test task.

    ./gradlew quarantinedTest -PmaxQuarantineTestRetries=3 -PmaxQuarantineTestRetryFailures=20

See [Test Retry Gradle Plugin](https://github.com/gradle/test-retry-gradle-plugin) for and [build.yml](.github/workflows/build.yml) more details.

# Running a Kafka broker in KRaft mode <a name="running-a-kafka-broker-in-kraft-mode"></a>
## Using compiled files <a name="using-compiled-files"></a>
    KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
    ./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/reconfig-server.properties
    ./bin/kafka-server-start.sh config/kraft/reconfig-server.properties

## Using docker image <a name="using-docker-image></a>
    docker run -p 9092:9092 apache/kafka:3.7.0

# Test Coverage <a name="test-coverage"></a>
## Generating test coverage reports <a name="generating-test-coverage-reports"></a>
* Generate coverage reports for the whole project: 
          
      ./gradlew reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false

* Generate coverage for a single module, i.e.: 

      ./gradlew clients:reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false

# Other actions <a name="other-actions"></a>
## Building a binary release gzipped tar ball <a name="building-a-binary-release-gzipped-tar-ball"></a>
    ./gradlew clean releaseTarGz

The release file can be found inside `./core/build/distributions/`.

## Building auto generated messages <a name="building-auto-generated-messages"></a>
Sometimes it is only necessary to rebuild the RPC auto-generated message data when switching between branches, as they could
fail due to code changes. You can just run:
      
    ./gradlew processMessages processTestMessages

## Cleaning the build <a name="cleaning-the-build"></a>
    ./gradlew clean

## Running a task for a specific project <a name="running-a-task-for-a-specific-project"></a>
This is for `core`, `examples` and `clients`

    ./gradlew core:jar
    ./gradlew core:test

Streams has multiple sub-projects, but you can run all the tests:

    ./gradlew :streams:testAll

## Listing all gradle tasks <a name="listing-all-gradle-tasks"></a>
    ./gradlew tasks

## Building IDE project <a name="building-ide-project"></a>
*Note that this is not strictly necessary (IntelliJ IDEA has good built-in support for Gradle projects, for example).*

    ./gradlew eclipse
    ./gradlew idea

The `eclipse` task has been configured to use `${project_dir}/build_eclipse` as Eclipse's build directory. Eclipse's default
build directory (`${project_dir}/bin`) clashes with Kafka's scripts directory and we don't use Gradle's build directory
to avoid known issues with this configuration.

## Publishing the streams quickstart archetype artifact to maven <a name="publishing-the-streams-quickstart-archetype-artifact-to-maven"></a>
For the Streams archetype project, one cannot use gradle to upload to maven; instead the `mvn deploy` command needs to be called at the quickstart folder:

    cd streams/quickstart
    mvn deploy

Please note for this to work you should create/update user maven settings (typically, `${USER_HOME}/.m2/settings.xml`) to assign the following variables

    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                           https://maven.apache.org/xsd/settings-1.0.0.xsd">
    ...                           
    <servers>
       ...
       <server>
          <id>apache.snapshots.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
       </server>
       <server>
          <id>apache.releases.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
        </server>
        ...
     </servers>
     ...

## Installing specific projects to the local Maven repository <a name="installing-specific-projects-to-the-local-maven-repository"></a>

    ./gradlew -PskipSigning=true :streams:publishToMavenLocal
    
## Building the test jar <a name="building-the-test-jar"></a>
    ./gradlew testJar

# Running code quality checks <a name="running-code-quality-checks"></a>
There are two code quality analysis tools that we regularly run, spotbugs and checkstyle.

## Checkstyle <a name="checkstyle"></a>
* Checkstyle enforces a consistent coding style in Kafka.
You can run checkstyle using:

      ./gradlew checkstyleMain checkstyleTest spotlessCheck

* The checkstyle warnings will be found in `reports/checkstyle/reports/main.html` and `reports/checkstyle/reports/test.html` files in the
subproject build directories. They are also printed to the console. The build will fail if Checkstyle fails.
For experiments (or regression testing purposes) add `-PcheckstyleVersion=X.y.z` switch (to override project-defined checkstyle version).

## Spotless <a name="spotless"></a>
* The import order is a part of static check. please call `spotlessApply` to optimize the imports of Java codes before filing pull request.

      ./gradlew spotlessApply

## Spotbugs <a name="spotbugs"></a>
* Spotbugs uses static analysis to look for bugs in the code.
You can run spotbugs using:
        
      ./gradlew spotbugsMain spotbugsTest -x test

* The spotbugs warnings will be found in `reports/spotbugs/main.html` and `reports/spotbugs/test.html` files in the subproject build
directories.  Use -PxmlSpotBugsReport=true to generate an XML report instead of an HTML one.

## JMH microbenchmarks <a name="jmh-microbenchmarks"></a>
We use [JMH](https://openjdk.java.net/projects/code-tools/jmh/) to write microbenchmarks that produce reliable results in the JVM.
    
See [jmh-benchmarks/README.md](https://github.com/apache/kafka/blob/trunk/jmh-benchmarks/README.md) for details on how to run the microbenchmarks.

## Dependency Analysis <a name="dependency-analysis"></a>

The gradle [dependency debugging documentation](https://docs.gradle.org/current/userguide/viewing_debugging_dependencies.html) mentions using the `dependencies` or `dependencyInsight` tasks to debug dependencies for the root project or individual subprojects.

Alternatively, use the `allDeps` or `allDepInsight` tasks for recursively iterating through all subprojects:

    ./gradlew allDeps

    ./gradlew allDepInsight --configuration runtimeClasspath --dependency com.fasterxml.jackson.core:jackson-databind

These take the same arguments as the builtin variants.

## Determining if any dependencies could be updated <a name="determining-if-any-dependencies-could-be-updated"></a>
    ./gradlew dependencyUpdates

# Common build options <a name="common-build-options"></a>

The following options should be set with a `-P` switch, for example `./gradlew -PmaxParallelForks=1 test`.

* `commitId`: sets the build commit ID as .git/HEAD might not be correct if there are local commits added for build purposes.
* `mavenUrl`: sets the URL of the maven deployment repository (`file://path/to/repo` can be used to point to a local repository).
* `maxParallelForks`: maximum number of test processes to start in parallel. Defaults to the number of processors available to the JVM.
* `maxScalacThreads`: maximum number of worker threads for the scalac backend. Defaults to the lowest of `8` and the number of processors
available to the JVM. The value must be between 1 and 16 (inclusive). 
* `ignoreFailures`: ignore test failures from junit
* `showStandardStreams`: shows standard out and standard error of the test JVM(s) on the console.
* `skipSigning`: skips signing of artifacts.
* `testLoggingEvents`: unit test events to be logged, separated by comma. For example `./gradlew -PtestLoggingEvents=started,passed,skipped,failed test`.
* `xmlSpotBugsReport`: enable XML reports for spotBugs. This also disables HTML reports as only one can be enabled at a time.
* `maxTestRetries`: maximum number of retries for a failing test case.
* `maxTestRetryFailures`: maximum number of test failures before retrying is disabled for subsequent tests.
* `enableTestCoverage`: enables test coverage plugins and tasks, including bytecode enhancement of classes required to track said
coverage. Note that this introduces some overhead when running tests and hence why it's disabled by default (the overhead
varies, but 15-20% is a reasonable estimate).
* `keepAliveMode`: configures the keep alive mode for the Gradle compilation daemon - reuse improves start-up time. The values should 
be one of `daemon` or `session` (the default is `daemon`). `daemon` keeps the daemon alive until it's explicitly stopped while
`session` keeps it alive until the end of the build session. This currently only affects the Scala compiler, see
https://github.com/gradle/gradle/pull/21034 for a PR that attempts to do the same for the Java compiler.
* `scalaOptimizerMode`: configures the optimizing behavior of the scala compiler, the value should be one of `none`, `method`, `inline-kafka` or
`inline-scala` (the default is `inline-kafka`). `none` is the scala compiler default, which only eliminates unreachable code. `method` also
includes method-local optimizations. `inline-kafka` adds inlining of methods within the kafka packages. Finally, `inline-scala` also
includes inlining of methods within the scala library (which avoids lambda allocations for methods like `Option.exists`). `inline-scala` is
only safe if the Scala library version is the same at compile time and runtime. Since we cannot guarantee this for all cases (for example, users
may depend on the kafka jar for integration tests where they may include a scala library with a different version), we don't enable it by
default. See https://www.lightbend.com/blog/scala-inliner-optimizer for more details.

# Running system tests <a name="running-system-tests"></a>

See [tests/README.md](tests/README.md).

# Running in Vagrant <a name="running-in-vagrant"></a>

See [vagrant/README.md](vagrant/README.md).

# Contributing Kafka <a name="contributing-kafka"></a>

Apache Kafka is interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

To contribute follow the instructions here:
 * https://kafka.apache.org/contributing.html 
