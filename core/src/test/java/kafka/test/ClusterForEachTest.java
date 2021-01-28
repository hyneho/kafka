/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.test;

import kafka.test.annotations.AutoStart;
import kafka.test.annotations.ClusterProperty;
import kafka.test.annotations.ClusterTemplate;
import kafka.test.annotations.ClusterTest;
import kafka.test.annotations.ClusterTestDefaults;
import kafka.test.annotations.ClusterTests;
import kafka.test.annotations.Type;
import kafka.test.junit.ClusterForEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;


@ClusterTestDefaults(clusterType = Type.Zk)   // Set defaults for a few params in @ClusterTest(s)
@ExtendWith(ClusterForEach.class)
public class ClusterForEachTest {

    private final ClusterInstance clusterInstance;
    private final ClusterConfig config;

    ClusterForEachTest(ClusterInstance clusterInstance, ClusterConfig config) {     // Constructor injections
        this.clusterInstance = clusterInstance;
        this.config = config;
    }

    // Static methods can generate cluster configurations
    static void generate1(ClusterGenerator clusterGenerator) {
        clusterGenerator.accept(ClusterConfig.defaultClusterBuilder().name("Generated Test").build());
    }

    // BeforeEach run after class construction, but before cluster initialization and test invocation
    @BeforeEach
    public void beforeEach(ClusterConfig config) {
        Assertions.assertSame(this.config, config, "Injected objects should be the same");
        config.serverProperties().put("before", "each");
    }

    // AfterEach runs after test invocation and cluster teardown
    @AfterEach
    public void afterEach(ClusterConfig config) {
        Assertions.assertSame(this.config, config, "Injected objects should be the same");
    }

    // With no params, configuration comes from the annotation defaults as well as @ClusterTestDefaults (if present)
    @ClusterTest
    public void testClusterTest(ClusterConfig config, ClusterInstance clusterInstance) {
        Assertions.assertSame(this.config, config, "Injected objects should be the same");
        Assertions.assertSame(this.clusterInstance, clusterInstance, "Injected objects should be the same");
        Assertions.assertEquals(clusterInstance.clusterType(), ClusterInstance.ClusterType.Zk); // From the class level default
        Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("before"), "each");
    }

    // generate1 is a template method which generates any number of cluster configs
    @ClusterTemplate("generate1")
    public void testClusterTemplate() {
        Assertions.assertEquals(clusterInstance.clusterType(), ClusterInstance.ClusterType.Zk,
            "generate1 provided a Zk cluster, so we should see that here");
        Assertions.assertEquals(clusterInstance.config().name().orElse(""), "Generated Test",
            "generate 1 named this cluster config, so we should see that here");
        Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("before"), "each");
    }

    // Multiple @ClusterTest can be used with @ClusterTests
    @ClusterTests({
        @ClusterTest(name = "cluster-tests-1", clusterType = Type.Zk, properties = {
            @ClusterProperty(key = "foo", value = "bar"),
            @ClusterProperty(key = "spam", value = "eggs")
        }),
        @ClusterTest(name = "cluster-tests-2", clusterType = Type.Zk, properties = {
            @ClusterProperty(key = "foo", value = "baz"),
            @ClusterProperty(key = "spam", value = "eggz")
        })
    })
    public void testClusterTests() {
        if (clusterInstance.config().name().filter(name -> name.equals("cluster-tests-1")).isPresent()) {
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("foo"), "bar");
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("spam"), "eggs");
        } else if (clusterInstance.config().name().filter(name -> name.equals("cluster-tests-2")).isPresent()) {
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("foo"), "baz");
            Assertions.assertEquals(clusterInstance.config().serverProperties().getProperty("spam"), "eggz");
        } else {
            Assertions.fail("Unknown cluster config " + clusterInstance.config().name());
        }
    }

    @ClusterTest(autoStart = AutoStart.No)
    public void testNoAutoStart() {
        Assertions.assertFalse(clusterInstance.anyBroker().isPresent());
        clusterInstance.start();
        Assertions.assertTrue(clusterInstance.anyBroker().isPresent());
    }
}
