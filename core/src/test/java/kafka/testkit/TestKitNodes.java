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

package kafka.testkit;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

public class TestKitNodes {
    public static class Builder {
        private boolean combined = false;
        private Uuid clusterId = null;
        private BootstrapMetadata bootstrapMetadata = BootstrapMetadata.
            fromVersion(MetadataVersion.latestTesting(), "testkit");
        private final NavigableMap<Integer, ControllerNode.Builder> controllerNodeBuilders = new TreeMap<>();
        private final NavigableMap<Integer, BrokerNode.Builder> brokerNodeBuilders = new TreeMap<>();

        public Builder setClusterId(Uuid clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder setBootstrapMetadataVersion(MetadataVersion metadataVersion) {
            this.bootstrapMetadata = BootstrapMetadata.fromVersion(metadataVersion, "testkit");
            return this;
        }

        public Builder setBootstrapMetadata(BootstrapMetadata bootstrapMetadata) {
            this.bootstrapMetadata = bootstrapMetadata;
            return this;
        }

        public Builder setCombined(boolean combined) {
            this.combined = combined;
            return this;
        }

        public Builder setNumControllerNodes(int numControllerNodes) {
            if (numControllerNodes < 0) {
                throw new RuntimeException("Invalid negative value for numControllerNodes");
            }

            while (controllerNodeBuilders.size() > numControllerNodes) {
                controllerNodeBuilders.pollFirstEntry();
            }
            while (controllerNodeBuilders.size() < numControllerNodes) {
                int nextId = startControllerId();
                if (!controllerNodeBuilders.isEmpty()) {
                    nextId = controllerNodeBuilders.lastKey() + 1;
                }
                controllerNodeBuilders.put(nextId, ControllerNode.builder().setId(nextId));
            }
            return this;
        }

        public Builder setNumBrokerNodes(int numBrokerNodes) {
            return setBrokerNodes(numBrokerNodes, 1, ignored -> Collections.emptyMap());
        }

        /**
         * Set broker nodes
         *
         * @param numBrokerNodes number of broker nodes
         * @param disksPerBroker number of disk per broker.
         * @param perBrokerProperties a function that accept broker id and returns corresponding broker properties
         * @return Builder
         */
        public Builder setBrokerNodes(int numBrokerNodes, int disksPerBroker, Function<Integer, Map<String, String>> perBrokerProperties) {
            if (numBrokerNodes < 0) {
                throw new RuntimeException("Invalid negative value for numBrokerNodes");
            }
            if (disksPerBroker <= 0) {
                throw new RuntimeException("Invalid value for disksPerBroker");
            }
            while (brokerNodeBuilders.size() > numBrokerNodes) {
                brokerNodeBuilders.pollFirstEntry();
            }
            while (brokerNodeBuilders.size() < numBrokerNodes) {
                int nextId = startBrokerId();
                if (!brokerNodeBuilders.isEmpty()) {
                    nextId = brokerNodeBuilders.lastKey() + 1;
                }
                Map<String, String> brokerProperties = Optional.ofNullable(perBrokerProperties.apply(nextId))
                        .orElseGet(Collections::emptyMap);
                BrokerNode.Builder brokerNodeBuilder = BrokerNode.builder()
                        .setId(nextId)
                        .setNumLogDirectories(disksPerBroker)
                        .setPropertyOverrides(brokerProperties);
                brokerNodeBuilders.put(nextId, brokerNodeBuilder);
            }
            return this;
        }

        public TestKitNodes build() {
            String baseDirectory = TestUtils.tempDirectory().getAbsolutePath();
            try {
                if (clusterId == null) {
                    clusterId = Uuid.randomUuid();
                }
                TreeMap<Integer, ControllerNode> controllerNodes = new TreeMap<>();
                for (ControllerNode.Builder builder : controllerNodeBuilders.values()) {
                    ControllerNode node = builder
                        .setBaseDirectory(baseDirectory)
                        .setClusterId(clusterId)
                        .setCombined(brokerNodeBuilders.containsKey(builder.id()))
                        .build();
                    if (controllerNodes.put(node.id(), node) != null) {
                        throw new RuntimeException("Duplicate builder for controller " + node.id());
                    }
                }
                TreeMap<Integer, BrokerNode> brokerNodes = new TreeMap<>();
                for (BrokerNode.Builder builder : brokerNodeBuilders.values()) {
                    BrokerNode node = builder
                        .setBaseDirectory(baseDirectory)
                        .setClusterId(clusterId)
                        .setCombined(controllerNodeBuilders.containsKey(builder.id()))
                        .build();
                    if (brokerNodes.put(node.id(), node) != null) {
                        throw new RuntimeException("Duplicate builder for broker " + node.id());
                    }
                }
                return new TestKitNodes(baseDirectory,
                    clusterId,
                    bootstrapMetadata,
                    controllerNodes,
                    brokerNodes);
            } catch (Exception e) {
                try {
                    Files.delete(Paths.get(baseDirectory));
                } catch (Exception x) {
                    throw new RuntimeException("Failed to delete base directory " + baseDirectory, x);
                }
                throw e;
            }
        }

        private int startBrokerId() {
            return 0;
        }

        private int startControllerId() {
            if (combined) {
                return startBrokerId();
            }
            return startBrokerId() + 3000;
        }
    }

    private final String baseDirectory;
    private final Uuid clusterId;
    private final BootstrapMetadata bootstrapMetadata;
    private final NavigableMap<Integer, ControllerNode> controllerNodes;
    private final NavigableMap<Integer, BrokerNode> brokerNodes;

    private TestKitNodes(
        String baseDirectory,
        Uuid clusterId,
        BootstrapMetadata bootstrapMetadata,
        NavigableMap<Integer, ControllerNode> controllerNodes,
        NavigableMap<Integer, BrokerNode> brokerNodes
    ) {
        this.baseDirectory = Objects.requireNonNull(baseDirectory);
        this.clusterId = Objects.requireNonNull(clusterId);
        this.bootstrapMetadata = Objects.requireNonNull(bootstrapMetadata);
        this.controllerNodes = Objects.requireNonNull(controllerNodes);
        this.brokerNodes = Objects.requireNonNull(brokerNodes);
    }

    public boolean isCombined(int node) {
        return controllerNodes.containsKey(node) && brokerNodes.containsKey(node);
    }

    public String baseDirectory() {
        return baseDirectory;
    }

    public Uuid clusterId() {
        return clusterId;
    }

    public MetadataVersion bootstrapMetadataVersion() {
        return bootstrapMetadata.metadataVersion();
    }

    public Map<Integer, ControllerNode> controllerNodes() {
        return controllerNodes;
    }

    public BootstrapMetadata bootstrapMetadata() {
        return bootstrapMetadata;
    }

    public NavigableMap<Integer, BrokerNode> brokerNodes() {
        return brokerNodes;
    }

    public ListenerName interBrokerListenerName() {
        return new ListenerName("EXTERNAL");
    }

    public ListenerName externalListenerName() {
        return new ListenerName("EXTERNAL");
    }

    public ListenerName controllerListenerName() {
        return new ListenerName("CONTROLLER");
    }
}
