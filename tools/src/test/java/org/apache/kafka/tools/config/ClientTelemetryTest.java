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

package org.apache.kafka.tools.config;

import java.util.List;
import java.util.concurrent.ExecutionException;
import kafka.admin.ConfigCommand;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Arrays.asList;
import static org.apache.kafka.tools.config.ConfigCommandTest.toArray;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
public class ClientTelemetryTest {
    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    public void testIntervalMsParser(ClusterInstance clusterInstance) {
        List<String> alterOpts = asList("--bootstrap-server", clusterInstance.bootstrapServers(),
            "--alter", "--entity-type", "client-metrics", "--entity-name", "test", "--add-config", "interval.ms=bbb");
        try (Admin client = clusterInstance.admin()) {
            ConfigCommand.ConfigCommandOptions addOpts = new ConfigCommand.ConfigCommandOptions(toArray(alterOpts));

            Throwable e = assertThrows(ExecutionException.class, () -> ConfigCommand.alterConfig(client, addOpts));
            assertTrue(e.getMessage().contains(InvalidConfigurationException.class.getSimpleName()));
        }
    }
}
