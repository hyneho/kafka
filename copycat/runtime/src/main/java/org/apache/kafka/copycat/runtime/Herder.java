/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime;

import org.apache.kafka.copycat.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.copycat.runtime.rest.entities.TaskInfo;
import org.apache.kafka.copycat.util.Callback;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * The herder interface tracks and manages workers and connectors. It is the main interface for external components
 * to make changes to the state of the cluster. For example, in distributed mode, an implementation of this class
 * knows how to accept a connector configuration, may need to route it to the current leader worker for the cluster so
 * the config can be written to persistent storage, and then ensures the new connector is correctly instantiated on one
 * of the workers.
 * </p>
 * <p>
 * This class must implement all the actions that can be taken on the cluster (add/remove connectors, pause/resume tasks,
 * get state of connectors and tasks, etc). The non-Java interfaces to the cluster (REST API and CLI) are very simple
 * wrappers of the functionality provided by this interface.
 * </p>
 * <p>
 * In standalone mode, this implementation of this class will be trivial because no coordination is needed. In that case,
 * the implementation will mainly be delegating tasks directly to other components. For example, when creating a new
 * connector in standalone mode, there is no need to persist the config and the connector and its tasks must run in the
 * same process, so the standalone herder implementation can immediately instantiate and start the connector and its
 * tasks.
 * </p>
 */
public interface Herder {

    void start();

    void stop();

    /**
     * Submit a connector job to the cluster. This works from any node by forwarding the request to
     * the leader herder if necessary.
     *
     * @param connectorProps user-specified properties for this job
     * @param callback       callback to invoke when the request completes
     */
    void addConnector(Map<String, String> connectorProps, Callback<String> callback);

    /**
     * Delete a connector job by name.
     *
     * @param name     name of the connector job to shutdown and delete
     * @param callback callback to invoke when the request completes
     */
    void deleteConnector(String name, Callback<Void> callback);

    /**
     * Get a list of connectors currently running in this cluster. This is a full list of connectors in the cluster gathered
     * from the current configuration. However, note
     *
     * @returns A list of connector names
     * @throws org.apache.kafka.copycat.runtime.distributed.NotLeaderException if this node can not resolve the request
     *         (e.g., because it has not joined the cluster or does not have configs in sync with the group) and it is
     *         also not the leader
     * @throws org.apache.kafka.copycat.errors.CopycatException if this node is the leader, but still cannot resolve the
     *         request (e.g., it is not in sync with other worker's config state)
     */
    void connectors(Callback<Collection<String>> callback);

    /**
     * Get the definition and status of a connector.
     */
    void connectorInfo(String connName, Callback<ConnectorInfo> callback);

    /**
     * Get the configuration for a connector.
     * @param connName name of the connector
     * @param callback callback to invoke with the configuration
     */
    void connectorConfig(String connName, Callback<Map<String, String>> callback);

    /**
     * Set the configuration for a connector.
     * @param connName name of the connector
     * @param callback callback to invoke with the configuration
     */
    void putConnectorConfig(String connName, Map<String, String> config, Callback<Void> callback);

    /**
     * Requests reconfiguration of the task. This should only be triggered by
     * {@link HerderConnectorContext}.
     *
     * @param connName name of the connector that should be reconfigured
     */
    void requestTaskReconfiguration(String connName);

    /**
     * Get the configurations for the current set of tasks of a connector.
     * @param connName connector to update
     * @param callback callback to invoke upon completion
     */
    void taskConfigs(String connName, Callback<List<TaskInfo>> callback);

    /**
     * Set the configurations for the tasks of a connector. This should always include all tasks in the connector; if
     * there are existing configurations and fewer are provided, this will reduce the number of tasks, and if more are
     * provided it will increase the number of tasks.
     * @param connName connector to update
     * @param configs list of configurations
     * @param callback callback to invoke upon completion
     */
    void putTaskConfigs(String connName, List<Map<String, String>> configs, Callback<Void> callback);
}