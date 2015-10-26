/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.runtime.rest.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.copycat.errors.AlreadyExistsException;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.errors.NotFoundException;
import org.apache.kafka.copycat.runtime.ConnectorConfig;
import org.apache.kafka.copycat.runtime.Herder;
import org.apache.kafka.copycat.runtime.distributed.NotLeaderException;
import org.apache.kafka.copycat.runtime.rest.RestServer;
import org.apache.kafka.copycat.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.copycat.runtime.rest.entities.ConnectorTaskId;
import org.apache.kafka.copycat.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.copycat.runtime.rest.entities.TaskInfo;
import org.apache.kafka.copycat.util.Callback;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RestServer.class)
@PowerMockIgnore("javax.management.*")
public class ConnectorsResourceTest {
    // Note trailing / and that we do *not* use LEADER_URL to construct our reference values. This checks that we handle
    // URL construction properly, avoiding //, which will mess up routing in the REST server
    private static final String LEADER_URL = "http://leader:8083/";
    private static final String CONNECTOR_NAME = "test";
    private static final String CONNECTOR2_NAME = "test2";
    private static final Map<String, String> CONNECTOR_CONFIG = new HashMap<>();
    static {
        CONNECTOR_CONFIG.put("name", CONNECTOR_NAME);
        CONNECTOR_CONFIG.put("sample_config", "test_config");
    }
    private static final List<ConnectorTaskId> CONNECTOR_TASK_NAMES = Arrays.asList(
            new ConnectorTaskId(CONNECTOR_NAME, 0),
            new ConnectorTaskId(CONNECTOR_NAME, 1)
    );
    private static final List<Map<String, String>> TASK_CONFIGS = new ArrayList<>();
    static {
        TASK_CONFIGS.add(Collections.singletonMap("config", "value"));
        TASK_CONFIGS.add(Collections.singletonMap("config", "other_value"));
    }
    private static final List<TaskInfo> TASK_INFOS = new ArrayList<>();
    static {
        TASK_INFOS.add(new TaskInfo(new ConnectorTaskId(CONNECTOR_NAME, 0), TASK_CONFIGS.get(0)));
        TASK_INFOS.add(new TaskInfo(new ConnectorTaskId(CONNECTOR_NAME, 1), TASK_CONFIGS.get(1)));
    }


    @Mock
    private Herder herder;
    private ConnectorsResource connectorsResource;

    @Before
    public void setUp() throws NoSuchMethodException {
        PowerMock.mockStatic(RestServer.class,
                RestServer.class.getMethod("httpRequest", String.class, String.class, Object.class, TypeReference.class));
        connectorsResource = new ConnectorsResource(herder);
    }

    @Test
    public void testListConnectors() throws Throwable {
        final Capture<Callback<Collection<String>>> cb = Capture.newInstance();
        herder.connectors(EasyMock.capture(cb));
        expectAndCallbackResult(cb, Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));

        PowerMock.replayAll();

        Collection<String> connectors = connectorsResource.listConnectors();
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), new HashSet<>(connectors));

        PowerMock.verifyAll();
    }

    @Test
    public void testListConnectorsNotLeader() throws Throwable {
        final Capture<Callback<Collection<String>>> cb = Capture.newInstance();
        herder.connectors(EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);
        // Should forward request
        EasyMock.expect(RestServer.httpRequest(EasyMock.eq("http://leader:8083/connectors"), EasyMock.eq("GET"),
                EasyMock.isNull(), EasyMock.anyObject(TypeReference.class)))
                .andReturn(Arrays.asList(CONNECTOR2_NAME, CONNECTOR_NAME));

        PowerMock.replayAll();

        Collection<String> connectors = connectorsResource.listConnectors();
        // Ordering isn't guaranteed, compare sets
        assertEquals(new HashSet<>(Arrays.asList(CONNECTOR_NAME, CONNECTOR2_NAME)), new HashSet<>(connectors));

        PowerMock.verifyAll();
    }

    @Test(expected = CopycatException.class)
    public void testListConnectorsNotSynced() throws Throwable {
        final Capture<Callback<Collection<String>>> cb = Capture.newInstance();
        herder.connectors(EasyMock.capture(cb));
        expectAndCallbackException(cb, new CopycatException("not synced"));

        PowerMock.replayAll();

        // throws
        connectorsResource.listConnectors();
    }

    @Test
    public void testCreateConnector() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));

        final Capture<Callback<String>> cb = Capture.newInstance();
        herder.addConnector(EasyMock.eq(body.config()), EasyMock.capture(cb));
        expectAndCallbackResult(cb, CONNECTOR_NAME);

        PowerMock.replayAll();

        connectorsResource.createConnector(body);

        PowerMock.verifyAll();
    }

    @Test
    public void testCreateConnectorNotLeader() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));

        final Capture<Callback<String>> cb = Capture.newInstance();
        herder.addConnector(EasyMock.eq(body.config()), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);
        // Should forward request
        EasyMock.expect(RestServer.httpRequest("http://leader:8083/connectors", "POST", body, null)).andReturn(null);

        PowerMock.replayAll();

        connectorsResource.createConnector(body);

        PowerMock.verifyAll();
    }

    @Test(expected = AlreadyExistsException.class)
    public void testCreateConnectorExists() throws Throwable {
        CreateConnectorRequest body = new CreateConnectorRequest(CONNECTOR_NAME, Collections.singletonMap(ConnectorConfig.NAME_CONFIG, CONNECTOR_NAME));

        final Capture<Callback<String>> cb = Capture.newInstance();
        herder.addConnector(EasyMock.eq(body.config()), EasyMock.capture(cb));
        expectAndCallbackException(cb, new AlreadyExistsException("already exists"));

        PowerMock.replayAll();

        connectorsResource.createConnector(body);

        PowerMock.verifyAll();
    }

    @Test
    public void testDeleteConnector() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.deleteConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, null);

        PowerMock.replayAll();

        connectorsResource.destroyConnector(CONNECTOR_NAME);

        PowerMock.verifyAll();
    }

    @Test
    public void testDeleteConnectorNotLeader() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.deleteConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackNotLeaderException(cb);
        // Should forward request
        EasyMock.expect(RestServer.httpRequest("http://leader:8083/connectors/" + CONNECTOR_NAME, "DELETE", null, null)).andReturn(null);

        PowerMock.replayAll();

        connectorsResource.destroyConnector(CONNECTOR_NAME);

        PowerMock.verifyAll();
    }

    // Not found exceptions should pass through to caller so they can be processed for 404s
    @Test(expected = NotFoundException.class)
    public void testDeleteConnectorNotFound() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.deleteConnector(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        connectorsResource.destroyConnector(CONNECTOR_NAME);

        PowerMock.verifyAll();
    }

    @Test
    public void testGetConnector() throws Throwable {
        final Capture<Callback<ConnectorInfo>> cb = Capture.newInstance();
        herder.connectorInfo(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES));

        PowerMock.replayAll();

        ConnectorInfo connInfo = connectorsResource.getConnector(CONNECTOR_NAME);
        assertEquals(new ConnectorInfo(CONNECTOR_NAME, CONNECTOR_CONFIG, CONNECTOR_TASK_NAMES), connInfo);

        PowerMock.verifyAll();
    }

    @Test
    public void testGetConnectorConfig() throws Throwable {
        final Capture<Callback<Map<String, String>>> cb = Capture.newInstance();
        herder.connectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, CONNECTOR_CONFIG);

        PowerMock.replayAll();

        Map<String, String> connConfig = connectorsResource.getConnectorConfig(CONNECTOR_NAME);
        assertEquals(CONNECTOR_CONFIG, connConfig);

        PowerMock.verifyAll();
    }

    @Test(expected = NotFoundException.class)
    public void testGetConnectorConfigConnectorNotFound() throws Throwable {
        final Capture<Callback<Map<String, String>>> cb = Capture.newInstance();
        herder.connectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        connectorsResource.getConnectorConfig(CONNECTOR_NAME);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorConfig() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.putConnectorConfig(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(CONNECTOR_CONFIG), EasyMock.capture(cb));
        expectAndCallbackResult(cb, null);

        PowerMock.replayAll();

        connectorsResource.putConnectorConfig(CONNECTOR_NAME, CONNECTOR_CONFIG);

        PowerMock.verifyAll();
    }

    @Test
    public void testGetConnectorTaskConfigs() throws Throwable {
        final Capture<Callback<List<TaskInfo>>> cb = Capture.newInstance();
        herder.taskConfigs(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackResult(cb, TASK_INFOS);

        PowerMock.replayAll();

        List<TaskInfo> taskInfos = connectorsResource.getTaskConfigs(CONNECTOR_NAME);
        assertEquals(TASK_INFOS, taskInfos);

        PowerMock.verifyAll();
    }

    @Test(expected = NotFoundException.class)
    public void testGetConnectorTaskConfigsConnectorNotFound() throws Throwable {
        final Capture<Callback<List<TaskInfo>>> cb = Capture.newInstance();
        herder.taskConfigs(EasyMock.eq(CONNECTOR_NAME), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("connector not found"));

        PowerMock.replayAll();

        connectorsResource.getTaskConfigs(CONNECTOR_NAME);

        PowerMock.verifyAll();
    }

    @Test
    public void testPutConnectorTaskConfigs() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.putTaskConfigs(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(TASK_CONFIGS), EasyMock.capture(cb));
        expectAndCallbackResult(cb, null);

        PowerMock.replayAll();

        connectorsResource.putTaskConfigs(CONNECTOR_NAME, TASK_CONFIGS);

        PowerMock.verifyAll();
    }

    @Test(expected = NotFoundException.class)
    public void testPutConnectorTaskConfigsConnectorNotFound() throws Throwable {
        final Capture<Callback<Void>> cb = Capture.newInstance();
        herder.putTaskConfigs(EasyMock.eq(CONNECTOR_NAME), EasyMock.eq(TASK_CONFIGS), EasyMock.capture(cb));
        expectAndCallbackException(cb, new NotFoundException("not found"));

        PowerMock.replayAll();

        connectorsResource.putTaskConfigs(CONNECTOR_NAME, TASK_CONFIGS);

        PowerMock.verifyAll();
    }

    private  <T> void expectAndCallbackResult(final Capture<Callback<T>> cb, final T value) {
        PowerMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                cb.getValue().onCompletion(null, value);
                return null;
            }
        });
    }

    private  <T> void expectAndCallbackException(final Capture<Callback<T>> cb, final Throwable t) {
        PowerMock.expectLastCall().andAnswer(new IAnswer<Void>() {
            @Override
            public Void answer() throws Throwable {
                cb.getValue().onCompletion(t, null);
                return null;
            }
        });
    }

    private  <T> void expectAndCallbackNotLeaderException(final Capture<Callback<T>> cb) {
        expectAndCallbackException(cb, new NotLeaderException("not leader test", LEADER_URL));
    }
}
