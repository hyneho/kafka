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
package org.apache.kafka.connect.runtime;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConnectorConfigTest<R extends ConnectRecord<R>> {

    public static final Plugins MOCK_PLUGINS = new Plugins(new HashMap<String, String>()) {
        @Override
        public Set<PluginDesc<Transformation>> transformations() {
            return Collections.emptySet();
        }
    };

    public static abstract class TestConnector extends Connector {
    }

    public static class SimpleTransformation<R extends ConnectRecord<R>> implements Transformation<R>  {

        int magicNumber = 0;

        @Override
        public void configure(Map<String, ?> props) {
            magicNumber = Integer.parseInt((String) props.get("magic.number"));
        }

        @Override
        public R apply(R record) {
            return null;
        }

        @Override
        public void close() {
            magicNumber = 0;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef()
                    .define("magic.number", ConfigDef.Type.INT, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Range.atLeast(42), ConfigDef.Importance.HIGH, "");
        }
    }

    @Test
    public void noTransforms() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test(expected = ConfigException.class)
    public void danglingTransformAlias() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "dangler");
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test(expected = ConfigException.class)
    public void emptyConnectorName() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "");
        props.put("connector.class", TestConnector.class.getName());
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test(expected = ConfigException.class)
    public void wrongTransformationType() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", "uninstantiable");
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test(expected = ConfigException.class)
    public void unconfiguredTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test
    public void misconfiguredTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "40");
        try {
            new ConnectorConfig(MOCK_PLUGINS, props);
            fail();
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("Value must be at least 42"));
        }
    }

    @Test
    public void singleTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        final ConnectorConfig config = new ConnectorConfig(MOCK_PLUGINS, props);
        final List<Transformation<R>> transformations = config.transformations();
        assertEquals(1, transformations.size());
        final SimpleTransformation xform = (SimpleTransformation) transformations.get(0);
        assertEquals(42, xform.magicNumber);
    }

    @Test(expected = ConfigException.class)
    public void multipleTransformsOneDangling() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a, b");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test
    public void multipleTransforms() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a, b");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.b.type", SimpleTransformation.class.getName());
        props.put("transforms.b.magic.number", "84");
        final ConnectorConfig config = new ConnectorConfig(MOCK_PLUGINS, props);
        final List<Transformation<R>> transformations = config.transformations();
        assertEquals(2, transformations.size());
        assertEquals(42, ((SimpleTransformation) transformations.get(0)).magicNumber);
        assertEquals(84, ((SimpleTransformation) transformations.get(1)).magicNumber);
    }

    @Test
    public void abstractTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", AbstractTransformation.class.getName());
        try {
            new ConnectorConfig(MOCK_PLUGINS, props);
        } catch (ConfigException ex) {
            assertTrue(
                ex.getMessage().contains("Transformation is abstract and cannot be created.")
            );
        }
    }
    @Test
    public void abstractKeyValueTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", AbstractKeyValueTransformation.class.getName());
        try {
            new ConnectorConfig(MOCK_PLUGINS, props);
        } catch (ConfigException ex) {
            assertTrue(
                ex.getMessage().contains("Transformation is abstract and cannot be created.")
            );
            assertTrue(
                ex.getMessage().contains(AbstractKeyValueTransformation.Key.class.getName())
            );
            assertTrue(
                ex.getMessage().contains(AbstractKeyValueTransformation.Value.class.getName())
            );
        }
    }



    @Test(expected = ConfigException.class)
    public void wrongPredicateType() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", TestConnector.class.getName());
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test
    public void singleConditionalTransform() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("transforms.a.negate", "true");
        props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", TestPredicate.class.getName());
        props.put("predicates.my-pred.int", "84");
        assertPredicatedTransform(props, true);
    }

    @Test
    public void predicateNegationDefaultsToFalse() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", TestPredicate.class.getName());
        props.put("predicates.my-pred.int", "84");
        assertPredicatedTransform(props, false);
    }

    @Test(expected = ConfigException.class)
    public void abstractPredicate() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", AbstractTestPredicate.class.getName());
        props.put("predicates.my-pred.int", "84");
        assertPredicatedTransform(props, false);
    }

    private void assertPredicatedTransform(Map<String, String> props, boolean expectedNegated) {
        final ConnectorConfig config = new ConnectorConfig(MOCK_PLUGINS, props);
        final List<Transformation<R>> transformations = config.transformations();
        assertEquals(1, transformations.size());
        assertTrue(transformations.get(0) instanceof PredicatedTransformation);
        PredicatedTransformation<?> predicated = (PredicatedTransformation<?>) transformations.get(0);

        assertEquals(expectedNegated, predicated.negate);

        assertTrue(predicated.delegate instanceof ConnectorConfigTest.SimpleTransformation);
        assertEquals(42, ((SimpleTransformation<?>) predicated.delegate).magicNumber);

        assertTrue(predicated.predicate instanceof ConnectorConfigTest.TestPredicate);
        assertEquals(84, ((TestPredicate<?>) predicated.predicate).param);

        predicated.close();

        assertEquals(0, ((SimpleTransformation<?>) predicated.delegate).magicNumber);
        assertEquals(0, ((TestPredicate<?>) predicated.predicate).param);
    }

    @Test
    public void misconfiguredPredicate() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("transforms.a.negate", "true");
        props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", TestPredicate.class.getName());
        props.put("predicates.my-pred.int", "79");
        try {
            new ConnectorConfig(MOCK_PLUGINS, props);
            fail();
        } catch (ConfigException e) {
            assertTrue(e.getMessage().contains("Value must be at least 80"));
        }
    }

    @Ignore("Is this really an error. There's no actual need for the predicates config (unlike transforms where it defines the order).")
    @Test(expected = ConfigException.class)
    public void missingPredicates() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        //props.put("predicates", "my-pred");
        props.put("predicates.my-pred.type", TestPredicate.class.getName());
        props.put("predicates.my-pred.int", "84");
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test(expected = ConfigException.class)
    public void missingPredicateConfig() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.predicate", "my-pred");
        props.put("predicates", "my-pred");
        //props.put("predicates.my-pred.type", TestPredicate.class.getName());
        //props.put("predicates.my-pred.int", "84");
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    @Test(expected = ConfigException.class)
    public void negatedButNoPredicate() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        props.put("connector.class", TestConnector.class.getName());
        props.put("transforms", "a");
        props.put("transforms.a.type", SimpleTransformation.class.getName());
        props.put("transforms.a.magic.number", "42");
        props.put("transforms.a.negate", "true");
        new ConnectorConfig(MOCK_PLUGINS, props);
    }

    public static class TestPredicate<R extends ConnectRecord<R>> implements Predicate<R>  {

        int param;

        public TestPredicate() { }

        @Override
        public ConfigDef config() {
            return new ConfigDef().define("int", ConfigDef.Type.INT, 80, ConfigDef.Range.atLeast(80), ConfigDef.Importance.MEDIUM,
                    "A test parameter");
        }

        @Override
        public boolean test(R record) {
            return false;
        }

        @Override
        public void close() {
            param = 0;
        }

        @Override
        public void configure(Map<String, ?> configs) {
            param = Integer.parseInt((String) configs.get("int"));
        }
    }

    public static abstract class AbstractTestPredicate<R extends ConnectRecord<R>> implements Predicate<R>  {

        public AbstractTestPredicate() { }

    }

    public static abstract class AbstractTransformation<R extends ConnectRecord<R>> implements Transformation<R>  {

    }

    public static abstract class AbstractKeyValueTransformation<R extends ConnectRecord<R>> implements Transformation<R>  {
        @Override
        public R apply(R record) {
            return null;
        }

        @Override
        public ConfigDef config() {
            return new ConfigDef();
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }


        public static class Key extends AbstractKeyValueTransformation {


        }
        public static class Value extends AbstractKeyValueTransformation {

        }
    }


}
