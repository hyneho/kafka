package org.apache.kafka.connect.runtime.isolation;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PluginsRecommenders {

    private final Plugins plugins;
    private final ConverterPluginRecommender converterPluginRecommender;
    private final ConnectorPluginVersionRecommender connectorPluginVersionRecommender;
    private final HeaderConverterPluginRecommender headerConverterPluginRecommender;
    private final KeyConverterPluginVersionRecommender keyConverterPluginVersionRecommender;
    private final ValueConverterPluginVersionRecommender valueConverterPluginVersionRecommender;
    private final HeaderConverterPluginVersionRecommender headerConverterPluginVersionRecommender;

    public PluginsRecommenders() {
        this(null);
    }

    public PluginsRecommenders(Plugins plugins) {
        this.plugins = plugins;
        this.converterPluginRecommender = new ConverterPluginRecommender();
        this.connectorPluginVersionRecommender = new ConnectorPluginVersionRecommender();
        this.headerConverterPluginRecommender = new HeaderConverterPluginRecommender();
        this.keyConverterPluginVersionRecommender = new KeyConverterPluginVersionRecommender();
        this.valueConverterPluginVersionRecommender = new ValueConverterPluginVersionRecommender();
        this.headerConverterPluginVersionRecommender = new HeaderConverterPluginVersionRecommender();
    }

    public ConverterPluginRecommender converterPluginRecommender() {
        return converterPluginRecommender;
    }

    public ConnectorPluginVersionRecommender connectorPluginVersionRecommender() {
        return connectorPluginVersionRecommender;
    }

    public HeaderConverterPluginRecommender headerConverterPluginRecommender() {
        return headerConverterPluginRecommender;
    }

    public KeyConverterPluginVersionRecommender keyConverterPluginVersionRecommender() {
        return keyConverterPluginVersionRecommender;
    }

    public ValueConverterPluginVersionRecommender valueConverterPluginVersionRecommender() {
        return valueConverterPluginVersionRecommender;
    }

    public HeaderConverterPluginVersionRecommender headerConverterPluginVersionRecommender() {
        return headerConverterPluginVersionRecommender;
    }

    public class ConnectorPluginVersionRecommender implements ConfigDef.Recommender {

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return Collections.emptyList();
            }
            String connectorClassOrAlias = (String) parsedConfig.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG);
            if (connectorClassOrAlias == null) {
                //should never happen
                return Collections.emptyList();
            }
            List<Object> sourceConnectors = plugins.sourceConnectors(connectorClassOrAlias).stream()
                    .map(PluginDesc::version).distinct().collect(Collectors.toList());
            if (!sourceConnectors.isEmpty()) {
                return sourceConnectors;
            }
            return plugins.sinkConnectors(connectorClassOrAlias).stream()
                    .map(PluginDesc::version).distinct().collect(Collectors.toList());
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return parsedConfig.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG) != null;
        }

    }

    public class ConverterPluginRecommender implements ConfigDef.Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return Collections.emptyList();
            }
            return plugins.converters().stream()
                    .map(PluginDesc::pluginClass).distinct().collect(Collectors.toList());
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    public class HeaderConverterPluginRecommender implements ConfigDef.Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return Collections.emptyList();
            }
            return plugins.headerConverters().stream()
                    .map(PluginDesc::pluginClass).distinct().collect(Collectors.toList());
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    public abstract class ConverterPluginVersionRecommender implements ConfigDef.Recommender {

        abstract protected String converterConfig();

        protected Function<String, List<Object>> recommendations() {
            return (converterClass) -> plugins.converters(converterClass).stream()
                    .map(PluginDesc::version).distinct().collect(Collectors.toList());
        }


        @SuppressWarnings({"rawtypes"})
        @Override
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return Collections.emptyList();
            }
            if (parsedConfig.get(converterConfig()) == null) {
                return Collections.emptyList();
            }
            Class converterClass = (Class) parsedConfig.get(converterConfig());
            return recommendations().apply(converterClass.getName());
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return parsedConfig.get(converterConfig()) != null;
        }
    }

    public class KeyConverterPluginVersionRecommender extends ConverterPluginVersionRecommender {

        @Override
        protected String converterConfig() {
            return ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
        }

    }

    public class ValueConverterPluginVersionRecommender extends ConverterPluginVersionRecommender {

        @Override
        protected String converterConfig() {
            return ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
        }
    }

    public class HeaderConverterPluginVersionRecommender extends ConverterPluginVersionRecommender {

        @Override
        protected String converterConfig() {
            return ConnectorConfig.HEADER_CONVERTER_CLASS_CONFIG;
        }

        @Override
        protected Function<String, List<Object>> recommendations() {
            return (converterClass) -> plugins.headerConverters(converterClass).stream()
                    .map(PluginDesc::version).distinct().collect(Collectors.toList());
        }
    }
}
