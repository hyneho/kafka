package org.apache.kafka.connect.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PluginVersionUtils {

    private static Plugins plugins = null;

    public static final String UNDEFINED_VERSION = "undefined";

    public static void setPlugins(Plugins plugins) {
        PluginVersionUtils.plugins = plugins;
    }

    public static VersionRange connectorVersionRequirement(String version) throws InvalidVersionSpecificationException {
        if (version == null || version.equals("latest")) {
            return null;
        }
        version = version.trim();

        // check first if the given version is valid
        VersionRange range = VersionRange.createFromVersionSpec(version);

        // now if the version is not enclosed we consider it as a hard requirement and enclose it in []
        if (range.hasRestrictions()) {
            return range;
        }
        version = "[" + version + "]";
        return VersionRange.createFromVersionSpec(version);
    }

    public static class PluginVersionValidator implements ConfigDef.Validator {

        @Override
        public void ensureValid(String name, Object value) {

            try {
                connectorVersionRequirement((String) value);
            } catch (InvalidVersionSpecificationException e) {
                throw new ConfigException(name, value, e.getMessage());
            }
        }

    }

    public static <T> String getVersionOrUndefined(T obj) {
        if (obj == null) {
            return UNDEFINED_VERSION;
        }
        try (LoaderSwap swap = Plugins.swapLoader(obj.getClass().getClassLoader())) {
            if (obj instanceof Versioned) {
                return ((Versioned) obj).version();
            }
        }
        return UNDEFINED_VERSION;
    }

    public static class ConnectorPluginVersionRecommender implements ConfigDef.Recommender {

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

    public static class ConverterPluginRecommender implements ConfigDef.Recommender {

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

    public static class HeaderConverterPluginRecommender implements ConfigDef.Recommender {

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

    public static abstract class ConverterPluginVersionRecommender implements ConfigDef.Recommender {

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

    public static class KeyConverterPluginVersionRecommender
            extends PluginVersionUtils.ConverterPluginVersionRecommender {

        @Override
        protected String converterConfig() {
            return ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
        }

    }

    public static class ValueConverterPluginVersionRecommender
            extends PluginVersionUtils.ConverterPluginVersionRecommender {

        @Override
        protected String converterConfig() {
            return ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
        }
    }

    public static class HeaderConverterPluginVersionRecommender
            extends PluginVersionUtils.ConverterPluginVersionRecommender {

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

    // Recommender for transformation and predicate plugins
    public static abstract class SMTPluginRecommender<T> implements ConfigDef.Recommender {

        abstract protected Function<String, Set<PluginDesc<T>>> plugins();

        protected final String classOrAliasConfig;

        public SMTPluginRecommender(String classOrAliasConfig) {
            this.classOrAliasConfig = classOrAliasConfig;
        }

        @Override
        @SuppressWarnings({"rawtypes"})
        public List<Object> validValues(String name, Map<String, Object> parsedConfig) {
            if (plugins == null) {
                return Collections.emptyList();
            }
            if (parsedConfig.get(classOrAliasConfig) == null) {
                return Collections.emptyList();
            }

            Class classOrAlias = (Class) parsedConfig.get(classOrAliasConfig);
            return plugins().apply(classOrAlias.getName())
                    .stream().map(PluginDesc::version).distinct().collect(Collectors.toList());
        }

        @Override
        public boolean visible(String name, Map<String, Object> parsedConfig) {
            return true;
        }
    }

    public static class TransformationPluginRecommender extends SMTPluginRecommender<Transformation<?>> {

        public TransformationPluginRecommender(String classOrAliasConfig) {
            super(classOrAliasConfig);
        }

        @Override
        protected Function<String, Set<PluginDesc<Transformation<?>>>> plugins() {
            return classOrAlias -> plugins.transformations(classOrAlias);
        }
    }

    public static class PredicatePluginRecommender extends SMTPluginRecommender<Predicate<?>> {

        public PredicatePluginRecommender(String classOrAliasConfig) {
            super(classOrAliasConfig);
        }

        @Override
        protected Function<String, Set<PluginDesc<Predicate<?>>>> plugins() {
            return classOrAlias -> plugins.predicates(classOrAlias);
        }
    }
}



