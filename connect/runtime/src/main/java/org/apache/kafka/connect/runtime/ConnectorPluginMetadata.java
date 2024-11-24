package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.util.PluginVersionUtils;

public class ConnectorPluginMetadata {

    private final String connectorClass;
    private final String connectorVersion;
    private final ConnectorType connectorType;
    private final String taskClass;
    private final String taskVersion;
    private final String keyConverterClass;
    private final String keyConverterVersion;
    private final String valueConverterClass;
    private final String valueConverterVersion;
    private final String headerConverterClass;
    private final String headerConverterVersion;

    private static final String UNDEFINED_VERSION = "undefined";

    public ConnectorPluginMetadata(
            Connector connector,
            Task task,
            Converter keyConverter,
            Converter valueConverter,
            Converter headerConverter,
            TransformationChain transformationChain
    ) {

        assert connector != null;
        assert task != null;
        assert keyConverter != null;
        assert valueConverter != null;
        assert headerConverter != null;

        this.connectorClass = connector.getClass().getName();
        this.connectorVersion = PluginVersionUtils.getVersionOrUndefined(connector);
        this.connectorType = getConnectorType(connector);
        this.taskClass = task.getClass().getName();
        this.taskVersion = PluginVersionUtils.getVersionOrUndefined(task);
        this.keyConverterClass = keyConverter.getClass().getName();
        this.keyConverterVersion = PluginVersionUtils.getVersionOrUndefined(keyConverter);
        this.valueConverterClass = valueConverter.getClass().getName();
        this.valueConverterVersion = PluginVersionUtils.getVersionOrUndefined(valueConverter);
        this.headerConverterClass = headerConverter.getClass().getName();
        this.headerConverterVersion = PluginVersionUtils.getVersionOrUndefined(headerConverter);
    }


    public ConnectorType getConnectorType(Connector connector) {
        try (LoaderSwap swap = Plugins.swapLoader(connector.getClass().getClassLoader())) {
            if (connector instanceof SourceConnector) {
                return ConnectorType.SOURCE;
            } else if (connector instanceof SinkConnector) {
                return ConnectorType.SINK;
            } else {
                return ConnectorType.UNKNOWN;
            }
        }
    }
}
