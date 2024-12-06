package org.apache.kafka.connect.util;

import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.maven.artifact.versioning.InvalidVersionSpecificationException;
import org.apache.maven.artifact.versioning.VersionRange;

public class PluginVersionUtils {

    private static Plugins plugins = null;

    public static void setPlugins(Plugins plugins) {
        PluginVersionUtils.plugins = plugins;
    }

    public static VersionRange connectorVersionRequirement(String version) throws InvalidVersionSpecificationException {
        if (version == null || version.equals("latest")) {
            return null;
        }
        version = version.trim();

        // check first if the given version is valid
        VersionRange.createFromVersionSpec(version);

        // now if the version is not enclosed we consider it as a hard requirement and enclose it in []
        if (!version.startsWith("[") && !version.startsWith("(")) {
            version = "[" + version + "]";
        }
        return VersionRange.createFromVersionSpec(version);
    }
}