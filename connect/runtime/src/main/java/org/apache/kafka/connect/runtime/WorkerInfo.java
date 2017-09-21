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

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Connect Worker system and runtime information.
 */
public class WorkerInfo {
    private static final Logger log = LoggerFactory.getLogger(WorkerInfo.class);
    private static final RuntimeMXBean RUNTIME;

    static {
        RUNTIME = ManagementFactory.getRuntimeMXBean();
    }

    private final Map<String, Object> values;

    /**
     * Constructor.
     */
    public WorkerInfo() {
        this.values = new LinkedHashMap<>();
        addRuntimeInfo();
    }

    /**
     * Log the values of this object at level INFO.
     */
    // Equivalent to logAll in AbstractConfig
    public void logAll() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName());
        b.append(" values: ");
        b.append(Utils.NL);

        for (Map.Entry<String, Object> entry : new TreeMap<>(values).entrySet()) {
            b.append('\t');
            b.append(entry.getKey());
            b.append(" = ");
            b.append(format(entry.getValue()));
            b.append(Utils.NL);
        }
        log.info(b.toString());
    }

    private static Object format(Object value) {
        return value == null ? "NA" : value;
    }

    /**
     * Collect general runtime information.
     */
    protected void addRuntimeInfo() {
        List<String> jvmArgs = RUNTIME.getInputArguments();
        values.put("jvm.args", jvmArgs);
    }

}
