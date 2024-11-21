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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Objects;

public class AutoOffsetResetStrategy {
    public static final String EARLIEST_STRATEGY_NAME = "earliest";
    public static final String LATEST_STRATEGY_NAME = "latest";
    public static final String NONE_STRATEGY_NAME = "none";

    public static final AutoOffsetResetStrategy EARLIEST = new AutoOffsetResetStrategy(EARLIEST_STRATEGY_NAME);
    public static final AutoOffsetResetStrategy LATEST = new AutoOffsetResetStrategy(LATEST_STRATEGY_NAME);
    public static final AutoOffsetResetStrategy NONE = new AutoOffsetResetStrategy(NONE_STRATEGY_NAME);

    private final String name;

    private AutoOffsetResetStrategy(String name) {
        this.name = name;
    }

    public static boolean isValid(String offsetStrategy) {
        return EARLIEST_STRATEGY_NAME.equals(offsetStrategy) ||
                LATEST_STRATEGY_NAME.equals(offsetStrategy) ||
                NONE_STRATEGY_NAME.equals(offsetStrategy);
    }

    public static AutoOffsetResetStrategy fromString(String offsetStrategy) {
        if (offsetStrategy == null) {
            throw new IllegalArgumentException("auto offset reset strategy is null");
        }
        switch (offsetStrategy) {
            case EARLIEST_STRATEGY_NAME:
                return EARLIEST;
            case LATEST_STRATEGY_NAME:
                return LATEST;
            case NONE_STRATEGY_NAME:
                return NONE;
            default:
                throw new IllegalArgumentException("Unknown auto offset reset strategy: " + offsetStrategy);
        }
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoOffsetResetStrategy that = (AutoOffsetResetStrategy) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }

    @Override
    public String toString() {
        return "AutoOffsetResetStrategy{" +
                "name='" + name + '\'' +
                '}';
    }

    public static class Validator implements ConfigDef.Validator {
        @Override
        public void ensureValid(String name, Object value) {
            String strategy = (String) value;
            if (!AutoOffsetResetStrategy.isValid(strategy)) {
                throw new ConfigException(name, value, "Invalid value " + strategy + " for configuration " +
                        name + ": the value must be either 'earliest', 'latest', or 'none'.");
            }
        }
    }
}
