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
package org.apache.kafka.network.quota;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IpQuotaEntity implements ConnectionQuotaEntity {

    public static final String IP_METRIC_TAG = "ip";
    public static final String IP_THROTTLE_PREFIX = "ip-";
    private static final long INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS = TimeUnit.HOURS.toSeconds(1);

    private final InetAddress ip;

    public IpQuotaEntity(InetAddress ip) {
        this.ip = ip;
    }

    @Override
    public String sensorName() {
        return ConnectionQuotaEntity.CONNECTION_RATE_SENSOR_NAME + "-" + ip.getHostAddress();
    }

    @Override
    public String metricName() {
        return ConnectionQuotaEntity.CONNECTION_RATE_METRIC_NAME;
    }

    @Override
    public long sensorExpiration() {
        return INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS;
    }

    @Override
    public Map<String, String> metricTags() {
        return Collections.singletonMap(IP_METRIC_TAG, ip.getHostAddress());
    }
}
