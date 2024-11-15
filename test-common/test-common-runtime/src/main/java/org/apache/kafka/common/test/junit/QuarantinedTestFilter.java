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

package org.apache.kafka.common.test.junit;

import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.TestTag;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.PostDiscoveryFilter;

import java.util.Optional;

public class QuarantinedTestFilter implements PostDiscoveryFilter {

    public static final String RUN_QUARANTINED_PROP = "kafka.test.run.quarantined";
    private static final TestTag FLAKY_TEST_TAG = TestTag.create("flaky");

    private final QuarantinedTestSelector selector;

    public QuarantinedTestFilter() {
        selector = QuarantinedDataLoader.loadTestCatalog();
    }

    @Override
    public FilterResult apply(TestDescriptor testDescriptor) {
        Optional<TestSource> sourceOpt = testDescriptor.getSource();
        if (sourceOpt.isEmpty()) {
            return FilterResult.included(null);
        }

        TestSource source = sourceOpt.get();
        if (!(source instanceof MethodSource)) {
            return FilterResult.included(null);
        }

        boolean runQuarantined = System.getProperty(RUN_QUARANTINED_PROP, "false")
            .equalsIgnoreCase("true");

        MethodSource methodSource = (MethodSource) source;

        // Check if test is quarantined
        Optional<String> reason = selector.selectQuarantined(
            methodSource.getClassName(), methodSource.getMethodName());

        // If the  only run if we've been given "kafka.test.run.quarantined=true"
        if (reason.isPresent()) {
            if (runQuarantined) {
                return FilterResult.included("run quarantined test");
            } else {
                return FilterResult.excluded(reason.get());
            }
        }

        // Only include "flaky" tag if given "kafka.test.run.quarantined=true", otherwise skip it
        boolean isFlaky = testDescriptor.getTags().contains(FLAKY_TEST_TAG);
        if (runQuarantined) {
            if (isFlaky) {
                return FilterResult.included("run flaky test");
            } else {
                return FilterResult.excluded("skip non-flaky test");
            }
        } else {
            if (isFlaky) {
                return FilterResult.excluded("skip flaky test");
            } else {
                // Base case
                return FilterResult.included(null);
            }
        }
    }
}
