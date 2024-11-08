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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;
import java.util.Optional;

/**
 * test: run any test without "quarantined" tag and that is not auto-quarantined
 */
public class QuarantinedExecutionCondition implements ExecutionCondition {

    private final QuarantinedTestSelector selector;

    public QuarantinedExecutionCondition() {
        selector = QuarantinedDataLoader.loadTestCatalog();
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        boolean runQuarantined = System.getProperty("kafka.test.run.quarantined", "false")
            .equalsIgnoreCase("true");

        Optional<Method> methodOpt = extensionContext.getTestMethod();
        if (methodOpt.isPresent()) {
            Class<?> clazz = extensionContext.getRequiredTestClass();

            // Check if test is quarantined
            Optional<String> reason = selector.selectQuarantined(
                clazz.getCanonicalName(), methodOpt.get().getName());

            // If the  only run if we've been given "kafka.test.run.quarantined=true"
            if (reason.isPresent()) {
                if (runQuarantined) {
                    return ConditionEvaluationResult.enabled("run quarantined test");
                } else {
                    return ConditionEvaluationResult.disabled("skip quarantined test", reason.get());
                }
            }

            // Only include "flaky" tag if given "kafka.test.run.quarantined=true", otherwise skip it
            boolean isFlaky = extensionContext.getTags().contains("flaky");
            if (runQuarantined) {
                if (isFlaky) {
                    return ConditionEvaluationResult.enabled("run flaky test");
                } else {
                    return ConditionEvaluationResult.disabled("skip non-flaky test");
                }
            } else {
                if (isFlaky) {
                    return ConditionEvaluationResult.disabled("skip flaky test");
                } else {
                    // Base case
                    return ConditionEvaluationResult.enabled(null);
                }
            }
        } else {
            // No method. Just return
            return ConditionEvaluationResult.enabled(null);
        }
    }
}
