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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class RemoveMembersFromConsumerGroupResultTest {

    private final String instanceOne = "instance-1";
    private final String instanceTwo = "instance-2";
    private List<MemberIdentity> memberIdentities;
    private Map<MemberIdentity, Errors> errorsMap;

    private KafkaFutureImpl<Map<MemberIdentity, Errors>> memberFutures;

    @Before
    public void setUp() {
        memberFutures = new KafkaFutureImpl<>();
        memberIdentities = Arrays.asList(
            new MemberIdentity().setGroupInstanceId(instanceOne),
            new MemberIdentity().setGroupInstanceId(instanceTwo)
        );
        errorsMap = new HashMap<>();
        errorsMap.put(memberIdentities.get(0), Errors.NONE);
        errorsMap.put(memberIdentities.get(1), Errors.FENCED_INSTANCE_ID);
    }

    @Test
    public void testTopLevelErrorConstructor() throws InterruptedException {
        memberFutures.completeExceptionally(Errors.GROUP_AUTHORIZATION_FAILED.exception());
        RemoveMembersFromConsumerGroupResult topLevelErrorResult =
            new RemoveMembersFromConsumerGroupResult(memberFutures, memberIdentities);
        TestUtils.assertFutureError(topLevelErrorResult.all(), GroupAuthorizationException.class);
    }

    @Test
    public void testMemberLevelErrorConstructor() throws InterruptedException, ExecutionException {
        memberFutures.complete(errorsMap);
        assertFalse(memberFutures.isCompletedExceptionally());
        RemoveMembersFromConsumerGroupResult memberLevelErrorResult =
            new RemoveMembersFromConsumerGroupResult(memberFutures, memberIdentities);

        TestUtils.assertFutureError(memberLevelErrorResult.all(), FencedInstanceIdException.class);
        assertNull(memberLevelErrorResult.memberResult(memberIdentities.get(0)).get());
        TestUtils.assertFutureError(memberLevelErrorResult.memberResult(memberIdentities.get(1)), FencedInstanceIdException.class);
    }

    @Test
    public void testMemberMissingErrorConstructor() throws InterruptedException, ExecutionException {
        errorsMap.remove(memberIdentities.get(1));
        memberFutures.complete(errorsMap);
        assertFalse(memberFutures.isCompletedExceptionally());
        RemoveMembersFromConsumerGroupResult missingMemberResult =
            new RemoveMembersFromConsumerGroupResult(memberFutures, memberIdentities);

        TestUtils.assertFutureError(missingMemberResult.all(), IllegalArgumentException.class);
        assertNull(missingMemberResult.memberResult(memberIdentities.get(0)).get());
        TestUtils.assertFutureError(missingMemberResult.memberResult(memberIdentities.get(1)), IllegalArgumentException.class);
    }

    @Test
    public void testNoErrorConstructor() throws ExecutionException, InterruptedException {
        Map<MemberIdentity, Errors> errorsMap = new HashMap<>();
        errorsMap.put(memberIdentities.get(0), Errors.NONE);
        errorsMap.put(memberIdentities.get(1), Errors.NONE);
        RemoveMembersFromConsumerGroupResult noErrorResult =
            new RemoveMembersFromConsumerGroupResult(memberFutures, memberIdentities);
        memberFutures.complete(errorsMap);

        assertNull(noErrorResult.all().get());
        assertNull(noErrorResult.memberResult(memberIdentities.get(0)).get());
        assertNull(noErrorResult.memberResult(memberIdentities.get(1)).get());
    }
}
