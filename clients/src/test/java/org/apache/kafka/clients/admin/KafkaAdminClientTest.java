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

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResultCollection;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResultCollection;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResultCollection;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData.DescribeLogDirsTopic;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.CredentialInfo;
import org.apache.kafka.common.message.ElectLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection;
import org.apache.kafka.common.message.UnregisterBrokerResponseData;
import org.apache.kafka.common.message.WriteTxnMarkersResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.requests.AlterClientQuotasResponse;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse;
import org.apache.kafka.common.requests.AlterUserScramCredentialsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeClientQuotasResponse;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.DescribeProducersResponse;
import org.apache.kafka.common.requests.DescribeTransactionsRequest;
import org.apache.kafka.common.requests.DescribeTransactionsResponse;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsResponse;
import org.apache.kafka.common.requests.ElectLeadersResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.requests.ListPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.ListTransactionsRequest;
import org.apache.kafka.common.requests.ListTransactionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetDeleteResponse;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.UnregisterBrokerResponse;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.UpdateFeaturesResponse;
import org.apache.kafka.common.requests.WriteTxnMarkersRequest;
import org.apache.kafka.common.requests.WriteTxnMarkersResponse;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.admin.KafkaAdminClient.LEAVE_GROUP_REASON;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import static org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment;
import static org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A unit test for KafkaAdminClient.
 *
 * See AdminClientIntegrationTest for an integration test.
 */
@Timeout(120)
public class KafkaAdminClientTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaAdminClientTest.class);
    private static final String GROUP_ID = "group-0";

    @Test
    public void testDefaultApiTimeoutAndRequestTimeoutConflicts() {
        final AdminClientConfig config = newConfMap(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "500");
        KafkaException exception = assertThrows(KafkaException.class,
            () -> KafkaAdminClient.createInternal(config, null));
        assertTrue(exception.getCause() instanceof ConfigException);
    }

    @Test
    public void testGetOrCreateListValue() {
        Map<String, List<String>> map = new HashMap<>();
        List<String> fooList = KafkaAdminClient.getOrCreateListValue(map, "foo");
        assertNotNull(fooList);
        fooList.add("a");
        fooList.add("b");
        List<String> fooList2 = KafkaAdminClient.getOrCreateListValue(map, "foo");
        assertEquals(fooList, fooList2);
        assertTrue(fooList2.contains("a"));
        assertTrue(fooList2.contains("b"));
        List<String> barList = KafkaAdminClient.getOrCreateListValue(map, "bar");
        assertNotNull(barList);
        assertTrue(barList.isEmpty());
    }

    @Test
    public void testCalcTimeoutMsRemainingAsInt() {
        assertEquals(0, KafkaAdminClient.calcTimeoutMsRemainingAsInt(1000, 1000));
        assertEquals(100, KafkaAdminClient.calcTimeoutMsRemainingAsInt(1000, 1100));
        assertEquals(Integer.MAX_VALUE, KafkaAdminClient.calcTimeoutMsRemainingAsInt(0, Long.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, KafkaAdminClient.calcTimeoutMsRemainingAsInt(Long.MAX_VALUE, 0));
    }

    @Test
    public void testPrettyPrintException() {
        assertEquals("Null exception.", KafkaAdminClient.prettyPrintException(null));
        assertEquals("TimeoutException", KafkaAdminClient.prettyPrintException(new TimeoutException()));
        assertEquals("TimeoutException: The foobar timed out.",
                KafkaAdminClient.prettyPrintException(new TimeoutException("The foobar timed out.")));
    }

    private static Map<String, Object> newStrMap(String... vals) {
        Map<String, Object> map = new HashMap<>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121");
        map.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        if (vals.length % 2 != 0) {
            throw new IllegalStateException();
        }
        for (int i = 0; i < vals.length; i += 2) {
            map.put(vals[i], vals[i + 1]);
        }
        return map;
    }

    private static AdminClientConfig newConfMap(String... vals) {
        return new AdminClientConfig(newStrMap(vals));
    }

    @Test
    public void testGenerateClientId() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String id = KafkaAdminClient.generateClientId(newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, ""));
            assertFalse(ids.contains(id), "Got duplicate id " + id);
            ids.add(id);
        }
        assertEquals("myCustomId",
                KafkaAdminClient.generateClientId(newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, "myCustomId")));
    }

    private static Cluster mockCluster(int numNodes, int controllerIndex) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; i++)
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        return new Cluster("mockClusterId", nodes.values(),
            Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), nodes.get(controllerIndex));
    }

    private static Cluster mockBootstrapCluster() {
        return Cluster.bootstrap(ClientUtils.parseAndValidateAddresses(
                singletonList("localhost:8121"), ClientDnsLookup.USE_ALL_DNS_IPS));
    }

    private static AdminClientUnitTestEnv mockClientEnv(String... configVals) {
        return new AdminClientUnitTestEnv(mockCluster(3, 0), configVals);
    }

    private static AdminClientUnitTestEnv mockClientEnv(Time time, String... configVals) {
        return new AdminClientUnitTestEnv(time, mockCluster(3, 0), configVals);
    }

    @Test
    public void testCloseAdminClient() {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
        }
    }

    /**
     * Test if admin client can be closed in the callback invoked when
     * an api call completes. If calling {@link Admin#close()} in callback, AdminClient thread hangs
     */
    @Test @Timeout(10)
    public void testCloseAdminClientInCallback() throws InterruptedException {
        MockTime time = new MockTime();
        AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, mockCluster(3, 0));

        final ListTopicsResult result = env.adminClient().listTopics(new ListTopicsOptions().timeoutMs(1000));
        final KafkaFuture<Collection<TopicListing>> kafkaFuture = result.listings();
        final Semaphore callbackCalled = new Semaphore(0);
        kafkaFuture.whenComplete((topicListings, throwable) -> {
            env.close();
            callbackCalled.release();
        });

        time.sleep(2000); // Advance time to timeout and complete listTopics request
        callbackCalled.acquire();
    }

    private static OffsetDeleteResponse prepareOffsetDeleteResponse(Errors error) {
        return new OffsetDeleteResponse(
            new OffsetDeleteResponseData()
                .setErrorCode(error.code())
                .setTopics(new OffsetDeleteResponseTopicCollection())
        );
    }

    private static OffsetDeleteResponse prepareOffsetDeleteResponse(String topic, int partition, Errors error) {
        return new OffsetDeleteResponse(
            new OffsetDeleteResponseData()
                .setErrorCode(Errors.NONE.code())
                .setTopics(new OffsetDeleteResponseTopicCollection(Stream.of(
                    new OffsetDeleteResponseTopic()
                        .setName(topic)
                        .setPartitions(new OffsetDeleteResponsePartitionCollection(Collections.singletonList(
                            new OffsetDeleteResponsePartition()
                                .setPartitionIndex(partition)
                                .setErrorCode(error.code())
                        ).iterator()))
                ).collect(Collectors.toList()).iterator()))
        );
    }

    private static OffsetCommitResponse prepareOffsetCommitResponse(TopicPartition tp, Errors error) {
        Map<TopicPartition, Errors> responseData = new HashMap<>();
        responseData.put(tp, error);
        return new OffsetCommitResponse(0, responseData);
    }

    private static CreateTopicsResponse prepareCreateTopicsResponse(String topicName, Errors error) {
        CreateTopicsResponseData data = new CreateTopicsResponseData();
        data.topics().add(new CreatableTopicResult()
            .setName(topicName)
            .setErrorCode(error.code()));
        return new CreateTopicsResponse(data);
    }

    public static CreateTopicsResponse prepareCreateTopicsResponse(int throttleTimeMs, CreatableTopicResult... topics) {
        CreateTopicsResponseData data = new CreateTopicsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setTopics(new CreatableTopicResultCollection(Arrays.stream(topics).iterator()));
        return new CreateTopicsResponse(data);
    }

    public static CreatableTopicResult creatableTopicResult(String name, Errors error) {
        return new CreatableTopicResult()
            .setName(name)
            .setErrorCode(error.code());
    }

    public static DeleteTopicsResponse prepareDeleteTopicsResponse(int throttleTimeMs, DeletableTopicResult... topics) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setResponses(new DeletableTopicResultCollection(Arrays.stream(topics).iterator()));
        return new DeleteTopicsResponse(data);
    }

    public static DeletableTopicResult deletableTopicResult(String topicName, Errors error) {
        return new DeletableTopicResult()
            .setName(topicName)
            .setErrorCode(error.code());
    }

    public static DeletableTopicResult deletableTopicResultWithId(Uuid topicId, Errors error) {
        return new DeletableTopicResult()
                .setTopicId(topicId)
                .setErrorCode(error.code());
    }

    public static CreatePartitionsResponse prepareCreatePartitionsResponse(int throttleTimeMs, CreatePartitionsTopicResult... topics) {
        CreatePartitionsResponseData data = new CreatePartitionsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setResults(Arrays.asList(topics));
        return new CreatePartitionsResponse(data);
    }

    public static CreatePartitionsTopicResult createPartitionsTopicResult(String name, Errors error) {
        return createPartitionsTopicResult(name, error, null);
    }

    public static CreatePartitionsTopicResult createPartitionsTopicResult(String name, Errors error, String errorMessage) {
        return new CreatePartitionsTopicResult()
            .setName(name)
            .setErrorCode(error.code())
            .setErrorMessage(errorMessage);
    }

    private static DeleteTopicsResponse prepareDeleteTopicsResponse(String topicName, Errors error) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
            .setName(topicName)
            .setErrorCode(error.code()));
        return new DeleteTopicsResponse(data);
    }

    private static DeleteTopicsResponse prepareDeleteTopicsResponseWithTopicId(Uuid id, Errors error) {
        DeleteTopicsResponseData data = new DeleteTopicsResponseData();
        data.responses().add(new DeletableTopicResult()
                .setTopicId(id)
                .setErrorCode(error.code()));
        return new DeleteTopicsResponse(data);
    }

    private static FindCoordinatorResponse prepareFindCoordinatorResponse(Errors error, Node node) {
        return prepareFindCoordinatorResponse(error, GROUP_ID, node);
    }

    private static FindCoordinatorResponse prepareFindCoordinatorResponse(Errors error, String key, Node node) {
        return FindCoordinatorResponse.prepareResponse(error, key, node);
    }

    private static FindCoordinatorResponse prepareOldFindCoordinatorResponse(Errors error, Node node) {
        return FindCoordinatorResponse.prepareOldResponse(error, node);
    }

    private static MetadataResponse prepareMetadataResponse(Cluster cluster, Errors error) {
        return prepareMetadataResponse(cluster, error, error);
    }

    private static MetadataResponse prepareMetadataResponse(Cluster cluster, Errors topicError, Errors partitionError) {
        List<MetadataResponseTopic> metadata = new ArrayList<>();
        for (String topic : cluster.topics()) {
            List<MetadataResponsePartition> pms = new ArrayList<>();
            for (PartitionInfo pInfo : cluster.availablePartitionsForTopic(topic)) {
                MetadataResponsePartition pm  = new MetadataResponsePartition()
                    .setErrorCode(partitionError.code())
                    .setPartitionIndex(pInfo.partition())
                    .setLeaderId(pInfo.leader().id())
                    .setLeaderEpoch(234)
                    .setReplicaNodes(Arrays.stream(pInfo.replicas()).map(Node::id).collect(Collectors.toList()))
                    .setIsrNodes(Arrays.stream(pInfo.inSyncReplicas()).map(Node::id).collect(Collectors.toList()))
                    .setOfflineReplicas(Arrays.stream(pInfo.offlineReplicas()).map(Node::id).collect(Collectors.toList()));
                pms.add(pm);
            }
            MetadataResponseTopic tm = new MetadataResponseTopic()
                .setErrorCode(topicError.code())
                .setName(topic)
                .setIsInternal(false)
                .setPartitions(pms);
            metadata.add(tm);
        }
        return MetadataResponse.prepareResponse(true,
                0,
                cluster.nodes(),
                cluster.clusterResource().clusterId(),
                cluster.controller().id(),
                metadata,
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED);
    }

    private static DescribeGroupsResponseData prepareDescribeGroupsResponseData(String groupId,
                                                                                List<String> groupInstances,
                                                                                List<TopicPartition> topicPartitions) {
        final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
        List<DescribedGroupMember> describedGroupMembers = groupInstances.stream().map(groupInstance -> DescribeGroupsResponse.groupMember(JoinGroupRequest.UNKNOWN_MEMBER_ID,
                groupInstance, "clientId0", "clientHost", new byte[memberAssignment.remaining()], null)).collect(Collectors.toList());
        DescribeGroupsResponseData data = new DescribeGroupsResponseData();
        data.groups().add(DescribeGroupsResponse.groupMetadata(
                groupId,
                Errors.NONE,
                "",
                ConsumerProtocol.PROTOCOL_TYPE,
                "",
                describedGroupMembers,
                Collections.emptySet()));
        return data;
    }

    private static FeatureMetadata defaultFeatureMetadata() {
        return new FeatureMetadata(
            Utils.mkMap(Utils.mkEntry("test_feature_1", new FinalizedVersionRange((short) 2, (short) 3))),
            Optional.of(1L),
            Utils.mkMap(Utils.mkEntry("test_feature_1", new SupportedVersionRange((short) 1, (short) 5))));
    }

    private static Features<org.apache.kafka.common.feature.SupportedVersionRange> convertSupportedFeaturesMap(Map<String, SupportedVersionRange> features) {
        final Map<String, org.apache.kafka.common.feature.SupportedVersionRange> featuresMap = new HashMap<>();
        for (final Map.Entry<String, SupportedVersionRange> entry : features.entrySet()) {
            final SupportedVersionRange versionRange = entry.getValue();
            featuresMap.put(
                entry.getKey(),
                new org.apache.kafka.common.feature.SupportedVersionRange(versionRange.minVersion(),
                                                                          versionRange.maxVersion()));
        }

        return Features.supportedFeatures(featuresMap);
    }

    private static Features<org.apache.kafka.common.feature.FinalizedVersionRange> convertFinalizedFeaturesMap(Map<String, FinalizedVersionRange> features) {
        final Map<String, org.apache.kafka.common.feature.FinalizedVersionRange> featuresMap = new HashMap<>();
        for (final Map.Entry<String, FinalizedVersionRange> entry : features.entrySet()) {
            final FinalizedVersionRange versionRange = entry.getValue();
            featuresMap.put(
                entry.getKey(),
                new org.apache.kafka.common.feature.FinalizedVersionRange(
                    versionRange.minVersionLevel(), versionRange.maxVersionLevel()));
        }

        return Features.finalizedFeatures(featuresMap);
    }

    private static ApiVersionsResponse prepareApiVersionsResponseForDescribeFeatures(Errors error) {
        if (error == Errors.NONE) {
            return ApiVersionsResponse.createApiVersionsResponse(
                0,
                ApiVersionsResponse.filterApis(RecordVersion.current(), ApiMessageType.ListenerType.ZK_BROKER),
                convertSupportedFeaturesMap(defaultFeatureMetadata().supportedFeatures()),
                convertFinalizedFeaturesMap(defaultFeatureMetadata().finalizedFeatures()),
                defaultFeatureMetadata().finalizedFeaturesEpoch().get()
            );
        }
        return new ApiVersionsResponse(
            new ApiVersionsResponseData()
                .setThrottleTimeMs(0)
                .setErrorCode(error.code()));
    }

    /**
     * Test that the client properly times out when we don't receive any metadata.
     */
    @Test
    public void testTimeoutWithoutMetadata() throws Exception {
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, mockBootstrapCluster(),
                newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                    singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(1000)).all();
            TestUtils.assertFutureError(future, TimeoutException.class);
        }
    }

    @Test
    public void testConnectionFailureOnMetadataUpdate() throws Exception {
        // This tests the scenario in which we successfully connect to the bootstrap server, but
        // the server disconnects before sending the full response

        Cluster cluster = mockBootstrapCluster();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster)) {
            Cluster discoveredCluster = mockCluster(3, 0);
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(request -> request instanceof MetadataRequest, null, true);
            env.kafkaClient().prepareResponse(request -> request instanceof MetadataRequest,
                    RequestTestUtils.metadataResponse(discoveredCluster.nodes(), discoveredCluster.clusterResource().clusterId(),
                            1, Collections.emptyList()));
            env.kafkaClient().prepareResponse(body -> body instanceof CreateTopicsRequest,
                    prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                    singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            future.get();
        }
    }

    @Test
    public void testUnreachableBootstrapServer() throws Exception {
        // This tests the scenario in which the bootstrap server is unreachable for a short while,
        // which prevents AdminClient from being able to send the initial metadata request

        Cluster cluster = Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 8121)));
        Map<Node, Long> unreachableNodes = Collections.singletonMap(cluster.nodes().get(0), 200L);
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                AdminClientUnitTestEnv.clientConfigs(), unreachableNodes)) {
            Cluster discoveredCluster = mockCluster(3, 0);
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(body -> body instanceof MetadataRequest,
                    RequestTestUtils.metadataResponse(discoveredCluster.nodes(), discoveredCluster.clusterResource().clusterId(),
                            1, Collections.emptyList()));
            env.kafkaClient().prepareResponse(body -> body instanceof CreateTopicsRequest,
                prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                    singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();

            future.get();
        }
    }

    /**
     * Test that we propagate exceptions encountered when fetching metadata.
     */
    @Test
    public void testPropagatedMetadataFetchException() throws Exception {
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM,
                mockCluster(3, 0),
                newStrMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121",
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().createPendingAuthenticationError(env.cluster().nodeById(0),
                    TimeUnit.DAYS.toMillis(1));
            env.kafkaClient().prepareResponse(prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                new CreateTopicsOptions().timeoutMs(1000)).all();
            TestUtils.assertFutureError(future, SaslAuthenticationException.class);
        }
    }

    @Test
    public void testCreateTopics() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("myTopic"),
                prepareCreateTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                    singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000)).all();
            future.get();
        }
    }

    @Test
    public void testCreateTopicsPartialResponse() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("myTopic", "myTopic2"),
                prepareCreateTopicsResponse("myTopic", Errors.NONE));
            CreateTopicsResult topicsResult = env.adminClient().createTopics(
                    asList(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2))),
                           new NewTopic("myTopic2", Collections.singletonMap(0, asList(0, 1, 2)))),
                    new CreateTopicsOptions().timeoutMs(10000));
            topicsResult.values().get("myTopic").get();
            TestUtils.assertFutureThrows(topicsResult.values().get("myTopic2"), ApiException.class);
        }
    }

    @Test
    public void testCreateTopicsRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
                mockCluster(3, 0),
                newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return body instanceof CreateTopicsRequest;
            }, null, true);

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return body instanceof CreateTopicsRequest;
            }, prepareCreateTopicsResponse("myTopic", Errors.NONE));

            KafkaFuture<Void> future = env.adminClient().createTopics(
                singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                new CreateTopicsOptions().timeoutMs(10000)).all();

            // Wait until the first attempt has failed, then advance the time
            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1,
                "Failed awaiting CreateTopics first request failure");

            // Wait until the retry call added to the queue in AdminClient
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1,
                "Failed to add retry CreateTopics call");

            time.sleep(retryBackoff);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, "CreateTopics retry did not await expected backoff");
        }
    }

    @Test
    public void testCreateTopicsHandleNotControllerException() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                prepareCreateTopicsResponse("myTopic", Errors.NOT_CONTROLLER),
                env.cluster().nodeById(0));
            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(env.cluster().nodes(),
                env.cluster().clusterResource().clusterId(),
                1,
                Collections.<MetadataResponse.TopicMetadata>emptyList()));
            env.kafkaClient().prepareResponseFrom(
                prepareCreateTopicsResponse("myTopic", Errors.NONE),
                env.cluster().nodeById(1));
            KafkaFuture<Void> future = env.adminClient().createTopics(
                singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
                new CreateTopicsOptions().timeoutMs(10000)).all();
            future.get();
        }
    }

    @Test
    public void testCreateTopicsRetryThrottlingExceptionWhenEnabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreateTopicsResponse(1000,
                    creatableTopicResult("topic1", Errors.NONE),
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    creatableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic2"),
                prepareCreateTopicsResponse(1000,
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic2"),
                prepareCreateTopicsResponse(0,
                    creatableTopicResult("topic2", Errors.NONE)));

            CreateTopicsResult result = env.adminClient().createTopics(
                asList(
                    new NewTopic("topic1", 1, (short) 1),
                    new NewTopic("topic2", 1, (short) 1),
                    new NewTopic("topic3", 1, (short) 1)),
                new CreateTopicsOptions().retryOnQuotaViolation(true));

            assertNull(result.values().get("topic1").get());
            assertNull(result.values().get("topic2").get());
            TestUtils.assertFutureThrows(result.values().get("topic3"), TopicExistsException.class);
        }
    }

    @Test
    public void testCreateTopicsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut() throws Exception {
        long defaultApiTimeout = 60000;
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = mockClientEnv(time,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(defaultApiTimeout))) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreateTopicsResponse(1000,
                    creatableTopicResult("topic1", Errors.NONE),
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    creatableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic2"),
                prepareCreateTopicsResponse(1000,
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            CreateTopicsResult result = env.adminClient().createTopics(
                asList(
                    new NewTopic("topic1", 1, (short) 1),
                    new NewTopic("topic2", 1, (short) 1),
                    new NewTopic("topic3", 1, (short) 1)),
                new CreateTopicsOptions().retryOnQuotaViolation(true));

            // Wait until the prepared attempts have consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                "Failed awaiting CreateTopics requests");

            // Wait until the next request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                "Failed awaiting next CreateTopics request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            assertNull(result.values().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(result.values().get("topic2"),
                ThrottlingQuotaExceededException.class);
            assertEquals(0, e.throttleTimeMs());
            TestUtils.assertFutureThrows(result.values().get("topic3"), TopicExistsException.class);
        }
    }

    @Test
    public void testCreateTopicsDontRetryThrottlingExceptionWhenDisabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreateTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreateTopicsResponse(1000,
                    creatableTopicResult("topic1", Errors.NONE),
                    creatableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    creatableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            CreateTopicsResult result = env.adminClient().createTopics(
                asList(
                    new NewTopic("topic1", 1, (short) 1),
                    new NewTopic("topic2", 1, (short) 1),
                    new NewTopic("topic3", 1, (short) 1)),
                new CreateTopicsOptions().retryOnQuotaViolation(false));

            assertNull(result.values().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(result.values().get("topic2"),
                ThrottlingQuotaExceededException.class);
            assertEquals(1000, e.throttleTimeMs());
            TestUtils.assertFutureThrows(result.values().get("topic3"), TopicExistsException.class);
        }
    }

    private MockClient.RequestMatcher expectCreateTopicsRequestWithTopics(final String... topics) {
        return body -> {
            if (body instanceof CreateTopicsRequest) {
                CreateTopicsRequest request = (CreateTopicsRequest) body;
                for (String topic : topics) {
                    if (request.data().topics().find(topic) == null)
                        return false;
                }
                return topics.length == request.data().topics().size();
            }
            return false;
        };
    }

    @Test
    public void testDeleteTopics() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("myTopic"),
                prepareDeleteTopicsResponse("myTopic", Errors.NONE));
            KafkaFuture<Void> future = env.adminClient().deleteTopics(singletonList("myTopic"),
                new DeleteTopicsOptions()).all();
            assertNull(future.get());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("myTopic"),
                prepareDeleteTopicsResponse("myTopic", Errors.TOPIC_DELETION_DISABLED));
            future = env.adminClient().deleteTopics(singletonList("myTopic"),
                new DeleteTopicsOptions()).all();
            TestUtils.assertFutureError(future, TopicDeletionDisabledException.class);

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("myTopic"),
                prepareDeleteTopicsResponse("myTopic", Errors.UNKNOWN_TOPIC_OR_PARTITION));
            future = env.adminClient().deleteTopics(singletonList("myTopic"),
                new DeleteTopicsOptions()).all();
            TestUtils.assertFutureError(future, UnknownTopicOrPartitionException.class);
            
            // With topic IDs
            Uuid topicId = Uuid.randomUuid();

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId),
                    prepareDeleteTopicsResponseWithTopicId(topicId, Errors.NONE));
            future = env.adminClient().deleteTopics(TopicCollection.ofTopicIds(singletonList(topicId)),
                    new DeleteTopicsOptions()).all();
            assertNull(future.get());

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId),
                    prepareDeleteTopicsResponseWithTopicId(topicId, Errors.TOPIC_DELETION_DISABLED));
            future = env.adminClient().deleteTopics(TopicCollection.ofTopicIds(singletonList(topicId)),
                    new DeleteTopicsOptions()).all();
            TestUtils.assertFutureError(future, TopicDeletionDisabledException.class);

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId),
                    prepareDeleteTopicsResponseWithTopicId(topicId, Errors.UNKNOWN_TOPIC_ID));
            future = env.adminClient().deleteTopics(TopicCollection.ofTopicIds(singletonList(topicId)),
                    new DeleteTopicsOptions()).all();
            TestUtils.assertFutureError(future, UnknownTopicIdException.class);
        }
    }
    

    @Test
    public void testDeleteTopicsPartialResponse() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("myTopic", "myOtherTopic"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("myTopic", Errors.NONE)));

            DeleteTopicsResult result = env.adminClient().deleteTopics(
                asList("myTopic", "myOtherTopic"), new DeleteTopicsOptions());

            result.topicNameValues().get("myTopic").get();
            TestUtils.assertFutureThrows(result.topicNameValues().get("myOtherTopic"), ApiException.class);
            
            // With topic IDs
            Uuid topicId1 = Uuid.randomUuid();
            Uuid topicId2 = Uuid.randomUuid();
            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId1, Errors.NONE)));

            DeleteTopicsResult resultIds = env.adminClient().deleteTopics(
                    TopicCollection.ofTopicIds(asList(topicId1, topicId2)), new DeleteTopicsOptions());

            resultIds.topicIdValues().get(topicId1).get();
            TestUtils.assertFutureThrows(resultIds.topicIdValues().get(topicId2), ApiException.class);
        }
    }

    @Test
    public void testDeleteTopicsRetryThrottlingExceptionWhenEnabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("topic1", Errors.NONE),
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic2"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic2"),
                prepareDeleteTopicsResponse(0,
                    deletableTopicResult("topic2", Errors.NONE)));

            DeleteTopicsResult result = env.adminClient().deleteTopics(
                asList("topic1", "topic2", "topic3"),
                new DeleteTopicsOptions().retryOnQuotaViolation(true));

            assertNull(result.topicNameValues().get("topic1").get());
            assertNull(result.topicNameValues().get("topic2").get());
            TestUtils.assertFutureThrows(result.topicNameValues().get("topic3"), TopicExistsException.class);
            
            // With topic IDs
            Uuid topicId1 = Uuid.randomUuid();
            Uuid topicId2 = Uuid.randomUuid();
            Uuid topicId3 = Uuid.randomUuid();
            
            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2, topicId3),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId1, Errors.NONE),
                            deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                            deletableTopicResultWithId(topicId3, Errors.UNKNOWN_TOPIC_ID)));

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId2),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED)));

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId2),
                    prepareDeleteTopicsResponse(0,
                            deletableTopicResultWithId(topicId2, Errors.NONE)));

            DeleteTopicsResult resultIds = env.adminClient().deleteTopics(
                    TopicCollection.ofTopicIds(asList(topicId1, topicId2, topicId3)),
                    new DeleteTopicsOptions().retryOnQuotaViolation(true));

            assertNull(resultIds.topicIdValues().get(topicId1).get());
            assertNull(resultIds.topicIdValues().get(topicId2).get());
            TestUtils.assertFutureThrows(resultIds.topicIdValues().get(topicId3), UnknownTopicIdException.class);
        }
    }

    @Test
    public void testDeleteTopicsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut() throws Exception {
        long defaultApiTimeout = 60000;
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = mockClientEnv(time,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(defaultApiTimeout))) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("topic1", Errors.NONE),
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic2"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            DeleteTopicsResult result = env.adminClient().deleteTopics(
                asList("topic1", "topic2", "topic3"),
                new DeleteTopicsOptions().retryOnQuotaViolation(true));

            // Wait until the prepared attempts have consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                "Failed awaiting DeleteTopics requests");

            // Wait until the next request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                "Failed awaiting next DeleteTopics request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            assertNull(result.topicNameValues().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(result.topicNameValues().get("topic2"),
                ThrottlingQuotaExceededException.class);
            assertEquals(0, e.throttleTimeMs());
            TestUtils.assertFutureThrows(result.topicNameValues().get("topic3"), TopicExistsException.class);
            
            // With topic IDs
            Uuid topicId1 = Uuid.randomUuid();
            Uuid topicId2 = Uuid.randomUuid();
            Uuid topicId3 = Uuid.randomUuid();
            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2, topicId3),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId1, Errors.NONE),
                            deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                            deletableTopicResultWithId(topicId3, Errors.UNKNOWN_TOPIC_ID)));

            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId2),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED)));

            DeleteTopicsResult resultIds = env.adminClient().deleteTopics(
                    TopicCollection.ofTopicIds(asList(topicId1, topicId2, topicId3)),
                    new DeleteTopicsOptions().retryOnQuotaViolation(true));

            // Wait until the prepared attempts have consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                    "Failed awaiting DeleteTopics requests");

            // Wait until the next request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                    "Failed awaiting next DeleteTopics request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            assertNull(resultIds.topicIdValues().get(topicId1).get());
            e = TestUtils.assertFutureThrows(resultIds.topicIdValues().get(topicId2),
                    ThrottlingQuotaExceededException.class);
            assertEquals(0, e.throttleTimeMs());
            TestUtils.assertFutureThrows(resultIds.topicIdValues().get(topicId3), UnknownTopicIdException.class);
        }
    }

    @Test
    public void testDeleteTopicsDontRetryThrottlingExceptionWhenDisabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectDeleteTopicsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareDeleteTopicsResponse(1000,
                    deletableTopicResult("topic1", Errors.NONE),
                    deletableTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    deletableTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            DeleteTopicsResult result = env.adminClient().deleteTopics(
                asList("topic1", "topic2", "topic3"),
                new DeleteTopicsOptions().retryOnQuotaViolation(false));

            assertNull(result.topicNameValues().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(result.topicNameValues().get("topic2"),
                ThrottlingQuotaExceededException.class);
            assertEquals(1000, e.throttleTimeMs());
            TestUtils.assertFutureError(result.topicNameValues().get("topic3"), TopicExistsException.class);

            // With topic IDs
            Uuid topicId1 = Uuid.randomUuid();
            Uuid topicId2 = Uuid.randomUuid();
            Uuid topicId3 = Uuid.randomUuid();
            env.kafkaClient().prepareResponse(
                    expectDeleteTopicsRequestWithTopicIds(topicId1, topicId2, topicId3),
                    prepareDeleteTopicsResponse(1000,
                            deletableTopicResultWithId(topicId1, Errors.NONE),
                            deletableTopicResultWithId(topicId2, Errors.THROTTLING_QUOTA_EXCEEDED),
                            deletableTopicResultWithId(topicId3, Errors.UNKNOWN_TOPIC_ID)));

            DeleteTopicsResult resultIds = env.adminClient().deleteTopics(
                    TopicCollection.ofTopicIds(asList(topicId1, topicId2, topicId3)),
                    new DeleteTopicsOptions().retryOnQuotaViolation(false));

            assertNull(resultIds.topicIdValues().get(topicId1).get());
            e = TestUtils.assertFutureThrows(resultIds.topicIdValues().get(topicId2),
                    ThrottlingQuotaExceededException.class);
            assertEquals(1000, e.throttleTimeMs());
            TestUtils.assertFutureError(resultIds.topicIdValues().get(topicId3), UnknownTopicIdException.class);
        }
    }

    private MockClient.RequestMatcher expectDeleteTopicsRequestWithTopics(final String... topics) {
        return body -> {
            if (body instanceof DeleteTopicsRequest) {
                DeleteTopicsRequest request = (DeleteTopicsRequest) body;
                return request.topicNames().equals(Arrays.asList(topics));
            }
            return false;
        };
    }

    private MockClient.RequestMatcher expectDeleteTopicsRequestWithTopicIds(final Uuid... topicIds) {
        return body -> {
            if (body instanceof DeleteTopicsRequest) {
                DeleteTopicsRequest request = (DeleteTopicsRequest) body;
                return request.topicIds().equals(Arrays.asList(topicIds));
            }
            return false;
        };
    }

    @Test
    public void testInvalidTopicNames() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            List<String> sillyTopicNames = asList("", null);
            Map<String, KafkaFuture<Void>> deleteFutures = env.adminClient().deleteTopics(sillyTopicNames).topicNameValues();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureError(deleteFutures.get(sillyTopicName), InvalidTopicException.class);
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());

            Map<String, KafkaFuture<TopicDescription>> describeFutures =
                    env.adminClient().describeTopics(sillyTopicNames).topicNameValues();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureError(describeFutures.get(sillyTopicName), InvalidTopicException.class);
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());

            List<NewTopic> newTopics = new ArrayList<>();
            for (String sillyTopicName : sillyTopicNames) {
                newTopics.add(new NewTopic(sillyTopicName, 1, (short) 1));
            }

            Map<String, KafkaFuture<Void>> createFutures = env.adminClient().createTopics(newTopics).values();
            for (String sillyTopicName : sillyTopicNames) {
                TestUtils.assertFutureError(createFutures .get(sillyTopicName), InvalidTopicException.class);
            }
            assertEquals(0, env.kafkaClient().inFlightRequestCount());
        }
    }

    @Test
    public void testMetadataRetries() throws Exception {
        // We should continue retrying on metadata update failures in spite of retry configuration

        String topic = "topic";
        Cluster bootstrapCluster = Cluster.bootstrap(singletonList(new InetSocketAddress("localhost", 9999)));
        Cluster initializedCluster = mockCluster(3, 0);

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, bootstrapCluster,
                newStrMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999",
                        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000000",
                        AdminClientConfig.RETRIES_CONFIG, "0"))) {

            // The first request fails with a disconnect
            env.kafkaClient().prepareResponse(null, true);

            // The next one succeeds and gives us the controller id
            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(initializedCluster.nodes(),
                    initializedCluster.clusterResource().clusterId(),
                    initializedCluster.controller().id(),
                    Collections.emptyList()));

            // Then we respond to the DescribeTopic request
            Node leader = initializedCluster.nodes().get(0);
            MetadataResponse.PartitionMetadata partitionMetadata = new MetadataResponse.PartitionMetadata(
                    Errors.NONE, new TopicPartition(topic, 0), Optional.of(leader.id()), Optional.of(10),
                    singletonList(leader.id()), singletonList(leader.id()), singletonList(leader.id()));
            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(initializedCluster.nodes(),
                    initializedCluster.clusterResource().clusterId(), 1,
                    singletonList(new MetadataResponse.TopicMetadata(Errors.NONE, topic, Uuid.ZERO_UUID, false,
                            singletonList(partitionMetadata), MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED))));

            DescribeTopicsResult result = env.adminClient().describeTopics(singleton(topic));
            Map<String, TopicDescription> topicDescriptions = result.allTopicNames().get();
            assertEquals(leader, topicDescriptions.get(topic).partitions().get(0).leader());
            assertNull(topicDescriptions.get(topic).authorizedOperations());
        }
    }

    @Test
    public void testAdminClientApisAuthenticationFailure() throws Exception {
        Cluster cluster = mockBootstrapCluster();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(Time.SYSTEM, cluster,
                newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().createPendingAuthenticationError(cluster.nodes().get(0),
                    TimeUnit.DAYS.toMillis(1));
            callAdminClientApisAndExpectAnAuthenticationError(env);
            callClientQuotasApisAndExpectAnAuthenticationError(env);
        }
    }

    private void callAdminClientApisAndExpectAnAuthenticationError(AdminClientUnitTestEnv env) throws InterruptedException {
        ExecutionException e = assertThrows(ExecutionException.class, () -> env.adminClient().createTopics(
            singleton(new NewTopic("myTopic", Collections.singletonMap(0, asList(0, 1, 2)))),
            new CreateTopicsOptions().timeoutMs(10000)).all().get());
        assertTrue(e.getCause() instanceof AuthenticationException,
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        Map<String, NewPartitions> counts = new HashMap<>();
        counts.put("my_topic", NewPartitions.increaseTo(3));
        counts.put("other_topic", NewPartitions.increaseTo(3, asList(asList(2), asList(3))));
        e = assertThrows(ExecutionException.class, () -> env.adminClient().createPartitions(counts).all().get());
        assertTrue(e.getCause() instanceof AuthenticationException,
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().createAcls(asList(ACL1, ACL2)).all().get());
        assertTrue(e.getCause() instanceof AuthenticationException,
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().describeAcls(FILTER1).values().get());
        assertTrue(e.getCause() instanceof AuthenticationException,
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().deleteAcls(asList(FILTER1, FILTER2)).all().get());
        assertTrue(e.getCause() instanceof AuthenticationException,
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        e = assertThrows(ExecutionException.class, () -> env.adminClient().describeConfigs(
            singleton(new ConfigResource(ConfigResource.Type.BROKER, "0"))).all().get());
        assertTrue(e.getCause() instanceof AuthenticationException,
            "Expected an authentication error, but got " + Utils.stackTrace(e));
    }

    private void callClientQuotasApisAndExpectAnAuthenticationError(AdminClientUnitTestEnv env) throws InterruptedException {
        ExecutionException e = assertThrows(ExecutionException.class,
            () -> env.adminClient().describeClientQuotas(ClientQuotaFilter.all()).entities().get());
        assertTrue(e.getCause() instanceof AuthenticationException,
            "Expected an authentication error, but got " + Utils.stackTrace(e));

        ClientQuotaEntity entity = new ClientQuotaEntity(Collections.singletonMap(ClientQuotaEntity.USER, "user"));
        ClientQuotaAlteration alteration = new ClientQuotaAlteration(entity, asList(new ClientQuotaAlteration.Op("consumer_byte_rate", 1000.0)));
        e = assertThrows(ExecutionException.class,
            () -> env.adminClient().alterClientQuotas(asList(alteration)).all().get());

        assertTrue(e.getCause() instanceof AuthenticationException,
            "Expected an authentication error, but got " + Utils.stackTrace(e));
    }

    private static final AclBinding ACL1 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic3", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));
    private static final AclBinding ACL2 = new AclBinding(new ResourcePattern(ResourceType.TOPIC, "mytopic4", PatternType.LITERAL),
        new AccessControlEntry("User:ANONYMOUS", "*", AclOperation.DESCRIBE, AclPermissionType.DENY));
    private static final AclBindingFilter FILTER1 = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:ANONYMOUS", null, AclOperation.ANY, AclPermissionType.ANY));
    private static final AclBindingFilter FILTER2 = new AclBindingFilter(new ResourcePatternFilter(ResourceType.ANY, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY));
    private static final AclBindingFilter UNKNOWN_FILTER = new AclBindingFilter(
        new ResourcePatternFilter(ResourceType.UNKNOWN, null, PatternType.LITERAL),
        new AccessControlEntryFilter("User:bob", null, AclOperation.ANY, AclPermissionType.ANY));

    @Test
    public void testDescribeAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where we get back ACL1 and ACL2.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(new DescribeAclsResponseData()
                .setResources(DescribeAclsResponse.aclsResources(asList(ACL1, ACL2))), ApiKeys.DESCRIBE_ACLS.latestVersion()));
            assertCollectionIs(env.adminClient().describeAcls(FILTER1).values().get(), ACL1, ACL2);

            // Test a call where we get back no results.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(new DescribeAclsResponseData(),
                    ApiKeys.DESCRIBE_ACLS.latestVersion()));
            assertTrue(env.adminClient().describeAcls(FILTER2).values().get().isEmpty());

            // Test a call where we get back an error.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(new DescribeAclsResponseData()
                .setErrorCode(Errors.SECURITY_DISABLED.code())
                .setErrorMessage("Security is disabled"), ApiKeys.DESCRIBE_ACLS.latestVersion()));
            TestUtils.assertFutureError(env.adminClient().describeAcls(FILTER2).values(), SecurityDisabledException.class);

            // Test a call where we supply an invalid filter.
            TestUtils.assertFutureError(env.adminClient().describeAcls(UNKNOWN_FILTER).values(),
                InvalidRequestException.class);
        }
    }

    @Test
    public void testCreateAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where we successfully create two ACLs.
            env.kafkaClient().prepareResponse(new CreateAclsResponse(new CreateAclsResponseData().setResults(asList(
                new CreateAclsResponseData.AclCreationResult(),
                new CreateAclsResponseData.AclCreationResult()))));
            CreateAclsResult results = env.adminClient().createAcls(asList(ACL1, ACL2));
            assertCollectionIs(results.values().keySet(), ACL1, ACL2);
            for (KafkaFuture<Void> future : results.values().values())
                future.get();
            results.all().get();

            // Test a call where we fail to create one ACL.
            env.kafkaClient().prepareResponse(new CreateAclsResponse(new CreateAclsResponseData().setResults(asList(
                new CreateAclsResponseData.AclCreationResult()
                    .setErrorCode(Errors.SECURITY_DISABLED.code())
                    .setErrorMessage("Security is disabled"),
                new CreateAclsResponseData.AclCreationResult()))));
            results = env.adminClient().createAcls(asList(ACL1, ACL2));
            assertCollectionIs(results.values().keySet(), ACL1, ACL2);
            TestUtils.assertFutureError(results.values().get(ACL1), SecurityDisabledException.class);
            results.values().get(ACL2).get();
            TestUtils.assertFutureError(results.all(), SecurityDisabledException.class);
        }
    }

    @Test
    public void testDeleteAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where one filter has an error.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(new DeleteAclsResponseData()
                .setThrottleTimeMs(0)
                .setFilterResults(asList(
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setMatchingAcls(asList(
                            DeleteAclsResponse.matchingAcl(ACL1, ApiError.NONE),
                            DeleteAclsResponse.matchingAcl(ACL2, ApiError.NONE))),
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setErrorCode(Errors.SECURITY_DISABLED.code())
                        .setErrorMessage("No security"))),
                    ApiKeys.DELETE_ACLS.latestVersion()));
            DeleteAclsResult results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            Map<AclBindingFilter, KafkaFuture<FilterResults>> filterResults = results.values();
            FilterResults filter1Results = filterResults.get(FILTER1).get();
            assertNull(filter1Results.values().get(0).exception());
            assertEquals(ACL1, filter1Results.values().get(0).binding());
            assertNull(filter1Results.values().get(1).exception());
            assertEquals(ACL2, filter1Results.values().get(1).binding());
            TestUtils.assertFutureError(filterResults.get(FILTER2), SecurityDisabledException.class);
            TestUtils.assertFutureError(results.all(), SecurityDisabledException.class);

            // Test a call where one deletion result has an error.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(new DeleteAclsResponseData()
                .setThrottleTimeMs(0)
                .setFilterResults(asList(
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setMatchingAcls(asList(
                            DeleteAclsResponse.matchingAcl(ACL1, ApiError.NONE),
                            new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                                .setErrorCode(Errors.SECURITY_DISABLED.code())
                                .setErrorMessage("No security")
                                .setPermissionType(AclPermissionType.ALLOW.code())
                                .setOperation(AclOperation.ALTER.code())
                                .setResourceType(ResourceType.CLUSTER.code())
                                .setPatternType(FILTER2.patternFilter().patternType().code()))),
                    new DeleteAclsResponseData.DeleteAclsFilterResult())),
                    ApiKeys.DELETE_ACLS.latestVersion()));
            results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            assertTrue(results.values().get(FILTER2).get().values().isEmpty());
            TestUtils.assertFutureError(results.all(), SecurityDisabledException.class);

            // Test a call where there are no errors.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(new DeleteAclsResponseData()
                .setThrottleTimeMs(0)
                .setFilterResults(asList(
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setMatchingAcls(asList(DeleteAclsResponse.matchingAcl(ACL1, ApiError.NONE))),
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setMatchingAcls(asList(DeleteAclsResponse.matchingAcl(ACL2, ApiError.NONE))))),
                    ApiKeys.DELETE_ACLS.latestVersion()));
            results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            Collection<AclBinding> deleted = results.all().get();
            assertCollectionIs(deleted, ACL1, ACL2);
        }
    }

    @Test
    public void testElectLeaders()  throws Exception {
        TopicPartition topic1 = new TopicPartition("topic", 0);
        TopicPartition topic2 = new TopicPartition("topic", 2);
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            for (ElectionType electionType : ElectionType.values()) {
                env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

                // Test a call where one partition has an error.
                ApiError value = ApiError.fromThrowable(new ClusterAuthorizationException(null));
                List<ReplicaElectionResult> electionResults = new ArrayList<>();
                ReplicaElectionResult electionResult = new ReplicaElectionResult();
                electionResult.setTopic(topic1.topic());
                // Add partition 1 result
                PartitionResult partition1Result = new PartitionResult();
                partition1Result.setPartitionId(topic1.partition());
                partition1Result.setErrorCode(value.error().code());
                partition1Result.setErrorMessage(value.message());
                electionResult.partitionResult().add(partition1Result);

                // Add partition 2 result
                PartitionResult partition2Result = new PartitionResult();
                partition2Result.setPartitionId(topic2.partition());
                partition2Result.setErrorCode(value.error().code());
                partition2Result.setErrorMessage(value.message());
                electionResult.partitionResult().add(partition2Result);

                electionResults.add(electionResult);

                env.kafkaClient().prepareResponse(new ElectLeadersResponse(0, Errors.NONE.code(),
                        electionResults, ApiKeys.ELECT_LEADERS.latestVersion()));
                ElectLeadersResult results = env.adminClient().electLeaders(
                        electionType,
                        new HashSet<>(asList(topic1, topic2)));
                assertEquals(results.partitions().get().get(topic2).get().getClass(), ClusterAuthorizationException.class);

                // Test a call where there are no errors. By mutating the internal of election results
                partition1Result.setErrorCode(ApiError.NONE.error().code());
                partition1Result.setErrorMessage(ApiError.NONE.message());

                partition2Result.setErrorCode(ApiError.NONE.error().code());
                partition2Result.setErrorMessage(ApiError.NONE.message());

                env.kafkaClient().prepareResponse(new ElectLeadersResponse(0, Errors.NONE.code(), electionResults,
                        ApiKeys.ELECT_LEADERS.latestVersion()));
                results = env.adminClient().electLeaders(electionType, new HashSet<>(asList(topic1, topic2)));
                assertFalse(results.partitions().get().get(topic1).isPresent());
                assertFalse(results.partitions().get().get(topic2).isPresent());

                // Now try a timeout
                results = env.adminClient().electLeaders(
                        electionType,
                        new HashSet<>(asList(topic1, topic2)),
                        new ElectLeadersOptions().timeoutMs(100));
                TestUtils.assertFutureError(results.partitions(), TimeoutException.class);
            }
        }
    }

    @Test
    public void testDescribeBrokerConfigs() throws Exception {
        ConfigResource broker0Resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        ConfigResource broker1Resource = new ConfigResource(ConfigResource.Type.BROKER, "1");
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(new DescribeConfigsResponse(
                    new DescribeConfigsResponseData().setResults(asList(new DescribeConfigsResponseData.DescribeConfigsResult()
                            .setResourceName(broker0Resource.name()).setResourceType(broker0Resource.type().id()).setErrorCode(Errors.NONE.code())
                            .setConfigs(emptyList())))), env.cluster().nodeById(0));
            env.kafkaClient().prepareResponseFrom(new DescribeConfigsResponse(
                    new DescribeConfigsResponseData().setResults(asList(new DescribeConfigsResponseData.DescribeConfigsResult()
                            .setResourceName(broker1Resource.name()).setResourceType(broker1Resource.type().id()).setErrorCode(Errors.NONE.code())
                            .setConfigs(emptyList())))), env.cluster().nodeById(1));
            Map<ConfigResource, KafkaFuture<Config>> result = env.adminClient().describeConfigs(asList(
                    broker0Resource,
                    broker1Resource)).values();
            assertEquals(new HashSet<>(asList(broker0Resource, broker1Resource)), result.keySet());
            result.get(broker0Resource).get();
            result.get(broker1Resource).get();
        }
    }

    @Test
    public void testDescribeBrokerAndLogConfigs() throws Exception {
        ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        ConfigResource brokerLoggerResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "0");
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(new DescribeConfigsResponse(
                new DescribeConfigsResponseData().setResults(asList(new DescribeConfigsResponseData.DescribeConfigsResult()
                    .setResourceName(brokerResource.name()).setResourceType(brokerResource.type().id()).setErrorCode(Errors.NONE.code())
                    .setConfigs(emptyList()),
                new DescribeConfigsResponseData.DescribeConfigsResult()
                    .setResourceName(brokerLoggerResource.name()).setResourceType(brokerLoggerResource.type().id()).setErrorCode(Errors.NONE.code())
                    .setConfigs(emptyList())))), env.cluster().nodeById(0));
            Map<ConfigResource, KafkaFuture<Config>> result = env.adminClient().describeConfigs(asList(
                    brokerResource,
                    brokerLoggerResource)).values();
            assertEquals(new HashSet<>(asList(brokerResource, brokerLoggerResource)), result.keySet());
            result.get(brokerResource).get();
            result.get(brokerLoggerResource).get();
        }
    }

    @Test
    public void testDescribeConfigsPartialResponse() throws Exception {
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, "topic");
        ConfigResource topic2 = new ConfigResource(ConfigResource.Type.TOPIC, "topic2");
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(new DescribeConfigsResponse(
                    new DescribeConfigsResponseData().setResults(asList(new DescribeConfigsResponseData.DescribeConfigsResult()
                                    .setResourceName(topic.name()).setResourceType(topic.type().id()).setErrorCode(Errors.NONE.code())
                                    .setConfigs(emptyList())))));
            Map<ConfigResource, KafkaFuture<Config>> result = env.adminClient().describeConfigs(asList(
                    topic,
                    topic2)).values();
            assertEquals(new HashSet<>(asList(topic, topic2)), result.keySet());
            result.get(topic);
            TestUtils.assertFutureThrows(result.get(topic2), ApiException.class);
        }
    }

    @Test
    public void testDescribeConfigsUnrequested() throws Exception {
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, "topic");
        ConfigResource unrequested = new ConfigResource(ConfigResource.Type.TOPIC, "unrequested");
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(new DescribeConfigsResponse(
                new DescribeConfigsResponseData().setResults(asList(new DescribeConfigsResponseData.DescribeConfigsResult()
                        .setResourceName(topic.name()).setResourceType(topic.type().id()).setErrorCode(Errors.NONE.code())
                        .setConfigs(emptyList()),
                new DescribeConfigsResponseData.DescribeConfigsResult()
                        .setResourceName(unrequested.name()).setResourceType(unrequested.type().id()).setErrorCode(Errors.NONE.code())
                        .setConfigs(emptyList())))));
            Map<ConfigResource, KafkaFuture<Config>> result = env.adminClient().describeConfigs(asList(
                    topic)).values();
            assertEquals(new HashSet<>(asList(topic)), result.keySet());
            assertNotNull(result.get(topic).get());
            assertNull(result.get(unrequested));
        }
    }

    private static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir, TopicPartition tp, long partitionSize, long offsetLag) {
        return prepareDescribeLogDirsResponse(error, logDir,
                prepareDescribeLogDirsTopics(partitionSize, offsetLag, tp.topic(), tp.partition(), false));
    }

    private static List<DescribeLogDirsTopic> prepareDescribeLogDirsTopics(
            long partitionSize, long offsetLag, String topic, int partition, boolean isFuture) {
        return singletonList(new DescribeLogDirsTopic()
                .setName(topic)
                .setPartitions(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsPartition()
                        .setPartitionIndex(partition)
                        .setPartitionSize(partitionSize)
                        .setIsFutureKey(isFuture)
                        .setOffsetLag(offsetLag))));
    }

    private static DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir,
                                                                   List<DescribeLogDirsTopic> topics) {
        return new DescribeLogDirsResponse(
                new DescribeLogDirsResponseData().setResults(singletonList(new DescribeLogDirsResponseData.DescribeLogDirsResult()
                        .setErrorCode(error.code())
                        .setLogDir(logDir)
                        .setTopics(topics)
                )));
    }

    private static DescribeLogDirsResponse prepareEmptyDescribeLogDirsResponse(Optional<Errors> error) {
        DescribeLogDirsResponseData data = new DescribeLogDirsResponseData();
        if (error.isPresent()) data.setErrorCode(error.get().code());
        return new DescribeLogDirsResponse(data);
    }

    @Test
    public void testDescribeLogDirs() throws ExecutionException, InterruptedException {
        Set<Integer> brokers = singleton(0);
        String logDir = "/var/data/kafka";
        TopicPartition tp = new TopicPartition("topic", 12);
        long partitionSize = 1234567890;
        long offsetLag = 24;

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                    prepareDescribeLogDirsResponse(Errors.NONE, logDir, tp, partitionSize, offsetLag),
                    env.cluster().nodeById(0));

            DescribeLogDirsResult result = env.adminClient().describeLogDirs(brokers);

            Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = result.descriptions();
            assertEquals(brokers, descriptions.keySet());
            assertNotNull(descriptions.get(0));
            assertDescriptionContains(descriptions.get(0).get(), logDir, tp, partitionSize, offsetLag);

            Map<Integer, Map<String, LogDirDescription>> allDescriptions = result.allDescriptions().get();
            assertEquals(brokers, allDescriptions.keySet());
            assertDescriptionContains(allDescriptions.get(0), logDir, tp, partitionSize, offsetLag);

            // Empty results when not authorized with version < 3
            env.kafkaClient().prepareResponseFrom(
                    prepareEmptyDescribeLogDirsResponse(Optional.empty()),
                    env.cluster().nodeById(0));
            final DescribeLogDirsResult errorResult = env.adminClient().describeLogDirs(brokers);
            ExecutionException exception = assertThrows(ExecutionException.class, () -> errorResult.allDescriptions().get());
            assertTrue(exception.getCause() instanceof ClusterAuthorizationException);

            // Empty results with an error with version >= 3
            env.kafkaClient().prepareResponseFrom(
                    prepareEmptyDescribeLogDirsResponse(Optional.of(Errors.UNKNOWN_SERVER_ERROR)),
                    env.cluster().nodeById(0));
            final DescribeLogDirsResult errorResult2 = env.adminClient().describeLogDirs(brokers);
            exception = assertThrows(ExecutionException.class, () -> errorResult2.allDescriptions().get());
            assertTrue(exception.getCause() instanceof UnknownServerException);
        }
    }

    private static void assertDescriptionContains(Map<String, LogDirDescription> descriptionsMap, String logDir,
                                           TopicPartition tp, long partitionSize, long offsetLag) {
        assertNotNull(descriptionsMap);
        assertEquals(singleton(logDir), descriptionsMap.keySet());
        assertNull(descriptionsMap.get(logDir).error());
        Map<TopicPartition, ReplicaInfo> descriptionsReplicaInfos = descriptionsMap.get(logDir).replicaInfos();
        assertEquals(singleton(tp), descriptionsReplicaInfos.keySet());
        assertEquals(partitionSize, descriptionsReplicaInfos.get(tp).size());
        assertEquals(offsetLag, descriptionsReplicaInfos.get(tp).offsetLag());
        assertFalse(descriptionsReplicaInfos.get(tp).isFuture());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDescribeLogDirsDeprecated() throws ExecutionException, InterruptedException {
        Set<Integer> brokers = singleton(0);
        TopicPartition tp = new TopicPartition("topic", 12);
        String logDir = "/var/data/kafka";
        Errors error = Errors.NONE;
        int offsetLag = 24;
        long partitionSize = 1234567890;

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                    prepareDescribeLogDirsResponse(error, logDir, tp, partitionSize, offsetLag),
                    env.cluster().nodeById(0));

            DescribeLogDirsResult result = env.adminClient().describeLogDirs(brokers);

            Map<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse.LogDirInfo>>> deprecatedValues = result.values();
            assertEquals(brokers, deprecatedValues.keySet());
            assertNotNull(deprecatedValues.get(0));
            assertDescriptionContains(deprecatedValues.get(0).get(), logDir, tp, error, offsetLag, partitionSize);

            Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> deprecatedAll = result.all().get();
            assertEquals(brokers, deprecatedAll.keySet());
            assertDescriptionContains(deprecatedAll.get(0), logDir, tp, error, offsetLag, partitionSize);
        }
    }

    @SuppressWarnings("deprecation")
    private static  void assertDescriptionContains(Map<String, DescribeLogDirsResponse.LogDirInfo> descriptionsMap,
                                           String logDir, TopicPartition tp, Errors error,
                                           int offsetLag, long partitionSize) {
        assertNotNull(descriptionsMap);
        assertEquals(singleton(logDir), descriptionsMap.keySet());
        assertEquals(error, descriptionsMap.get(logDir).error);
        Map<TopicPartition, DescribeLogDirsResponse.ReplicaInfo> allReplicaInfos =
                descriptionsMap.get(logDir).replicaInfos;
        assertEquals(singleton(tp), allReplicaInfos.keySet());
        assertEquals(partitionSize, allReplicaInfos.get(tp).size);
        assertEquals(offsetLag, allReplicaInfos.get(tp).offsetLag);
        assertFalse(allReplicaInfos.get(tp).isFuture);
    }

    @Test
    public void testDescribeLogDirsOfflineDir() throws ExecutionException, InterruptedException {
        Set<Integer> brokers = singleton(0);
        String logDir = "/var/data/kafka";
        Errors error = Errors.KAFKA_STORAGE_ERROR;

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                    prepareDescribeLogDirsResponse(error, logDir, emptyList()),
                    env.cluster().nodeById(0));

            DescribeLogDirsResult result = env.adminClient().describeLogDirs(brokers);

            Map<Integer, KafkaFuture<Map<String, LogDirDescription>>> descriptions = result.descriptions();
            assertEquals(brokers, descriptions.keySet());
            assertNotNull(descriptions.get(0));
            Map<String, LogDirDescription> descriptionsMap = descriptions.get(0).get();
            assertEquals(singleton(logDir), descriptionsMap.keySet());
            assertEquals(error.exception().getClass(), descriptionsMap.get(logDir).error().getClass());
            assertEquals(emptySet(), descriptionsMap.get(logDir).replicaInfos().keySet());

            Map<Integer, Map<String, LogDirDescription>> allDescriptions = result.allDescriptions().get();
            assertEquals(brokers, allDescriptions.keySet());
            Map<String, LogDirDescription> allMap = allDescriptions.get(0);
            assertNotNull(allMap);
            assertEquals(singleton(logDir), allMap.keySet());
            assertEquals(error.exception().getClass(), allMap.get(logDir).error().getClass());
            assertEquals(emptySet(), allMap.get(logDir).replicaInfos().keySet());
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDescribeLogDirsOfflineDirDeprecated() throws ExecutionException, InterruptedException {
        Set<Integer> brokers = singleton(0);
        String logDir = "/var/data/kafka";
        Errors error = Errors.KAFKA_STORAGE_ERROR;

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(
                    prepareDescribeLogDirsResponse(error, logDir, emptyList()),
                    env.cluster().nodeById(0));

            DescribeLogDirsResult result = env.adminClient().describeLogDirs(brokers);

            Map<Integer, KafkaFuture<Map<String, DescribeLogDirsResponse.LogDirInfo>>> deprecatedValues = result.values();
            assertEquals(brokers, deprecatedValues.keySet());
            assertNotNull(deprecatedValues.get(0));
            Map<String, DescribeLogDirsResponse.LogDirInfo> valuesMap = deprecatedValues.get(0).get();
            assertEquals(singleton(logDir), valuesMap.keySet());
            assertEquals(error, valuesMap.get(logDir).error);
            assertEquals(emptySet(), valuesMap.get(logDir).replicaInfos.keySet());

            Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> deprecatedAll = result.all().get();
            assertEquals(brokers, deprecatedAll.keySet());
            Map<String, DescribeLogDirsResponse.LogDirInfo> allMap = deprecatedAll.get(0);
            assertNotNull(allMap);
            assertEquals(singleton(logDir), allMap.keySet());
            assertEquals(error, allMap.get(logDir).error);
            assertEquals(emptySet(), allMap.get(logDir).replicaInfos.keySet());
        }
    }

    @Test
    public void testDescribeReplicaLogDirs() throws ExecutionException, InterruptedException {
        TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 12, 1);
        TopicPartitionReplica tpr2 = new TopicPartitionReplica("topic", 12, 2);

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            String broker1log0 = "/var/data/kafka0";
            String broker1log1 = "/var/data/kafka1";
            String broker2log0 = "/var/data/kafka2";
            int broker1Log0OffsetLag = 24;
            int broker1Log0PartitionSize = 987654321;
            int broker1Log1PartitionSize = 123456789;
            int broker1Log1OffsetLag = 4321;
            env.kafkaClient().prepareResponseFrom(
                    new DescribeLogDirsResponse(
                            new DescribeLogDirsResponseData().setResults(asList(
                                    prepareDescribeLogDirsResult(tpr1, broker1log0, broker1Log0PartitionSize, broker1Log0OffsetLag, false),
                                    prepareDescribeLogDirsResult(tpr1, broker1log1, broker1Log1PartitionSize, broker1Log1OffsetLag, true)))),
                    env.cluster().nodeById(tpr1.brokerId()));
            env.kafkaClient().prepareResponseFrom(
                    prepareDescribeLogDirsResponse(Errors.KAFKA_STORAGE_ERROR, broker2log0),
                    env.cluster().nodeById(tpr2.brokerId()));

            DescribeReplicaLogDirsResult result = env.adminClient().describeReplicaLogDirs(asList(tpr1, tpr2));

            Map<TopicPartitionReplica, KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> values = result.values();
            assertEquals(TestUtils.toSet(asList(tpr1, tpr2)), values.keySet());

            assertNotNull(values.get(tpr1));
            assertEquals(broker1log0, values.get(tpr1).get().getCurrentReplicaLogDir());
            assertEquals(broker1Log0OffsetLag, values.get(tpr1).get().getCurrentReplicaOffsetLag());
            assertEquals(broker1log1, values.get(tpr1).get().getFutureReplicaLogDir());
            assertEquals(broker1Log1OffsetLag, values.get(tpr1).get().getFutureReplicaOffsetLag());

            assertNotNull(values.get(tpr2));
            assertNull(values.get(tpr2).get().getCurrentReplicaLogDir());
            assertEquals(-1, values.get(tpr2).get().getCurrentReplicaOffsetLag());
            assertNull(values.get(tpr2).get().getFutureReplicaLogDir());
            assertEquals(-1, values.get(tpr2).get().getFutureReplicaOffsetLag());
        }
    }

    private static DescribeLogDirsResponseData.DescribeLogDirsResult prepareDescribeLogDirsResult(TopicPartitionReplica tpr, String logDir, int partitionSize, int offsetLag, boolean isFuture) {
        return new DescribeLogDirsResponseData.DescribeLogDirsResult()
                .setErrorCode(Errors.NONE.code())
                .setLogDir(logDir)
                .setTopics(prepareDescribeLogDirsTopics(partitionSize, offsetLag, tpr.topic(), tpr.partition(), isFuture));
    }

    @Test
    public void testDescribeReplicaLogDirsUnexpected() throws ExecutionException, InterruptedException {
        TopicPartitionReplica expected = new TopicPartitionReplica("topic", 12, 1);
        TopicPartitionReplica unexpected = new TopicPartitionReplica("topic", 12, 2);

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            String broker1log0 = "/var/data/kafka0";
            String broker1log1 = "/var/data/kafka1";
            int broker1Log0PartitionSize = 987654321;
            int broker1Log0OffsetLag = 24;
            int broker1Log1PartitionSize = 123456789;
            int broker1Log1OffsetLag = 4321;
            env.kafkaClient().prepareResponseFrom(
                    new DescribeLogDirsResponse(
                            new DescribeLogDirsResponseData().setResults(asList(
                                    prepareDescribeLogDirsResult(expected, broker1log0, broker1Log0PartitionSize, broker1Log0OffsetLag, false),
                                    prepareDescribeLogDirsResult(unexpected, broker1log1, broker1Log1PartitionSize, broker1Log1OffsetLag, true)))),
                    env.cluster().nodeById(expected.brokerId()));

            DescribeReplicaLogDirsResult result = env.adminClient().describeReplicaLogDirs(asList(expected));

            Map<TopicPartitionReplica, KafkaFuture<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> values = result.values();
            assertEquals(TestUtils.toSet(asList(expected)), values.keySet());

            assertNotNull(values.get(expected));
            assertEquals(broker1log0, values.get(expected).get().getCurrentReplicaLogDir());
            assertEquals(broker1Log0OffsetLag, values.get(expected).get().getCurrentReplicaOffsetLag());
            assertEquals(broker1log1, values.get(expected).get().getFutureReplicaLogDir());
            assertEquals(broker1Log1OffsetLag, values.get(expected).get().getFutureReplicaOffsetLag());
        }
    }

    @Test
    public void testCreatePartitions() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where one filter has an error.
            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("my_topic", "other_topic"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("my_topic", Errors.NONE),
                    createPartitionsTopicResult("other_topic", Errors.INVALID_TOPIC_EXCEPTION,
                        "some detailed reason")));


            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("my_topic", NewPartitions.increaseTo(3));
            counts.put("other_topic", NewPartitions.increaseTo(3, asList(asList(2), asList(3))));

            CreatePartitionsResult results = env.adminClient().createPartitions(counts);
            Map<String, KafkaFuture<Void>> values = results.values();
            KafkaFuture<Void> myTopicResult = values.get("my_topic");
            myTopicResult.get();
            KafkaFuture<Void> otherTopicResult = values.get("other_topic");
            try {
                otherTopicResult.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e0) {
                assertTrue(e0.getCause() instanceof InvalidTopicException);
                InvalidTopicException e = (InvalidTopicException) e0.getCause();
                assertEquals("some detailed reason", e.getMessage());
            }
        }
    }

    @Test
    public void testCreatePartitionsRetryThrottlingExceptionWhenEnabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("topic1", Errors.NONE),
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    createPartitionsTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic2"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic2"),
                prepareCreatePartitionsResponse(0,
                    createPartitionsTopicResult("topic2", Errors.NONE)));

            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("topic1", NewPartitions.increaseTo(1));
            counts.put("topic2", NewPartitions.increaseTo(2));
            counts.put("topic3", NewPartitions.increaseTo(3));

            CreatePartitionsResult result = env.adminClient().createPartitions(
                counts, new CreatePartitionsOptions().retryOnQuotaViolation(true));

            assertNull(result.values().get("topic1").get());
            assertNull(result.values().get("topic2").get());
            TestUtils.assertFutureThrows(result.values().get("topic3"), TopicExistsException.class);
        }
    }

    @Test
    public void testCreatePartitionsRetryThrottlingExceptionWhenEnabledUntilRequestTimeOut() throws Exception {
        long defaultApiTimeout = 60000;
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = mockClientEnv(time,
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(defaultApiTimeout))) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("topic1", Errors.NONE),
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    createPartitionsTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic2"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED)));

            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("topic1", NewPartitions.increaseTo(1));
            counts.put("topic2", NewPartitions.increaseTo(2));
            counts.put("topic3", NewPartitions.increaseTo(3));

            CreatePartitionsResult result = env.adminClient().createPartitions(
                counts, new CreatePartitionsOptions().retryOnQuotaViolation(true));

            // Wait until the prepared attempts have consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                "Failed awaiting CreatePartitions requests");

            // Wait until the next request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                "Failed awaiting next CreatePartitions request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            assertNull(result.values().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(result.values().get("topic2"),
                ThrottlingQuotaExceededException.class);
            assertEquals(0, e.throttleTimeMs());
            TestUtils.assertFutureThrows(result.values().get("topic3"), TopicExistsException.class);
        }
    }

    @Test
    public void testCreatePartitionsDontRetryThrottlingExceptionWhenDisabled() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                expectCreatePartitionsRequestWithTopics("topic1", "topic2", "topic3"),
                prepareCreatePartitionsResponse(1000,
                    createPartitionsTopicResult("topic1", Errors.NONE),
                    createPartitionsTopicResult("topic2", Errors.THROTTLING_QUOTA_EXCEEDED),
                    createPartitionsTopicResult("topic3", Errors.TOPIC_ALREADY_EXISTS)));

            Map<String, NewPartitions> counts = new HashMap<>();
            counts.put("topic1", NewPartitions.increaseTo(1));
            counts.put("topic2", NewPartitions.increaseTo(2));
            counts.put("topic3", NewPartitions.increaseTo(3));

            CreatePartitionsResult result = env.adminClient().createPartitions(
                counts, new CreatePartitionsOptions().retryOnQuotaViolation(false));

            assertNull(result.values().get("topic1").get());
            ThrottlingQuotaExceededException e = TestUtils.assertFutureThrows(result.values().get("topic2"),
                ThrottlingQuotaExceededException.class);
            assertEquals(1000, e.throttleTimeMs());
            TestUtils.assertFutureThrows(result.values().get("topic3"), TopicExistsException.class);
        }
    }

    private MockClient.RequestMatcher expectCreatePartitionsRequestWithTopics(final String... topics) {
        return body -> {
            if (body instanceof CreatePartitionsRequest) {
                CreatePartitionsRequest request = (CreatePartitionsRequest) body;
                for (String topic : topics) {
                    if (request.data().topics().find(topic) == null)
                        return false;
                }
                return topics.length == request.data().topics().size();
            }
            return false;
        };
    }

    @Test
    public void testDeleteRecordsTopicAuthorizationError() {
        String topic = "foo";
        TopicPartition partition = new TopicPartition(topic, 0);

        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
            topics.add(new MetadataResponse.TopicMetadata(Errors.TOPIC_AUTHORIZATION_FAILED, topic, false,
                    Collections.emptyList()));

            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), env.cluster().controller().id(), topics));

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(partition, RecordsToDelete.beforeOffset(10L));
            DeleteRecordsResult results = env.adminClient().deleteRecords(recordsToDelete);

            TestUtils.assertFutureThrows(results.lowWatermarks().get(partition), TopicAuthorizationException.class);
        }
    }

    @Test
    public void testDeleteRecordsMultipleSends() throws Exception {
        String topic = "foo";
        TopicPartition tp0 = new TopicPartition(topic, 0);
        TopicPartition tp1 = new TopicPartition(topic, 1);

        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, mockCluster(3, 0))) {
            List<Node> nodes = env.cluster().nodes();

            List<MetadataResponse.PartitionMetadata> partitionMetadata = new ArrayList<>();
            partitionMetadata.add(new MetadataResponse.PartitionMetadata(Errors.NONE, tp0,
                    Optional.of(nodes.get(0).id()), Optional.of(5), singletonList(nodes.get(0).id()),
                    singletonList(nodes.get(0).id()), Collections.emptyList()));
            partitionMetadata.add(new MetadataResponse.PartitionMetadata(Errors.NONE, tp1,
                    Optional.of(nodes.get(1).id()), Optional.of(5), singletonList(nodes.get(1).id()),
                    singletonList(nodes.get(1).id()), Collections.emptyList()));

            List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
            topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, topic, false, partitionMetadata));

            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), env.cluster().controller().id(), topicMetadata));

            env.kafkaClient().prepareResponseFrom(new DeleteRecordsResponse(new DeleteRecordsResponseData().setTopics(
                    new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection(singletonList(new DeleteRecordsResponseData.DeleteRecordsTopicResult()
                            .setName(tp0.topic())
                            .setPartitions(new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection(singletonList(new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                                    .setPartitionIndex(tp0.partition())
                                    .setErrorCode(Errors.NONE.code())
                                    .setLowWatermark(3)).iterator()))).iterator()))), nodes.get(0));

            env.kafkaClient().disconnect(nodes.get(1).idString());
            env.kafkaClient().createPendingAuthenticationError(nodes.get(1), 100);

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(tp0, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(tp1, RecordsToDelete.beforeOffset(10L));
            DeleteRecordsResult results = env.adminClient().deleteRecords(recordsToDelete);

            assertEquals(3L, results.lowWatermarks().get(tp0).get().lowWatermark());
            TestUtils.assertFutureThrows(results.lowWatermarks().get(tp1), AuthenticationException.class);
        }
    }

    @Test
    public void testDeleteRecords() throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        partitionInfos.add(new PartitionInfo("my_topic", 0, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 1, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 2, null, new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 3, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        partitionInfos.add(new PartitionInfo("my_topic", 4, nodes.get(0), new Node[] {nodes.get(0)}, new Node[] {nodes.get(0)}));
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                partitionInfos, Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));

        TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
        TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
        TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);
        TopicPartition myTopicPartition3 = new TopicPartition("my_topic", 3);
        TopicPartition myTopicPartition4 = new TopicPartition("my_topic", 4);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            DeleteRecordsResponseData m = new DeleteRecordsResponseData();
            m.topics().add(new DeleteRecordsResponseData.DeleteRecordsTopicResult().setName(myTopicPartition0.topic())
                    .setPartitions(new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection(asList(
                        new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                            .setPartitionIndex(myTopicPartition0.partition())
                            .setLowWatermark(3)
                            .setErrorCode(Errors.NONE.code()),
                        new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                            .setPartitionIndex(myTopicPartition1.partition())
                            .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                            .setErrorCode(Errors.OFFSET_OUT_OF_RANGE.code()),
                        new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                            .setPartitionIndex(myTopicPartition3.partition())
                            .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                            .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()),
                        new DeleteRecordsResponseData.DeleteRecordsPartitionResult()
                            .setPartitionIndex(myTopicPartition4.partition())
                            .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                            .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    ).iterator())));

            List<MetadataResponse.TopicMetadata> t = new ArrayList<>();
            List<MetadataResponse.PartitionMetadata> p = new ArrayList<>();
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, myTopicPartition0,
                    Optional.of(nodes.get(0).id()), Optional.of(5), singletonList(nodes.get(0).id()),
                    singletonList(nodes.get(0).id()), Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, myTopicPartition1,
                    Optional.of(nodes.get(0).id()), Optional.of(5), singletonList(nodes.get(0).id()),
                    singletonList(nodes.get(0).id()), Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.LEADER_NOT_AVAILABLE, myTopicPartition2,
                    Optional.empty(), Optional.empty(), singletonList(nodes.get(0).id()),
                    singletonList(nodes.get(0).id()), Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, myTopicPartition3,
                    Optional.of(nodes.get(0).id()), Optional.of(5), singletonList(nodes.get(0).id()),
                    singletonList(nodes.get(0).id()), Collections.emptyList()));
            p.add(new MetadataResponse.PartitionMetadata(Errors.NONE, myTopicPartition4,
                    Optional.of(nodes.get(0).id()), Optional.of(5), singletonList(nodes.get(0).id()),
                    singletonList(nodes.get(0).id()), Collections.emptyList()));

            t.add(new MetadataResponse.TopicMetadata(Errors.NONE, "my_topic", false, p));

            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(cluster.nodes(), cluster.clusterResource().clusterId(), cluster.controller().id(), t));
            env.kafkaClient().prepareResponse(new DeleteRecordsResponse(m));

            Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
            recordsToDelete.put(myTopicPartition0, RecordsToDelete.beforeOffset(3L));
            recordsToDelete.put(myTopicPartition1, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition2, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition3, RecordsToDelete.beforeOffset(10L));
            recordsToDelete.put(myTopicPartition4, RecordsToDelete.beforeOffset(10L));

            DeleteRecordsResult results = env.adminClient().deleteRecords(recordsToDelete);

            // success on records deletion for partition 0
            Map<TopicPartition, KafkaFuture<DeletedRecords>> values = results.lowWatermarks();
            KafkaFuture<DeletedRecords> myTopicPartition0Result = values.get(myTopicPartition0);
            long lowWatermark = myTopicPartition0Result.get().lowWatermark();
            assertEquals(lowWatermark, 3);

            // "offset out of range" failure on records deletion for partition 1
            KafkaFuture<DeletedRecords> myTopicPartition1Result = values.get(myTopicPartition1);
            try {
                myTopicPartition1Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e0) {
                assertTrue(e0.getCause() instanceof OffsetOutOfRangeException);
            }

            // "leader not available" failure on metadata request for partition 2
            KafkaFuture<DeletedRecords> myTopicPartition2Result = values.get(myTopicPartition2);
            try {
                myTopicPartition2Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e1) {
                assertTrue(e1.getCause() instanceof LeaderNotAvailableException);
            }

            // "not leader for partition" failure on records deletion for partition 3
            KafkaFuture<DeletedRecords> myTopicPartition3Result = values.get(myTopicPartition3);
            try {
                myTopicPartition3Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e1) {
                assertTrue(e1.getCause() instanceof NotLeaderOrFollowerException);
            }

            // "unknown topic or partition" failure on records deletion for partition 4
            KafkaFuture<DeletedRecords> myTopicPartition4Result = values.get(myTopicPartition4);
            try {
                myTopicPartition4Result.get();
                fail("get() should throw ExecutionException");
            } catch (ExecutionException e1) {
                assertTrue(e1.getCause() instanceof UnknownTopicOrPartitionException);
            }
        }
    }

    @Test
    public void testDescribeCluster() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(4, 0),
            AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Prepare the describe cluster response used for the first describe cluster
            env.kafkaClient().prepareResponse(
                prepareDescribeClusterResponse(0,
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    2,
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED));

            // Prepare the describe cluster response used for the second describe cluster
            env.kafkaClient().prepareResponse(
                prepareDescribeClusterResponse(0,
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    3,
                    1 << AclOperation.DESCRIBE.code() | 1 << AclOperation.ALTER.code()));

            // Test DescribeCluster with the authorized operations omitted.
            final DescribeClusterResult result = env.adminClient().describeCluster();
            assertEquals(env.cluster().clusterResource().clusterId(), result.clusterId().get());
            assertEquals(new HashSet<>(env.cluster().nodes()), new HashSet<>(result.nodes().get()));
            assertEquals(2, result.controller().get().id());
            assertNull(result.authorizedOperations().get());

            // Test DescribeCluster with the authorized operations included.
            final DescribeClusterResult result2 = env.adminClient().describeCluster();
            assertEquals(env.cluster().clusterResource().clusterId(), result2.clusterId().get());
            assertEquals(new HashSet<>(env.cluster().nodes()), new HashSet<>(result2.nodes().get()));
            assertEquals(3, result2.controller().get().id());
            assertEquals(new HashSet<>(Arrays.asList(AclOperation.DESCRIBE, AclOperation.ALTER)),
                result2.authorizedOperations().get());
        }
    }

    @Test
    public void testDescribeClusterHandleError() {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(4, 0),
            AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Prepare the describe cluster response used for the first describe cluster
            String errorMessage = "my error";
            env.kafkaClient().prepareResponse(
                new DescribeClusterResponse(new DescribeClusterResponseData()
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage(errorMessage)));

            final DescribeClusterResult result = env.adminClient().describeCluster();
            TestUtils.assertFutureThrows(result.clusterId(),
                InvalidRequestException.class, errorMessage);
            TestUtils.assertFutureThrows(result.controller(),
                InvalidRequestException.class, errorMessage);
            TestUtils.assertFutureThrows(result.nodes(),
                InvalidRequestException.class, errorMessage);
            TestUtils.assertFutureThrows(result.authorizedOperations(),
                InvalidRequestException.class, errorMessage);
        }
    }

    private static DescribeClusterResponse prepareDescribeClusterResponse(
        int throttleTimeMs,
        Collection<Node> brokers,
        String clusterId,
        int controllerId,
        int clusterAuthorizedOperations
    ) {
        DescribeClusterResponseData data = new DescribeClusterResponseData()
            .setErrorCode(Errors.NONE.code())
            .setThrottleTimeMs(throttleTimeMs)
            .setControllerId(controllerId)
            .setClusterId(clusterId)
            .setClusterAuthorizedOperations(clusterAuthorizedOperations);

        brokers.forEach(broker ->
            data.brokers().add(new DescribeClusterBroker()
                .setHost(broker.host())
                .setPort(broker.port())
                .setBrokerId(broker.id())
                .setRack(broker.rack())));

        return new DescribeClusterResponse(data);
    }

    @Test
    public void testDescribeClusterFailBack() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(4, 0),
            AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Reject the describe cluster request with an unsupported exception
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof DescribeClusterRequest);

            // Prepare the metadata response used for the first describe cluster
            env.kafkaClient().prepareResponse(
                RequestTestUtils.metadataResponse(
                    0,
                    env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(),
                    2,
                    Collections.emptyList(),
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                    ApiKeys.METADATA.latestVersion()));

            final DescribeClusterResult result = env.adminClient().describeCluster();
            assertEquals(env.cluster().clusterResource().clusterId(), result.clusterId().get());
            assertEquals(new HashSet<>(env.cluster().nodes()), new HashSet<>(result.nodes().get()));
            assertEquals(2, result.controller().get().id());
            assertNull(result.authorizedOperations().get());
        }
    }

    @Test
    public void testListConsumerGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(4, 0),
                AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata response should be retried
            env.kafkaClient().prepareResponse(
                     RequestTestUtils.metadataResponse(
                            Collections.emptyList(),
                            env.cluster().clusterResource().clusterId(),
                            -1,
                            Collections.emptyList()));

            env.kafkaClient().prepareResponse(
                     RequestTestUtils.metadataResponse(
                            env.cluster().nodes(),
                            env.cluster().clusterResource().clusterId(),
                            env.cluster().controller().id(),
                            Collections.emptyList()));

            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setGroups(Arrays.asList(
                                    new ListGroupsResponseData.ListedGroup()
                                            .setGroupId("group-1")
                                            .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                            .setGroupState("Stable"),
                                    new ListGroupsResponseData.ListedGroup()
                                            .setGroupId("group-connect-1")
                                            .setProtocolType("connector")
                                            .setGroupState("Stable")
                            ))),
                    env.cluster().nodeById(0));

            // handle retriable errors
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                                    .setGroups(Collections.emptyList())
                    ),
                    env.cluster().nodeById(1));
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                                    .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
                                    .setGroups(Collections.emptyList())
                    ),
                    env.cluster().nodeById(1));
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                                    .setErrorCode(Errors.NONE.code())
                                    .setGroups(Arrays.asList(
                                            new ListGroupsResponseData.ListedGroup()
                                                    .setGroupId("group-2")
                                                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                                    .setGroupState("Stable"),
                                            new ListGroupsResponseData.ListedGroup()
                                                    .setGroupId("group-connect-2")
                                                    .setProtocolType("connector")
                                                    .setGroupState("Stable")
                            ))),
                    env.cluster().nodeById(1));

            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                                    .setErrorCode(Errors.NONE.code())
                                    .setGroups(Arrays.asList(
                                            new ListGroupsResponseData.ListedGroup()
                                                    .setGroupId("group-3")
                                                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                                    .setGroupState("Stable"),
                                            new ListGroupsResponseData.ListedGroup()
                                                    .setGroupId("group-connect-3")
                                                    .setProtocolType("connector")
                                                    .setGroupState("Stable")
                                    ))),
                    env.cluster().nodeById(2));

            // fatal error
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(
                            new ListGroupsResponseData()
                                    .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())
                                    .setGroups(Collections.emptyList())),
                    env.cluster().nodeById(3));

            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups();
            TestUtils.assertFutureError(result.all(), UnknownServerException.class);

            Collection<ConsumerGroupListing> listings = result.valid().get();
            assertEquals(3, listings.size());

            Set<String> groupIds = new HashSet<>();
            for (ConsumerGroupListing listing : listings) {
                groupIds.add(listing.groupId());
                assertTrue(listing.state().isPresent());
            }

            assertEquals(Utils.mkSet("group-1", "group-2", "group-3"), groupIds);
            assertEquals(1, result.errors().get().size());
        }
    }

    @Test
    public void testListConsumerGroupsMetadataFailure() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Empty metadata causes the request to fail since we have no list of brokers
            // to send the ListGroups requests to
            env.kafkaClient().prepareResponse(
                     RequestTestUtils.metadataResponse(
                            Collections.emptyList(),
                            env.cluster().clusterResource().clusterId(),
                            -1,
                            Collections.emptyList()));

            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups();
            TestUtils.assertFutureError(result.all(), KafkaException.class);
        }
    }

    @Test
    public void testListConsumerGroupsWithStates() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            env.kafkaClient().prepareResponseFrom(
                new ListGroupsResponse(new ListGroupsResponseData()
                    .setErrorCode(Errors.NONE.code())
                    .setGroups(Arrays.asList(
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-1")
                                .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)
                                .setGroupState("Stable"),
                            new ListGroupsResponseData.ListedGroup()
                                .setGroupId("group-2")
                                .setGroupState("Empty")))),
                env.cluster().nodeById(0));

            final ListConsumerGroupsOptions options = new ListConsumerGroupsOptions();
            final ListConsumerGroupsResult result = env.adminClient().listConsumerGroups(options);
            Collection<ConsumerGroupListing> listings = result.valid().get();

            assertEquals(2, listings.size());
            List<ConsumerGroupListing> expected = new ArrayList<>();
            expected.add(new ConsumerGroupListing("group-2", true, Optional.of(ConsumerGroupState.EMPTY)));
            expected.add(new ConsumerGroupListing("group-1", false, Optional.of(ConsumerGroupState.STABLE)));
            assertEquals(expected, listings);
            assertEquals(0, result.errors().get().size());
        }
    }

    @Test
    public void testListConsumerGroupsWithStatesOlderBrokerVersion() throws Exception {
        ApiVersion listGroupV3 = new ApiVersion()
                .setApiKey(ApiKeys.LIST_GROUPS.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 3);
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(Collections.singletonList(listGroupV3)));

            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));

            // Check we can list groups with older broker if we don't specify states
            env.kafkaClient().prepareResponseFrom(
                    new ListGroupsResponse(new ListGroupsResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setGroups(Collections.singletonList(
                                new ListGroupsResponseData.ListedGroup()
                                    .setGroupId("group-1")
                                    .setProtocolType(ConsumerProtocol.PROTOCOL_TYPE)))),
                    env.cluster().nodeById(0));
            ListConsumerGroupsOptions options = new ListConsumerGroupsOptions();
            ListConsumerGroupsResult result = env.adminClient().listConsumerGroups(options);
            Collection<ConsumerGroupListing> listing = result.all().get();
            assertEquals(1, listing.size());
            List<ConsumerGroupListing> expected = Collections.singletonList(new ConsumerGroupListing("group-1", false, Optional.empty()));
            assertEquals(expected, listing);

            // But we cannot set a state filter with older broker
            env.kafkaClient().prepareResponse(prepareMetadataResponse(env.cluster(), Errors.NONE));
            env.kafkaClient().prepareUnsupportedVersionResponse(
                body -> body instanceof ListGroupsRequest);

            options = new ListConsumerGroupsOptions().inStates(singleton(ConsumerGroupState.STABLE));
            result = env.adminClient().listConsumerGroups(options);
            TestUtils.assertFutureThrows(result.all(), UnsupportedVersionException.class);
        }
    }

    @Test
    public void testOffsetCommitNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final TopicPartition tp1 = new TopicPartition("foo", 0);

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(prepareOffsetCommitResponse(tp1, Errors.NOT_COORDINATOR));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1, new OffsetAndMetadata(123L));
            final AlterConsumerGroupOffsetsResult result = env.adminClient().alterConsumerGroupOffsets(GROUP_ID, offsets);

            TestUtils.assertFutureError(result.all(), TimeoutException.class);
        }
    }

    @Test
    public void testOffsetCommitWithMultipleErrors() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final TopicPartition foo0 = new TopicPartition("foo", 0);
            final TopicPartition foo1 = new TopicPartition("foo", 1);

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Map<TopicPartition, Errors> responseData = new HashMap<>();
            responseData.put(foo0, Errors.NONE);
            responseData.put(foo1, Errors.UNKNOWN_TOPIC_OR_PARTITION);
            env.kafkaClient().prepareResponse(new OffsetCommitResponse(0, responseData));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(foo0, new OffsetAndMetadata(123L));
            offsets.put(foo1, new OffsetAndMetadata(456L));
            final AlterConsumerGroupOffsetsResult result = env.adminClient()
                .alterConsumerGroupOffsets(GROUP_ID, offsets);

            assertNull(result.partitionResult(foo0).get());
            TestUtils.assertFutureError(result.partitionResult(foo1), UnknownTopicOrPartitionException.class);

            TestUtils.assertFutureError(result.all(), UnknownTopicOrPartitionException.class);
        }
    }

    @Test
    public void testOffsetCommitRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            final TopicPartition tp1 = new TopicPartition("foo", 0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, prepareOffsetCommitResponse(tp1, Errors.NOT_COORDINATOR));


            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, prepareOffsetCommitResponse(tp1, Errors.NONE));


            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1, new OffsetAndMetadata(123L));
            final KafkaFuture<Void> future = env.adminClient().alterConsumerGroupOffsets(GROUP_ID, offsets).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting CommitOffsets first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry CommitOffsets call on first failure");
            time.sleep(retryBackoff);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, "CommitOffsets retry did not await expected backoff");
        }
    }

    @Test
    public void testDescribeConsumerGroupNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NOT_COORDINATOR,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList(GROUP_ID));

            TestUtils.assertFutureError(result.all(), TimeoutException.class);
        }
    }

    @Test
    public void testDescribeConsumerGroupRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NOT_COORDINATOR,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));

            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, new DescribeGroupsResponse(data));

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NONE,
                "",
                ConsumerProtocol.PROTOCOL_TYPE,
                "",
                Collections.emptyList(),
                Collections.emptySet()));

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, new DescribeGroupsResponse(data));

            final KafkaFuture<Map<String, ConsumerGroupDescription>> future =
                env.adminClient().describeConsumerGroups(singletonList(GROUP_ID)).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting DescribeConsumerGroup first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry DescribeConsumerGroup call on first failure");
            time.sleep(retryBackoff);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, "DescribeConsumerGroup retry did not await expected backoff!");
        }
    }


    @Test
    public void testDescribeConsumerGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS,  Node.noNode()));

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            //Retriable errors should be retried
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.COORDINATOR_LOAD_IN_PROGRESS,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            /*
             * We need to return two responses here, one with NOT_COORDINATOR error when calling describe consumer group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for COORDINATOR_NOT_AVAILABLE error response
             */
            data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                    GROUP_ID,
                    Errors.NOT_COORDINATOR,
                    "",
                    "",
                    "",
                    Collections.emptyList(),
                    Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.COORDINATOR_NOT_AVAILABLE,
                "",
                "",
                "",
                Collections.emptyList(),
                Collections.emptySet()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            data = new DescribeGroupsResponseData();
            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);

            final List<TopicPartition> topicPartitions = new ArrayList<>();
            topicPartitions.add(0, myTopicPartition0);
            topicPartitions.add(1, myTopicPartition1);
            topicPartitions.add(2, myTopicPartition2);

            final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
            byte[] memberAssignmentBytes = new byte[memberAssignment.remaining()];
            memberAssignment.get(memberAssignmentBytes);

            DescribedGroupMember memberOne = DescribeGroupsResponse.groupMember("0", "instance1", "clientId0", "clientHost", memberAssignmentBytes, null);
            DescribedGroupMember memberTwo = DescribeGroupsResponse.groupMember("1", "instance2", "clientId1", "clientHost", memberAssignmentBytes, null);

            List<MemberDescription> expectedMemberDescriptions = new ArrayList<>();
            expectedMemberDescriptions.add(convertToMemberDescriptions(memberOne,
                                                                       new MemberAssignment(new HashSet<>(topicPartitions))));
            expectedMemberDescriptions.add(convertToMemberDescriptions(memberTwo,
                                                                       new MemberAssignment(new HashSet<>(topicPartitions))));
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                    GROUP_ID,
                    Errors.NONE,
                    "",
                    ConsumerProtocol.PROTOCOL_TYPE,
                    "",
                    asList(memberOne, memberTwo),
                    Collections.emptySet()));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList(GROUP_ID));
            final ConsumerGroupDescription groupDescription = result.describedGroups().get(GROUP_ID).get();

            assertEquals(1, result.describedGroups().size());
            assertEquals(GROUP_ID, groupDescription.groupId());
            assertEquals(2, groupDescription.members().size());
            assertEquals(expectedMemberDescriptions, groupDescription.members());
        }
    }

    @Test
    public void testDescribeMultipleConsumerGroups() {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);

            final List<TopicPartition> topicPartitions = new ArrayList<>();
            topicPartitions.add(0, myTopicPartition0);
            topicPartitions.add(1, myTopicPartition1);
            topicPartitions.add(2, myTopicPartition2);

            final ByteBuffer memberAssignment = ConsumerProtocol.serializeAssignment(new ConsumerPartitionAssignor.Assignment(topicPartitions));
            byte[] memberAssignmentBytes = new byte[memberAssignment.remaining()];
            memberAssignment.get(memberAssignmentBytes);

            DescribeGroupsResponseData group0Data = new DescribeGroupsResponseData();
            group0Data.groups().add(DescribeGroupsResponse.groupMetadata(
                    GROUP_ID,
                    Errors.NONE,
                    "",
                    ConsumerProtocol.PROTOCOL_TYPE,
                    "",
                    asList(
                            DescribeGroupsResponse.groupMember("0", null, "clientId0", "clientHost", memberAssignmentBytes, null),
                            DescribeGroupsResponse.groupMember("1", null, "clientId1", "clientHost", memberAssignmentBytes, null)
                    ),
                    Collections.emptySet()));

            DescribeGroupsResponseData groupConnectData = new DescribeGroupsResponseData();
            group0Data.groups().add(DescribeGroupsResponse.groupMetadata(
                    "group-connect-0",
                    Errors.NONE,
                    "",
                    "connect",
                    "",
                    asList(
                            DescribeGroupsResponse.groupMember("0", null, "clientId0", "clientHost", memberAssignmentBytes, null),
                            DescribeGroupsResponse.groupMember("1", null, "clientId1", "clientHost", memberAssignmentBytes, null)
                    ),
                    Collections.emptySet()));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(group0Data));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(groupConnectData));

            Collection<String> groups = new HashSet<>();
            groups.add(GROUP_ID);
            groups.add("group-connect-0");
            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(groups);
            assertEquals(2, result.describedGroups().size());
            assertEquals(groups, result.describedGroups().keySet());
        }
    }

    @Test
    public void testDescribeConsumerGroupsWithAuthorizedOperationsOmitted() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();
            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NONE,
                "",
                ConsumerProtocol.PROTOCOL_TYPE,
                "",
                Collections.emptyList(),
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList(GROUP_ID));
            final ConsumerGroupDescription groupDescription = result.describedGroups().get(GROUP_ID).get();

            assertNull(groupDescription.authorizedOperations());
        }
    }

    @Test
    public void testDescribeNonConsumerGroups() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DescribeGroupsResponseData data = new DescribeGroupsResponseData();

            data.groups().add(DescribeGroupsResponse.groupMetadata(
                GROUP_ID,
                Errors.NONE,
                "",
                "non-consumer",
                "",
                asList(),
                Collections.emptySet()));

            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            final DescribeConsumerGroupsResult result = env.adminClient().describeConsumerGroups(singletonList(GROUP_ID));

            TestUtils.assertFutureError(result.describedGroups().get(GROUP_ID), IllegalArgumentException.class);
        }
    }

    @Test
    public void testListConsumerGroupOffsetsNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.NOT_COORDINATOR, Collections.emptyMap()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final ListConsumerGroupOffsetsResult result = env.adminClient().listConsumerGroupOffsets(GROUP_ID);


            TestUtils.assertFutureError(result.partitionsToOffsetAndMetadata(), TimeoutException.class);
        }
    }

    @Test
    public void testListConsumerGroupOffsetsRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, new OffsetFetchResponse(Errors.NOT_COORDINATOR, Collections.emptyMap()));

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, new OffsetFetchResponse(Errors.NONE, Collections.emptyMap()));

            final KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> future = env.adminClient().listConsumerGroupOffsets("group-0").partitionsToOffsetAndMetadata();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting ListConsumerGroupOffsets first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry ListConsumerGroupOffsets call on first failure");
            time.sleep(retryBackoff);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, "ListConsumerGroupOffsets retry did not await expected backoff!");
        }
    }

    @Test
    public void testListConsumerGroupOffsetsRetriableErrors() throws Exception {
        // Retriable errors should be retried

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                new OffsetFetchResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Collections.emptyMap()));
            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling list consumer offsets
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.kafkaClient().prepareResponse(
                new OffsetFetchResponse(Errors.NOT_COORDINATOR, Collections.emptyMap()));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                new OffsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE, Collections.emptyMap()));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                new OffsetFetchResponse(Errors.NONE, Collections.emptyMap()));

            final ListConsumerGroupOffsetsResult errorResult1 = env.adminClient().listConsumerGroupOffsets(GROUP_ID);

            assertEquals(Collections.emptyMap(), errorResult1.partitionsToOffsetAndMetadata().get());
        }
    }

    @Test
    public void testListConsumerGroupOffsetsNonRetriableErrors() throws Exception {
        // Non-retriable errors throw an exception
        final List<Errors> nonRetriableErrors = Arrays.asList(
            Errors.GROUP_AUTHORIZATION_FAILED, Errors.INVALID_GROUP_ID, Errors.GROUP_ID_NOT_FOUND);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            for (Errors error : nonRetriableErrors) {
                env.kafkaClient().prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

                env.kafkaClient().prepareResponse(
                    new OffsetFetchResponse(error, Collections.emptyMap()));

                ListConsumerGroupOffsetsResult errorResult = env.adminClient().listConsumerGroupOffsets(GROUP_ID);

                TestUtils.assertFutureError(errorResult.partitionsToOffsetAndMetadata(), error.exception().getClass());
            }
        }
    }

    @Test
    public void testListConsumerGroupOffsets() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode()));

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            // Retriable errors should be retried
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Collections.emptyMap()));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR error when calling list consumer group offsets
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.NOT_COORDINATOR, Collections.emptyMap()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.COORDINATOR_NOT_AVAILABLE, Collections.emptyMap()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            TopicPartition myTopicPartition0 = new TopicPartition("my_topic", 0);
            TopicPartition myTopicPartition1 = new TopicPartition("my_topic", 1);
            TopicPartition myTopicPartition2 = new TopicPartition("my_topic", 2);
            TopicPartition myTopicPartition3 = new TopicPartition("my_topic", 3);

            final Map<TopicPartition, OffsetFetchResponse.PartitionData> responseData = new HashMap<>();
            responseData.put(myTopicPartition0, new OffsetFetchResponse.PartitionData(10,
                    Optional.empty(), "", Errors.NONE));
            responseData.put(myTopicPartition1, new OffsetFetchResponse.PartitionData(0,
                    Optional.empty(), "", Errors.NONE));
            responseData.put(myTopicPartition2, new OffsetFetchResponse.PartitionData(20,
                    Optional.empty(), "", Errors.NONE));
            responseData.put(myTopicPartition3, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET,
                    Optional.empty(), "", Errors.NONE));
            env.kafkaClient().prepareResponse(new OffsetFetchResponse(Errors.NONE, responseData));

            final ListConsumerGroupOffsetsResult result = env.adminClient().listConsumerGroupOffsets(GROUP_ID);
            final Map<TopicPartition, OffsetAndMetadata> partitionToOffsetAndMetadata = result.partitionsToOffsetAndMetadata().get();

            assertEquals(4, partitionToOffsetAndMetadata.size());
            assertEquals(10, partitionToOffsetAndMetadata.get(myTopicPartition0).offset());
            assertEquals(0, partitionToOffsetAndMetadata.get(myTopicPartition1).offset());
            assertEquals(20, partitionToOffsetAndMetadata.get(myTopicPartition2).offset());
            assertTrue(partitionToOffsetAndMetadata.containsKey(myTopicPartition3));
            assertNull(partitionToOffsetAndMetadata.get(myTopicPartition3));
        }
    }

    @Test
    public void testDeleteConsumerGroupsNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();
        final List<String> groupIds = singletonList("groupId");

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            final DeletableGroupResultCollection validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                .setGroupId("groupId")
                .setErrorCode(Errors.NOT_COORDINATOR.code()));
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(validResponse)
            ));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DeleteConsumerGroupsResult result = env.adminClient().deleteConsumerGroups(groupIds);

            TestUtils.assertFutureError(result.all(), TimeoutException.class);
        }
    }

    @Test
    public void testDeleteConsumerGroupsRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;
        final List<String> groupIds = singletonList(GROUP_ID);

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            DeletableGroupResultCollection validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.NOT_COORDINATOR.code()));


            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, new DeleteGroupsResponse(new DeleteGroupsResponseData().setResults(validResponse)));

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                .setGroupId(GROUP_ID)
                .setErrorCode(Errors.NONE.code()));

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, new DeleteGroupsResponse(new DeleteGroupsResponseData().setResults(validResponse)));

            final KafkaFuture<Void> future = env.adminClient().deleteConsumerGroups(groupIds).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting DeleteConsumerGroups first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry DeleteConsumerGroups call on first failure");
            time.sleep(retryBackoff);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, "DeleteConsumerGroups retry did not await expected backoff!");
        }
    }

    @Test
    public void testDeleteConsumerGroupsWithOlderBroker() throws Exception {
        final List<String> groupIds = singletonList("groupId");
        ApiVersion findCoordinatorV3 = new ApiVersion()
                .setApiKey(ApiKeys.FIND_COORDINATOR.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 3);
        ApiVersion describeGroups = new ApiVersion()
                .setApiKey(ApiKeys.DESCRIBE_GROUPS.id)
                .setMinVersion((short) 0)
                .setMaxVersion(ApiKeys.DELETE_GROUPS.latestVersion());

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(Arrays.asList(findCoordinatorV3, describeGroups)));

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE,  Node.noNode()));
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));

            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DeletableGroupResultCollection validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                                  .setGroupId("groupId")
                                  .setErrorCode(Errors.NONE.code()));
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(validResponse)
            ));

            final DeleteConsumerGroupsResult result = env.adminClient().deleteConsumerGroups(groupIds);

            final KafkaFuture<Void> results = result.deletedGroups().get("groupId");
            assertNull(results.get());

            // should throw error for non-retriable errors
            env.kafkaClient().prepareResponse(
                prepareOldFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED, Node.noNode()));

            DeleteConsumerGroupsResult errorResult = env.adminClient().deleteConsumerGroups(groupIds);
            TestUtils.assertFutureError(errorResult.deletedGroups().get("groupId"), GroupAuthorizationException.class);

            // Retriable errors should be retried
            env.kafkaClient().prepareResponse(
                prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DeletableGroupResultCollection errorResponse = new DeletableGroupResultCollection();
            errorResponse.add(new DeletableGroupResult()
                                   .setGroupId("groupId")
                                   .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())
            );
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(errorResponse)));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling delete a consumer group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */

            DeletableGroupResultCollection coordinatorMoved = new DeletableGroupResultCollection();
            coordinatorMoved.add(new DeletableGroupResult()
                                     .setGroupId("groupId")
                                     .setErrorCode(Errors.NOT_COORDINATOR.code())
            );

            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(coordinatorMoved)));
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            coordinatorMoved = new DeletableGroupResultCollection();
            coordinatorMoved.add(new DeletableGroupResult()
                .setGroupId("groupId")
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
            );

            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(coordinatorMoved)));
            env.kafkaClient().prepareResponse(prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                new DeleteGroupsResponseData()
                    .setResults(validResponse)));

            errorResult = env.adminClient().deleteConsumerGroups(groupIds);

            final KafkaFuture<Void> errorResults = errorResult.deletedGroups().get("groupId");
            assertNull(errorResults.get());
        }
    }

    @Test
    public void testDeleteMultipleConsumerGroupsWithOlderBroker() throws Exception {
        final List<String> groupIds = asList("group1", "group2");
        ApiVersion findCoordinatorV3 = new ApiVersion()
                .setApiKey(ApiKeys.FIND_COORDINATOR.id)
                .setMinVersion((short) 0)
                .setMaxVersion((short) 3);
        ApiVersion describeGroups = new ApiVersion()
                .setApiKey(ApiKeys.DESCRIBE_GROUPS.id)
                .setMinVersion((short) 0)
                .setMaxVersion(ApiKeys.DELETE_GROUPS.latestVersion());

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(
                    NodeApiVersions.create(Arrays.asList(findCoordinatorV3, describeGroups)));

            // Dummy response for MockClient to handle the UnsupportedVersionException correctly to switch from batched to un-batched
            env.kafkaClient().prepareResponse(null);
            // Retriable FindCoordinatorResponse errors should be retried
            for (int i = 0; i < groupIds.size(); i++) {
                env.kafkaClient().prepareResponse(
                        prepareOldFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode()));
            }
            for (int i = 0; i < groupIds.size(); i++) {
                env.kafkaClient().prepareResponse(
                        prepareOldFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            }

            final DeletableGroupResultCollection validResponse = new DeletableGroupResultCollection();
            validResponse.add(new DeletableGroupResult()
                    .setGroupId("group1")
                    .setErrorCode(Errors.NONE.code()));
            validResponse.add(new DeletableGroupResult()
                    .setGroupId("group2")
                    .setErrorCode(Errors.NONE.code()));
            env.kafkaClient().prepareResponse(new DeleteGroupsResponse(
                    new DeleteGroupsResponseData()
                            .setResults(validResponse)
            ));

            final DeleteConsumerGroupsResult result = env.adminClient()
                    .deleteConsumerGroups(groupIds);

            final KafkaFuture<Void> results = result.deletedGroups().get("group1");
            assertNull(results.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {
            final TopicPartition tp1 = new TopicPartition("foo", 0);

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            final DeleteConsumerGroupOffsetsResult result = env.adminClient()
                .deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            TestUtils.assertFutureError(result.all(), TimeoutException.class);
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            final TopicPartition tp1 = new TopicPartition("foo", 0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            mockClient.prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR));


            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            mockClient.prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, prepareOffsetDeleteResponse("foo", 0, Errors.NONE));

            final KafkaFuture<Void> future = env.adminClient().deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet())).all();

            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting DeleteConsumerGroupOffsets first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry DeleteConsumerGroupOffsets call on first failure");
            time.sleep(retryBackoff);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, "DeleteConsumerGroupOffsets retry did not await expected backoff!");
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsets() throws Exception {
        // Happy path

        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final TopicPartition tp2 = new TopicPartition("bar", 0);
        final TopicPartition tp3 = new TopicPartition("foobar", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(new OffsetDeleteResponse(
                new OffsetDeleteResponseData()
                    .setTopics(new OffsetDeleteResponseTopicCollection(Stream.of(
                        new OffsetDeleteResponseTopic()
                            .setName("foo")
                            .setPartitions(new OffsetDeleteResponsePartitionCollection(Collections.singletonList(
                                new OffsetDeleteResponsePartition()
                                    .setPartitionIndex(0)
                                    .setErrorCode(Errors.NONE.code())
                            ).iterator())),
                        new OffsetDeleteResponseTopic()
                            .setName("bar")
                            .setPartitions(new OffsetDeleteResponsePartitionCollection(Collections.singletonList(
                                new OffsetDeleteResponsePartition()
                                    .setPartitionIndex(0)
                                    .setErrorCode(Errors.GROUP_SUBSCRIBED_TO_TOPIC.code())
                            ).iterator()))
                    ).collect(Collectors.toList()).iterator()))
                )
            );

            final DeleteConsumerGroupOffsetsResult errorResult = env.adminClient().deleteConsumerGroupOffsets(
                GROUP_ID, Stream.of(tp1, tp2).collect(Collectors.toSet()));

            assertNull(errorResult.partitionResult(tp1).get());
            TestUtils.assertFutureError(errorResult.all(), GroupSubscribedToTopicException.class);
            TestUtils.assertFutureError(errorResult.partitionResult(tp2), GroupSubscribedToTopicException.class);
            assertThrows(IllegalArgumentException.class, () -> errorResult.partitionResult(tp3));
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsRetriableErrors() throws Exception {
        // Retriable errors should be retried

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling delete a consumer group
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse(Errors.NOT_COORDINATOR));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse(Errors.COORDINATOR_NOT_AVAILABLE));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse("foo", 0, Errors.NONE));

            final DeleteConsumerGroupOffsetsResult errorResult1 = env.adminClient()
                .deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            assertNull(errorResult1.all().get());
            assertNull(errorResult1.partitionResult(tp1).get());
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsNonRetriableErrors() throws Exception {
        // Non-retriable errors throw an exception

        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final List<Errors> nonRetriableErrors = Arrays.asList(
            Errors.GROUP_AUTHORIZATION_FAILED, Errors.INVALID_GROUP_ID, Errors.GROUP_ID_NOT_FOUND);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            for (Errors error : nonRetriableErrors) {
                env.kafkaClient().prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

                env.kafkaClient().prepareResponse(
                    prepareOffsetDeleteResponse(error));

                DeleteConsumerGroupOffsetsResult errorResult = env.adminClient()
                    .deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

                TestUtils.assertFutureError(errorResult.all(), error.exception().getClass());
                TestUtils.assertFutureError(errorResult.partitionResult(tp1), error.exception().getClass());
            }
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsFindCoordinatorRetriableErrors() throws Exception {
        // Retriable FindCoordinatorResponse errors should be retried

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode()));
            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetDeleteResponse("foo", 0, Errors.NONE));

            final DeleteConsumerGroupOffsetsResult result = env.adminClient()
                .deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            assertNull(result.all().get());
            assertNull(result.partitionResult(tp1).get());
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsFindCoordinatorNonRetriableErrors() throws Exception {
        // Non-retriable FindCoordinatorResponse errors throw an exception

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED,  Node.noNode()));

            final DeleteConsumerGroupOffsetsResult errorResult = env.adminClient()
                .deleteConsumerGroupOffsets(GROUP_ID, Stream.of(tp1).collect(Collectors.toSet()));

            TestUtils.assertFutureError(errorResult.all(), GroupAuthorizationException.class);
            TestUtils.assertFutureError(errorResult.partitionResult(tp1), GroupAuthorizationException.class);
        }
    }

    @Test
    public void testIncrementalAlterConfigs()  throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            //test error scenarios
            IncrementalAlterConfigsResponseData responseData =  new IncrementalAlterConfigsResponseData();
            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("")
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                    .setErrorMessage("authorization error"));

            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("topic1")
                    .setResourceType(ConfigResource.Type.TOPIC.id())
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Config value append is not allowed for config"));

            env.kafkaClient().prepareResponse(new IncrementalAlterConfigsResponse(responseData));

            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "");
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "topic1");

            AlterConfigOp alterConfigOp1 = new AlterConfigOp(
                    new ConfigEntry("log.segment.bytes", "1073741"),
                    AlterConfigOp.OpType.SET);

            AlterConfigOp alterConfigOp2 = new AlterConfigOp(
                    new ConfigEntry("compression.type", "gzip"),
                    AlterConfigOp.OpType.APPEND);

            final Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(brokerResource, singletonList(alterConfigOp1));
            configs.put(topicResource, singletonList(alterConfigOp2));

            AlterConfigsResult result = env.adminClient().incrementalAlterConfigs(configs);
            TestUtils.assertFutureError(result.values().get(brokerResource), ClusterAuthorizationException.class);
            TestUtils.assertFutureError(result.values().get(topicResource), InvalidRequestException.class);

            // Test a call where there are no errors.
            responseData =  new IncrementalAlterConfigsResponseData();
            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("")
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setErrorCode(Errors.NONE.code())
                    .setErrorMessage(ApiError.NONE.message()));

            env.kafkaClient().prepareResponse(new IncrementalAlterConfigsResponse(responseData));
            env.adminClient().incrementalAlterConfigs(Collections.singletonMap(brokerResource, singletonList(alterConfigOp1))).all().get();
        }
    }

    @Test
    public void testRemoveMembersFromGroupNumRetries() throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            AdminClientConfig.RETRIES_CONFIG, "0")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NOT_COORDINATOR.code())));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Collection<MemberToRemove> membersToRemove = Arrays.asList(new MemberToRemove("instance-1"), new MemberToRemove("instance-2"));

            final RemoveMembersFromConsumerGroupResult result = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID, new RemoveMembersFromConsumerGroupOptions(membersToRemove));

            TestUtils.assertFutureError(result.all(), TimeoutException.class);
        }
    }

    @Test
    public void testRemoveMembersFromGroupRetryBackoff() throws Exception {
        MockTime time = new MockTime();
        int retryBackoff = 100;

        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time,
            mockCluster(3, 0),
            newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoff))) {
            MockClient mockClient = env.kafkaClient();

            mockClient.setNodeApiVersions(NodeApiVersions.create());

            AtomicLong firstAttemptTime = new AtomicLong(0);
            AtomicLong secondAttemptTime = new AtomicLong(0);

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(body -> {
                firstAttemptTime.set(time.milliseconds());
                return true;
            }, new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NOT_COORDINATOR.code())));

            mockClient.prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            MemberResponse responseOne = new MemberResponse()
                .setGroupInstanceId("instance-1")
                .setErrorCode(Errors.NONE.code());
            env.kafkaClient().prepareResponse(body -> {
                secondAttemptTime.set(time.milliseconds());
                return true;
            }, new LeaveGroupResponse(new LeaveGroupResponseData()
                .setErrorCode(Errors.NONE.code())
                .setMembers(Collections.singletonList(responseOne))));

            Collection<MemberToRemove> membersToRemove = singletonList(new MemberToRemove("instance-1"));

            final KafkaFuture<Void> future = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID, new RemoveMembersFromConsumerGroupOptions(membersToRemove)).all();


            TestUtils.waitForCondition(() -> mockClient.numAwaitingResponses() == 1, "Failed awaiting RemoveMembersFromGroup first request failure");
            TestUtils.waitForCondition(() -> ((KafkaAdminClient) env.adminClient()).numPendingCalls() == 1, "Failed to add retry RemoveMembersFromGroup call on first failure");
            time.sleep(retryBackoff);

            future.get();

            long actualRetryBackoff = secondAttemptTime.get() - firstAttemptTime.get();
            assertEquals(retryBackoff, actualRetryBackoff, "RemoveMembersFromGroup retry did not await expected backoff!");
        }
    }

    @Test
    public void testRemoveMembersFromGroupRetriableErrors() throws Exception {
        // Retriable errors should be retried

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                    new LeaveGroupResponse(new LeaveGroupResponseData()
                        .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())));

            /*
             * We need to return two responses here, one for NOT_COORDINATOR call when calling remove member
             * api using coordinator that has moved. This will retry whole operation. So we need to again respond with a
             * FindCoordinatorResponse.
             *
             * And the same reason for the following COORDINATOR_NOT_AVAILABLE error response
             */
            env.kafkaClient().prepareResponse(
                    new LeaveGroupResponse(new LeaveGroupResponseData()
                            .setErrorCode(Errors.NOT_COORDINATOR.code())));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                new LeaveGroupResponse(new LeaveGroupResponseData()
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            MemberResponse memberResponse = new MemberResponse()
                    .setGroupInstanceId("instance-1")
                    .setErrorCode(Errors.NONE.code());
            env.kafkaClient().prepareResponse(
                    new LeaveGroupResponse(new LeaveGroupResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setMembers(Collections.singletonList(memberResponse))));

            MemberToRemove memberToRemove = new MemberToRemove("instance-1");
            Collection<MemberToRemove> membersToRemove = singletonList(memberToRemove);

            final RemoveMembersFromConsumerGroupResult result = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID, new RemoveMembersFromConsumerGroupOptions(membersToRemove));

            assertNull(result.all().get());
            assertNull(result.memberResult(memberToRemove).get());
        }
    }

    @Test
    public void testRemoveMembersFromGroupNonRetriableErrors() throws Exception {
        // Non-retriable errors throw an exception

        final List<Errors> nonRetriableErrors = Arrays.asList(
            Errors.GROUP_AUTHORIZATION_FAILED, Errors.INVALID_GROUP_ID, Errors.GROUP_ID_NOT_FOUND);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            for (Errors error : nonRetriableErrors) {
                env.kafkaClient().prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

                env.kafkaClient().prepareResponse(
                        new LeaveGroupResponse(new LeaveGroupResponseData()
                                .setErrorCode(error.code())));

                MemberToRemove memberToRemove = new MemberToRemove("instance-1");
                Collection<MemberToRemove> membersToRemove = singletonList(memberToRemove);

                final RemoveMembersFromConsumerGroupResult result = env.adminClient().removeMembersFromConsumerGroup(
                    GROUP_ID, new RemoveMembersFromConsumerGroupOptions(membersToRemove));

                TestUtils.assertFutureError(result.all(), error.exception().getClass());
                TestUtils.assertFutureError(result.memberResult(memberToRemove), error.exception().getClass());
            }
        }
    }

    @Test
    public void testRemoveMembersFromGroup() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            final String instanceOne = "instance-1";
            final String instanceTwo = "instance-2";

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Retriable FindCoordinatorResponse errors should be retried
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            // Retriable errors should be retried
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.COORDINATOR_LOAD_IN_PROGRESS.code())));

            // Inject a top-level non-retriable error
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())));

            Collection<MemberToRemove> membersToRemove = Arrays.asList(new MemberToRemove(instanceOne),
                                                                       new MemberToRemove(instanceTwo));
            final RemoveMembersFromConsumerGroupResult unknownErrorResult = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );

            MemberToRemove memberOne = new MemberToRemove(instanceOne);
            MemberToRemove memberTwo = new MemberToRemove(instanceTwo);

            TestUtils.assertFutureError(unknownErrorResult.memberResult(memberOne), UnknownServerException.class);
            TestUtils.assertFutureError(unknownErrorResult.memberResult(memberTwo), UnknownServerException.class);

            MemberResponse responseOne = new MemberResponse()
                                             .setGroupInstanceId(instanceOne)
                                             .setErrorCode(Errors.UNKNOWN_MEMBER_ID.code());

            MemberResponse responseTwo = new MemberResponse()
                                             .setGroupInstanceId(instanceTwo)
                                             .setErrorCode(Errors.NONE.code());

            // Inject one member level error.
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.NONE.code())
                                                                         .setMembers(Arrays.asList(responseOne, responseTwo))));

            final RemoveMembersFromConsumerGroupResult memberLevelErrorResult = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );

            TestUtils.assertFutureError(memberLevelErrorResult.all(), UnknownMemberIdException.class);
            TestUtils.assertFutureError(memberLevelErrorResult.memberResult(memberOne), UnknownMemberIdException.class);
            assertNull(memberLevelErrorResult.memberResult(memberTwo).get());

            // Return with missing member.
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(new LeaveGroupResponseData()
                                                                         .setErrorCode(Errors.NONE.code())
                                                                         .setMembers(Collections.singletonList(responseTwo))));

            final RemoveMembersFromConsumerGroupResult missingMemberResult = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );

            TestUtils.assertFutureError(missingMemberResult.all(), IllegalArgumentException.class);
            // The memberOne was not included in the response.
            TestUtils.assertFutureError(missingMemberResult.memberResult(memberOne), IllegalArgumentException.class);
            assertNull(missingMemberResult.memberResult(memberTwo).get());


            // Return with success.
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(
                    new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()).setMembers(
                        Arrays.asList(responseTwo,
                                      new MemberResponse().setGroupInstanceId(instanceOne).setErrorCode(Errors.NONE.code())
                        ))
            ));

            final RemoveMembersFromConsumerGroupResult noErrorResult = env.adminClient().removeMembersFromConsumerGroup(
                GROUP_ID,
                new RemoveMembersFromConsumerGroupOptions(membersToRemove)
            );
            assertNull(noErrorResult.all().get());
            assertNull(noErrorResult.memberResult(memberOne).get());
            assertNull(noErrorResult.memberResult(memberTwo).get());

            // Test the "removeAll" scenario
            final List<TopicPartition> topicPartitions = Arrays.asList(1, 2, 3).stream().map(partition -> new TopicPartition("my_topic", partition))
                    .collect(Collectors.toList());
            // construct the DescribeGroupsResponse
            DescribeGroupsResponseData data = prepareDescribeGroupsResponseData(GROUP_ID, Arrays.asList(instanceOne, instanceTwo), topicPartitions);

            // Return with partial failure for "removeAll" scenario
            // 1 prepare response for AdminClient.describeConsumerGroups
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            // 2 KafkaAdminClient encounter partial failure when trying to delete all members
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(
                    new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()).setMembers(
                            Arrays.asList(responseOne, responseTwo))
            ));
            final RemoveMembersFromConsumerGroupResult partialFailureResults = env.adminClient().removeMembersFromConsumerGroup(
                    GROUP_ID,
                    new RemoveMembersFromConsumerGroupOptions()
            );
            ExecutionException exception = assertThrows(ExecutionException.class, () -> partialFailureResults.all().get());
            assertTrue(exception.getCause() instanceof KafkaException);
            assertTrue(exception.getCause().getCause() instanceof UnknownMemberIdException);

            // Return with success for "removeAll" scenario
            // 1 prepare response for AdminClient.describeConsumerGroups
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new DescribeGroupsResponse(data));

            // 2. KafkaAdminClient should delete all members correctly
            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(new LeaveGroupResponse(
                    new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()).setMembers(
                            Arrays.asList(responseTwo,
                                    new MemberResponse().setGroupInstanceId(instanceOne).setErrorCode(Errors.NONE.code())
                            ))
            ));
            final RemoveMembersFromConsumerGroupResult successResult = env.adminClient().removeMembersFromConsumerGroup(
                    GROUP_ID,
                    new RemoveMembersFromConsumerGroupOptions()
            );
            assertNull(successResult.all().get());
        }
    }

    private void testRemoveMembersFromGroup(String reason, String expectedReason) throws Exception {
        final Cluster cluster = mockCluster(3, 0);
        final Time time = new MockTime();

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster)) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));
            env.kafkaClient().prepareResponse(body -> {
                if (!(body instanceof LeaveGroupRequest)) {
                    return false;
                }
                LeaveGroupRequestData leaveGroupRequest = ((LeaveGroupRequest) body).data();

                return leaveGroupRequest.members().stream().allMatch(
                        member -> member.reason().equals(expectedReason)
                );
            }, new LeaveGroupResponse(new LeaveGroupResponseData().setErrorCode(Errors.NONE.code()).setMembers(
                    Arrays.asList(
                            new MemberResponse().setGroupInstanceId("instance-1"),
                            new MemberResponse().setGroupInstanceId("instance-2")
                    ))
            ));

            Collection<MemberToRemove> membersToRemove = Arrays.asList(new MemberToRemove("instance-1"), new MemberToRemove("instance-2"));

            RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions(membersToRemove);
            options.reason(reason);

            final RemoveMembersFromConsumerGroupResult result = env.adminClient().removeMembersFromConsumerGroup(
                    GROUP_ID, options);

            assertNull(result.all().get());
        }
    }

    @Test
    public void testRemoveMembersFromGroupReason() throws Exception {
        testRemoveMembersFromGroup("testing remove members reason", LEAVE_GROUP_REASON + ": testing remove members reason");
    }

    @Test
    public void testRemoveMembersFromGroupDefaultReason() throws Exception {
        testRemoveMembersFromGroup(null, LEAVE_GROUP_REASON);
    }

    @Test
    public void testAlterPartitionReassignments() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            TopicPartition tp1 = new TopicPartition("A", 0);
            TopicPartition tp2 = new TopicPartition("B", 0);
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
            reassignments.put(tp1, Optional.empty());
            reassignments.put(tp2, Optional.of(new NewPartitionReassignment(Arrays.asList(1, 2, 3))));

            // 1. server returns less responses than number of partitions we sent
            AlterPartitionReassignmentsResponseData responseData1 = new AlterPartitionReassignmentsResponseData();
            ReassignablePartitionResponse normalPartitionResponse = new ReassignablePartitionResponse().setPartitionIndex(0);
            responseData1.setResponses(Collections.singletonList(
                    new ReassignableTopicResponse()
                            .setName("A")
                            .setPartitions(Collections.singletonList(normalPartitionResponse))));
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(responseData1));
            AlterPartitionReassignmentsResult result1 = env.adminClient().alterPartitionReassignments(reassignments);
            Future<Void> future1 = result1.all();
            Future<Void> future2 = result1.values().get(tp1);
            TestUtils.assertFutureError(future1, UnknownServerException.class);
            TestUtils.assertFutureError(future2, UnknownServerException.class);

            // 2. NOT_CONTROLLER error handling
            AlterPartitionReassignmentsResponseData controllerErrResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setErrorCode(Errors.NOT_CONTROLLER.code())
                            .setErrorMessage(Errors.NOT_CONTROLLER.message())
                            .setResponses(Arrays.asList(
                                new ReassignableTopicResponse()
                                        .setName("A")
                                        .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                new ReassignableTopicResponse()
                                        .setName("B")
                                        .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            MetadataResponse controllerNodeResponse = RequestTestUtils.metadataResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), 1, Collections.emptyList());
            AlterPartitionReassignmentsResponseData normalResponse =
                    new AlterPartitionReassignmentsResponseData()
                            .setResponses(Arrays.asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(controllerErrResponseData));
            env.kafkaClient().prepareResponse(controllerNodeResponse);
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(normalResponse));
            AlterPartitionReassignmentsResult controllerErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            controllerErrResult.all().get();
            controllerErrResult.values().get(tp1).get();
            controllerErrResult.values().get(tp2).get();

            // 3. partition-level error
            AlterPartitionReassignmentsResponseData partitionLevelErrData =
                    new AlterPartitionReassignmentsResponseData()
                            .setResponses(Arrays.asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(new ReassignablePartitionResponse()
                                                .setPartitionIndex(0).setErrorMessage(Errors.INVALID_REPLICA_ASSIGNMENT.message())
                                                .setErrorCode(Errors.INVALID_REPLICA_ASSIGNMENT.code())
                                            )),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(partitionLevelErrData));
            AlterPartitionReassignmentsResult partitionLevelErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            TestUtils.assertFutureError(partitionLevelErrResult.values().get(tp1), Errors.INVALID_REPLICA_ASSIGNMENT.exception().getClass());
            partitionLevelErrResult.values().get(tp2).get();

            // 4. top-level error
            String errorMessage = "this is custom error message";
            AlterPartitionReassignmentsResponseData topLevelErrResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                            .setErrorMessage(errorMessage)
                            .setResponses(Arrays.asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(topLevelErrResponseData));
            AlterPartitionReassignmentsResult topLevelErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            assertEquals(errorMessage, TestUtils.assertFutureThrows(topLevelErrResult.all(), Errors.CLUSTER_AUTHORIZATION_FAILED.exception().getClass()).getMessage());
            assertEquals(errorMessage, TestUtils.assertFutureThrows(topLevelErrResult.values().get(tp1), Errors.CLUSTER_AUTHORIZATION_FAILED.exception().getClass()).getMessage());
            assertEquals(errorMessage, TestUtils.assertFutureThrows(topLevelErrResult.values().get(tp2), Errors.CLUSTER_AUTHORIZATION_FAILED.exception().getClass()).getMessage());

            // 5. unrepresentable topic name error
            TopicPartition invalidTopicTP = new TopicPartition("", 0);
            TopicPartition invalidPartitionTP = new TopicPartition("ABC", -1);
            Map<TopicPartition, Optional<NewPartitionReassignment>> invalidTopicReassignments = new HashMap<>();
            invalidTopicReassignments.put(invalidPartitionTP, Optional.of(new NewPartitionReassignment(Arrays.asList(1, 2, 3))));
            invalidTopicReassignments.put(invalidTopicTP, Optional.of(new NewPartitionReassignment(Arrays.asList(1, 2, 3))));
            invalidTopicReassignments.put(tp1, Optional.of(new NewPartitionReassignment(Arrays.asList(1, 2, 3))));

            AlterPartitionReassignmentsResponseData singlePartResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setResponses(Collections.singletonList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(singlePartResponseData));
            AlterPartitionReassignmentsResult unrepresentableTopicResult = env.adminClient().alterPartitionReassignments(invalidTopicReassignments);
            TestUtils.assertFutureError(unrepresentableTopicResult.values().get(invalidTopicTP), InvalidTopicException.class);
            TestUtils.assertFutureError(unrepresentableTopicResult.values().get(invalidPartitionTP), InvalidTopicException.class);
            unrepresentableTopicResult.values().get(tp1).get();

            // Test success scenario
            AlterPartitionReassignmentsResponseData noErrResponseData =
                    new AlterPartitionReassignmentsResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setErrorMessage(Errors.NONE.message())
                            .setResponses(Arrays.asList(
                                    new ReassignableTopicResponse()
                                            .setName("A")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)),
                                    new ReassignableTopicResponse()
                                            .setName("B")
                                            .setPartitions(Collections.singletonList(normalPartitionResponse)))
                            );
            env.kafkaClient().prepareResponse(new AlterPartitionReassignmentsResponse(noErrResponseData));
            AlterPartitionReassignmentsResult noErrResult = env.adminClient().alterPartitionReassignments(reassignments);
            noErrResult.all().get();
            noErrResult.values().get(tp1).get();
            noErrResult.values().get(tp2).get();
        }
    }

    @Test
    public void testListPartitionReassignments() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            TopicPartition tp1 = new TopicPartition("A", 0);
            OngoingPartitionReassignment tp1PartitionReassignment = new OngoingPartitionReassignment()
                    .setPartitionIndex(0)
                    .setRemovingReplicas(Arrays.asList(1, 2, 3))
                    .setAddingReplicas(Arrays.asList(4, 5, 6))
                    .setReplicas(Arrays.asList(1, 2, 3, 4, 5, 6));
            OngoingTopicReassignment tp1Reassignment = new OngoingTopicReassignment().setName("A")
                    .setPartitions(Collections.singletonList(tp1PartitionReassignment));

            TopicPartition tp2 = new TopicPartition("B", 0);
            OngoingPartitionReassignment tp2PartitionReassignment = new OngoingPartitionReassignment()
                    .setPartitionIndex(0)
                    .setRemovingReplicas(Arrays.asList(1, 2, 3))
                    .setAddingReplicas(Arrays.asList(4, 5, 6))
                    .setReplicas(Arrays.asList(1, 2, 3, 4, 5, 6));
            OngoingTopicReassignment tp2Reassignment = new OngoingTopicReassignment().setName("B")
                    .setPartitions(Collections.singletonList(tp2PartitionReassignment));

            // 1. NOT_CONTROLLER error handling
            ListPartitionReassignmentsResponseData notControllerData = new ListPartitionReassignmentsResponseData()
                    .setErrorCode(Errors.NOT_CONTROLLER.code())
                    .setErrorMessage(Errors.NOT_CONTROLLER.message());
            MetadataResponse controllerNodeResponse = RequestTestUtils.metadataResponse(env.cluster().nodes(),
                    env.cluster().clusterResource().clusterId(), 1, Collections.emptyList());
            ListPartitionReassignmentsResponseData reassignmentsData = new ListPartitionReassignmentsResponseData()
                    .setTopics(Arrays.asList(tp1Reassignment, tp2Reassignment));
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(notControllerData));
            env.kafkaClient().prepareResponse(controllerNodeResponse);
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(reassignmentsData));

            ListPartitionReassignmentsResult noControllerResult = env.adminClient().listPartitionReassignments();
            noControllerResult.reassignments().get(); // no error

            // 2. UNKNOWN_TOPIC_OR_EXCEPTION_ERROR
            ListPartitionReassignmentsResponseData unknownTpData = new ListPartitionReassignmentsResponseData()
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code())
                    .setErrorMessage(Errors.UNKNOWN_TOPIC_OR_PARTITION.message());
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(unknownTpData));

            ListPartitionReassignmentsResult unknownTpResult = env.adminClient().listPartitionReassignments(new HashSet<>(Arrays.asList(tp1, tp2)));
            TestUtils.assertFutureError(unknownTpResult.reassignments(), UnknownTopicOrPartitionException.class);

            // 3. Success
            ListPartitionReassignmentsResponseData responseData = new ListPartitionReassignmentsResponseData()
                    .setTopics(Arrays.asList(tp1Reassignment, tp2Reassignment));
            env.kafkaClient().prepareResponse(new ListPartitionReassignmentsResponse(responseData));
            ListPartitionReassignmentsResult responseResult = env.adminClient().listPartitionReassignments();

            Map<TopicPartition, PartitionReassignment> reassignments = responseResult.reassignments().get();

            PartitionReassignment tp1Result = reassignments.get(tp1);
            assertEquals(tp1PartitionReassignment.addingReplicas(), tp1Result.addingReplicas());
            assertEquals(tp1PartitionReassignment.removingReplicas(), tp1Result.removingReplicas());
            assertEquals(tp1PartitionReassignment.replicas(), tp1Result.replicas());
            assertEquals(tp1PartitionReassignment.replicas(), tp1Result.replicas());
            PartitionReassignment tp2Result = reassignments.get(tp2);
            assertEquals(tp2PartitionReassignment.addingReplicas(), tp2Result.addingReplicas());
            assertEquals(tp2PartitionReassignment.removingReplicas(), tp2Result.removingReplicas());
            assertEquals(tp2PartitionReassignment.replicas(), tp2Result.replicas());
            assertEquals(tp2PartitionReassignment.replicas(), tp2Result.replicas());
        }
    }

    @Test
    public void testAlterConsumerGroupOffsets() throws Exception {
        // Happy path

        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final TopicPartition tp2 = new TopicPartition("bar", 0);
        final TopicPartition tp3 = new TopicPartition("foobar", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            Map<TopicPartition, Errors> responseData = new HashMap<>();
            responseData.put(tp1, Errors.NONE);
            responseData.put(tp2, Errors.NONE);
            env.kafkaClient().prepareResponse(new OffsetCommitResponse(0, responseData));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1, new OffsetAndMetadata(123L));
            offsets.put(tp2, new OffsetAndMetadata(456L));
            final AlterConsumerGroupOffsetsResult result = env.adminClient().alterConsumerGroupOffsets(
                GROUP_ID, offsets);

            assertNull(result.all().get());
            assertNull(result.partitionResult(tp1).get());
            assertNull(result.partitionResult(tp2).get());
            TestUtils.assertFutureError(result.partitionResult(tp3), IllegalArgumentException.class);
        }
    }

    @Test
    public void testAlterConsumerGroupOffsetsRetriableErrors() throws Exception {
        // Retriable errors should be retried

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.COORDINATOR_NOT_AVAILABLE));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.COORDINATOR_LOAD_IN_PROGRESS));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.NOT_COORDINATOR));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.REBALANCE_IN_PROGRESS));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.NONE));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1, new OffsetAndMetadata(123L));
            final AlterConsumerGroupOffsetsResult result1 = env.adminClient()
                .alterConsumerGroupOffsets(GROUP_ID, offsets);

            assertNull(result1.all().get());
            assertNull(result1.partitionResult(tp1).get());
        }
    }

    @Test
    public void testAlterConsumerGroupOffsetsNonRetriableErrors() throws Exception {
        // Non-retriable errors throw an exception

        final TopicPartition tp1 = new TopicPartition("foo", 0);
        final List<Errors> nonRetriableErrors = Arrays.asList(
            Errors.GROUP_AUTHORIZATION_FAILED, Errors.INVALID_GROUP_ID, Errors.GROUP_ID_NOT_FOUND);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            for (Errors error : nonRetriableErrors) {
                env.kafkaClient().prepareResponse(
                    prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

                env.kafkaClient().prepareResponse(prepareOffsetCommitResponse(tp1, error));

                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                offsets.put(tp1,  new OffsetAndMetadata(123L));
                AlterConsumerGroupOffsetsResult errorResult = env.adminClient()
                    .alterConsumerGroupOffsets(GROUP_ID, offsets);

                TestUtils.assertFutureError(errorResult.all(), error.exception().getClass());
                TestUtils.assertFutureError(errorResult.partitionResult(tp1), error.exception().getClass());
            }
        }
    }

    @Test
    public void testAlterConsumerGroupOffsetsFindCoordinatorRetriableErrors() throws Exception {
        // Retriable FindCoordinatorResponse errors should be retried

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_NOT_AVAILABLE, Node.noNode()));
            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.COORDINATOR_LOAD_IN_PROGRESS, Node.noNode()));

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.NONE, env.cluster().controller()));

            env.kafkaClient().prepareResponse(
                prepareOffsetCommitResponse(tp1, Errors.NONE));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1,  new OffsetAndMetadata(123L));
            final AlterConsumerGroupOffsetsResult result = env.adminClient()
                .alterConsumerGroupOffsets(GROUP_ID, offsets);

            assertNull(result.all().get());
            assertNull(result.partitionResult(tp1).get());
        }
    }

    @Test
    public void testAlterConsumerGroupOffsetsFindCoordinatorNonRetriableErrors() throws Exception {
        // Non-retriable FindCoordinatorResponse errors throw an exception

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(mockCluster(1, 0))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(
                prepareFindCoordinatorResponse(Errors.GROUP_AUTHORIZATION_FAILED,  Node.noNode()));

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(tp1,  new OffsetAndMetadata(123L));
            final AlterConsumerGroupOffsetsResult errorResult = env.adminClient()
                .alterConsumerGroupOffsets(GROUP_ID, offsets);

            TestUtils.assertFutureError(errorResult.all(), GroupAuthorizationException.class);
            TestUtils.assertFutureError(errorResult.partitionResult(tp1), GroupAuthorizationException.class);
        }
    }

    @Test
    public void testListOffsets() throws Exception {
        // Happy path

        Node node0 = new Node(0, "localhost", 8120);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0}));
        pInfos.add(new PartitionInfo("bar", 0, node0, new Node[]{node0}, new Node[]{node0}));
        pInfos.add(new PartitionInfo("baz", 0, node0, new Node[]{node0}, new Node[]{node0}));
        pInfos.add(new PartitionInfo("qux", 0, node0, new Node[]{node0}, new Node[]{node0}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                Arrays.asList(node0),
                pInfos,
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                node0);

        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("bar", 0);
        final TopicPartition tp2 = new TopicPartition("baz", 0);
        final TopicPartition tp3 = new TopicPartition("qux", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 123L, 321);
            ListOffsetsTopicResponse t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -1L, 234L, 432);
            ListOffsetsTopicResponse t2 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp2, Errors.NONE, 123456789L, 345L, 543);
            ListOffsetsTopicResponse t3 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp3, Errors.NONE, 234567890L, 456L, 654);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0, t1, t2, t3));
            env.kafkaClient().prepareResponse(new ListOffsetsResponse(responseData));

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            partitions.put(tp1, OffsetSpec.earliest());
            partitions.put(tp2, OffsetSpec.forTimestamp(System.currentTimeMillis()));
            partitions.put(tp3, OffsetSpec.maxTimestamp());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);

            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get();
            assertFalse(offsets.isEmpty());
            assertEquals(123L, offsets.get(tp0).offset());
            assertEquals(321, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp0).timestamp());
            assertEquals(234L, offsets.get(tp1).offset());
            assertEquals(432, offsets.get(tp1).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp1).timestamp());
            assertEquals(345L, offsets.get(tp2).offset());
            assertEquals(543, offsets.get(tp2).leaderEpoch().get().intValue());
            assertEquals(123456789L, offsets.get(tp2).timestamp());
            assertEquals(456L, offsets.get(tp3).offset());
            assertEquals(654, offsets.get(tp3).leaderEpoch().get().intValue());
            assertEquals(234567890L, offsets.get(tp3).timestamp());
            assertEquals(offsets.get(tp0), result.partitionResult(tp0).get());
            assertEquals(offsets.get(tp1), result.partitionResult(tp1).get());
            assertEquals(offsets.get(tp2), result.partitionResult(tp2).get());
            assertEquals(offsets.get(tp3), result.partitionResult(tp3).get());
            try {
                result.partitionResult(new TopicPartition("unknown", 0)).get();
                fail("should have thrown IllegalArgumentException");
            } catch (IllegalArgumentException expected) { }
        }
    }

    @Test
    public void testListOffsetsRetriableErrorOnMetadata() throws Exception {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        final Cluster cluster = new Cluster(
            "mockClusterId",
            nodes,
            Collections.singleton(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node})),
            Collections.emptySet(),
            Collections.emptySet(),
            node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE));
            // metadata refresh because of UNKNOWN_TOPIC_OR_PARTITION
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));
            // listoffsets response from broker 0
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(Collections.singletonList(ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 123L, 321)));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node);

            ListOffsetsResult result = env.adminClient().listOffsets(Collections.singletonMap(tp0, OffsetSpec.latest()));

            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get(3, TimeUnit.SECONDS);
            assertEquals(1, offsets.size());
            assertEquals(123L, offsets.get(tp0).offset());
            assertEquals(321, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp0).timestamp());
        }
    }

    @Test
    public void testListOffsetsRetriableErrors() throws Exception {

        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        List<Node> nodes = Arrays.asList(node0, node1);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        pInfos.add(new PartitionInfo("foo", 1, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        pInfos.add(new PartitionInfo("bar", 0, node1, new Node[]{node1, node0}, new Node[]{node1, node0}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                node0);

        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);
        final TopicPartition tp2 = new TopicPartition("bar", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));
            // listoffsets response from broker 0
            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.LEADER_NOT_AVAILABLE, -1L, 123L, 321);
            ListOffsetsTopicResponse t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -1L, 987L, 789);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0, t1));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node0);
            // listoffsets response from broker 1
            ListOffsetsTopicResponse t2 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp2, Errors.NONE, -1L, 456L, 654);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t2));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node1);

            // metadata refresh because of LEADER_NOT_AVAILABLE
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));
            // listoffsets response from broker 0
            t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 345L, 543);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node0);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            partitions.put(tp1, OffsetSpec.latest());
            partitions.put(tp2, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);

            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get();
            assertFalse(offsets.isEmpty());
            assertEquals(345L, offsets.get(tp0).offset());
            assertEquals(543, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp0).timestamp());
            assertEquals(987L, offsets.get(tp1).offset());
            assertEquals(789, offsets.get(tp1).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp1).timestamp());
            assertEquals(456L, offsets.get(tp2).offset());
            assertEquals(654, offsets.get(tp2).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp2).timestamp());
        }
    }

    @Test
    public void testListOffsetsNonRetriableErrors() throws Exception {

        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        List<Node> nodes = Arrays.asList(node0, node1);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                node0);

        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.TOPIC_AUTHORIZATION_FAILED, -1L, -1L, -1);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0));
            env.kafkaClient().prepareResponse(new ListOffsetsResponse(responseData));

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);

            TestUtils.assertFutureError(result.all(), TopicAuthorizationException.class);
        }
    }

    @Test
    public void testListOffsetsMaxTimestampUnsupportedSingleOffsetSpec() {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        final Cluster cluster = new Cluster(
            "mockClusterId",
            nodes,
            Collections.singleton(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node})),
            Collections.emptySet(),
            Collections.emptySet(),
            node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster, AdminClientConfig.RETRIES_CONFIG, "2")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(
                    ApiKeys.LIST_OFFSETS.id, (short) 0, (short) 6));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            // listoffsets response from broker 0
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ListOffsetsRequest);

            ListOffsetsResult result = env.adminClient().listOffsets(Collections.singletonMap(tp0, OffsetSpec.maxTimestamp()));

            TestUtils.assertFutureThrows(result.all(), UnsupportedVersionException.class);
        }
    }

    @Test
    public void testListOffsetsMaxTimestampUnsupportedMultipleOffsetSpec() throws Exception {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node}));
        pInfos.add(new PartitionInfo("foo", 1, node, new Node[]{node}, new Node[]{node}));
        final Cluster cluster = new Cluster(
            "mockClusterId",
            nodes,
            pInfos,
            Collections.emptySet(),
            Collections.emptySet(),
            node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster,
            AdminClientConfig.RETRIES_CONFIG, "2")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(
                    ApiKeys.LIST_OFFSETS.id, (short) 0, (short) 6));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            // listoffsets response from broker 0
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ListOffsetsRequest);

            ListOffsetsTopicResponse topicResponse = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -1L, 345L, 543);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                .setThrottleTimeMs(0)
                .setTopics(Arrays.asList(topicResponse));
            env.kafkaClient().prepareResponseFrom(
                // ensure that no max timestamp requests are retried
                request -> request instanceof ListOffsetsRequest && ((ListOffsetsRequest) request).topics().stream()
                    .flatMap(t -> t.partitions().stream())
                    .noneMatch(p -> p.timestamp() == ListOffsetsRequest.MAX_TIMESTAMP),
                new ListOffsetsResponse(responseData), node);

            ListOffsetsResult result = env.adminClient().listOffsets(new HashMap<TopicPartition, OffsetSpec>() {{
                    put(tp0, OffsetSpec.maxTimestamp());
                    put(tp1, OffsetSpec.latest());
                }});

            TestUtils.assertFutureThrows(result.partitionResult(tp0), UnsupportedVersionException.class);

            ListOffsetsResultInfo tp1Offset = result.partitionResult(tp1).get();
            assertEquals(345L, tp1Offset.offset());
            assertEquals(543, tp1Offset.leaderEpoch().get().intValue());
            assertEquals(-1L, tp1Offset.timestamp());
        }
    }

    @Test
    public void testListOffsetsUnsupportedNonMaxTimestamp() {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node}));
        final Cluster cluster = new Cluster(
            "mockClusterId",
            nodes,
            pInfos,
            Collections.emptySet(),
            Collections.emptySet(),
            node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster,
            AdminClientConfig.RETRIES_CONFIG, "2")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(
                    ApiKeys.LIST_OFFSETS.id, (short) 0, (short) 0));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            // listoffsets response from broker 0
            env.kafkaClient().prepareUnsupportedVersionResponse(
                request -> request instanceof ListOffsetsRequest);

            ListOffsetsResult result = env.adminClient().listOffsets(
                Collections.singletonMap(tp0, OffsetSpec.latest()));

            TestUtils.assertFutureThrows(result.partitionResult(tp0), UnsupportedVersionException.class);
        }
    }

    @Test
    public void testListOffsetsNonMaxTimestampDowngradedImmediately() throws Exception {
        Node node = new Node(0, "localhost", 8120);
        List<Node> nodes = Collections.singletonList(node);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node, new Node[]{node}, new Node[]{node}));
        final Cluster cluster = new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.emptySet(),
                Collections.emptySet(),
                node);
        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster,
                AdminClientConfig.RETRIES_CONFIG, "2")) {

            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create(
                    ApiKeys.LIST_OFFSETS.id, (short) 0, (short) 6));

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 123L, 321);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0));

            // listoffsets response from broker 0
            env.kafkaClient().prepareResponse(
                    request -> request instanceof ListOffsetsRequest,
                    new ListOffsetsResponse(responseData));

            ListOffsetsResult result = env.adminClient().listOffsets(
                    Collections.singletonMap(tp0, OffsetSpec.latest()));

            ListOffsetsResultInfo tp0Offset = result.partitionResult(tp0).get();
            assertEquals(123L, tp0Offset.offset());
            assertEquals(321, tp0Offset.leaderEpoch().get().intValue());
            assertEquals(-1L, tp0Offset.timestamp());
        }
    }

    private Map<String, FeatureUpdate> makeTestFeatureUpdates() {
        return Utils.mkMap(
            Utils.mkEntry("test_feature_1", new FeatureUpdate((short) 2, false)),
            Utils.mkEntry("test_feature_2", new FeatureUpdate((short) 3, true)));
    }

    private Map<String, ApiError> makeTestFeatureUpdateErrors(final Map<String, FeatureUpdate> updates, final Errors error) {
        final Map<String, ApiError> errors = new HashMap<>();
        for (Map.Entry<String, FeatureUpdate> entry : updates.entrySet()) {
            errors.put(entry.getKey(), new ApiError(error));
        }
        return errors;
    }

    private void testUpdateFeatures(Map<String, FeatureUpdate> featureUpdates,
                                    ApiError topLevelError,
                                    Map<String, ApiError> featureUpdateErrors) throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponse(
                body -> body instanceof UpdateFeaturesRequest,
                UpdateFeaturesResponse.createWithErrors(topLevelError, featureUpdateErrors, 0));
            final Map<String, KafkaFuture<Void>> futures = env.adminClient().updateFeatures(
                featureUpdates,
                new UpdateFeaturesOptions().timeoutMs(10000)).values();
            for (final Map.Entry<String, KafkaFuture<Void>> entry : futures.entrySet()) {
                final KafkaFuture<Void> future = entry.getValue();
                final ApiError error = featureUpdateErrors.get(entry.getKey());
                if (topLevelError.error() == Errors.NONE) {
                    assertNotNull(error);
                    if (error.error() == Errors.NONE) {
                        future.get();
                    } else {
                        final ExecutionException e = assertThrows(ExecutionException.class, future::get);
                        assertEquals(e.getCause().getClass(), error.exception().getClass());
                    }
                } else {
                    final ExecutionException e = assertThrows(ExecutionException.class, future::get);
                    assertEquals(e.getCause().getClass(), topLevelError.exception().getClass());
                }
            }
        }
    }

    @Test
    public void testUpdateFeaturesDuringSuccess() throws Exception {
        final Map<String, FeatureUpdate> updates = makeTestFeatureUpdates();
        testUpdateFeatures(updates, ApiError.NONE, makeTestFeatureUpdateErrors(updates, Errors.NONE));
    }

    @Test
    public void testUpdateFeaturesTopLevelError() throws Exception {
        final Map<String, FeatureUpdate> updates = makeTestFeatureUpdates();
        testUpdateFeatures(updates, new ApiError(Errors.INVALID_REQUEST), new HashMap<>());
    }

    @Test
    public void testUpdateFeaturesInvalidRequestError() throws Exception {
        final Map<String, FeatureUpdate> updates = makeTestFeatureUpdates();
        testUpdateFeatures(updates, ApiError.NONE, makeTestFeatureUpdateErrors(updates, Errors.INVALID_REQUEST));
    }

    @Test
    public void testUpdateFeaturesUpdateFailedError() throws Exception {
        final Map<String, FeatureUpdate> updates = makeTestFeatureUpdates();
        testUpdateFeatures(updates, ApiError.NONE, makeTestFeatureUpdateErrors(updates, Errors.FEATURE_UPDATE_FAILED));
    }

    @Test
    public void testUpdateFeaturesPartialSuccess() throws Exception {
        final Map<String, ApiError> errors = makeTestFeatureUpdateErrors(makeTestFeatureUpdates(), Errors.NONE);
        errors.put("test_feature_2", new ApiError(Errors.INVALID_REQUEST));
        testUpdateFeatures(makeTestFeatureUpdates(), ApiError.NONE, errors);
    }

    @Test
    public void testUpdateFeaturesHandleNotControllerException() throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof UpdateFeaturesRequest,
                UpdateFeaturesResponse.createWithErrors(
                    new ApiError(Errors.NOT_CONTROLLER),
                    Utils.mkMap(),
                    0),
                env.cluster().nodeById(0));
            final int controllerId = 1;
            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(env.cluster().nodes(),
                env.cluster().clusterResource().clusterId(),
                controllerId,
                Collections.emptyList()));
            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof UpdateFeaturesRequest,
                UpdateFeaturesResponse.createWithErrors(
                    ApiError.NONE,
                    Utils.mkMap(Utils.mkEntry("test_feature_1", ApiError.NONE),
                                Utils.mkEntry("test_feature_2", ApiError.NONE)),
                    0),
                env.cluster().nodeById(controllerId));
            final KafkaFuture<Void> future = env.adminClient().updateFeatures(
                Utils.mkMap(
                    Utils.mkEntry("test_feature_1", new FeatureUpdate((short) 2, false)),
                    Utils.mkEntry("test_feature_2", new FeatureUpdate((short) 3, true))),
                new UpdateFeaturesOptions().timeoutMs(10000)
            ).all();
            future.get();
        }
    }

    @Test
    public void testUpdateFeaturesShouldFailRequestForEmptyUpdates() {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            assertThrows(
                IllegalArgumentException.class,
                () -> env.adminClient().updateFeatures(
                    new HashMap<>(), new UpdateFeaturesOptions()));
        }
    }

    @Test
    public void testUpdateFeaturesShouldFailRequestForInvalidFeatureName() {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            assertThrows(
                IllegalArgumentException.class,
                () -> env.adminClient().updateFeatures(
                    Utils.mkMap(Utils.mkEntry("feature", new FeatureUpdate((short) 2, false)),
                                Utils.mkEntry("", new FeatureUpdate((short) 2, false))),
                    new UpdateFeaturesOptions()));
        }
    }

    @Test
    public void testUpdateFeaturesShouldFailRequestInClientWhenDowngradeFlagIsNotSetDuringDeletion() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new FeatureUpdate((short) 0, false));
    }

    @Test
    public void testDescribeFeaturesSuccess() throws Exception {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponse(
                body -> body instanceof ApiVersionsRequest,
                prepareApiVersionsResponseForDescribeFeatures(Errors.NONE));
            final KafkaFuture<FeatureMetadata> future = env.adminClient().describeFeatures(
                new DescribeFeaturesOptions().timeoutMs(10000)).featureMetadata();
            final FeatureMetadata metadata = future.get();
            assertEquals(defaultFeatureMetadata(), metadata);
        }
    }

    @Test
    public void testDescribeFeaturesFailure() {
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().prepareResponse(
                body -> body instanceof ApiVersionsRequest,
                prepareApiVersionsResponseForDescribeFeatures(Errors.INVALID_REQUEST));
            final DescribeFeaturesOptions options = new DescribeFeaturesOptions();
            options.timeoutMs(10000);
            final KafkaFuture<FeatureMetadata> future = env.adminClient().describeFeatures(options).featureMetadata();
            final ExecutionException e = assertThrows(ExecutionException.class, future::get);
            assertEquals(e.getCause().getClass(), Errors.INVALID_REQUEST.exception().getClass());
        }
    }

    @Test
    public void testListOffsetsMetadataRetriableErrors() throws Exception {

        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        List<Node> nodes = Arrays.asList(node0, node1);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0}));
        pInfos.add(new PartitionInfo("foo", 1, node1, new Node[]{node1}, new Node[]{node1}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                node0);

        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.LEADER_NOT_AVAILABLE));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.UNKNOWN_TOPIC_OR_PARTITION));
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            // listoffsets response from broker 0
            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 345L, 543);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node0);
            // listoffsets response from broker 1
            ListOffsetsTopicResponse t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -1L, 789L, 987);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t1));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node1);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            partitions.put(tp1, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);

            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get();
            assertFalse(offsets.isEmpty());
            assertEquals(345L, offsets.get(tp0).offset());
            assertEquals(543, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp0).timestamp());
            assertEquals(789L, offsets.get(tp1).offset());
            assertEquals(987, offsets.get(tp1).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp1).timestamp());
        }
    }

    @Test
    public void testListOffsetsWithMultiplePartitionsLeaderChange() throws Exception {
        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        Node node2 = new Node(2, "localhost", 8122);
        List<Node> nodes = Arrays.asList(node0, node1, node2);

        final PartitionInfo oldPInfo1 = new PartitionInfo("foo", 0, node0,
            new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
        final PartitionInfo oldPnfo2 = new PartitionInfo("foo", 1, node0,
            new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
        List<PartitionInfo> oldPInfos = Arrays.asList(oldPInfo1, oldPnfo2);

        final Cluster oldCluster = new Cluster("mockClusterId", nodes, oldPInfos,
            Collections.emptySet(), Collections.emptySet(), node0);
        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(oldCluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(oldCluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NOT_LEADER_OR_FOLLOWER, -1L, 345L, 543);
            ListOffsetsTopicResponse t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.LEADER_NOT_AVAILABLE, -2L, 123L, 456);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0, t1));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node0);

            final PartitionInfo newPInfo1 = new PartitionInfo("foo", 0, node1,
                new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
            final PartitionInfo newPInfo2 = new PartitionInfo("foo", 1, node2,
                new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
            List<PartitionInfo> newPInfos = Arrays.asList(newPInfo1, newPInfo2);

            final Cluster newCluster = new Cluster("mockClusterId", nodes, newPInfos,
                Collections.emptySet(), Collections.emptySet(), node0);

            env.kafkaClient().prepareResponse(prepareMetadataResponse(newCluster, Errors.NONE));

            t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -1L, 345L, 543);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node1);

            t1 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp1, Errors.NONE, -2L, 123L, 456);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t1));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node2);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            partitions.put(tp1, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);
            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get();

            assertFalse(offsets.isEmpty());
            assertEquals(345L, offsets.get(tp0).offset());
            assertEquals(543, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-1L, offsets.get(tp0).timestamp());
            assertEquals(123L, offsets.get(tp1).offset());
            assertEquals(456, offsets.get(tp1).leaderEpoch().get().intValue());
            assertEquals(-2L, offsets.get(tp1).timestamp());
        }
    }

    @Test
    public void testListOffsetsWithLeaderChange() throws Exception {
        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        Node node2 = new Node(2, "localhost", 8122);
        List<Node> nodes = Arrays.asList(node0, node1, node2);

        final PartitionInfo oldPartitionInfo = new PartitionInfo("foo", 0, node0,
            new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
        final Cluster oldCluster = new Cluster("mockClusterId", nodes, singletonList(oldPartitionInfo),
            Collections.emptySet(), Collections.emptySet(), node0);
        final TopicPartition tp0 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(oldCluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(oldCluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NOT_LEADER_OR_FOLLOWER, -1L, 345L, 543);
            ListOffsetsResponseData responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node0);

            // updating leader from node0 to node1 and metadata refresh because of NOT_LEADER_OR_FOLLOWER
            final PartitionInfo newPartitionInfo = new PartitionInfo("foo", 0, node1,
                new Node[]{node0, node1, node2}, new Node[]{node0, node1, node2});
            final Cluster newCluster = new Cluster("mockClusterId", nodes, singletonList(newPartitionInfo),
                Collections.emptySet(), Collections.emptySet(), node0);

            env.kafkaClient().prepareResponse(prepareMetadataResponse(newCluster, Errors.NONE));

            t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -2L, 123L, 456);
            responseData = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(responseData), node1);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);
            Map<TopicPartition, ListOffsetsResultInfo> offsets = result.all().get();

            assertFalse(offsets.isEmpty());
            assertEquals(123L, offsets.get(tp0).offset());
            assertEquals(456, offsets.get(tp0).leaderEpoch().get().intValue());
            assertEquals(-2L, offsets.get(tp0).timestamp());
        }
    }

    @Test
    public void testListOffsetsMetadataNonRetriableErrors() throws Exception {

        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        List<Node> nodes = Arrays.asList(node0, node1);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                node0);

        final TopicPartition tp1 = new TopicPartition("foo", 0);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.TOPIC_AUTHORIZATION_FAILED));

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp1, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);

            TestUtils.assertFutureError(result.all(), TopicAuthorizationException.class);
        }
    }

    @Test
    public void testListOffsetsPartialResponse() throws Exception {
        Node node0 = new Node(0, "localhost", 8120);
        Node node1 = new Node(1, "localhost", 8121);
        List<Node> nodes = Arrays.asList(node0, node1);
        List<PartitionInfo> pInfos = new ArrayList<>();
        pInfos.add(new PartitionInfo("foo", 0, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        pInfos.add(new PartitionInfo("foo", 1, node0, new Node[]{node0, node1}, new Node[]{node0, node1}));
        final Cluster cluster =
            new Cluster(
                "mockClusterId",
                nodes,
                pInfos,
                Collections.<String>emptySet(),
                Collections.<String>emptySet(),
                node0);

        final TopicPartition tp0 = new TopicPartition("foo", 0);
        final TopicPartition tp1 = new TopicPartition("foo", 1);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(cluster)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            ListOffsetsTopicResponse t0 = ListOffsetsResponse.singletonListOffsetsTopicResponse(tp0, Errors.NONE, -2L, 123L, 456);
            ListOffsetsResponseData data = new ListOffsetsResponseData()
                    .setThrottleTimeMs(0)
                    .setTopics(Arrays.asList(t0));
            env.kafkaClient().prepareResponseFrom(new ListOffsetsResponse(data), node0);

            Map<TopicPartition, OffsetSpec> partitions = new HashMap<>();
            partitions.put(tp0, OffsetSpec.latest());
            partitions.put(tp1, OffsetSpec.latest());
            ListOffsetsResult result = env.adminClient().listOffsets(partitions);
            assertNotNull(result.partitionResult(tp0).get());
            TestUtils.assertFutureThrows(result.partitionResult(tp1), ApiException.class);
            TestUtils.assertFutureThrows(result.all(), ApiException.class);
        }
    }

    @Test
    public void testGetSubLevelError() {
        List<MemberIdentity> memberIdentities = Arrays.asList(
            new MemberIdentity().setGroupInstanceId("instance-0"),
            new MemberIdentity().setGroupInstanceId("instance-1"));
        Map<MemberIdentity, Errors> errorsMap = new HashMap<>();
        errorsMap.put(memberIdentities.get(0), Errors.NONE);
        errorsMap.put(memberIdentities.get(1), Errors.FENCED_INSTANCE_ID);
        assertEquals(IllegalArgumentException.class, KafkaAdminClient.getSubLevelError(errorsMap,
                                                                                       new MemberIdentity().setGroupInstanceId("non-exist-id"), "For unit test").getClass());
        assertNull(KafkaAdminClient.getSubLevelError(errorsMap, memberIdentities.get(0), "For unit test"));
        assertEquals(FencedInstanceIdException.class, KafkaAdminClient.getSubLevelError(
            errorsMap, memberIdentities.get(1), "For unit test").getClass());
    }

    @Test
    public void testSuccessfulRetryAfterRequestTimeout() throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        MockTime time = new MockTime();
        Node node0 = new Node(0, "localhost", 8121);
        nodes.put(0, node0);
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                Arrays.asList(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0})),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        final int requestTimeoutMs = 1000;
        final int retryBackoffMs = 100;
        final int apiTimeoutMs = 3000;

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final ListTopicsResult result = env.adminClient()
                    .listTopics(new ListTopicsOptions().timeoutMs(apiTimeoutMs));

            // Wait until the first attempt has been sent, then advance the time
            TestUtils.waitForCondition(() -> env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for Metadata request to be sent");
            time.sleep(requestTimeoutMs + 1);

            // Wait for the request to be timed out before backing off
            TestUtils.waitForCondition(() -> !env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for inFlightRequests to be timed out");
            time.sleep(retryBackoffMs);

            // Since api timeout bound is not hit, AdminClient should retry
            TestUtils.waitForCondition(() -> env.kafkaClient().hasInFlightRequests(),
                    "Failed to retry Metadata request");
            env.kafkaClient().respond(prepareMetadataResponse(cluster, Errors.NONE));

            assertEquals(1, result.listings().get().size());
            assertEquals("foo", result.listings().get().iterator().next().name());
        }
    }

    @Test
    public void testDefaultApiTimeout() throws Exception {
        testApiTimeout(1500, 3000, OptionalInt.empty());
    }

    @Test
    public void testDefaultApiTimeoutOverride() throws Exception {
        testApiTimeout(1500, 10000, OptionalInt.of(3000));
    }

    private void testApiTimeout(int requestTimeoutMs,
                                int defaultApiTimeoutMs,
                                OptionalInt overrideApiTimeoutMs) throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        MockTime time = new MockTime();
        Node node0 = new Node(0, "localhost", 8121);
        nodes.put(0, node0);
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                Arrays.asList(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0})),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        final int retryBackoffMs = 100;
        final int effectiveTimeoutMs = overrideApiTimeoutMs.orElse(defaultApiTimeoutMs);
        assertEquals(2 * requestTimeoutMs, effectiveTimeoutMs,
            "This test expects the effective timeout to be twice the request timeout");

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs),
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(defaultApiTimeoutMs))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            ListTopicsOptions options = new ListTopicsOptions();
            overrideApiTimeoutMs.ifPresent(options::timeoutMs);

            final ListTopicsResult result = env.adminClient().listTopics(options);

            // Wait until the first attempt has been sent, then advance the time
            TestUtils.waitForCondition(() -> env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for Metadata request to be sent");
            time.sleep(requestTimeoutMs + 1);

            // Wait for the request to be timed out before backing off
            TestUtils.waitForCondition(() -> !env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for inFlightRequests to be timed out");

            // Since api timeout bound is not hit, AdminClient should retry
            TestUtils.waitForCondition(() -> {
                boolean hasInflightRequests = env.kafkaClient().hasInFlightRequests();
                if (!hasInflightRequests)
                    time.sleep(retryBackoffMs);
                return hasInflightRequests;
            }, "Timed out waiting for Metadata request to be sent");
            time.sleep(requestTimeoutMs + 1);

            TestUtils.assertFutureThrows(result.future, TimeoutException.class);
        }
    }

    @Test
    public void testRequestTimeoutExceedingDefaultApiTimeout() throws Exception {
        HashMap<Integer, Node> nodes = new HashMap<>();
        MockTime time = new MockTime();
        Node node0 = new Node(0, "localhost", 8121);
        nodes.put(0, node0);
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                Arrays.asList(new PartitionInfo("foo", 0, node0, new Node[]{node0}, new Node[]{node0})),
                Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), nodes.get(0));

        // This test assumes the default api timeout value of 60000. When the request timeout
        // is set to something larger, we should adjust the api timeout accordingly for compatibility.

        final int retryBackoffMs = 100;
        final int requestTimeoutMs = 120000;

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(retryBackoffMs),
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(requestTimeoutMs))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            ListTopicsOptions options = new ListTopicsOptions();

            final ListTopicsResult result = env.adminClient().listTopics(options);

            // Wait until the first attempt has been sent, then advance the time by the default api timeout
            TestUtils.waitForCondition(() -> env.kafkaClient().hasInFlightRequests(),
                    "Timed out waiting for Metadata request to be sent");
            time.sleep(60001);

            // The in-flight request should not be cancelled
            assertTrue(env.kafkaClient().hasInFlightRequests());

            // Now sleep the remaining time for the request timeout to expire
            time.sleep(60000);
            TestUtils.assertFutureThrows(result.future, TimeoutException.class);
        }
    }

    private ClientQuotaEntity newClientQuotaEntity(String... args) {
        assertTrue(args.length % 2 == 0);

        Map<String, String> entityMap = new HashMap<>(args.length / 2);
        for (int index = 0; index < args.length; index += 2) {
            entityMap.put(args[index], args[index + 1]);
        }
        return new ClientQuotaEntity(entityMap);
    }

    @Test
    public void testDescribeClientQuotas() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final String value = "value";

            Map<ClientQuotaEntity, Map<String, Double>> responseData = new HashMap<>();
            ClientQuotaEntity entity1 = newClientQuotaEntity(ClientQuotaEntity.USER, "user-1", ClientQuotaEntity.CLIENT_ID, value);
            ClientQuotaEntity entity2 = newClientQuotaEntity(ClientQuotaEntity.USER, "user-2", ClientQuotaEntity.CLIENT_ID, value);
            responseData.put(entity1, Collections.singletonMap("consumer_byte_rate", 10000.0));
            responseData.put(entity2, Collections.singletonMap("producer_byte_rate", 20000.0));

            env.kafkaClient().prepareResponse(DescribeClientQuotasResponse.fromQuotaEntities(responseData, 0));

            ClientQuotaFilter filter = ClientQuotaFilter.contains(asList(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, value)));

            DescribeClientQuotasResult result = env.adminClient().describeClientQuotas(filter);
            Map<ClientQuotaEntity, Map<String, Double>> resultData = result.entities().get();
            assertEquals(resultData.size(), 2);
            assertTrue(resultData.containsKey(entity1));
            Map<String, Double> config1 = resultData.get(entity1);
            assertEquals(config1.size(), 1);
            assertEquals(config1.get("consumer_byte_rate"), 10000.0, 1e-6);
            assertTrue(resultData.containsKey(entity2));
            Map<String, Double> config2 = resultData.get(entity2);
            assertEquals(config2.size(), 1);
            assertEquals(config2.get("producer_byte_rate"), 20000.0, 1e-6);
        }
    }

    @Test
    public void testEqualsOfClientQuotaFilterComponent() {
        assertEquals(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER));

        assertEquals(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));

        // match = null is different from match = Empty
        assertNotEquals(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));

        assertEquals(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"));

        assertNotEquals(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER));

        assertNotEquals(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));
    }

    @Test
    public void testAlterClientQuotas() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            ClientQuotaEntity goodEntity = newClientQuotaEntity(ClientQuotaEntity.USER, "user-1");
            ClientQuotaEntity unauthorizedEntity = newClientQuotaEntity(ClientQuotaEntity.USER, "user-0");
            ClientQuotaEntity invalidEntity = newClientQuotaEntity("", "user-0");

            Map<ClientQuotaEntity, ApiError> responseData = new HashMap<>(2);
            responseData.put(goodEntity, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Authorization failed"));
            responseData.put(unauthorizedEntity, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Authorization failed"));
            responseData.put(invalidEntity, new ApiError(Errors.INVALID_REQUEST, "Invalid quota entity"));

            env.kafkaClient().prepareResponse(AlterClientQuotasResponse.fromQuotaEntities(responseData, 0));

            List<ClientQuotaAlteration> entries = new ArrayList<>(3);
            entries.add(new ClientQuotaAlteration(goodEntity, singleton(new ClientQuotaAlteration.Op("consumer_byte_rate", 10000.0))));
            entries.add(new ClientQuotaAlteration(unauthorizedEntity, singleton(new ClientQuotaAlteration.Op("producer_byte_rate", 10000.0))));
            entries.add(new ClientQuotaAlteration(invalidEntity, singleton(new ClientQuotaAlteration.Op("producer_byte_rate", 100.0))));

            AlterClientQuotasResult result = env.adminClient().alterClientQuotas(entries);
            result.values().get(goodEntity);
            TestUtils.assertFutureError(result.values().get(unauthorizedEntity), ClusterAuthorizationException.class);
            TestUtils.assertFutureError(result.values().get(invalidEntity), InvalidRequestException.class);
        }
    }

    @Test
    public void testAlterReplicaLogDirsSuccess() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            createAlterLogDirsResponse(env, env.cluster().nodeById(0), Errors.NONE, 0);
            createAlterLogDirsResponse(env, env.cluster().nodeById(1), Errors.NONE, 0);

            TopicPartitionReplica tpr0 = new TopicPartitionReplica("topic", 0, 0);
            TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 0, 1);

            Map<TopicPartitionReplica, String> logDirs = new HashMap<>();
            logDirs.put(tpr0, "/data0");
            logDirs.put(tpr1, "/data1");
            AlterReplicaLogDirsResult result = env.adminClient().alterReplicaLogDirs(logDirs);
            assertNull(result.values().get(tpr0).get());
            assertNull(result.values().get(tpr1).get());
        }
    }

    @Test
    public void testAlterReplicaLogDirsLogDirNotFound() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            createAlterLogDirsResponse(env, env.cluster().nodeById(0), Errors.NONE, 0);
            createAlterLogDirsResponse(env, env.cluster().nodeById(1), Errors.LOG_DIR_NOT_FOUND, 0);

            TopicPartitionReplica tpr0 = new TopicPartitionReplica("topic", 0, 0);
            TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 0, 1);

            Map<TopicPartitionReplica, String> logDirs = new HashMap<>();
            logDirs.put(tpr0, "/data0");
            logDirs.put(tpr1, "/data1");
            AlterReplicaLogDirsResult result = env.adminClient().alterReplicaLogDirs(logDirs);
            assertNull(result.values().get(tpr0).get());
            TestUtils.assertFutureError(result.values().get(tpr1), LogDirNotFoundException.class);
        }
    }

    @Test
    public void testAlterReplicaLogDirsUnrequested() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            createAlterLogDirsResponse(env, env.cluster().nodeById(0), Errors.NONE, 1, 2);

            TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 1, 0);

            Map<TopicPartitionReplica, String> logDirs = new HashMap<>();
            logDirs.put(tpr1, "/data1");
            AlterReplicaLogDirsResult result = env.adminClient().alterReplicaLogDirs(logDirs);
            assertNull(result.values().get(tpr1).get());
        }
    }

    @Test
    public void testAlterReplicaLogDirsPartialResponse() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            createAlterLogDirsResponse(env, env.cluster().nodeById(0), Errors.NONE, 1);

            TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 1, 0);
            TopicPartitionReplica tpr2 = new TopicPartitionReplica("topic", 2, 0);

            Map<TopicPartitionReplica, String> logDirs = new HashMap<>();
            logDirs.put(tpr1, "/data1");
            logDirs.put(tpr2, "/data1");
            AlterReplicaLogDirsResult result = env.adminClient().alterReplicaLogDirs(logDirs);
            assertNull(result.values().get(tpr1).get());
            TestUtils.assertFutureThrows(result.values().get(tpr2), ApiException.class);
        }
    }

    @Test
    public void testAlterReplicaLogDirsPartialFailure() throws Exception {
        long defaultApiTimeout = 60000;
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = mockClientEnv(time, AdminClientConfig.RETRIES_CONFIG, "0")) {

            // Provide only one prepared response from node 1
            env.kafkaClient().prepareResponseFrom(
                prepareAlterLogDirsResponse(Errors.NONE, "topic", 2),
                env.cluster().nodeById(1));

            TopicPartitionReplica tpr1 = new TopicPartitionReplica("topic", 1, 0);
            TopicPartitionReplica tpr2 = new TopicPartitionReplica("topic", 2, 1);

            Map<TopicPartitionReplica, String> logDirs = new HashMap<>();
            logDirs.put(tpr1, "/data1");
            logDirs.put(tpr2, "/data1");

            AlterReplicaLogDirsResult result = env.adminClient().alterReplicaLogDirs(logDirs);

            // Wait until the prepared attempt has been consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                "Failed awaiting requests");

            // Wait until the request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                "Failed awaiting request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            TestUtils.assertFutureThrows(result.values().get(tpr1), ApiException.class);
            assertNull(result.values().get(tpr2).get());
        }
    }

    @Test
    public void testDescribeUserScramCredentials() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final String user0Name = "user0";
            final ScramMechanism user0ScramMechanism0 = ScramMechanism.SCRAM_SHA_256;
            final int user0Iterations0 = 4096;
            final ScramMechanism user0ScramMechanism1 = ScramMechanism.SCRAM_SHA_512;
            final int user0Iterations1 = 8192;

            final CredentialInfo user0CredentialInfo0 = new CredentialInfo();
            user0CredentialInfo0.setMechanism(user0ScramMechanism0.type());
            user0CredentialInfo0.setIterations(user0Iterations0);
            final CredentialInfo user0CredentialInfo1 = new CredentialInfo();
            user0CredentialInfo1.setMechanism(user0ScramMechanism1.type());
            user0CredentialInfo1.setIterations(user0Iterations1);

            final String user1Name = "user1";
            final ScramMechanism user1ScramMechanism = ScramMechanism.SCRAM_SHA_256;
            final int user1Iterations = 4096;

            final CredentialInfo user1CredentialInfo = new CredentialInfo();
            user1CredentialInfo.setMechanism(user1ScramMechanism.type());
            user1CredentialInfo.setIterations(user1Iterations);

            final DescribeUserScramCredentialsResponseData responseData = new DescribeUserScramCredentialsResponseData();
            responseData.setResults(Arrays.asList(
                    new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                            .setUser(user0Name)
                            .setCredentialInfos(Arrays.asList(user0CredentialInfo0, user0CredentialInfo1)),
                    new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                            .setUser(user1Name)
                            .setCredentialInfos(singletonList(user1CredentialInfo))));
            final DescribeUserScramCredentialsResponse response = new DescribeUserScramCredentialsResponse(responseData);

            final Set<String> usersRequestedSet = new HashSet<>();
            usersRequestedSet.add(user0Name);
            usersRequestedSet.add(user1Name);

            for (final List<String> users : asList(null, new ArrayList<String>(), asList(user0Name, null, user1Name))) {
                env.kafkaClient().prepareResponse(response);

                final DescribeUserScramCredentialsResult result = env.adminClient().describeUserScramCredentials(users);
                final Map<String, UserScramCredentialsDescription> descriptionResults = result.all().get();
                final KafkaFuture<UserScramCredentialsDescription> user0DescriptionFuture = result.description(user0Name);
                final KafkaFuture<UserScramCredentialsDescription> user1DescriptionFuture = result.description(user1Name);

                final Set<String> usersDescribedFromUsersSet = new HashSet<>(result.users().get());
                assertEquals(usersRequestedSet, usersDescribedFromUsersSet);

                final Set<String> usersDescribedFromMapKeySet = descriptionResults.keySet();
                assertEquals(usersRequestedSet, usersDescribedFromMapKeySet);

                final UserScramCredentialsDescription userScramCredentialsDescription0 = descriptionResults.get(user0Name);
                assertEquals(user0Name, userScramCredentialsDescription0.name());
                assertEquals(2, userScramCredentialsDescription0.credentialInfos().size());
                assertEquals(user0ScramMechanism0, userScramCredentialsDescription0.credentialInfos().get(0).mechanism());
                assertEquals(user0Iterations0, userScramCredentialsDescription0.credentialInfos().get(0).iterations());
                assertEquals(user0ScramMechanism1, userScramCredentialsDescription0.credentialInfos().get(1).mechanism());
                assertEquals(user0Iterations1, userScramCredentialsDescription0.credentialInfos().get(1).iterations());
                assertEquals(userScramCredentialsDescription0, user0DescriptionFuture.get());

                final UserScramCredentialsDescription userScramCredentialsDescription1 = descriptionResults.get(user1Name);
                assertEquals(user1Name, userScramCredentialsDescription1.name());
                assertEquals(1, userScramCredentialsDescription1.credentialInfos().size());
                assertEquals(user1ScramMechanism, userScramCredentialsDescription1.credentialInfos().get(0).mechanism());
                assertEquals(user1Iterations, userScramCredentialsDescription1.credentialInfos().get(0).iterations());
                assertEquals(userScramCredentialsDescription1, user1DescriptionFuture.get());
            }
        }
    }

    @Test
    public void testAlterUserScramCredentialsUnknownMechanism() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final String user0Name = "user0";
            ScramMechanism user0ScramMechanism0 = ScramMechanism.UNKNOWN;

            final String user1Name = "user1";
            ScramMechanism user1ScramMechanism0 = ScramMechanism.UNKNOWN;

            final String user2Name = "user2";
            ScramMechanism user2ScramMechanism0 = ScramMechanism.SCRAM_SHA_256;

            AlterUserScramCredentialsResponseData responseData = new AlterUserScramCredentialsResponseData();
            responseData.setResults(Arrays.asList(
                    new AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult().setUser(user2Name)));

            env.kafkaClient().prepareResponse(new AlterUserScramCredentialsResponse(responseData));

            AlterUserScramCredentialsResult result = env.adminClient().alterUserScramCredentials(Arrays.asList(
                    new UserScramCredentialDeletion(user0Name, user0ScramMechanism0),
                    new UserScramCredentialUpsertion(user1Name, new ScramCredentialInfo(user1ScramMechanism0, 8192), "password"),
                    new UserScramCredentialUpsertion(user2Name, new ScramCredentialInfo(user2ScramMechanism0, 4096), "password")));
            Map<String, KafkaFuture<Void>> resultData = result.values();
            assertEquals(3, resultData.size());
            Arrays.asList(user0Name, user1Name).stream().forEach(u -> {
                assertTrue(resultData.containsKey(u));
                try {
                    resultData.get(u).get();
                    fail("Expected request for user " + u + " to complete exceptionally, but it did not");
                } catch (Exception expected) {
                    // ignore
                }
            });
            assertTrue(resultData.containsKey(user2Name));
            try {
                resultData.get(user2Name).get();
            } catch (Exception e) {
                fail("Expected request for user " + user2Name + " to NOT complete excdptionally, but it did");
            }
            try {
                result.all().get();
                fail("Expected 'result.all().get()' to throw an exception since at least one user failed, but it did not");
            } catch (final Exception expected) {
                // ignore, expected
            }
        }
    }

    @Test
    public void testAlterUserScramCredentials() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final String user0Name = "user0";
            ScramMechanism user0ScramMechanism0 = ScramMechanism.SCRAM_SHA_256;
            ScramMechanism user0ScramMechanism1 = ScramMechanism.SCRAM_SHA_512;
            final String user1Name = "user1";
            ScramMechanism user1ScramMechanism0 = ScramMechanism.SCRAM_SHA_256;
            final String user2Name = "user2";
            ScramMechanism user2ScramMechanism0 = ScramMechanism.SCRAM_SHA_512;
            AlterUserScramCredentialsResponseData responseData = new AlterUserScramCredentialsResponseData();
            responseData.setResults(Arrays.asList(user0Name, user1Name, user2Name).stream().map(u ->
                    new AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
                    .setUser(u).setErrorCode(Errors.NONE.code())).collect(Collectors.toList()));

            env.kafkaClient().prepareResponse(new AlterUserScramCredentialsResponse(responseData));

            AlterUserScramCredentialsResult result = env.adminClient().alterUserScramCredentials(Arrays.asList(
                    new UserScramCredentialDeletion(user0Name, user0ScramMechanism0),
                    new UserScramCredentialUpsertion(user0Name, new ScramCredentialInfo(user0ScramMechanism1, 8192), "password"),
                    new UserScramCredentialUpsertion(user1Name, new ScramCredentialInfo(user1ScramMechanism0, 8192), "password"),
                    new UserScramCredentialDeletion(user2Name, user2ScramMechanism0)));
            Map<String, KafkaFuture<Void>> resultData = result.values();
            assertEquals(3, resultData.size());
            Arrays.asList(user0Name, user1Name, user2Name).stream().forEach(u -> {
                assertTrue(resultData.containsKey(u));
                assertFalse(resultData.get(u).isCompletedExceptionally());
            });
        }
    }

    private void createAlterLogDirsResponse(AdminClientUnitTestEnv env, Node node, Errors error, int... partitions) {
        env.kafkaClient().prepareResponseFrom(
            prepareAlterLogDirsResponse(error, "topic", partitions), node);
    }

    private AlterReplicaLogDirsResponse prepareAlterLogDirsResponse(Errors error, String topic, int... partitions) {
        return new AlterReplicaLogDirsResponse(
            new AlterReplicaLogDirsResponseData().setResults(singletonList(
                new AlterReplicaLogDirTopicResult()
                    .setTopicName(topic)
                    .setPartitions(Arrays.stream(partitions).boxed().map(partitionId ->
                        new AlterReplicaLogDirPartitionResult()
                            .setPartitionIndex(partitionId)
                            .setErrorCode(error.code())).collect(Collectors.toList())))));
    }

    @Test
    public void testDescribeLogDirsPartialFailure() throws Exception {
        long defaultApiTimeout = 60000;
        MockTime time = new MockTime();

        try (AdminClientUnitTestEnv env = mockClientEnv(time, AdminClientConfig.RETRIES_CONFIG, "0")) {

            env.kafkaClient().prepareResponseFrom(
                prepareDescribeLogDirsResponse(Errors.NONE, "/data"),
                env.cluster().nodeById(1));

            DescribeLogDirsResult result = env.adminClient().describeLogDirs(Arrays.asList(0, 1));

            // Wait until the prepared attempt has been consumed
            TestUtils.waitForCondition(() -> env.kafkaClient().numAwaitingResponses() == 0,
                "Failed awaiting requests");

            // Wait until the request is sent out
            TestUtils.waitForCondition(() -> env.kafkaClient().inFlightRequestCount() == 1,
                "Failed awaiting request");

            // Advance time past the default api timeout to time out the inflight request
            time.sleep(defaultApiTimeout + 1);

            TestUtils.assertFutureThrows(result.descriptions().get(0), ApiException.class);
            assertNotNull(result.descriptions().get(1).get());
        }
    }

    @Test
    public void testUnregisterBrokerSuccess() throws InterruptedException, ExecutionException {
        int nodeId = 1;
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(
                    NodeApiVersions.create(ApiKeys.UNREGISTER_BROKER.id, (short) 0, (short) 0));
            env.kafkaClient().prepareResponse(prepareUnregisterBrokerResponse(Errors.NONE, 0));
            UnregisterBrokerResult result = env.adminClient().unregisterBroker(nodeId);
            // Validate response
            assertNotNull(result.all());
            result.all().get();
        }
    }

    @Test
    public void testUnregisterBrokerFailure() {
        int nodeId = 1;
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(
                    NodeApiVersions.create(ApiKeys.UNREGISTER_BROKER.id, (short) 0, (short) 0));
            env.kafkaClient().prepareResponse(prepareUnregisterBrokerResponse(Errors.UNKNOWN_SERVER_ERROR, 0));
            UnregisterBrokerResult result = env.adminClient().unregisterBroker(nodeId);
            // Validate response
            assertNotNull(result.all());
            TestUtils.assertFutureThrows(result.all(), Errors.UNKNOWN_SERVER_ERROR.exception().getClass());
        }
    }

    @Test
    public void testUnregisterBrokerTimeoutAndSuccessRetry() throws ExecutionException, InterruptedException {
        int nodeId = 1;
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(
                    NodeApiVersions.create(ApiKeys.UNREGISTER_BROKER.id, (short) 0, (short) 0));
            env.kafkaClient().prepareResponse(prepareUnregisterBrokerResponse(Errors.REQUEST_TIMED_OUT, 0));
            env.kafkaClient().prepareResponse(prepareUnregisterBrokerResponse(Errors.NONE, 0));

            UnregisterBrokerResult result = env.adminClient().unregisterBroker(nodeId);

            // Validate response
            assertNotNull(result.all());
            result.all().get();
        }
    }

    @Test
    public void testUnregisterBrokerTimeoutAndFailureRetry() {
        int nodeId = 1;
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(
                    NodeApiVersions.create(ApiKeys.UNREGISTER_BROKER.id, (short) 0, (short) 0));
            env.kafkaClient().prepareResponse(prepareUnregisterBrokerResponse(Errors.REQUEST_TIMED_OUT, 0));
            env.kafkaClient().prepareResponse(prepareUnregisterBrokerResponse(Errors.UNKNOWN_SERVER_ERROR, 0));

            UnregisterBrokerResult result = env.adminClient().unregisterBroker(nodeId);

            // Validate response
            assertNotNull(result.all());
            TestUtils.assertFutureThrows(result.all(), Errors.UNKNOWN_SERVER_ERROR.exception().getClass());
        }
    }

    @Test
    public void testUnregisterBrokerTimeoutMaxRetry() {
        int nodeId = 1;
        try (final AdminClientUnitTestEnv env = mockClientEnv(Time.SYSTEM, AdminClientConfig.RETRIES_CONFIG, "1")) {
            env.kafkaClient().setNodeApiVersions(
                    NodeApiVersions.create(ApiKeys.UNREGISTER_BROKER.id, (short) 0, (short) 0));
            env.kafkaClient().prepareResponse(prepareUnregisterBrokerResponse(Errors.REQUEST_TIMED_OUT, 0));
            env.kafkaClient().prepareResponse(prepareUnregisterBrokerResponse(Errors.REQUEST_TIMED_OUT, 0));

            UnregisterBrokerResult result = env.adminClient().unregisterBroker(nodeId);

            // Validate response
            assertNotNull(result.all());
            TestUtils.assertFutureThrows(result.all(), Errors.REQUEST_TIMED_OUT.exception().getClass());
        }
    }

    @Test
    public void testUnregisterBrokerTimeoutMaxWait() {
        int nodeId = 1;
        try (final AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(
                    NodeApiVersions.create(ApiKeys.UNREGISTER_BROKER.id, (short) 0, (short) 0));

            UnregisterBrokerOptions options = new UnregisterBrokerOptions();
            options.timeoutMs = 10;
            UnregisterBrokerResult result = env.adminClient().unregisterBroker(nodeId, options);

            // Validate response
            assertNotNull(result.all());
            TestUtils.assertFutureThrows(result.all(), Errors.REQUEST_TIMED_OUT.exception().getClass());
        }
    }

    @Test
    public void testDescribeProducers() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            TopicPartition topicPartition = new TopicPartition("foo", 0);

            Node leader = env.cluster().nodes().iterator().next();
            expectMetadataRequest(env, topicPartition, leader);

            List<ProducerState> expected = Arrays.asList(
                new ProducerState(12345L, 15, 30, env.time().milliseconds(),
                    OptionalInt.of(99), OptionalLong.empty()),
                new ProducerState(12345L, 15, 30, env.time().milliseconds(),
                    OptionalInt.empty(), OptionalLong.of(23423L))
            );

            DescribeProducersResponse response = buildDescribeProducersResponse(
                topicPartition,
                expected
            );

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof DescribeProducersRequest,
                response,
                leader
            );

            DescribeProducersResult result = env.adminClient().describeProducers(singleton(topicPartition));
            KafkaFuture<DescribeProducersResult.PartitionProducerState> partitionFuture =
                result.partitionResult(topicPartition);
            assertEquals(new HashSet<>(expected), new HashSet<>(partitionFuture.get().activeProducers()));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testDescribeProducersTimeout(boolean timeoutInMetadataLookup) throws Exception {
        MockTime time = new MockTime();
        try (AdminClientUnitTestEnv env = mockClientEnv(time)) {
            TopicPartition topicPartition = new TopicPartition("foo", 0);
            int requestTimeoutMs = 15000;

            if (!timeoutInMetadataLookup) {
                Node leader = env.cluster().nodes().iterator().next();
                expectMetadataRequest(env, topicPartition, leader);
            }

            DescribeProducersOptions options = new DescribeProducersOptions().timeoutMs(requestTimeoutMs);
            DescribeProducersResult result = env.adminClient().describeProducers(
                singleton(topicPartition), options);
            assertFalse(result.all().isDone());

            time.sleep(requestTimeoutMs);
            TestUtils.waitForCondition(() -> result.all().isDone(),
                "Future failed to timeout after expiration of timeout");

            assertTrue(result.all().isCompletedExceptionally());
            TestUtils.assertFutureThrows(result.all(), TimeoutException.class);
            assertFalse(env.kafkaClient().hasInFlightRequests());
        }
    }

    @Test
    public void testDescribeProducersRetryAfterDisconnect() throws Exception {
        MockTime time = new MockTime();
        int retryBackoffMs = 100;
        Cluster cluster = mockCluster(3, 0);
        Map<String, Object> configOverride = newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoffMs);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster, configOverride)) {
            TopicPartition topicPartition = new TopicPartition("foo", 0);
            Iterator<Node> nodeIterator = env.cluster().nodes().iterator();

            Node initialLeader = nodeIterator.next();
            expectMetadataRequest(env, topicPartition, initialLeader);

            List<ProducerState> expected = Arrays.asList(
                new ProducerState(12345L, 15, 30, env.time().milliseconds(),
                    OptionalInt.of(99), OptionalLong.empty()),
                new ProducerState(12345L, 15, 30, env.time().milliseconds(),
                    OptionalInt.empty(), OptionalLong.of(23423L))
            );

            DescribeProducersResponse response = buildDescribeProducersResponse(
                topicPartition,
                expected
            );

            env.kafkaClient().prepareResponseFrom(
                request -> {
                    // We need a sleep here because the client will attempt to
                    // backoff after the disconnect
                    env.time().sleep(retryBackoffMs);
                    return request instanceof DescribeProducersRequest;
                },
                response,
                initialLeader,
                true
            );

            Node retryLeader = nodeIterator.next();
            expectMetadataRequest(env, topicPartition, retryLeader);

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof DescribeProducersRequest,
                response,
                retryLeader
            );

            DescribeProducersResult result = env.adminClient().describeProducers(singleton(topicPartition));
            KafkaFuture<DescribeProducersResult.PartitionProducerState> partitionFuture =
                result.partitionResult(topicPartition);
            assertEquals(new HashSet<>(expected), new HashSet<>(partitionFuture.get().activeProducers()));
        }
    }

    @Test
    public void testDescribeTransactions() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            String transactionalId = "foo";
            Node coordinator = env.cluster().nodes().iterator().next();
            TransactionDescription expected = new TransactionDescription(
                coordinator.id(), TransactionState.COMPLETE_COMMIT, 12345L,
                15, 10000L, OptionalLong.empty(), emptySet());

            env.kafkaClient().prepareResponse(
                request -> request instanceof FindCoordinatorRequest,
                prepareFindCoordinatorResponse(Errors.NONE, transactionalId, coordinator)
            );

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof DescribeTransactionsRequest,
                new DescribeTransactionsResponse(new DescribeTransactionsResponseData().setTransactionStates(
                    singletonList(new DescribeTransactionsResponseData.TransactionState()
                        .setErrorCode(Errors.NONE.code())
                        .setProducerEpoch((short) expected.producerEpoch())
                        .setProducerId(expected.producerId())
                        .setTransactionalId(transactionalId)
                        .setTransactionTimeoutMs(10000)
                        .setTransactionStartTimeMs(-1)
                        .setTransactionState(expected.state().toString())
                    )
                )),
                coordinator
            );

            DescribeTransactionsResult result = env.adminClient().describeTransactions(singleton(transactionalId));
            KafkaFuture<TransactionDescription> future = result.description(transactionalId);
            assertEquals(expected, future.get());
        }
    }

    @Test
    public void testRetryDescribeTransactionsAfterNotCoordinatorError() throws Exception {
        MockTime time = new MockTime();
        int retryBackoffMs = 100;
        Cluster cluster = mockCluster(3, 0);
        Map<String, Object> configOverride = newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoffMs);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster, configOverride)) {
            String transactionalId = "foo";

            Iterator<Node> nodeIterator = env.cluster().nodes().iterator();
            Node coordinator1 = nodeIterator.next();
            Node coordinator2 = nodeIterator.next();

            env.kafkaClient().prepareResponse(
                request -> request instanceof FindCoordinatorRequest,
                new FindCoordinatorResponse(new FindCoordinatorResponseData()
                        .setCoordinators(Arrays.asList(new FindCoordinatorResponseData.Coordinator()
                                .setKey(transactionalId)
                                .setErrorCode(Errors.NONE.code())
                                .setNodeId(coordinator1.id())
                                .setHost(coordinator1.host())
                                .setPort(coordinator1.port()))))
            );

            env.kafkaClient().prepareResponseFrom(
                request -> {
                    if (!(request instanceof DescribeTransactionsRequest)) {
                        return false;
                    } else {
                        // Backoff needed here for the retry of FindCoordinator
                        time.sleep(retryBackoffMs);
                        return true;
                    }
                },
                new DescribeTransactionsResponse(new DescribeTransactionsResponseData().setTransactionStates(
                    singletonList(new DescribeTransactionsResponseData.TransactionState()
                        .setErrorCode(Errors.NOT_COORDINATOR.code())
                        .setTransactionalId(transactionalId)
                    )
                )),
                coordinator1
            );

            env.kafkaClient().prepareResponse(
                request -> request instanceof FindCoordinatorRequest,
                new FindCoordinatorResponse(new FindCoordinatorResponseData()
                        .setCoordinators(Arrays.asList(new FindCoordinatorResponseData.Coordinator()
                                .setKey(transactionalId)
                                .setErrorCode(Errors.NONE.code())
                                .setNodeId(coordinator2.id())
                                .setHost(coordinator2.host())
                                .setPort(coordinator2.port()))))
            );

            TransactionDescription expected = new TransactionDescription(
                coordinator2.id(), TransactionState.COMPLETE_COMMIT, 12345L,
                15, 10000L, OptionalLong.empty(), emptySet());

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof DescribeTransactionsRequest,
                new DescribeTransactionsResponse(new DescribeTransactionsResponseData().setTransactionStates(
                    singletonList(new DescribeTransactionsResponseData.TransactionState()
                        .setErrorCode(Errors.NONE.code())
                        .setProducerEpoch((short) expected.producerEpoch())
                        .setProducerId(expected.producerId())
                        .setTransactionalId(transactionalId)
                        .setTransactionTimeoutMs(10000)
                        .setTransactionStartTimeMs(-1)
                        .setTransactionState(expected.state().toString())
                    )
                )),
                coordinator2
            );

            DescribeTransactionsResult result = env.adminClient().describeTransactions(singleton(transactionalId));
            KafkaFuture<TransactionDescription> future = result.description(transactionalId);
            assertEquals(expected, future.get());
        }
    }

    @Test
    public void testAbortTransaction() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            TopicPartition topicPartition = new TopicPartition("foo", 13);
            AbortTransactionSpec abortSpec = new AbortTransactionSpec(
                topicPartition, 12345L, (short) 15, 200);
            Node leader = env.cluster().nodes().iterator().next();

            expectMetadataRequest(env, topicPartition, leader);

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof WriteTxnMarkersRequest,
                writeTxnMarkersResponse(abortSpec, Errors.NONE),
                leader
            );

            AbortTransactionResult result = env.adminClient().abortTransaction(abortSpec);
            assertNull(result.all().get());
        }
    }

    @Test
    public void testAbortTransactionFindLeaderAfterDisconnect() throws Exception {
        MockTime time = new MockTime();
        int retryBackoffMs = 100;
        Cluster cluster = mockCluster(3, 0);
        Map<String, Object> configOverride = newStrMap(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "" + retryBackoffMs);

        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster, configOverride)) {
            TopicPartition topicPartition = new TopicPartition("foo", 13);
            AbortTransactionSpec abortSpec = new AbortTransactionSpec(
                topicPartition, 12345L, (short) 15, 200);
            Iterator<Node> nodeIterator = env.cluster().nodes().iterator();
            Node firstLeader = nodeIterator.next();

            expectMetadataRequest(env, topicPartition, firstLeader);

            WriteTxnMarkersResponse response = writeTxnMarkersResponse(abortSpec, Errors.NONE);
            env.kafkaClient().prepareResponseFrom(
                request -> {
                    // We need a sleep here because the client will attempt to
                    // backoff after the disconnect
                    time.sleep(retryBackoffMs);
                    return request instanceof WriteTxnMarkersRequest;
                },
                response,
                firstLeader,
                true
            );

            Node retryLeader = nodeIterator.next();
            expectMetadataRequest(env, topicPartition, retryLeader);

            env.kafkaClient().prepareResponseFrom(
                request -> request instanceof WriteTxnMarkersRequest,
                response,
                retryLeader
            );

            AbortTransactionResult result = env.adminClient().abortTransaction(abortSpec);
            assertNull(result.all().get());
        }
    }

    @Test
    public void testListTransactions() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            MetadataResponseData.MetadataResponseBrokerCollection brokers =
                new MetadataResponseData.MetadataResponseBrokerCollection();

            env.cluster().nodes().forEach(node -> {
                brokers.add(new MetadataResponseData.MetadataResponseBroker()
                    .setHost(node.host())
                    .setNodeId(node.id())
                    .setPort(node.port())
                    .setRack(node.rack())
                );
            });

            env.kafkaClient().prepareResponse(
                request -> request instanceof MetadataRequest,
                new MetadataResponse(new MetadataResponseData().setBrokers(brokers),
                    MetadataResponseData.HIGHEST_SUPPORTED_VERSION)
            );

            List<TransactionListing> expected = Arrays.asList(
                new TransactionListing("foo", 12345L, TransactionState.ONGOING),
                new TransactionListing("bar", 98765L, TransactionState.PREPARE_ABORT),
                new TransactionListing("baz", 13579L, TransactionState.COMPLETE_COMMIT)
            );
            assertEquals(Utils.mkSet(0, 1, 2), env.cluster().nodes().stream().map(Node::id)
                .collect(Collectors.toSet()));

            env.cluster().nodes().forEach(node -> {
                ListTransactionsResponseData response = new ListTransactionsResponseData()
                    .setErrorCode(Errors.NONE.code());

                TransactionListing listing = expected.get(node.id());
                response.transactionStates().add(new ListTransactionsResponseData.TransactionState()
                    .setTransactionalId(listing.transactionalId())
                    .setProducerId(listing.producerId())
                    .setTransactionState(listing.state().toString())
                );

                env.kafkaClient().prepareResponseFrom(
                    request -> request instanceof ListTransactionsRequest,
                    new ListTransactionsResponse(response),
                    node
                );
            });

            ListTransactionsResult result = env.adminClient().listTransactions();
            assertEquals(new HashSet<>(expected), new HashSet<>(result.all().get()));
        }
    }

    private WriteTxnMarkersResponse writeTxnMarkersResponse(
        AbortTransactionSpec abortSpec,
        Errors error
    ) {
        WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult partitionResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerPartitionResult()
                .setPartitionIndex(abortSpec.topicPartition().partition())
                .setErrorCode(error.code());

        WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult topicResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerTopicResult()
                .setName(abortSpec.topicPartition().topic());
        topicResponse.partitions().add(partitionResponse);

        WriteTxnMarkersResponseData.WritableTxnMarkerResult markerResponse =
            new WriteTxnMarkersResponseData.WritableTxnMarkerResult()
                .setProducerId(abortSpec.producerId());
        markerResponse.topics().add(topicResponse);

        WriteTxnMarkersResponseData response = new WriteTxnMarkersResponseData();
        response.markers().add(markerResponse);

        return new WriteTxnMarkersResponse(response);
    }

    private DescribeProducersResponse buildDescribeProducersResponse(
        TopicPartition topicPartition,
        List<ProducerState> producerStates
    ) {
        DescribeProducersResponseData response = new DescribeProducersResponseData();

        DescribeProducersResponseData.TopicResponse topicResponse =
            new DescribeProducersResponseData.TopicResponse()
                .setName(topicPartition.topic());
        response.topics().add(topicResponse);

        DescribeProducersResponseData.PartitionResponse partitionResponse =
            new DescribeProducersResponseData.PartitionResponse()
                .setPartitionIndex(topicPartition.partition())
                .setErrorCode(Errors.NONE.code());
        topicResponse.partitions().add(partitionResponse);

        partitionResponse.setActiveProducers(producerStates.stream().map(producerState ->
            new DescribeProducersResponseData.ProducerState()
                .setProducerId(producerState.producerId())
                .setProducerEpoch(producerState.producerEpoch())
                .setCoordinatorEpoch(producerState.coordinatorEpoch().orElse(-1))
                .setLastSequence(producerState.lastSequence())
                .setLastTimestamp(producerState.lastTimestamp())
                .setCurrentTxnStartOffset(producerState.currentTransactionStartOffset().orElse(-1L))
        ).collect(Collectors.toList()));

        return new DescribeProducersResponse(response);
    }

    private void expectMetadataRequest(
        AdminClientUnitTestEnv env,
        TopicPartition topicPartition,
        Node leader
    ) {
        MetadataResponseData.MetadataResponseTopicCollection responseTopics =
            new MetadataResponseData.MetadataResponseTopicCollection();

        MetadataResponseTopic responseTopic = new MetadataResponseTopic()
            .setName(topicPartition.topic())
            .setErrorCode(Errors.NONE.code());
        responseTopics.add(responseTopic);

        MetadataResponsePartition responsePartition = new MetadataResponsePartition()
            .setErrorCode(Errors.NONE.code())
            .setPartitionIndex(topicPartition.partition())
            .setLeaderId(leader.id())
            .setReplicaNodes(singletonList(leader.id()))
            .setIsrNodes(singletonList(leader.id()));
        responseTopic.partitions().add(responsePartition);

        env.kafkaClient().prepareResponse(
            request -> {
                if (!(request instanceof MetadataRequest)) {
                    return false;
                }
                MetadataRequest metadataRequest = (MetadataRequest) request;
                return metadataRequest.topics().equals(singletonList(topicPartition.topic()));
            },
            new MetadataResponse(new MetadataResponseData().setTopics(responseTopics),
                MetadataResponseData.HIGHEST_SUPPORTED_VERSION)
        );
    }

    /**
     * Test that if the client can obtain a node assignment, but can't send to the given
     * node, it will disconnect and try a different node.
     */
    @Test
    public void testClientSideTimeoutAfterFailureToSend() throws Exception {
        Cluster cluster = mockCluster(3, 0);
        CompletableFuture<String> disconnectFuture = new CompletableFuture<>();
        MockTime time = new MockTime();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
                newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1",
                          AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "100000",
                          AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "1"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            for (Node node : cluster.nodes()) {
                env.kafkaClient().delayReady(node, 100);
            }

            // We use a countdown latch to ensure that we get to the first
            // call to `ready` before we increment the time below to trigger
            // the disconnect.
            CountDownLatch readyLatch = new CountDownLatch(2);

            env.kafkaClient().setDisconnectFuture(disconnectFuture);
            env.kafkaClient().setReadyCallback(node -> readyLatch.countDown());
            env.kafkaClient().prepareResponse(prepareMetadataResponse(cluster, Errors.NONE));

            final ListTopicsResult result = env.adminClient().listTopics();

            readyLatch.await(TestUtils.DEFAULT_MAX_WAIT_MS, TimeUnit.MILLISECONDS);
            log.debug("Advancing clock by 25 ms to trigger client-side disconnect.");
            time.sleep(25);
            disconnectFuture.get();

            log.debug("Enabling nodes to send requests again.");
            for (Node node : cluster.nodes()) {
                env.kafkaClient().delayReady(node, 0);
            }
            time.sleep(5);
            log.info("Waiting for result.");
            assertEquals(0, result.listings().get().size());
        }
    }

    /**
     * Test that if the client can send to a node, but doesn't receive a response, it will
     * disconnect and try a different node.
     */
    @Test
    public void testClientSideTimeoutAfterFailureToReceiveResponse() throws Exception {
        Cluster cluster = mockCluster(3, 0);
        CompletableFuture<String> disconnectFuture = new CompletableFuture<>();
        MockTime time = new MockTime();
        try (final AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(time, cluster,
            newStrMap(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "100000",
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "0"))) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().setDisconnectFuture(disconnectFuture);
            final ListTopicsResult result = env.adminClient().listTopics();
            TestUtils.waitForCondition(() -> {
                time.sleep(1);
                return disconnectFuture.isDone();
            }, 5000, 1, () -> "Timed out waiting for expected disconnect");
            assertFalse(disconnectFuture.isCompletedExceptionally());
            assertFalse(result.future.isDone());
            TestUtils.waitForCondition(env.kafkaClient()::hasInFlightRequests,
                "Timed out waiting for retry");
            env.kafkaClient().respond(prepareMetadataResponse(cluster, Errors.NONE));
            assertEquals(0, result.listings().get().size());
        }
    }

    @Test
    public void testFenceProducers() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            String transactionalId = "copyCat";
            Node transactionCoordinator = env.cluster().nodes().iterator().next();

            env.kafkaClient().prepareResponse(prepareFindCoordinatorResponse(Errors.NONE, transactionalId, transactionCoordinator));

            InitProducerIdResponseData initProducerIdResponseData = new InitProducerIdResponseData()
                    .setProducerId(4761)
                    .setProducerEpoch((short) 489);
            env.kafkaClient().prepareResponseFrom(
                    request -> request instanceof InitProducerIdRequest,
                    new InitProducerIdResponse(initProducerIdResponseData),
                    transactionCoordinator
            );

            FenceProducersResult result = env.adminClient().fenceProducers(Collections.singleton(transactionalId));
            assertNull(result.all().get());
            assertEquals(4761, result.producerId(transactionalId).get());
            assertEquals((short) 489, result.epochId(transactionalId).get());
        }
    }

    private UnregisterBrokerResponse prepareUnregisterBrokerResponse(Errors error, int throttleTimeMs) {
        return new UnregisterBrokerResponse(new UnregisterBrokerResponseData()
                .setErrorCode(error.code())
                .setErrorMessage(error.message())
                .setThrottleTimeMs(throttleTimeMs));
    }

    private DescribeLogDirsResponse prepareDescribeLogDirsResponse(Errors error, String logDir) {
        return new DescribeLogDirsResponse(new DescribeLogDirsResponseData()
            .setResults(Collections.singletonList(
                new DescribeLogDirsResponseData.DescribeLogDirsResult()
                    .setErrorCode(error.code())
                    .setLogDir(logDir))));
    }

    private static MemberDescription convertToMemberDescriptions(DescribedGroupMember member,
                                                                 MemberAssignment assignment) {
        return new MemberDescription(member.memberId(),
                                     Optional.ofNullable(member.groupInstanceId()),
                                     member.clientId(),
                                     member.clientHost(),
                                     assignment);
    }

    @SafeVarargs
    private static <T> void assertCollectionIs(Collection<T> collection, T... elements) {
        for (T element : elements) {
            assertTrue(collection.contains(element), "Did not find " + element);
        }
        assertEquals(elements.length, collection.size(), "There are unexpected extra elements in the collection.");
    }

    public static KafkaAdminClient createInternal(AdminClientConfig config, KafkaAdminClient.TimeoutProcessorFactory timeoutProcessorFactory) {
        return KafkaAdminClient.createInternal(config, timeoutProcessorFactory);
    }

    public static class FailureInjectingTimeoutProcessorFactory extends KafkaAdminClient.TimeoutProcessorFactory {

        private int numTries = 0;

        private int failuresInjected = 0;

        @Override
        public KafkaAdminClient.TimeoutProcessor create(long now) {
            return new FailureInjectingTimeoutProcessor(now);
        }

        synchronized boolean shouldInjectFailure() {
            numTries++;
            if (numTries == 1) {
                failuresInjected++;
                return true;
            }
            return false;
        }

        public synchronized int failuresInjected() {
            return failuresInjected;
        }

        public final class FailureInjectingTimeoutProcessor extends KafkaAdminClient.TimeoutProcessor {
            public FailureInjectingTimeoutProcessor(long now) {
                super(now);
            }

            boolean callHasExpired(KafkaAdminClient.Call call) {
                if ((!call.isInternal()) && shouldInjectFailure()) {
                    log.debug("Injecting timeout for {}.", call);
                    return true;
                } else {
                    boolean ret = super.callHasExpired(call);
                    log.debug("callHasExpired({}) = {}", call, ret);
                    return ret;
                }
            }
        }
    }
}
