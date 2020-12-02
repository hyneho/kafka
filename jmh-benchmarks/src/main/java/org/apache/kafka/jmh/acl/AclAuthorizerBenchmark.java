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

package org.apache.kafka.jmh.acl;

import kafka.security.authorizer.AclAuthorizer;
import kafka.security.authorizer.AclAuthorizer.VersionedAcls;
import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.Action;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import scala.collection.JavaConverters;
import scala.collection.immutable.TreeMap;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AclAuthorizerBenchmark {
    @Param({"10000", "40000", "80000"})
    private int resourceCount;
    //no. of. rules per resource
    @Param({"20", "100"})
    private int aclCount;

    private final int hostPreCount = 1000;
    private final String resourceNamePrefix = "foo-bar35_resource-";
    private final String resourceName = resourceNamePrefix + 95;

    private final AclAuthorizer aclAuthorizer = new AclAuthorizer();
    private final KafkaPrincipal principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test-user");
    private List<Action> actions = new ArrayList<>();
    private RequestContext context;

    private TreeMap<ResourcePattern, VersionedAcls> aclCache = new TreeMap<>(new AclAuthorizer.ResourceOrdering());
    private scala.collection.mutable.HashMap<AccessControlEntry, scala.collection.mutable.HashSet<ResourcePattern>> resourceCache =
        new scala.collection.mutable.HashMap<>();

    @Setup(Level.Trial)
    public void setup() throws Exception {
        prepareAclCache();
        setFieldValue(aclAuthorizer, AclAuthorizer.class.getDeclaredField("aclCache").getName(), aclCache);
        setFieldValue(aclAuthorizer, AclAuthorizer.class.getDeclaredField("resourceCache").getName(), resourceCache);
        // By adding `-95` to the resource name prefix, we cause the `TreeMap.from/to` call to return
        // most map entries. In such cases, we rely on the filtering based on `String.startsWith`
        // to return the matching ACLs. Using a more efficient data structure (e.g. a prefix
        // tree) should improve performance significantly).
        actions = Collections.singletonList(new Action(AclOperation.WRITE,
            new ResourcePattern(ResourceType.TOPIC, resourceName, PatternType.LITERAL),
            1, true, true));
        context = new RequestContext(new RequestHeader(ApiKeys.PRODUCE, Integer.valueOf(1).shortValue(),
            "someclient", 1), "1", InetAddress.getLocalHost(), principal,
            ListenerName.normalised("listener"), SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY, false);
    }

    private void setFieldValue(Object obj, String fieldName, Object value) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    private void prepareAclCache() throws UnknownHostException {
        Map<ResourcePattern, Set<AclEntry>> aclEntries = new HashMap<>();
        for (int resourceId = 0; resourceId < resourceCount; resourceId++) {
            ResourcePattern resource = new ResourcePattern(
                (resourceId % 10 == 0) ? ResourceType.GROUP : ResourceType.TOPIC,
                resourceNamePrefix + resourceId,
                (resourceId % 5 == 0) ? PatternType.PREFIXED : PatternType.LITERAL);

            Set<AclEntry> entries = aclEntries.computeIfAbsent(resource, k -> new HashSet<>());

            for (int aclId = 0; aclId < aclCount / 2; aclId++) {
                String acePrincipal = principal.toString() + (aclId == 0 ? "" : aclId);
                AccessControlEntry allowAce = new AccessControlEntry(
                    acePrincipal,
                    "*", AclOperation.WRITE, AclPermissionType.ALLOW);
                AccessControlEntry denyAce = new AccessControlEntry(
                    acePrincipal,
                    "*", AclOperation.WRITE, AclPermissionType.DENY);
                entries.add(new AclEntry(allowAce));
                // dominantly deny all the literal resource
                entries.add(new AclEntry(denyAce));
            }
        }

        ResourcePattern prefixedAllowResource = new ResourcePattern(ResourceType.TOPIC, resourceNamePrefix,
            PatternType.PREFIXED);
        // dominantly deny all the prefixed resource
        ResourcePattern prefixedDenyResource = new ResourcePattern(ResourceType.TOPIC,
            resourceNamePrefix.substring(0, resourceNamePrefix.length()-1),
            PatternType.PREFIXED);
        Set<AclEntry> prefixedAllowEntries = aclEntries.computeIfAbsent(prefixedAllowResource, k -> new HashSet<>());
        Set<AclEntry> prefixedDenyEntries = aclEntries.computeIfAbsent(prefixedDenyResource, k -> new HashSet<>());

        ResourcePattern literalResource = new ResourcePattern(ResourceType.TOPIC, resourceName,
            PatternType.LITERAL);
        // dominantly deny all the literal resource
        Set<AclEntry> literalEntries = aclEntries.computeIfAbsent(literalResource, k -> new HashSet<>());

        // get dynamic entries number for wildcard acl
        for (int hostId = 0; hostId < resourceCount / 10; hostId++) {
            AccessControlEntry allowAce = new AccessControlEntry(principal.toString(), "127.0.0." + hostId,
                AclOperation.WRITE, AclPermissionType.ALLOW);
            AccessControlEntry denyAce = new AccessControlEntry(principal.toString(), "127.0.0." + hostId,
                AclOperation.WRITE, AclPermissionType.DENY);
            prefixedAllowEntries.add(new AclEntry(allowAce));
            prefixedDenyEntries.add(new AclEntry(denyAce));
            literalEntries.add(new AclEntry(allowAce));
            literalEntries.add(new AclEntry(denyAce));
        }

        for (Map.Entry<ResourcePattern, Set<AclEntry>> entryMap : aclEntries.entrySet()) {
            aclCache = aclCache.updated(entryMap.getKey(),
                new VersionedAcls(JavaConverters.asScalaSetConverter(entryMap.getValue()).asScala().toSet(), 1));
            for (AclEntry entry : entryMap.getValue()) {
                ResourcePattern resource = entryMap.getKey();
                scala.collection.mutable.HashSet<ResourcePattern> patterns = resourceCache.getOrElseUpdate(
                    entry.ace(), scala.collection.mutable.HashSet::new);
                patterns.add(resource);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        aclAuthorizer.close();
}

    @Benchmark
    public void testAclsIterator() {
        aclAuthorizer.acls(AclBindingFilter.ANY);
    }

    @Benchmark
    public void testAuthorizer() {
        aclAuthorizer.authorize(context, actions);
    }

    @Benchmark
    public void testAuthorizeByResourceType() {
        aclAuthorizer.authorizeByResourceType(context, AclOperation.WRITE, ResourceType.TOPIC);
    }
}
