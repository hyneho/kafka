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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupBuilder;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class GroupStoreTest {
    @Test
    public void testListGroups() {
        SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        snapshotRegistry.idempotentCreateSnapshot(0);
        GroupStore groupStore = new GroupStore(snapshotRegistry, MetadataImage.EMPTY);

        ConsumerGroupBuilder cgb = new ConsumerGroupBuilder("consumerGroupId", 10);
        List<CoordinatorRecord> x = cgb.build(MetadataImage.EMPTY.topics());

        // add groups
        ConsumerGroup consumerGroup = mock(ConsumerGroup.class);
        ClassicGroup classicGroup = mock(ClassicGroup.class);
        ShareGroup shareGroup = mock(ShareGroup.class);
        groupStore.addGroup("consumer-group-id", consumerGroup);
        snapshotRegistry.idempotentCreateSnapshot(1);
        groupStore.addGroup("classic-group-id", classicGroup);
        snapshotRegistry.idempotentCreateSnapshot(2);
        groupStore.addGroup("share-group-id", shareGroup);
        snapshotRegistry.idempotentCreateSnapshot(3);

        // Test list group response without a group state or group type filter.
        List<ListGroupsResponseData.ListedGroup> actualAllGroupMap =
            groupStore.listGroups(Collections.emptySet(), Collections.emptySet(), 3);
        assertEquals(3, actualAllGroupMap.size());


        // List group with case-insensitive ‘empty’.
        actualAllGroupMap =
            groupStore.listGroups(Collections.singleton("empty"), Collections.emptySet(), 3);
        assertEquals(3, actualAllGroupMap.size());
    }
}
