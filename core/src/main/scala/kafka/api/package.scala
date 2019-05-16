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
package kafka

import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ElectLeadersRequest
import scala.collection.JavaConverters._
import scala.collection.breakOut

package object api {
  implicit final class ElectLeadersRequestOps(val self: ElectLeadersRequest) extends AnyVal {
    def topicPartitions: Set[TopicPartition] = {
      if (self.data.topicPartitions == null) {
        Set.empty
      } else {
        self.data.topicPartitions.asScala.flatMap { topicPartition =>
          topicPartition.partitionId.asScala.map { partitionId =>
            new TopicPartition(topicPartition.topic, partitionId)
          }
        }(breakOut)
      }
    }

    def electionType: ElectionType = {
      if (self.version == 0) {
        ElectionType.PREFERRED
      } else {
        byteToElectionType(self.data.electionType)
      }
    }

    private[this] def byteToElectionType(value: Byte): ElectionType = {
      value match {
        case ElectionType.PREFERRED.value => ElectionType.PREFERRED
        case ElectionType.UNCLEAN.value => ElectionType.UNCLEAN
        case _ =>
          throw new IllegalArgumentException(
            s"Value $value must be one of ${ElectionType.values().toList}"
          )
      }
    }
  }
}
