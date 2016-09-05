/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.common.TopicAndPartition
import kafka.server.QuotaType._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.SystemTime

object QuotaType {
  val Fetch = "Fetch"
  val Produce = "Produce"
  val LeaderReplication = "LeaderReplication"
  val FollowerReplication = "FollowerReplication"
}

object QuotaFactory {

  object UnboundedQuota extends ReadOnlyQuota {
    override def bound(): Int = Int.MaxValue
    override def isThrottled(topicAndPartition: TopicAndPartition): Boolean = false
    override def isQuotaExceededBy(bytes: Int): Boolean = false
  }

  case class QuotaManagers(fetch: ClientQuotaManager, produce: ClientQuotaManager, leader: ReplicationQuotaManager, follower: ReplicationQuotaManager)

  def time = new SystemTime

  def instantiate(cfg: KafkaConfig, metrics: Metrics): QuotaManagers = {
    QuotaManagers(
      new ClientQuotaManager(clientFetchConfig(cfg), metrics, Fetch, time),
      new ClientQuotaManager(clientProduceConfig(cfg), metrics, Produce, time),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, LeaderReplication, time),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, FollowerReplication, time)
    )
  }

  def clientProduceConfig(cfg: KafkaConfig): ClientQuotaManagerConfig =
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

  def clientFetchConfig(cfg: KafkaConfig): ClientQuotaManagerConfig =
    ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

  def replicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig =
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.replicationQuotaWindowSizeSeconds
    )
}