/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.utils.Logging
import kafka.cluster.BrokerEndPoint
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable
import scala.collection.{Map, Set}

abstract class AbstractFetcherManager(protected val name: String, clientId: String, numFetchers: Int = 1)
  extends Logging with KafkaMetricsGroup {
  // map of (source broker_id, fetcher_id per source broker) => fetcher.
  // package private for test
  private val partitionBrokerIdMap = new mutable.HashMap[TopicPartition, BrokerAndFetcherId]
  private[server] val fetcherThreadMap = new mutable.HashMap[BrokerIdAndFetcherId, AbstractFetcherThread]
  private val lock = new Object
  private var numFetchersPerBroker = numFetchers
  this.logIdent = "[" + name + "] "

  newGauge(
    "MaxLag",
    new Gauge[Long] {
      // current max lag across all fetchers/topics/partitions
      def value = fetcherThreadMap.foldLeft(0L)((curMaxAll, fetcherThreadMapEntry) => {
        fetcherThreadMapEntry._2.fetcherLagStats.stats.foldLeft(0L)((curMaxThread, fetcherLagStatsEntry) => {
          curMaxThread.max(fetcherLagStatsEntry._2.lag)
        }).max(curMaxAll)
      })
    },
    Map("clientId" -> clientId)
  )

  newGauge(
  "MinFetchRate", {
    new Gauge[Double] {
      // current min fetch rate across all fetchers/topics/partitions
      def value = {
        val headRate: Double =
          fetcherThreadMap.headOption.map(_._2.fetcherStats.requestRate.oneMinuteRate).getOrElse(0)

        fetcherThreadMap.foldLeft(headRate)((curMinAll, fetcherThreadMapEntry) => {
          fetcherThreadMapEntry._2.fetcherStats.requestRate.oneMinuteRate.min(curMinAll)
        })
      }
    }
  },
  Map("clientId" -> clientId)
  )

  def resizeThreadPool(newSize: Int): Unit = {
    def migratePartitions(newSize: Int): Unit = {
      fetcherThreadMap.foreach { case (id, thread) =>
        val removedPartitions = thread.partitionsAndOffsets
        removeFetcherForPartitions(removedPartitions.keySet)
        if (id.fetcherId >= newSize)
          thread.shutdown()
        addOrUpdateFetcherForPartitions(removedPartitions)
      }
    }
    lock synchronized {
      val currentSize = numFetchersPerBroker
      info(s"Resizing fetcher thread pool size from $currentSize to $newSize")
      numFetchersPerBroker = newSize
      if (newSize != currentSize) {
        // We could just migrate some partitions explicitly to new threads. But this is currently
        // reassigning all partitions using the new thread size so that hash-based allocation
        // works with partition add/delete as it did before.
        migratePartitions(newSize)
      }
      shutdownIdleFetcherThreads()
    }
  }

  // Visibility for testing
  private[server] def getFetcherId(topicPartition: TopicPartition) : Int = {
    lock synchronized {
      Utils.abs(31 * topicPartition.topic.hashCode() + topicPartition.partition) % numFetchersPerBroker
    }
  }

  // This method is only needed by ReplicaAlterDirManager
  def markPartitionsForTruncation(brokerId: Int, topicPartition: TopicPartition, truncationOffset: Long) {
    lock synchronized {
      val fetcherId = getFetcherId(topicPartition)
      val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerId, fetcherId)
      fetcherThreadMap.get(brokerIdAndFetcherId).foreach { thread =>
        thread.markPartitionsForTruncation(topicPartition, truncationOffset)
      }
    }
  }

  // to be defined in subclass to create a specific fetcher
  def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): AbstractFetcherThread

  private def addOrUpdateBrokerAndFetcherId(topicPartition: TopicPartition,
                                            initialFetchState: InitialFetchState): BrokerAndFetcherId = {
    val updatedBrokerAndFetcherId = BrokerAndFetcherId(initialFetchState.leader, getFetcherId(topicPartition))
    partitionBrokerIdMap.put(topicPartition, updatedBrokerAndFetcherId).foreach { oldBrokerAndFetcherId =>
      if (oldBrokerAndFetcherId != updatedBrokerAndFetcherId) {
        val oldBrokerIdAndFetcherId =  BrokerIdAndFetcherId(oldBrokerAndFetcherId.broker.id, oldBrokerAndFetcherId.fetcherId)
        fetcherThreadMap.get(oldBrokerIdAndFetcherId).foreach { fetcherThread =>
          info(s"Removed fetcher for partition $topicPartition from broker ${oldBrokerAndFetcherId.broker}")
          fetcherThread.removePartitions(Set(topicPartition))
        }
      }
    }
    updatedBrokerAndFetcherId
  }

  def addOrUpdateFetcherForPartitions(partitionAndOffsets: Map[TopicPartition, InitialFetchState]) {
    lock synchronized {
      val partitionsPerFetcher = partitionAndOffsets.groupBy { case (topicPartition, initialFetchState) =>
        addOrUpdateBrokerAndFetcherId(topicPartition, initialFetchState)
      }

      def addAndStartFetcherThread(brokerAndFetcherId: BrokerAndFetcherId, brokerIdAndFetcherId: BrokerIdAndFetcherId): AbstractFetcherThread = {
        val fetcherThread = createFetcherThread(brokerAndFetcherId.fetcherId, brokerAndFetcherId.broker)
        fetcherThreadMap.put(brokerIdAndFetcherId, fetcherThread)
        fetcherThread.start()
        fetcherThread
      }

      for ((brokerAndFetcherId, initialFetchOffsets) <- partitionsPerFetcher) {
        val brokerIdAndFetcherId = BrokerIdAndFetcherId(brokerAndFetcherId.broker.id, brokerAndFetcherId.fetcherId)
        val fetcherThread = fetcherThreadMap.get(brokerIdAndFetcherId) match {
          case Some(currentFetcherThread) if currentFetcherThread.sourceBroker == brokerAndFetcherId.broker =>
            // reuse the fetcher thread
            currentFetcherThread
          case Some(f) =>
            f.shutdown()
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
          case None =>
            addAndStartFetcherThread(brokerAndFetcherId, brokerIdAndFetcherId)
        }

        val initialOffsetAndEpochs = initialFetchOffsets.map { case (tp, brokerAndInitOffset) =>
          tp -> OffsetAndEpoch(brokerAndInitOffset.initOffset, brokerAndInitOffset.currentLeaderEpoch)
        }
        fetcherThread.addPartitions(initialOffsetAndEpochs)

        info(s"Added or updated fetcher to broker ${brokerAndFetcherId.broker} for " +
          s"partitions $initialOffsetAndEpochs")
      }
    }
  }

  def removeFetcherForPartitions(partitions: Set[TopicPartition]) {
    lock synchronized {
      for (fetcher <- fetcherThreadMap.values)
        fetcher.removePartitions(partitions)
      partitionBrokerIdMap --= partitions
    }
    info(s"Removed fetcher for partitions $partitions")
  }

  def shutdownIdleFetcherThreads() {
    lock synchronized {
      val keysToBeRemoved = new mutable.HashSet[BrokerIdAndFetcherId]
      for ((key, fetcher) <- fetcherThreadMap) {
        if (fetcher.partitionCount <= 0) {
          fetcher.shutdown()
          keysToBeRemoved += key
        }
      }
      fetcherThreadMap --= keysToBeRemoved
    }
  }

  def closeAllFetchers() {
    lock synchronized {
      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.initiateShutdown()
      }

      for ( (_, fetcher) <- fetcherThreadMap) {
        fetcher.shutdown()
      }
      fetcherThreadMap.clear()
      partitionBrokerIdMap.clear()
    }
  }
}

case class BrokerAndFetcherId(broker: BrokerEndPoint, fetcherId: Int)

case class InitialFetchState(leader: BrokerEndPoint, currentLeaderEpoch: Int, initOffset: Long)

case class BrokerIdAndFetcherId(brokerId: Int, fetcherId: Int)
