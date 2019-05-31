/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2017-2018 Alexis Seigneurin.
 *
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
package org.apache.kafka.streams.scala

import java.time.Duration
import java.util
import java.util.{Locale, Properties}
import java.util.regex.Pattern

import org.apache.kafka.common.serialization.{Serdes => SerdesJ}
import org.apache.kafka.streams.kstream.{
  Aggregator,
  ForeachAction,
  Initializer,
  JoinWindows,
  KeyValueMapper,
  Predicate,
  Reducer,
  Transformer,
  TransformerSupplier,
  ValueJoiner,
  ValueMapper,
  Joined => JoinedJ,
  KGroupedStream => KGroupedStreamJ,
  KStream => KStreamJ,
  KTable => KTableJ,
  Materialized => MaterializedJ
}
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext, TypedProcessorSupplier}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{StreamsBuilder => StreamsBuilderJ, _}
import org.junit.Assert._
import org.junit._

import _root_.scala.collection.JavaConverters._

/**
 * Test suite that verifies that the topology built by the Java and Scala APIs match.
 */
class TopologyTest {

  private val inputTopic = "input-topic"
  private val userClicksTopic = "user-clicks-topic"
  private val userRegionsTopic = "user-regions-topic"

  private val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

  @Test def shouldBuildIdenticalTopologyInJavaNScalaSimple(): Unit = {

    // build the Scala topology
    def getTopologyScala: TopologyDescription = {

      import Serdes._

      val streamBuilder = new StreamsBuilder
      val textLines = streamBuilder.stream[String, String](inputTopic)

      val _: KStream[String, String] =
        textLines.flatMapValues(v => pattern.split(v.toLowerCase))

      streamBuilder.build().describe()
    }

    // build the Java topology
    def getTopologyJava: TopologyDescription = {

      val streamBuilder = new StreamsBuilderJ
      val textLines = streamBuilder.stream[String, String](inputTopic)

      val _: KStreamJ[String, String] = textLines.flatMapValues(
        new ValueMapper[String, java.lang.Iterable[String]] {
          def apply(s: String): java.lang.Iterable[String] = pattern.split(s.toLowerCase).toIterable.asJava
        }
      )
      streamBuilder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala, getTopologyJava)
  }

  @Test def shouldBuildIdenticalTopologyInJavaNScalaAggregate(): Unit = {

    // build the Scala topology
    def getTopologyScala: TopologyDescription = {

      import Serdes._

      val streamBuilder = new StreamsBuilder
      val textLines = streamBuilder.stream[String, String](inputTopic)

      val _: KTable[String, Long] =
        textLines
          .flatMapValues(v => pattern.split(v.toLowerCase))
          .groupBy((_, v) => v)
          .count()

      streamBuilder.build().describe()
    }

    // build the Java topology
    def getTopologyJava: TopologyDescription = {

      val streamBuilder = new StreamsBuilderJ
      val textLines: KStreamJ[String, String] = streamBuilder.stream[String, String](inputTopic)

      val splits: KStreamJ[String, String] = textLines.flatMapValues(
        new ValueMapper[String, java.lang.Iterable[String]] {
          def apply(s: String): java.lang.Iterable[String] = pattern.split(s.toLowerCase).toIterable.asJava
        }
      )

      val grouped: KGroupedStreamJ[String, String] = splits.groupBy {
        new KeyValueMapper[String, String, String] {
          def apply(k: String, v: String): String = v
        }
      }

      grouped.count()

      streamBuilder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala, getTopologyJava)
  }

  @Test def shouldBuildIdenticalTopologyInJavaNScalaJoin(): Unit = {

    // build the Scala topology
    def getTopologyScala: TopologyDescription = {
      import Serdes._

      val builder = new StreamsBuilder()

      val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic)

      val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic)

      // clicks per region
      userClicksStream
        .leftJoin(userRegionsTable)((clicks, region) => (if (region == null) "UNKNOWN" else region, clicks))
        .map((_, regionWithClicks) => regionWithClicks)
        .groupByKey
        .reduce(_ + _)

      builder.build().describe()
    }

    // build the Java topology
    def getTopologyJava: TopologyDescription = {

      import java.lang.{Long => JLong}

      val builder: StreamsBuilderJ = new StreamsBuilderJ()

      val userClicksStream: KStreamJ[String, JLong] =
        builder.stream[String, JLong](userClicksTopic, Consumed.`with`[String, JLong])

      val userRegionsTable: KTableJ[String, String] =
        builder.table[String, String](userRegionsTopic, Consumed.`with`[String, String])

      // Join the stream against the table.
      val userClicksJoinRegion: KStreamJ[String, (String, JLong)] = userClicksStream
        .leftJoin(
          userRegionsTable,
          new ValueJoiner[JLong, String, (String, JLong)] {
            def apply(clicks: JLong, region: String): (String, JLong) =
              (if (region == null) "UNKNOWN" else region, clicks)
          },
          Joined.`with`[String, JLong, String]
        )

      // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
      val clicksByRegion: KStreamJ[String, JLong] = userClicksJoinRegion
        .map {
          new KeyValueMapper[String, (String, JLong), KeyValue[String, JLong]] {
            def apply(k: String, regionWithClicks: (String, JLong)) =
              new KeyValue[String, JLong](regionWithClicks._1, regionWithClicks._2)
          }
        }

      // Compute the total per region by summing the individual click counts per region.
      clicksByRegion
        .groupByKey(Grouped.`with`[String, JLong])
        .reduce {
          new Reducer[JLong] {
            def apply(v1: JLong, v2: JLong): JLong = v1 + v2
          }
        }

      builder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala, getTopologyJava)
  }

  @Test def shouldBuildIdenticalTopologyInJavaNScalaTransform(): Unit = {

    // build the Scala topology
    def getTopologyScala: TopologyDescription = {

      import Serdes._

      val streamBuilder = new StreamsBuilder
      val textLines = streamBuilder.stream[String, String](inputTopic)

      val _: KTable[String, Long] =
        textLines
          .transform(new TransformerSupplier[String, String, KeyValue[String, String]] {
            override def get(): Transformer[String, String, KeyValue[String, String]] =
              new Transformer[String, String, KeyValue[String, String]] {
                override def init(context: ProcessorContext): Unit = Unit

                override def transform(key: String, value: String): KeyValue[String, String] =
                  new KeyValue(key, value.toLowerCase)

                override def close(): Unit = Unit
              }
          })
          .groupBy((_, v) => v)
          .count()

      streamBuilder.build().describe()
    }

    // build the Java topology
    def getTopologyJava: TopologyDescription = {

      val streamBuilder = new StreamsBuilderJ
      val textLines: KStreamJ[String, String] = streamBuilder.stream[String, String](inputTopic)

      val lowered: KStreamJ[String, String] = textLines
        .transform(new TransformerSupplier[String, String, KeyValue[String, String]] {
          override def get(): Transformer[String, String, KeyValue[String, String]] =
            new Transformer[String, String, KeyValue[String, String]] {
              override def init(context: ProcessorContext): Unit = Unit

              override def transform(key: String, value: String): KeyValue[String, String] =
                new KeyValue(key, value.toLowerCase)

              override def close(): Unit = Unit
            }
        })

      val grouped: KGroupedStreamJ[String, String] = lowered.groupBy {
        new KeyValueMapper[String, String, String] {
          def apply(k: String, v: String): String = v
        }
      }

      // word counts
      grouped.count()

      streamBuilder.build().describe()
    }

    // should match
    assertEquals(getTopologyScala, getTopologyJava)
  }

  @Test def shouldBuildIdenticalTopologyInJavaNScalaProperties(): Unit = {

    val props = new Properties()
    props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE)

    val propsNoOptimization = new Properties()
    propsNoOptimization.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.NO_OPTIMIZATION)

    val AGGREGATION_TOPIC = "aggregationTopic"
    val REDUCE_TOPIC = "reduceTopic"
    val JOINED_TOPIC = "joinedTopic"

    // build the Scala topology
    def getTopologyScala: StreamsBuilder = {

      val aggregator = (_: String, v: String, agg: Int) => agg + v.length
      val reducer = (v1: String, v2: String) => v1 + ":" + v2
      val processorValueCollector: util.List[String] = new util.ArrayList[String]

      val builder: StreamsBuilder = new StreamsBuilder

      val sourceStream: KStream[String, String] =
        builder.stream(inputTopic)(Consumed.`with`(Serdes.String, Serdes.String))

      val mappedStream: KStream[String, String] =
        sourceStream.map((k: String, v: String) => (k.toUpperCase(Locale.getDefault), v))
      mappedStream
        .filter((k: String, _: String) => k == "B")
        .mapValues((v: String) => v.toUpperCase(Locale.getDefault))
        .process(() => new SimpleProcessor(processorValueCollector))

      val stream2 = mappedStream.groupByKey
        .aggregate(0)(aggregator)(Materialized.`with`(Serdes.String, Serdes.Integer))
        .toStream
      stream2.to(AGGREGATION_TOPIC)(Produced.`with`(Serdes.String, Serdes.Integer))

      // adding operators for case where the repartition node is further downstream
      val stream3 = mappedStream
        .filter((_: String, _: String) => true)
        .peek((k: String, v: String) => System.out.println(k + ":" + v))
        .groupByKey
        .reduce(reducer)(Materialized.`with`(Serdes.String, Serdes.String))
        .toStream
      stream3.to(REDUCE_TOPIC)(Produced.`with`(Serdes.String, Serdes.String))

      mappedStream
        .filter((k: String, _: String) => k == "A")
        .join(stream2)((v1: String, v2: Int) => v1 + ":" + v2.toString, JoinWindows.of(Duration.ofMillis(5000)))(
          Joined.`with`(Serdes.String, Serdes.String, Serdes.Integer)
        )
        .to(JOINED_TOPIC)

      mappedStream
        .filter((k: String, _: String) => k == "A")
        .join(stream3)((v1: String, v2: String) => v1 + ":" + v2.toString, JoinWindows.of(Duration.ofMillis(5000)))(
          Joined.`with`(Serdes.String, Serdes.String, Serdes.String)
        )
        .to(JOINED_TOPIC)

      builder
    }

    // build the Java topology
    def getTopologyJava: StreamsBuilderJ = {

      val keyValueMapper: KeyValueMapper[String, String, KeyValue[String, String]] =
        new KeyValueMapper[String, String, KeyValue[String, String]] {
          override def apply(key: String, value: String): KeyValue[String, String] =
            KeyValue.pair(key.toUpperCase(Locale.getDefault), value)
        }
      val initializer: Initializer[Integer] = new Initializer[Integer] {
        override def apply(): Integer = 0
      }
      val aggregator: Aggregator[String, String, Integer] = new Aggregator[String, String, Integer] {
        override def apply(key: String, value: String, aggregate: Integer): Integer = aggregate + value.length
      }
      val reducer: Reducer[String] = new Reducer[String] {
        override def apply(v1: String, v2: String): String = v1 + ":" + v2
      }
      val valueMapper: ValueMapper[String, String] = new ValueMapper[String, String] {
        override def apply(v: String): String = v.toUpperCase(Locale.getDefault)
      }
      val processorValueCollector = new util.ArrayList[String]
      val processorSupplier: TypedProcessorSupplier[String, String] = new TypedProcessorSupplier[String, String] {
        override def get() = new SimpleProcessor(processorValueCollector)
      }
      val valueJoiner2: ValueJoiner[String, Integer, String] = new ValueJoiner[String, Integer, String] {
        override def apply(value1: String, value2: Integer): String = value1 + ":" + value2.toString
      }
      val valueJoiner3: ValueJoiner[String, String, String] = new ValueJoiner[String, String, String] {
        override def apply(value1: String, value2: String): String = value1 + ":" + value2.toString
      }

      val builder = new StreamsBuilderJ

      val sourceStream = builder.stream(inputTopic, Consumed.`with`(Serdes.String, Serdes.String))

      val mappedStream: KStreamJ[String, String] =
        sourceStream.map(keyValueMapper)
      mappedStream
        .filter(new Predicate[String, String] {
          override def test(key: String, value: String): Boolean = key == "B"
        })
        .mapValues[String](valueMapper)
        .process(processorSupplier)

      val stream2 = mappedStream.groupByKey
        .aggregate(initializer, aggregator, MaterializedJ.`with`(Serdes.String, SerdesJ.Integer))
        .toStream
      stream2.to(AGGREGATION_TOPIC, Produced.`with`(Serdes.String, SerdesJ.Integer))

      // adding operators for case where the repartition node is further downstream
      val stream3 = mappedStream
        .filter(new Predicate[String, String] {
          override def test(k: String, v: String) = true
        })
        .peek(new ForeachAction[String, String] {
          override def apply(k: String, v: String) = System.out.println(k + ":" + v)
        })
        .groupByKey
        .reduce(reducer, MaterializedJ.`with`(Serdes.String, Serdes.String))
        .toStream
      stream3.to(REDUCE_TOPIC, Produced.`with`(Serdes.String, Serdes.String))

      mappedStream
        .filter(new Predicate[String, String] {
          override def test(key: String, value: String): Boolean = key == "A"
        })
        .join(stream2,
              valueJoiner2,
              JoinWindows.of(Duration.ofMillis(5000)),
              JoinedJ.`with`(Serdes.String, Serdes.String, SerdesJ.Integer))
        .to(JOINED_TOPIC)

      mappedStream
        .filter(new Predicate[String, String] {
          override def test(key: String, value: String): Boolean = key == "A"
        })
        .join(stream3,
              valueJoiner3,
              JoinWindows.of(Duration.ofMillis(5000)),
              JoinedJ.`with`(Serdes.String, Serdes.String, SerdesJ.String))
        .to(JOINED_TOPIC)

      builder
    }

    assertNotEquals(getTopologyScala.build(props).describe.toString,
                    getTopologyScala.build(propsNoOptimization).describe.toString)
    assertEquals(getTopologyScala.build(propsNoOptimization).describe.toString,
                 getTopologyJava.build(propsNoOptimization).describe.toString)
    assertEquals(getTopologyScala.build(props).describe.toString, getTopologyJava.build(props).describe.toString)
  }

  private class SimpleProcessor private[TopologyTest] (val valueList: util.List[String])
      extends AbstractProcessor[String, String] {
    override def process(key: String, value: String): Unit =
      valueList.add(value)
  }
}
