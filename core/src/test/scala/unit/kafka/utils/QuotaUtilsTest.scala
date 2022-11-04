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

package kafka.utils

import java.util.concurrent.TimeUnit

import org.apache.kafka.common.MetricName
import org.apache.kafka.common.metrics.{KafkaMetric, MetricConfig, Metrics, Quota, QuotaViolationException, Sensor}
import org.apache.kafka.common.metrics.stats.{Rate, Value}

import scala.jdk.CollectionConverters._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class QuotaUtilsTest {

  private val time = new MockTime
  private val numSamples = 10
  private val sampleWindowSec = 1
  private val maxThrottleTimeMs = 1000
  private val metricName = new MetricName("test-metric", "groupA", "testA", Map.empty.asJava)

  @Test
  def testThrottleTimeObservedRateEqualsQuota(): Unit = {
    val numSamples = 10
    val observedValue = 16.5

    assertEquals(0, throttleTime(observedValue, observedValue, numSamples))

    // should be independent of window size
    assertEquals(0, throttleTime(observedValue, observedValue, numSamples + 1))
  }

  @Test
  def testThrottleTimeObservedRateBelowQuota(): Unit = {
    val observedValue = 16.5
    val quota = 20.4
    assertTrue(throttleTime(observedValue, quota, numSamples) < 0)

    // should be independent of window size
    assertTrue(throttleTime(observedValue, quota, numSamples + 1) < 0)
  }

  @Test
  def testThrottleTimeObservedRateAboveQuota(): Unit = {
    val quota = 50.0
    val observedValue = 100.0
    assertEquals(2000, throttleTime(observedValue, quota, 3))
  }

  @Test
  def testBoundedThrottleTimeObservedRateEqualsQuota(): Unit = {
    val observedValue = 18.2
    assertEquals(0, boundedThrottleTime(observedValue, observedValue, numSamples, maxThrottleTimeMs))

    // should be independent of window size
    assertEquals(0, boundedThrottleTime(observedValue, observedValue, numSamples + 1, maxThrottleTimeMs))
  }

  @Test
  def testBoundedThrottleTimeObservedRateBelowQuota(): Unit = {
    val observedValue = 16.5
    val quota = 22.4

    assertTrue(boundedThrottleTime(observedValue, quota, numSamples, maxThrottleTimeMs) < 0)

    // should be independent of window size
    assertTrue(boundedThrottleTime(observedValue, quota, numSamples + 1, maxThrottleTimeMs) < 0)
  }

  @Test
  def testBoundedThrottleTimeObservedRateAboveQuotaBelowLimit(): Unit = {
    val quota = 50.0
    val observedValue = 55.0
    assertEquals(100, boundedThrottleTime(observedValue, quota, 2, maxThrottleTimeMs))
  }

  @Test
  def testBoundedThrottleTimeObservedRateAboveQuotaAboveLimit(): Unit = {
    val quota = 50.0
    val observedValue = 100.0
    assertEquals(maxThrottleTimeMs, boundedThrottleTime(observedValue, quota, numSamples, maxThrottleTimeMs))
  }

  @Test
  def testThrottleTimeThrowsExceptionIfProvidedNonRateMetric(): Unit = {
    val testMetric = new KafkaMetric(new Object(), metricName, new Value(), new MetricConfig, time);

    assertThrows(classOf[IllegalArgumentException], () => QuotaUtils.throttleTime(new QuotaViolationException(testMetric, 10.0, 20.0), time.milliseconds))
  }

  @Test
  def testBoundedThrottleTimeThrowsExceptionIfProvidedNonRateMetric(): Unit = {
    val testMetric = new KafkaMetric(new Object(), metricName, new Value(), new MetricConfig, time);

    assertThrows(classOf[IllegalArgumentException], () => QuotaUtils.boundedThrottleTime(new QuotaViolationException(testMetric, 10.0, 20.0),
      maxThrottleTimeMs, time.milliseconds))
  }

  @ParameterizedTest
  @CsvSource(Array(
    "1,0", "1,1", "1,2", "1,3", "1,4", "1,5", "1,6", "1,7", "1,8", "1,9", "1,10", "1,11", "1,12", "1,13", "1,14", "1,15", "1,16", "1,17", "1,18", "1,17", "1,19", "1,20", "1,30", "1,40",
    "2,0", "2,1", "2,2", "2,3", "2,4", "2,5", "2,6", "2,7", "2,8", "2,9", "2,10", "2,11", "2,12", "2,13", "2,14", "2,15", "2,16", "2,17", "2,18", "2,17", "2,19", "2,20", "2,30", "2,40",
    "10,0", "10,1", "10,2", "10,3", "10,4", "10,5", "10,6", "10,7", "10,8", "10,9", "10,10", "10,11", "10,12", "10,13", "10,14", "10,15", "10,16", "10,17", "10,18", "10,17", "10,19", "10,20", "10,30", "10,40"
  ))
  def testErrorBoundWorstCase(numSamples: Int, minInterval: Int): Unit = {
    val maxConnectionRate = 30
    val metricConfig = new MetricConfig()
      .timeWindow(sampleWindowSec, TimeUnit.SECONDS)
      .samples(numSamples)
      .quota(new Quota(maxConnectionRate, true))
    val metrics = new Metrics(time)
    val sensor = metrics.sensor("quotasensor", metricConfig, Long.MaxValue);
    sensor.add(metricName, new Rate, null)
    // start the window early, so that the main iteration fills the window at the end of the first sample
    val initialThrottle = recordAndGetThrottleTimeMs(sensor, time.milliseconds());
    time.sleep(Math.max(initialThrottle, TimeUnit.SECONDS.toMillis(sampleWindowSec)-1));
    var i = 0
    val startTimeMs = time.milliseconds()
    while (i < 10000) {
      i = i+1
      val timeMs = time.milliseconds()
      val deltaMs = timeMs - startTimeMs;
      // Skip the first few iterations before we have a full window of data to assert on
      if (deltaMs >= numSamples * TimeUnit.SECONDS.toMillis(sampleWindowSec)) {
        val errorBound = TestUtils.errorBoundForWindowedRateQuota(numSamples, sampleWindowSec, deltaMs)
        val actualRate = i.toDouble*1000/deltaMs
        val rateCap = errorBound * maxConnectionRate
        assertTrue(actualRate <= rateCap, s"Connection rate $actualRate must be below $rateCap ($errorBound * $maxConnectionRate)")
      }
      // Follow the throttle strategy under test
      val throttleTime = recordAndGetThrottleTimeMs(sensor, timeMs)
      val waitTime = Math.max(throttleTime, minInterval)
      time.sleep(waitTime)
    }
  }

  // following implementation from SocketServer
  private def recordAndGetThrottleTimeMs(sensor: Sensor, timeMs: Long): Int = {
    try {
      sensor.record(1.0, timeMs)
      0
    } catch {
      case e: QuotaViolationException =>
        val throttleTimeMs = QuotaUtils.boundedThrottleTime(e, maxThrottleTimeMs, timeMs).toInt
        throttleTimeMs
    }
  }

  // the `metric` passed into the returned QuotaViolationException will return windowSize = 'numSamples' - 1
  private def quotaViolationException(observedValue: Double, quota: Double, numSamples: Int): QuotaViolationException = {
    val metricConfig = new MetricConfig()
      .timeWindow(sampleWindowSec, TimeUnit.SECONDS)
      .samples(numSamples)
      .quota(new Quota(quota, true))
    val metric = new KafkaMetric(new Object(), metricName, new Rate(), metricConfig, time)
    new QuotaViolationException(metric, observedValue, quota)
  }

  private def throttleTime(observedValue: Double, quota: Double, numSamples: Int): Long = {
    val e = quotaViolationException(observedValue, quota, numSamples)
    QuotaUtils.throttleTime(e, time.milliseconds)
  }

  private def boundedThrottleTime(observedValue: Double, quota: Double, numSamples: Int, maxThrottleTime: Long): Long = {
    val e = quotaViolationException(observedValue, quota, numSamples)
    QuotaUtils.boundedThrottleTime(e, maxThrottleTime, time.milliseconds)
  }
}
