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

import java.util.concurrent._

import org.apache.kafka.common.utils.Time

class DelayedItem(val delayMs: Long, val time: Time) extends Logging {
  def this(delayMs: Long) = this(delayMs, Time.SYSTEM)

  private val dueNs = time.nanoseconds + TimeUnit.MILLISECONDS.toNanos(delayMs)

  /**
   * true if the item is still delayed
   */
  def isDelayed: Boolean = {
    time.nanoseconds < dueNs
  }

  def compareTo(d: DelayedItem): Int = {
    java.lang.Long.compare(dueNs, d.dueNs)
  }

  override def toString: String = {
    s"DelayedItem(delayMs=${TimeUnit.NANOSECONDS.toMillis(dueNs-time.nanoseconds())})"
  }
}
