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

package org.apache.kafka.jmh.java;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(2)
public class MapBenchmark {
    @Param({"10000"})
    private int times;

    @Param({"100"})
    private int mapSize;

    private Map<Integer, Integer> hashMap;
    private Map<Integer, Integer> concurrentHashMap;
    private Map<Integer, Integer> copyOnWriteMap;

    @Setup
    public void setup() {
        Map<Integer, Integer> mapTemplate = IntStream.range(0, mapSize).boxed()
                .collect(Collectors.toMap(i -> i, i -> i));
        hashMap = new HashMap<>(mapTemplate);
        concurrentHashMap = new ConcurrentHashMap<>(mapTemplate);
        copyOnWriteMap = new CopyOnWriteMap<>(mapTemplate);
    }

    @Benchmark
    public void testHashMap(Blackhole blackhole) {
        for (int i = 0; i < times; i++) {
            for (int j = 0; j < mapSize; j++) {
                blackhole.consume(hashMap.get(j));
            }
        }
    }

    @Benchmark
    public void testConcurrentHashMap(Blackhole blackhole) {
        for (int i = 0; i < times; i++) {
            for (int j = 0; j < mapSize; j++) {
                blackhole.consume(concurrentHashMap.get(j));
            }
        }
    }

    @Benchmark
    public void testCopyOnWriteMap(Blackhole blackhole) {
        for (int i = 0; i < times; i++) {
            for (int j = 0; j < mapSize; j++) {
                blackhole.consume(copyOnWriteMap.get(j));
            }
        }
    }
}
