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
 **/

package org.apache.kafka.copycat.file;

import org.apache.kafka.copycat.connector.TopicPartition;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class FileStreamSinkTaskTest {

    private FileStreamSinkTask task;
    private ByteArrayOutputStream os;
    private PrintStream printStream;
    private Properties config;

    @Before
    public void setup() {
        os = new ByteArrayOutputStream();
        printStream = new PrintStream(os);
        task = new FileStreamSinkTask(printStream);
        config = new Properties();
    }

    @Test
    public void testPutFlush() throws CopycatException {
        HashMap<TopicPartition, Long> offsets = new HashMap<TopicPartition, Long>();

        // We do not call task.start() since it would override the output stream

        task.put(Arrays.asList(
                new SinkRecord("topic1", 0, null, "line1", 1)
        ));
        offsets.put(new TopicPartition("topic1", 0), 1L);
        task.flush(offsets);
        assertEquals("line1\n", os.toString());

        task.put(Arrays.asList(
                new SinkRecord("topic1", 0, null, "line2", 2),
                new SinkRecord("topic2", 0, null, "line3", 1)
        ));
        offsets.put(new TopicPartition("topic1", 0), 2L);
        offsets.put(new TopicPartition("topic2", 0), 1L);
        task.flush(offsets);
        assertEquals("line1\nline2\nline3\n", os.toString());
    }
}
