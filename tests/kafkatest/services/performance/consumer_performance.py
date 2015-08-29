# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from kafkatest.services.performance import PerformanceService


class ConsumerPerformanceService(PerformanceService):
    """
        See ConsumerPerformance.scala as the source of truth on these settings, but for reference:

        "zookeeper" "The connection string for the zookeeper connection in the form host:port. Multiple URLS can
                     be given to allow fail-over. This option is only used with the old consumer."

        "broker-list", "A broker list to use for connecting if using the new consumer."

        "topic", "REQUIRED: The topic to consume from."

        "group", "The group id to consume on."

        "fetch-size", "The amount of data to fetch in a single request."

        "from-latest", "If the consumer does not already have an establishedoffset to consume from,
                        start with the latest message present in the log rather than the earliest message."

        "socket-buffer-size", "The size of the tcp RECV size."

        "threads", "Number of processing threads."

        "num-fetch-threads", "Number of fetcher threads. Defaults to 1"

        "new-consumer", "Use the new consumer implementation."
    """

    def __init__(self, context, num_nodes, kafka, topic, messages, new_consumer=False, settings={}):
        super(ConsumerPerformanceService, self).__init__(context, num_nodes)
        self.kafka = kafka
        self.topic = topic
        self.messages = messages
        self.new_consumer = new_consumer
        self.settings = settings

        # These less-frequently used settings can be updated manually after instantiation
        self.fetch_size = 1024 * 1024
        self.socket_buffer_size = 2 * 1024 * 1024
        self.threads = 10
        self.num_fetch_threads = 1
        self.group = None
        self.from_latest = False


    @property
    def args(self):
        """Dictionary of arguments used to start the Consumer Performance script."""
        args = {
            'topic': self.topic,
            'messages': self.messages,
            'fetch-size': self.fetch_size,
            'socket-buffer-size': self.socket_buffer_size,
            'threads': self.threads,
            'num-fetch-threads': self.num_fetch_threads
        }

        if self.new_consumer:
            args['new-consumer'] = ""
            args['broker-list'] = self.kafka.bootstrap_servers()
        else:
            args['zookeeper'] = self.kafka.zk.connect_setting()

        if self.group is not None:
            args['group'] = self.group

        if self.from_latest:
            args['from-latest'] = ""

        return args

    @property
    def start_cmd(self):
        cmd = "/opt/kafka/bin/kafka-consumer-perf-test.sh"
        for key, value in self.args.items():
            cmd += " --%s %s" % (key, value)

        for key, value in self.settings.items():
            cmd += " %s=%s" % (str(key), str(value))

        return cmd

    def _worker(self, idx, node):
        cmd = self.start_cmd
        self.logger.debug("Consumer performance %d command: %s", idx, cmd)
        last = None
        for line in node.account.ssh_capture(cmd):
            self.logger.debug("Consumer performance %d: %s", idx, line.strip())
            last = line
        # Parse and save the last line's information
        parts = last.split(',')

        self.results[idx-1] = {
            'total_mb': float(parts[2]),
            'mbps': float(parts[3]),
            'records_per_sec': float(parts[5]),
        }
