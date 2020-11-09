/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.util;

import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks and logs timings for messages being sent to a destination
 *
 * @org.apache.xbean.XBean
 */
public class AccessLogPlugin extends BrokerPluginSupport {

    private static final Logger LOG = LoggerFactory.getLogger("TIMING");
    private static final ThreadLocal<String> THREAD_MESSAGE_ID = new ThreadLocal<>();
    private final Timings timings = new Timings();

    public AccessLogPlugin() {

    }

    @Override
    public void send(final ProducerBrokerExchange producerExchange, final Message messageSend) throws Exception {
        THREAD_MESSAGE_ID.set(messageSend.getMessageId().toString());
        long start = System.currentTimeMillis();

        try {
            timings.start(messageSend);
            super.send(producerExchange, messageSend);
        } finally {
            final long end = System.currentTimeMillis();
            timings.record(messageSend.getMessageId().toString(), "whole_request", end - start);
            timings.end(messageSend);
            THREAD_MESSAGE_ID.set(null);
        }
    }

    public void record(final String messageId, final String what, final long duration) {
        String id = messageId;
        if (id == null) {
            id = THREAD_MESSAGE_ID.get();
        }

        if (id == null) {
            return;
        }

        timings.record(id, what, duration);
    }

    public void setThreadMessageId(final String messageId) {
        THREAD_MESSAGE_ID.set(messageId);
    }

    private class Timings {
        private Map<String, Timing> inflight = new ConcurrentHashMap<>();

        public void start(final Message message) {
            final String messageId = message.getMessageId().toString();
            final int messageSize = message.getContent() != null ? message.getContent().getLength() : -1;

            if (!inflight.containsKey(messageId)) {
                inflight.put(messageId, new Timing(messageId, messageSize));
            }
        }

        public void end(final Message message) {
            final String messageId = message.getMessageId().toString();
            final Timing timing = inflight.remove(messageId);
            LOG.debug(timing.toString());
        }

        public void record(final String messageId, final String what, final long duration) {
            final Timing timing = inflight.get(messageId);
            if (timing == null) {
                LOG.debug("No inflight timing for messageId: " + messageId + ", skipping");
                return;
            }

            timing.add(what, duration);
        }
    }

    private class Timing {
        private final String messageId;
        private final int messageSize;
        private final List<Breakdown> timingBreakdowns = new ArrayList<>();

        private Timing(final String messageId, final int messageSize) {
            this.messageId = messageId;
            this.messageSize = messageSize;
        }

        public void add(final String what, final long duration) {
            timingBreakdowns.add(new Breakdown(what, duration));
        }

        @Override
        public String toString() {
            return "Timing{" +
                    "messageId='" + messageId + '\'' +
                    "messageSize='" + messageSize + '\'' +
                    ", timingBreakdowns=" + timingBreakdowns +
                    '}';
        }
    }

    private class Breakdown {
        private final String what;
        private final Long timing;

        public Breakdown(String what, Long timing) {
            this.what = what;
            this.timing = timing;
        }

        public String getWhat() {
            return what;
        }

        public Long getTiming() {
            return timing;
        }

        @Override
        public String toString() {
            return "Breakdown{" +
                    "what='" + what + '\'' +
                    ", timing=" + timing +
                    '}';
        }
    }
}
