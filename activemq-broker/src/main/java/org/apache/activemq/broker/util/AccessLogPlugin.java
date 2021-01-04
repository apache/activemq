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
import org.apache.activemq.broker.jmx.AsyncAnnotatedMBean;
import org.apache.activemq.command.Message;
import org.apache.activemq.util.JMXSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Tracks and logs timings for messages being sent to a destination
 */
public class AccessLogPlugin extends BrokerPluginSupport {

    private static final Logger LOG = LoggerFactory.getLogger("TIMING");
    private static final ThreadLocal<String> THREAD_MESSAGE_ID = new ThreadLocal<>();

    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicInteger threshold = new AtomicInteger(0);

    private final Timings timings = new Timings();
    private RecordingCallback recordingCallback;

    @PostConstruct
    private void postConstruct() {
        LOG.info("Created AccessLogPlugin: {}", this.toString());
    }

    @Override
    public void start() throws Exception {
        super.start();

        if (getBrokerService().isUseJmx()) {
            AsyncAnnotatedMBean.registerMBean(
                    this.getBrokerService().getManagementContext(),
                    new AccessLogView(this),
                    createJmxName(getBrokerService().getBrokerObjectName().toString(), "AccessLogPlugin")
            );
        }
    }

    @Override
    public void stop() throws Exception {
        if (getBrokerService().isUseJmx()) {
            final ObjectName name = createJmxName(getBrokerService().getBrokerObjectName().toString(), "AccessLogPlugin");
            getBrokerService().getManagementContext().unregisterMBean(name);
        }

        super.stop();
    }

    public static ObjectName createJmxName(final String brokerObjectName, final String name) throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;

        objectNameStr += "," + "service=AccessLog";
        objectNameStr += "," + "instanceName=" + JMXSupport.encodeObjectNamePart(name);

        return new ObjectName(objectNameStr);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void setEnabled(final boolean enabled) {
        this.enabled.set(enabled);
    }

    public int getThreshold() {
        return threshold.get();
    }

    public void setThreshold(final int threshold) {
        this.threshold.set(threshold);
    }

    @Override
    public void send(final ProducerBrokerExchange producerExchange, final Message messageSend) throws Exception {
        if (! enabled.get()) {
            super.send(producerExchange, messageSend);
            return;
        }

        THREAD_MESSAGE_ID.set(messageSend.getMessageId().toString());
        long start = System.nanoTime();

        try {
            timings.start(messageSend);
            super.send(producerExchange, messageSend);
        } finally {
            timings.end(messageSend, start);
            THREAD_MESSAGE_ID.set(null);
        }
    }

    public void record(final String messageId, final String what, final long duration) {
        if (! enabled.get()) {
            return;
        }

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

    public void setCallback(RecordingCallback recordingCallback) {
        this.recordingCallback = recordingCallback;
    }

    private class Timings {
        private ConcurrentMap<String, Timing> inflight = new ConcurrentHashMap<>();

        public void start(final Message message) {
            final String messageId = message.getMessageId().toString();
            final int messageSize = message.getContent() != null ? message.getContent().getLength() : -1;

            inflight.computeIfAbsent(messageId, (key) -> {
                final String destination = message.getDestination() != null ? message.getDestination().toString() : "";
                return new Timing(key, destination, messageSize);
            });
        }

        public void end(final Message message, final long start) {
            final long duration = System.nanoTime() - start;
            final String messageId = message.getMessageId().toString();

            record(messageId, "whole_request", duration);

            final Timing timing = inflight.remove(messageId);

            final int th = threshold.get();
            if (th <= 0 || ((long)th < duration)) {
                if (LOG.isInfoEnabled()) {
                    LOG.info(timing.toString());
                }
                if (recordingCallback != null) {
                    recordingCallback.sendComplete(timing);
                }
            }
        }

        public void record(final String messageId, final String what, final long duration) {
            inflight.computeIfPresent(messageId, (key, timing) -> timing.add(what, duration));
        }
    }

    public class Timing {
        private final String messageId;
        private final String destination;
        private final int messageSize;
        private final List<Breakdown> timingBreakdowns = Collections.synchronizedList(new ArrayList<>());

        private Timing(final String messageId, final String destination, final int messageSize) {
            this.messageId = messageId;
            this.destination = destination;
            this.messageSize = messageSize;
        }

        public Timing add(final String what, final long duration) {
            timingBreakdowns.add(new Breakdown(what, duration));
            return this;
        }

        @Override
        public String toString() {
            return "Timing{" +
                    "messageId='" + messageId + '\'' +
                    ", destination='" + destination + '\'' +
                    ", messageSize='" + messageSize + '\'' +
                    ", timingBreakdowns=" + timingBreakdowns +
                    '}';
            }

        public List<Breakdown> getBreakdowns() {
            return timingBreakdowns;
        }
    }

    public class Breakdown {
        private final String what;
        private final Long timing;

        public Breakdown(final String what, final Long timing) {
            this.what = what;
            this.timing = timing;
        }

        public String getWhat() {
            return what;
        }

        public Long getTiming() {
            return timing / 1000000;
        }

        @Override
        public String toString() {
            return "Breakdown{" +
                    "what='" + getWhat() + '\'' +
                    ", timing=" + getTiming() +
                    '}';
        }
    }

    public interface RecordingCallback {
        void sendComplete(final Timing timing);
    }
}
