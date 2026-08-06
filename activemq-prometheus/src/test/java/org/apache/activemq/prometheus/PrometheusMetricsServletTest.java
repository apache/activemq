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
package org.apache.activemq.prometheus;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PrometheusMetricsServletTest {

    private static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";
    private static final ObjectName BROKER_NAME;
    private static final ObjectName QUEUE_NAME;
    private static final ObjectName SECOND_QUEUE_NAME;
    private static final ObjectName TOPIC_NAME;
    private static final ObjectName INVALID_BROKER_NAME;

    static {
        try {
            BROKER_NAME = new ObjectName("org.apache.activemq:type=Broker,brokerName=TestBroker");
            QUEUE_NAME = new ObjectName("org.apache.activemq:type=Broker,brokerName=TestBroker,"
                    + "destinationType=Queue,destinationName=test.queue");
            SECOND_QUEUE_NAME = new ObjectName("org.apache.activemq:type=Broker,brokerName=TestBroker,"
                    + "destinationType=Queue,destinationName=orders.queue");
            TOPIC_NAME = new ObjectName("org.apache.activemq:type=Broker,brokerName=TestBroker,"
                    + "destinationType=Topic,destinationName=events.topic");
            INVALID_BROKER_NAME = new ObjectName("org.apache.activemq:type=Broker,brokerName=InvalidBroker");
        } catch (Exception exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private MBeanServer mBeanServer;

    @Before
    public void setUp() throws Exception {
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        mBeanServer.registerMBean(new FakeBroker(), BROKER_NAME);
        mBeanServer.registerMBean(new FakeDestination(), QUEUE_NAME);
        mBeanServer.registerMBean(new FakeDestination(), SECOND_QUEUE_NAME);
        mBeanServer.registerMBean(new FakeDestination(), TOPIC_NAME);
    }

    @After
    public void tearDown() throws Exception {
        unregister(BROKER_NAME);
        unregister(QUEUE_NAME);
        unregister(SECOND_QUEUE_NAME);
        unregister(TOPIC_NAME);
        unregister(INVALID_BROKER_NAME);
    }

    @Test
    public void testDefaultResponseReturnsBrokerMetricsOnly() throws Exception {
        CapturedResponse response = invokeServlet(null);
        String output = response.output.toString();

        assertEquals(HttpServletResponse.SC_OK, response.status);
        assertEquals(CONTENT_TYPE, response.contentType);
        assertTrue(output.endsWith("\n"));

        // Broker metrics present
        assertTrue(output.contains("activemq_broker_connections_count{broker=\"TestBroker\"} 42"));
        assertTrue(output.contains("activemq_broker_messages_enqueued_total{broker=\"TestBroker\"} 50000"));

        // Percent usage reported as raw integer from MBean (no conversion)
        assertTrue(output.contains("activemq_broker_memory_percent_usage{broker=\"TestBroker\"} 25"));
        assertTrue(output.contains("activemq_broker_store_percent_usage{broker=\"TestBroker\"} 10"));
        assertTrue(output.contains("activemq_broker_temp_percent_usage{broker=\"TestBroker\"} 5"));

        // Destination metrics absent by default
        assertFalse(output.contains("activemq_queue_"));
        assertFalse(output.contains("activemq_topic_"));

        assertMetadataAppearsOncePerMetric(output);
        assertSamplesHavePrometheusSyntax(output);
    }

    @Test
    public void testPerObjectResponseIncludesDestinationMetrics() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("per_object", "true");
        CapturedResponse response = invokeServlet(params);
        String output = response.output.toString();

        assertEquals(HttpServletResponse.SC_OK, response.status);

        // Broker metrics still present
        assertTrue(output.contains("activemq_broker_connections_count{broker=\"TestBroker\"} 42"));

        // Destination metrics now present
        assertTrue(output.contains("activemq_queue_message_count{broker=\"TestBroker\",destination=\"test.queue\"} 100"));
        assertTrue(output.contains("activemq_queue_message_count{broker=\"TestBroker\",destination=\"orders.queue\"} 100"));
        assertTrue(output.contains("activemq_topic_message_count{broker=\"TestBroker\",destination=\"events.topic\"} 100"));

        // AverageEnqueueTime present (fractional value preserved)
        assertTrue(output.contains("activemq_queue_average_enqueue_time_milliseconds{broker=\"TestBroker\",destination=\"test.queue\"} 3.7"));

        // Percent usage reported as raw integer from MBean
        assertTrue(output.contains("activemq_queue_memory_percent_usage{broker=\"TestBroker\",destination=\"test.queue\"} 15"));

        assertMetadataAppearsOncePerMetric(output);
        assertSamplesHavePrometheusSyntax(output);
    }

    @Test
    public void testCollectionFailureReturnsServerErrorWithoutPartialMetrics() throws Exception {
        mBeanServer.registerMBean(new InvalidBroker(), INVALID_BROKER_NAME);

        CapturedResponse response = invokeServlet(null);

        assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, response.status);
        assertEquals("Metrics collection failed", response.errorMessage);
        assertEquals("", response.output.toString());
    }

    @Test
    public void testLabelSanitization() {
        assertEquals("hello", PrometheusMetricsServlet.sanitizeLabel("hello"));
        assertEquals("a\\\"b", PrometheusMetricsServlet.sanitizeLabel("a\"b"));
        assertEquals("a\\\\b", PrometheusMetricsServlet.sanitizeLabel("a\\b"));
        assertEquals("a\\nb", PrometheusMetricsServlet.sanitizeLabel("a\nb"));
        assertEquals("unknown", PrometheusMetricsServlet.sanitizeLabel(null));
    }

    private CapturedResponse invokeServlet(Map<String, String> params) throws Exception {
        CapturedResponse captured = new CapturedResponse();

        HttpServletRequest request = (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(), new Class<?>[] {HttpServletRequest.class},
                (proxy, method, arguments) -> {
                    if ("getParameter".equals(method.getName())) {
                        return params != null ? params.get(arguments[0]) : null;
                    }
                    return null;
                });

        HttpServletResponse response = (HttpServletResponse) Proxy.newProxyInstance(
                HttpServletResponse.class.getClassLoader(), new Class<?>[] {HttpServletResponse.class},
                (proxy, method, arguments) -> {
                    switch (method.getName()) {
                    case "getWriter":
                        return captured.writer;
                    case "setContentType":
                        captured.contentType = (String) arguments[0];
                        return null;
                    case "setStatus":
                        captured.status = (Integer) arguments[0];
                        return null;
                    case "sendError":
                        captured.status = (Integer) arguments[0];
                        captured.errorMessage = (String) arguments[1];
                        return null;
                    default:
                        throw new UnsupportedOperationException(method.getName());
                    }
                });

        new PrometheusMetricsServlet().doGet(request, response);
        captured.writer.flush();
        return captured;
    }

    private void assertMetadataAppearsOncePerMetric(String output) {
        for (String line : output.split("\\n")) {
            if (line.startsWith("# HELP ")) {
                String metric = line.substring("# HELP ".length(), line.indexOf(' ', "# HELP ".length()));
                assertEquals(1, countOccurrences(output, "# HELP " + metric + " "));
                assertTrue("Missing TYPE for " + metric,
                        output.contains("# TYPE " + metric + " "));
            }
        }
    }

    private void assertSamplesHavePrometheusSyntax(String output) {
        for (String line : output.split("\\n")) {
            if (!line.isEmpty() && !line.startsWith("#")) {
                assertTrue("Invalid Prometheus sample: " + line,
                        line.matches("[a-zA-Z_:][a-zA-Z0-9_:]*\\{[^}]+} -?[0-9]+(\\.[0-9]+)?"));
            }
        }
    }

    private int countOccurrences(String value, String search) {
        int count = 0;
        int index = 0;
        while ((index = value.indexOf(search, index)) >= 0) {
            count++;
            index += search.length();
        }
        return count;
    }

    private void unregister(ObjectName name) throws Exception {
        if (mBeanServer.isRegistered(name)) {
            mBeanServer.unregisterMBean(name);
        }
    }

    private static final class CapturedResponse {
        private final StringWriter output = new StringWriter();
        private final PrintWriter writer = new PrintWriter(output);
        private int status;
        private String contentType;
        private String errorMessage;
    }

    public interface InvalidBrokerMBean {
        int getBrokerName();
    }

    public static class InvalidBroker implements InvalidBrokerMBean {
        @Override
        public int getBrokerName() {
            return 1;
        }
    }

    public interface FakeBrokerMBean {
        String getBrokerName();

        int getCurrentConnectionsCount();

        long getTotalConnectionsCount();

        long getTotalEnqueueCount();

        long getTotalDequeueCount();

        long getTotalConsumerCount();

        long getTotalProducerCount();

        long getTotalMessageCount();

        int getMemoryPercentUsage();

        long getMemoryLimit();

        int getStorePercentUsage();

        long getStoreLimit();

        int getTempPercentUsage();

        long getTempLimit();

        long getUptimeMillis();
    }

    public static class FakeBroker implements FakeBrokerMBean {
        @Override
        public String getBrokerName() {
            return "TestBroker";
        }

        @Override
        public int getCurrentConnectionsCount() {
            return 42;
        }

        @Override
        public long getTotalConnectionsCount() {
            return 1000;
        }

        @Override
        public long getTotalEnqueueCount() {
            return 50000;
        }

        @Override
        public long getTotalDequeueCount() {
            return 49000;
        }

        @Override
        public long getTotalConsumerCount() {
            return 10;
        }

        @Override
        public long getTotalProducerCount() {
            return 5;
        }

        @Override
        public long getTotalMessageCount() {
            return 1000;
        }

        @Override
        public int getMemoryPercentUsage() {
            return 25;
        }

        @Override
        public long getMemoryLimit() {
            return 1073741824L;
        }

        @Override
        public int getStorePercentUsage() {
            return 10;
        }

        @Override
        public long getStoreLimit() {
            return 107374182400L;
        }

        @Override
        public int getTempPercentUsage() {
            return 5;
        }

        @Override
        public long getTempLimit() {
            return 53687091200L;
        }

        @Override
        public long getUptimeMillis() {
            return 86400000L;
        }
    }

    public interface FakeDestinationMBean {
        long getQueueSize();

        long getEnqueueCount();

        long getDequeueCount();

        long getDispatchCount();

        long getInFlightCount();

        long getExpiredCount();

        long getConsumerCount();

        long getProducerCount();

        int getMemoryPercentUsage();

        long getMemoryUsageByteCount();

        long getStoreMessageSize();

        double getAverageEnqueueTime();
    }

    public static class FakeDestination implements FakeDestinationMBean {
        @Override
        public long getQueueSize() {
            return 100;
        }

        @Override
        public long getEnqueueCount() {
            return 5000;
        }

        @Override
        public long getDequeueCount() {
            return 4900;
        }

        @Override
        public long getDispatchCount() {
            return 4950;
        }

        @Override
        public long getInFlightCount() {
            return 50;
        }

        @Override
        public long getExpiredCount() {
            return 10;
        }

        @Override
        public long getConsumerCount() {
            return 3;
        }

        @Override
        public long getProducerCount() {
            return 2;
        }

        @Override
        public int getMemoryPercentUsage() {
            return 15;
        }

        @Override
        public long getMemoryUsageByteCount() {
            return 161061273L;
        }

        @Override
        public long getStoreMessageSize() {
            return 524288000L;
        }

        @Override
        public double getAverageEnqueueTime() {
            return 3.7;
        }
    }
}
