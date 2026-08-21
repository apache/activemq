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
import javax.management.StandardMBean;

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
    private static final ObjectName INJECTION_NAME;
    private static final ObjectName INF_QUEUE;
    private static final ObjectName NAN_QUEUE;

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
            INJECTION_NAME = new ObjectName("org.apache.activemq:type=Broker,brokerName=Injection");
            INF_QUEUE = new ObjectName("org.apache.activemq:type=Broker,brokerName=TestBroker,"
                    + "destinationType=Queue,destinationName=inf.queue");
            NAN_QUEUE = new ObjectName("org.apache.activemq:type=Broker,brokerName=TestBroker,"
                    + "destinationType=Queue,destinationName=nan.queue");
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
        unregister(INJECTION_NAME);
        unregister(INF_QUEUE);
        unregister(NAN_QUEUE);
    }

    @Test
    public void testDefaultResponseReturnsBrokerMetricsOnly() throws Exception {
        CapturedResponse response = invokeServlet(null);
        String output = response.output.toString();

        assertEquals(HttpServletResponse.SC_OK, response.status);
        assertEquals(CONTENT_TYPE, response.contentType);
        assertTrue(output.endsWith("\n"));

        // Broker metrics present
        assertTrue(output.contains("activemq_broker_connections{broker=\"TestBroker\"} 42"));
        assertTrue(output.contains("activemq_broker_messages_enqueued_total{broker=\"TestBroker\"} 50000"));

        // Percent usage reported as raw integer from MBean (no conversion)
        assertTrue(output.contains("activemq_broker_memory_percent_usage{broker=\"TestBroker\"} 25"));
        assertTrue(output.contains("activemq_broker_queues{broker=\"TestBroker\"} 7"));
        assertTrue(output.contains("activemq_broker_topics{broker=\"TestBroker\"} 3"));
        assertTrue(output.contains("activemq_broker_job_scheduler_store_percent_usage{broker=\"TestBroker\"} 20"));
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
        assertTrue(output.contains("activemq_broker_connections{broker=\"TestBroker\"} 42"));

        // Destination metrics now present
        assertTrue(output.contains("activemq_queue_messages{broker=\"TestBroker\",destination=\"test.queue\"} 100"));
        assertTrue(output.contains("activemq_queue_messages{broker=\"TestBroker\",destination=\"orders.queue\"} 100"));
        assertTrue(output.contains("activemq_topic_messages{broker=\"TestBroker\",destination=\"events.topic\"} 100"));

        // AverageEnqueueTime present (fractional value preserved)
        assertTrue(output.contains("activemq_queue_average_enqueue_time_milliseconds{broker=\"TestBroker\",destination=\"test.queue\"} 3.7"));

        // Percent usage reported as raw integer from MBean
        assertTrue(output.contains("activemq_queue_memory_percent_usage{broker=\"TestBroker\",destination=\"test.queue\"} 15"));
        assertTrue(output.contains("activemq_queue_memory_limit_bytes{broker=\"TestBroker\",destination=\"test.queue\"} 536870912"));
        assertTrue(output.contains("# HELP activemq_queue_enqueued_total Total messages enqueued to this destination since last start"));

        assertMetadataAppearsOncePerMetric(output);
        assertSamplesHavePrometheusSyntax(output);
    }

    @Test
    public void testUnidentifiableBrokerIsSkippedAndScrapeStillSucceeds() throws Exception {
        // A broker whose identity attribute cannot be read must not fail the whole scrape.
        mBeanServer.registerMBean(new InvalidBroker(), INVALID_BROKER_NAME);

        CapturedResponse response = invokeServlet(null);
        String output = response.output.toString();

        assertEquals(HttpServletResponse.SC_OK, response.status);
        assertEquals(CONTENT_TYPE, response.contentType);

        // The valid broker is still reported.
        assertTrue(output.contains("activemq_broker_connections{broker=\"TestBroker\"} 42"));

        // The unidentifiable broker is dropped, not emitted as a phantom "unknown" series.
        assertFalse(output.contains("broker=\"unknown\""));

        assertMetadataAppearsOncePerMetric(output);
        assertSamplesHavePrometheusSyntax(output);
    }

    @Test
    public void testMaliciousBrokerNameIsEscaped() throws Exception {
        // Backslash + quote + newline + a metadata marker: prove the exact escaped form and that
        // it is emitted only as one quoted label value.
        final String evil = "\\\"My evil \n# TYPE Broker";
        final String escaped = "\\\\\\\"My evil \\n# TYPE Broker"; // \ -> \\, " -> \", newline -> \n
        assertEquals(escaped, PrometheusMetricsServlet.sanitizeLabel(evil));

        mBeanServer.registerMBean(new StandardMBean(new InjectionBroker(evil), FakeBrokerMBean.class), INJECTION_NAME);
        CapturedResponse response = invokeServlet(null);
        String output = response.output.toString();

        assertEquals(HttpServletResponse.SC_OK, response.status);
        assertTrue(output.contains("activemq_broker_connections{broker=\"" + escaped + "\"} 42"));
        assertSamplesHavePrometheusSyntax(output);
    }

    @Test
    public void testNonFiniteValuesRenderPerPrometheusSpec() throws Exception {
        mBeanServer.registerMBean(new StandardMBean(new InfinityDestination(), FakeDestinationMBean.class), INF_QUEUE);
        mBeanServer.registerMBean(new StandardMBean(new NanDestination(), FakeDestinationMBean.class), NAN_QUEUE);
        Map<String, String> params = new HashMap<>();
        params.put("per_object", "true");
        CapturedResponse response = invokeServlet(params);
        String output = response.output.toString();

        assertEquals(HttpServletResponse.SC_OK, response.status);
        assertTrue(output.contains("activemq_queue_average_enqueue_time_milliseconds{broker=\"TestBroker\",destination=\"inf.queue\"} +Inf"));
        assertTrue(output.contains("activemq_queue_average_enqueue_time_milliseconds{broker=\"TestBroker\",destination=\"nan.queue\"} NaN"));
    }

    @Test
    public void testLabelSanitization() {
        assertEquals("hello", PrometheusMetricsServlet.sanitizeLabel("hello"));
        assertEquals("a\\\"b", PrometheusMetricsServlet.sanitizeLabel("a\"b"));
        assertEquals("a\\\\b", PrometheusMetricsServlet.sanitizeLabel("a\\b"));
        assertEquals("a\\nb", PrometheusMetricsServlet.sanitizeLabel("a\nb"));
        assertEquals("a\\rb", PrometheusMetricsServlet.sanitizeLabel("a\rb"));
        assertEquals("unknown", PrometheusMetricsServlet.sanitizeLabel(null));
    }

    @Test
    public void testBrokerNameCannotInjectExpositionFormat() throws Exception {
        final String evil = "# TYPE injected_metric gauge\nactivemq_broker_evil 999";
        mBeanServer.registerMBean(new StandardMBean(new InjectionBroker(evil), FakeBrokerMBean.class), INJECTION_NAME);

        CapturedResponse response = invokeServlet(null);
        String output = response.output.toString();

        assertEquals(HttpServletResponse.SC_OK, response.status);

        // The malicious name appears only as one escaped, quoted label value (newline -> \n).
        assertTrue(output.contains("broker=\"# TYPE injected_metric gauge\\nactivemq_broker_evil 999\""));

        // It must NOT forge its own metadata line or an injected sample line.
        for (String line : output.split("\n")) {
            assertFalse("injected TYPE line leaked", line.equals("# TYPE injected_metric gauge"));
            assertFalse("injected sample leaked", line.equals("activemq_broker_evil 999"));
        }

        assertMetadataAppearsOncePerMetric(output);
        assertSamplesHavePrometheusSyntax(output);
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

    // A broker whose reported name contains Prometheus-meaningful characters, used to prove the
    // exposition format cannot be injected through a label value.
    public static class InjectionBroker extends FakeBroker {
        private final String brokerName;

        public InjectionBroker(String brokerName) {
            this.brokerName = brokerName;
        }

        @Override
        public String getBrokerName() {
            return brokerName;
        }
    }

    // Destinations whose double attribute returns non-finite values, to prove Prometheus-spec
    // rendering (+Inf / NaN) instead of Java's "Infinity".
    public static class InfinityDestination extends FakeDestination {
        @Override
        public double getAverageEnqueueTime() {
            return Double.POSITIVE_INFINITY;
        }
    }

    public static class NanDestination extends FakeDestination {
        @Override
        public double getAverageEnqueueTime() {
            return Double.NaN;
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

        int getTotalQueuesCount();

        int getTotalTopicsCount();

        int getJobSchedulerStorePercentUsage();

        long getJobSchedulerStoreLimit();
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

        @Override
        public int getTotalQueuesCount() {
            return 7;
        }

        @Override
        public int getTotalTopicsCount() {
            return 3;
        }

        @Override
        public int getJobSchedulerStorePercentUsage() {
            return 20;
        }

        @Override
        public long getJobSchedulerStoreLimit() {
            return 5368709120L;
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

        long getMemoryLimit();

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
        public long getMemoryLimit() {
            return 536870912L;
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
