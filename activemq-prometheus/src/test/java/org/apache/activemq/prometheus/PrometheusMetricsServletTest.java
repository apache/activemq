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

import static org.apache.activemq.prometheus.PrometheusConstants.INIT_PARAM_DESTINATION_TYPES;
import static org.apache.activemq.prometheus.PrometheusConstants.LABEL_BROKER;
import static org.apache.activemq.prometheus.PrometheusConstants.LABEL_DESTINATION;
import static org.apache.activemq.prometheus.PrometheusConstants.LABEL_DESTINATION_TYPE;
import static org.apache.activemq.prometheus.PrometheusConstants.PARAM_PER_OBJECT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import io.prometheus.metrics.expositionformats.PrometheusProtobufWriter;
import io.prometheus.metrics.expositionformats.generated.com_google_protobuf_4_35_0.Metrics;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.prometheus.PrometheusMetricsServlet.DestinationType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PrometheusMetricsServletTest {

    private static final String CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";
    private static final String ALL_DESTINATION_TYPES = "queue,topic,temp-queue,temp-topic";
    private static final String JMX_DOMAIN = ManagementContext.DEFAULT_DOMAIN;
    private static final ObjectName BROKER_NAME;
    private static final ObjectName QUEUE_NAME;
    private static final ObjectName SECOND_QUEUE_NAME;
    private static final ObjectName TOPIC_NAME;
    private static final ObjectName INVALID_BROKER_NAME;
    private static final ObjectName INJECTION_NAME;
    private static final ObjectName INF_QUEUE;
    private static final ObjectName NAN_QUEUE;
    private static final ObjectName TEMP_QUEUE_NAME;
    private static final ObjectName TEMP_TOPIC_NAME;

    static {
        // Names are built by the broker's own helpers so the test tracks the real JMX layout.
        try {
            BROKER_NAME = BrokerMBeanSupport.createBrokerObjectName(JMX_DOMAIN, "TestBroker");
            QUEUE_NAME = BrokerMBeanSupport.createDestinationName(BROKER_NAME, new ActiveMQQueue("test.queue"));
            SECOND_QUEUE_NAME = BrokerMBeanSupport.createDestinationName(BROKER_NAME, new ActiveMQQueue("orders.queue"));
            TOPIC_NAME = BrokerMBeanSupport.createDestinationName(BROKER_NAME, new ActiveMQTopic("events.topic"));
            INF_QUEUE = BrokerMBeanSupport.createDestinationName(BROKER_NAME, new ActiveMQQueue("inf.queue"));
            NAN_QUEUE = BrokerMBeanSupport.createDestinationName(BROKER_NAME, new ActiveMQQueue("nan.queue"));
            TEMP_QUEUE_NAME = BrokerMBeanSupport.createDestinationName(BROKER_NAME, new ActiveMQTempQueue("test.queue"));
            TEMP_TOPIC_NAME = BrokerMBeanSupport.createDestinationName(BROKER_NAME, new ActiveMQTempTopic("replies.temp"));
            INVALID_BROKER_NAME = BrokerMBeanSupport.createBrokerObjectName(JMX_DOMAIN, "InvalidBroker");
            INJECTION_NAME = BrokerMBeanSupport.createBrokerObjectName(JMX_DOMAIN, "Injection");
        } catch (Exception exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private MBeanServer mBeanServer;
    // Broker MBeans the servlet under test scrapes, in place of the BrokerRegistry.
    private final List<ObjectName> brokers = new ArrayList<>();
    private FakeBroker broker;

    @Before
    public void setUp() throws Exception {
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        broker = new FakeBroker();
        registerBroker(broker, BROKER_NAME);
        registerDestination(broker.queues, QUEUE_NAME, new FakeDestination("test.queue"));
        registerDestination(broker.queues, SECOND_QUEUE_NAME, new FakeDestination("orders.queue"));
        registerDestination(broker.topics, TOPIC_NAME, new FakeDestination("events.topic"));
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
        unregister(TEMP_QUEUE_NAME);
        unregister(TEMP_TOPIC_NAME);
    }

    @Test
    public void testDestinationTypeStringsMatchTheBrokerSource() {
        // Label values are the URI schemes of ActiveMQDestination.
        assertEquals(ActiveMQDestination.QUEUE_QUALIFIED_PREFIX, DestinationType.QUEUE.label + "://");
        assertEquals(ActiveMQDestination.TOPIC_QUALIFIED_PREFIX, DestinationType.TOPIC.label + "://");
        assertEquals(ActiveMQDestination.TEMP_QUEUE_QUALIFED_PREFIX, DestinationType.TEMP_QUEUE.label + "://");
        assertEquals(ActiveMQDestination.TEMP_TOPIC_QUALIFED_PREFIX, DestinationType.TEMP_TOPIC.label + "://");
    }

    @Test
    public void testFakeMBeansMatchTheBrokerInterfaces() throws Exception {
        // The fakes must have the same getters, with the same types, as the broker interfaces.
        assertGettersExistOn(FakeBrokerMBean.class, BrokerViewMBean.class);
        assertGettersExistOn(FakeDestinationMBean.class, DestinationViewMBean.class);
    }

    @Test
    public void testPerObjectParameterIsValidated() throws Exception {
        assertTrue(invokeServlet(params(PARAM_PER_OBJECT, "true")).body().contains("activemq_destination_"));
        assertFalse(invokeServlet(params(PARAM_PER_OBJECT, "false")).body().contains("activemq_destination_"));

        // Only one exact true or false is accepted.
        for (String[] bad : new String[][] {{"TRUE"}, {"yes"}, {""}, {"true", "true"}, {"false", "true"}}) {
            CapturedResponse response = invokeServlet(params(PARAM_PER_OBJECT, bad));
            assertEquals(Arrays.toString(bad), HttpServletResponse.SC_BAD_REQUEST, response.status);
            assertTrue(response.errorMessage, response.errorMessage.contains(PARAM_PER_OBJECT));
            assertEquals("", response.body());
        }
    }

    @Test
    public void testRegisteredBrokerIsScrapedThroughItsOwnMBeanName() throws Exception {
        // The default constructor reads the BrokerRegistry. One broker uses a custom JMX domain; the other has
        // JMX disabled and is not scraped.
        BrokerService broker = new BrokerService();
        broker.setBrokerName("RegistryBroker");
        broker.setPersistent(false);
        broker.getManagementContext().setCreateConnector(false);
        broker.getManagementContext().setJmxDomainName("test.prometheus");
        broker.setDestinations(new ActiveMQDestination[] {new ActiveMQQueue("registry.queue"), new ActiveMQTopic("registry.topic")});

        BrokerService noJmx = new BrokerService();
        noJmx.setBrokerName("NoJmxBroker");
        noJmx.setPersistent(false);
        noJmx.setUseJmx(false);

        broker.start();
        noJmx.start();
        try {
            broker.waitUntilStarted();
            noJmx.waitUntilStarted();
            String output = invokeServlet(perObject(), initServlet(new PrometheusMetricsServlet(), null)).body();

            assertTrue(output, output.contains("activemq_broker_current_connections{broker=\"RegistryBroker\"} 0.0"));
            assertTrue(output, output.contains("activemq_destination_messages{broker=\"RegistryBroker\",destination=\"registry.queue\",destination_type=\"queue\"} 0.0"));
            assertTrue(output, output.contains("activemq_destination_messages{broker=\"RegistryBroker\",destination=\"registry.topic\",destination_type=\"topic\"} 0.0"));
            assertFalse(output, output.contains("NoJmxBroker"));
            // Fake MBeans are not in the registry, so they are not scraped.
            assertFalse(output, output.contains("TestBroker"));
            assertMetadataAppearsOncePerMetric(output);
            assertSamplesHavePrometheusSyntax(output);
        } finally {
            noJmx.stop();
            broker.stop();
            noJmx.waitUntilStopped();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testDestinationTypesConfigurationIsValidated() throws Exception {
        assertEquals(EnumSet.of(DestinationType.QUEUE, DestinationType.TOPIC),
                PrometheusMetricsServlet.parseDestinationTypes(null));
        assertEquals(EnumSet.allOf(DestinationType.class),
                PrometheusMetricsServlet.parseDestinationTypes(" queue, topic ,temp-queue,temp-topic,"));
        assertEquals(EnumSet.noneOf(DestinationType.class), PrometheusMetricsServlet.parseDestinationTypes(""));

        try {
            PrometheusMetricsServlet.parseDestinationTypes("queue,tempqueue");
            fail("unknown destination type accepted");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("tempqueue"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("temp-queue"));
        }

        // An invalid init parameter must fail deployment, not fall back silently.
        try {
            newServlet("queue;topic");
            fail("servlet initialised with an invalid " + INIT_PARAM_DESTINATION_TYPES);
        } catch (ServletException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(INIT_PARAM_DESTINATION_TYPES));
        }

        assertEquals(EnumSet.of(DestinationType.QUEUE, DestinationType.TOPIC), newServlet(null).getEnabledDestinationTypes());
        assertEquals(EnumSet.allOf(DestinationType.class), newServlet(ALL_DESTINATION_TYPES).getEnabledDestinationTypes());
    }

    @Test
    public void testDefaultResponseReturnsBrokerMetricsOnly() throws Exception {
        CapturedResponse response = invokeServlet(null);
        String output = response.body();

        assertEquals(HttpServletResponse.SC_OK, response.status);
        assertEquals(CONTENT_TYPE, response.contentType);
        assertTrue(output.endsWith("\n"));

        assertTrue(output.contains("activemq_broker_current_connections{broker=\"TestBroker\"} 42.0"));
        assertTrue(output.contains("activemq_broker_messages_enqueued_total{broker=\"TestBroker\"} 50000.0"));

        // Percent usage reported as raw integer from MBean (no conversion)
        assertTrue(output.contains("activemq_broker_memory_percent_usage{broker=\"TestBroker\"} 25.0"));
        assertTrue(output.contains("activemq_broker_queues{broker=\"TestBroker\"} 7.0"));
        assertTrue(output.contains("activemq_broker_topics{broker=\"TestBroker\"} 3.0"));
        assertTrue(output.contains("activemq_broker_job_scheduler_store_percent_usage{broker=\"TestBroker\"} 20.0"));
        assertTrue(output.contains("activemq_broker_store_percent_usage{broker=\"TestBroker\"} 10.0"));
        assertTrue(output.contains("activemq_broker_temp_percent_usage{broker=\"TestBroker\"} 5.0"));

        // Destination metrics absent by default
        assertFalse(output.contains("activemq_destination_"));

        assertMetadataAppearsOncePerMetric(output);
        assertSamplesHavePrometheusSyntax(output);
    }

    @Test
    public void testDefaultResponseMatchesExpectedExposition() throws Exception {
        // Exact comparison of the broker-level exposition, so any change to names, help, types,
        // label layout or number formatting shows up as a diff against the committed file.
        String expected;
        try (InputStream in = getClass().getResourceAsStream("/expected-broker-metrics.txt")) {
            assertNotNull("expected-broker-metrics.txt missing from test resources", in);
            expected = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertEquals(expected, invokeServlet(null).body());
    }

    @Test
    public void testPerObjectResponseIncludesDestinationMetrics() throws Exception {
        CapturedResponse response = invokeServlet(perObject());
        String output = response.body();

        assertEquals(HttpServletResponse.SC_OK, response.status);

        // Broker metrics still present
        assertTrue(output.contains("activemq_broker_current_connections{broker=\"TestBroker\"} 42.0"));

        // One family per metric; the destination type is a label.
        assertTrue(output.contains("activemq_destination_messages{broker=\"TestBroker\",destination=\"test.queue\",destination_type=\"queue\"} 100.0"));
        assertTrue(output.contains("activemq_destination_messages{broker=\"TestBroker\",destination=\"orders.queue\",destination_type=\"queue\"} 100.0"));
        assertTrue(output.contains("activemq_destination_messages{broker=\"TestBroker\",destination=\"events.topic\",destination_type=\"topic\"} 100.0"));
        assertEquals(1, countOccurrences(output, "# TYPE activemq_destination_messages gauge\n"));

        // Fractional value preserved
        assertTrue(output.contains("activemq_destination_average_enqueue_time_milliseconds{broker=\"TestBroker\",destination=\"test.queue\",destination_type=\"queue\"} 3.7"));

        // In-flight gauge named messages_inflight (no reserved _count suffix).
        assertTrue(output.contains("activemq_destination_messages_inflight{broker=\"TestBroker\",destination=\"test.queue\",destination_type=\"queue\"} 50.0"));
        assertFalse(output.contains("message_inflight_count"));

        // Percent usage reported as raw MBean integer; large byte limit renders in scientific notation.
        assertTrue(output.contains("activemq_destination_memory_percent_usage{broker=\"TestBroker\",destination=\"test.queue\",destination_type=\"queue\"} 15.0"));
        assertTrue(output.contains("activemq_destination_memory_limit_bytes{broker=\"TestBroker\",destination=\"test.queue\",destination_type=\"queue\"} 5.36870912E8"));
        assertTrue(output.contains("# HELP activemq_destination_enqueued_total Total messages enqueued to this destination since last start"));
        assertTrue(output.contains("# TYPE activemq_destination_enqueued_total counter"));

        assertMetadataAppearsOncePerMetric(output);
        assertSamplesHavePrometheusSyntax(output);
    }

    @Test
    public void testTemporaryDestinationsAreOffByDefaultAndOptIn() throws Exception {
        registerDestination(broker.tempQueues, TEMP_QUEUE_NAME, new FakeDestination("test.queue"));
        registerDestination(broker.tempTopics, TEMP_TOPIC_NAME, new FakeDestination("replies.temp"));

        // Default configuration: per_object reports queues and topics only.
        String defaultOutput = invokeServlet(perObject()).body();
        assertTrue(defaultOutput.contains("destination_type=\"queue\""));
        assertTrue(defaultOutput.contains("destination_type=\"topic\""));
        assertFalse(defaultOutput.contains("destination_type=\"temp-queue\""));
        assertFalse(defaultOutput.contains("destination_type=\"temp-topic\""));

        // Opted in through the init parameter: temporary destinations appear in the same families.
        CapturedResponse response = invokeServlet(perObject(), newServlet(ALL_DESTINATION_TYPES));
        String output = response.body();

        assertEquals(HttpServletResponse.SC_OK, response.status);
        assertTrue(output.contains("activemq_destination_messages{broker=\"TestBroker\",destination=\"test.queue\",destination_type=\"queue\"} 100.0"));
        assertTrue(output.contains("activemq_destination_messages{broker=\"TestBroker\",destination=\"test.queue\",destination_type=\"temp-queue\"} 100.0"));
        assertTrue(output.contains("activemq_destination_messages{broker=\"TestBroker\",destination=\"replies.temp\",destination_type=\"temp-topic\"} 100.0"));
        assertTrue(output.contains("activemq_destination_enqueued_total{broker=\"TestBroker\",destination=\"test.queue\",destination_type=\"temp-queue\"} 5000.0"));
        assertEquals(1, countOccurrences(output, "# TYPE activemq_destination_enqueued_total counter\n"));

        // Restricting the list restricts the output.
        String topicsOnly = invokeServlet(perObject(), newServlet("topic")).body();
        assertTrue(topicsOnly.contains("destination_type=\"topic\""));
        assertFalse(topicsOnly.contains("destination_type=\"queue\""));
        assertFalse(topicsOnly.contains("destination_type=\"temp-topic\""));

        assertMetadataAppearsOncePerMetric(output);
        assertSamplesHavePrometheusSyntax(output);
    }

    @Test
    public void testSnapshotsRoundTripThroughPrometheusProtobuf() throws Exception {
        // Renders the snapshots with the Prometheus protobuf writer and reads them back with the Prometheus
        // generated classes.
        MetricSnapshots snapshots = newServlet(null).collect(true);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        new PrometheusProtobufWriter().write(bytes, snapshots);

        Map<String, Metrics.MetricFamily> families = new HashMap<>();
        try (InputStream in = new ByteArrayInputStream(bytes.toByteArray())) {
            for (Metrics.MetricFamily family; (family = Metrics.MetricFamily.parseDelimitedFrom(in)) != null;) {
                assertFalse("duplicate family " + family.getName(), families.containsKey(family.getName()));
                families.put(family.getName(), family);
            }
        }
        assertEquals(18 + 13, families.size());

        Metrics.MetricFamily connections = families.get("activemq_broker_current_connections");
        assertNotNull(families.keySet().toString(), connections);
        assertEquals(Metrics.MetricType.GAUGE, connections.getType());
        assertEquals("Current number of connections", connections.getHelp());
        assertEquals(1, connections.getMetricCount());
        assertEquals(labels(LABEL_BROKER, "TestBroker"), labels(connections.getMetric(0)));
        assertEquals(42.0, connections.getMetric(0).getGauge().getValue(), 0.0);

        Metrics.MetricFamily enqueued = families.get("activemq_broker_messages_enqueued_total");
        assertNotNull(families.keySet().toString(), enqueued);
        assertEquals(Metrics.MetricType.COUNTER, enqueued.getType());
        assertEquals(50000.0, enqueued.getMetric(0).getCounter().getValue(), 0.0);

        Metrics.MetricFamily messages = families.get("activemq_destination_messages");
        assertNotNull(families.keySet().toString(), messages);
        assertEquals(Metrics.MetricType.GAUGE, messages.getType());
        assertEquals("Number of messages in this destination", messages.getHelp());
        assertEquals(3, messages.getMetricCount());
        List<List<String>> seen = new ArrayList<>();
        for (Metrics.Metric metric : messages.getMetricList()) {
            assertEquals(100.0, metric.getGauge().getValue(), 0.0);
            seen.add(labels(metric));
        }
        assertTrue(seen.toString(), seen.contains(labels(LABEL_BROKER, "TestBroker", LABEL_DESTINATION, "test.queue", LABEL_DESTINATION_TYPE, "queue")));
        assertTrue(seen.toString(), seen.contains(labels(LABEL_BROKER, "TestBroker", LABEL_DESTINATION, "orders.queue", LABEL_DESTINATION_TYPE, "queue")));
        assertTrue(seen.toString(), seen.contains(labels(LABEL_BROKER, "TestBroker", LABEL_DESTINATION, "events.topic", LABEL_DESTINATION_TYPE, "topic")));

        Metrics.MetricFamily enqueueTime = families.get("activemq_destination_average_enqueue_time_milliseconds");
        assertNotNull(families.keySet().toString(), enqueueTime);
        assertEquals(3.7, enqueueTime.getMetric(0).getGauge().getValue(), 0.0);
    }

    @Test
    public void testUnidentifiableBrokerIsSkippedAndScrapeStillSucceeds() throws Exception {
        // A broker whose identity attribute cannot be read must not fail the whole scrape.
        registerBroker(new InvalidBroker(), INVALID_BROKER_NAME);

        CapturedResponse response = invokeServlet(null);
        String output = response.body();

        assertEquals(HttpServletResponse.SC_OK, response.status);
        assertEquals(CONTENT_TYPE, response.contentType);

        // The valid broker is still reported.
        assertTrue(output.contains("activemq_broker_current_connections{broker=\"TestBroker\"} 42.0"));

        // The unidentifiable broker is dropped, not emitted as a phantom "unknown" series.
        assertFalse(output.contains("broker=\"unknown\""));
        assertFalse(output.contains("InvalidBroker"));

        assertMetadataAppearsOncePerMetric(output);
        assertSamplesHavePrometheusSyntax(output);
    }

    @Test
    public void testNonFiniteValuesRenderPerPrometheusSpec() throws Exception {
        registerDestination(broker.queues, INF_QUEUE,
                new StandardMBean(new InfinityDestination("inf.queue"), FakeDestinationMBean.class));
        registerDestination(broker.queues, NAN_QUEUE,
                new StandardMBean(new NanDestination("nan.queue"), FakeDestinationMBean.class));
        CapturedResponse response = invokeServlet(perObject());
        String output = response.body();

        assertEquals(HttpServletResponse.SC_OK, response.status);
        assertTrue(output.contains("activemq_destination_average_enqueue_time_milliseconds{broker=\"TestBroker\",destination=\"inf.queue\",destination_type=\"queue\"} +Inf"));
        assertTrue(output.contains("activemq_destination_average_enqueue_time_milliseconds{broker=\"TestBroker\",destination=\"nan.queue\",destination_type=\"queue\"} NaN"));
    }

    @Test
    public void testBrokerNameCannotInjectExpositionFormat() throws Exception {
        final String evil = "# TYPE injected_metric gauge\nactivemq_broker_evil 999";
        registerBroker(new StandardMBean(new InjectionBroker(evil), FakeBrokerMBean.class), INJECTION_NAME);

        CapturedResponse response = invokeServlet(null);
        String output = response.body();

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

    private static Map<String, String[]> perObject() {
        return params(PARAM_PER_OBJECT, "true");
    }

    private static Map<String, String[]> params(String name, String... values) {
        Map<String, String[]> params = new HashMap<>();
        params.put(name, values);
        return params;
    }

    private void registerBroker(Object mbean, ObjectName name) throws Exception {
        mBeanServer.registerMBean(mbean, name);
        brokers.add(name);
    }

    // Registers a destination MBean and lists it on the fake broker, as the broker's own view does.
    private void registerDestination(List<ObjectName> list, ObjectName name, Object mbean) throws Exception {
        mBeanServer.registerMBean(mbean, name);
        list.add(name);
    }

    private List<PrometheusMetricsServlet.JmxBroker> jmxBrokers() {
        List<PrometheusMetricsServlet.JmxBroker> result = new ArrayList<>();
        for (ObjectName name : brokers) {
            result.add(new PrometheusMetricsServlet.JmxBroker(name, mBeanServer));
        }
        return result;
    }

    // Initialises a servlet the way the container would, with the given servlet init parameter.
    private PrometheusMetricsServlet newServlet(String destinationTypes) throws ServletException {
        return initServlet(new PrometheusMetricsServlet(this::jmxBrokers), destinationTypes);
    }

    private static PrometheusMetricsServlet initServlet(PrometheusMetricsServlet servlet, String destinationTypes)
            throws ServletException {
        ServletContext context = (ServletContext) Proxy.newProxyInstance(
                ServletContext.class.getClassLoader(), new Class<?>[] {ServletContext.class},
                (proxy, method, arguments) -> {
                    if ("getInitParameter".equals(method.getName())) {
                        return null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        ServletConfig config = (ServletConfig) Proxy.newProxyInstance(
                ServletConfig.class.getClassLoader(), new Class<?>[] {ServletConfig.class},
                (proxy, method, arguments) -> {
                    switch (method.getName()) {
                    case "getInitParameter":
                        return INIT_PARAM_DESTINATION_TYPES.equals(arguments[0]) ? destinationTypes : null;
                    case "getServletContext":
                        return context;
                    case "getServletName":
                        return "metrics";
                    default:
                        throw new UnsupportedOperationException(method.getName());
                    }
                });
        servlet.init(config);
        return servlet;
    }

    private CapturedResponse invokeServlet(Map<String, String[]> params) throws Exception {
        return invokeServlet(params, newServlet(null));
    }

    private CapturedResponse invokeServlet(Map<String, String[]> params, PrometheusMetricsServlet servlet) throws Exception {
        CapturedResponse captured = new CapturedResponse();

        HttpServletRequest request = (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(), new Class<?>[] {HttpServletRequest.class},
                (proxy, method, arguments) -> {
                    if ("getParameterValues".equals(method.getName())) {
                        return params != null ? params.get(arguments[0]) : null;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });

        HttpServletResponse response = (HttpServletResponse) Proxy.newProxyInstance(
                HttpServletResponse.class.getClassLoader(), new Class<?>[] {HttpServletResponse.class},
                (proxy, method, arguments) -> {
                    switch (method.getName()) {
                    case "getOutputStream":
                        return captured.stream;
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

        servlet.doGet(request, response);
        return captured;
    }

    private static List<String> labels(Metrics.Metric metric) {
        List<String> pairs = new ArrayList<>();
        for (Metrics.LabelPair pair : metric.getLabelList()) {
            pairs.add(pair.getName());
            pairs.add(pair.getValue());
        }
        return pairs;
    }

    private static List<String> labels(String... namesAndValues) {
        return List.of(namesAndValues);
    }

    private void assertMetadataAppearsOncePerMetric(String output) {
        for (String line : output.split("\n")) {
            if (line.startsWith("# HELP ")) {
                String metric = line.substring("# HELP ".length(), line.indexOf(' ', "# HELP ".length()));
                assertEquals(1, countOccurrences(output, "# HELP " + metric + " "));
                assertTrue("Missing TYPE for " + metric,
                        output.contains("# TYPE " + metric + " "));
            }
        }
    }

    private void assertSamplesHavePrometheusSyntax(String output) {
        for (String line : output.split("\n")) {
            if (!line.isEmpty() && !line.startsWith("#")) {
                // Accept float64 text: integers, decimals, scientific notation, and +Inf/-Inf/NaN.
                assertTrue("Invalid Prometheus sample: " + line,
                        line.matches("[a-zA-Z_:][a-zA-Z0-9_:]*\\{[^}]*\\} "
                                + "(-?[0-9]+(\\.[0-9]+)?([eE][-+]?[0-9]+)?|[-+]?Inf|NaN)"));
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
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final ServletOutputStream stream = new ServletOutputStream() {
            @Override
            public void write(int b) {
                bytes.write(b);
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public void setWriteListener(WriteListener writeListener) {
                // no-op: synchronous test capture
            }
        };
        private int status;
        private String contentType;
        private String errorMessage;

        private String body() {
            return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private static void assertGettersExistOn(final Class<?> fake, final Class<?> real) throws Exception {
        for (final Method getter : fake.getMethods()) {
            final Method expected = real.getMethod(getter.getName());
            assertEquals(fake.getSimpleName() + "." + getter.getName() + " return type",
                    expected.getReturnType(), getter.getReturnType());
        }
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
        public InfinityDestination(String name) {
            super(name);
        }

        @Override
        public double getAverageEnqueueTime() {
            return Double.POSITIVE_INFINITY;
        }
    }

    public static class NanDestination extends FakeDestination {
        public NanDestination(String name) {
            super(name);
        }

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

        ObjectName[] getQueues();

        ObjectName[] getTopics();

        ObjectName[] getTemporaryQueues();

        ObjectName[] getTemporaryTopics();
    }

    public static class FakeBroker implements FakeBrokerMBean {
        final List<ObjectName> queues = new ArrayList<>();
        final List<ObjectName> topics = new ArrayList<>();
        final List<ObjectName> tempQueues = new ArrayList<>();
        final List<ObjectName> tempTopics = new ArrayList<>();

        @Override
        public ObjectName[] getQueues() {
            return queues.toArray(new ObjectName[0]);
        }

        @Override
        public ObjectName[] getTopics() {
            return topics.toArray(new ObjectName[0]);
        }

        @Override
        public ObjectName[] getTemporaryQueues() {
            return tempQueues.toArray(new ObjectName[0]);
        }

        @Override
        public ObjectName[] getTemporaryTopics() {
            return tempTopics.toArray(new ObjectName[0]);
        }
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
        String getName();

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
        private final String name;

        public FakeDestination(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

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
