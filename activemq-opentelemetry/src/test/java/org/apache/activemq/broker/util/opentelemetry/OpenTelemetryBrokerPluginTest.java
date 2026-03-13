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
package org.apache.activemq.broker.util.opentelemetry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OpenTelemetryBrokerPluginTest {

    private BrokerService broker;
    private TransportConnector connector;
    private InMemorySpanExporter spanExporter;
    private Connection connection;
    private Session session;
    private final String queueName = "TEST.OTEL";

    @Before
    public void setUp() throws Exception {
        GlobalOpenTelemetry.resetForTest();

        spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();

        OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();

        OpenTelemetryBrokerPlugin plugin = new OpenTelemetryBrokerPlugin();

        broker = new BrokerService();
        broker.setBrokerName("otelTestBroker");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setPlugins(new BrokerPlugin[]{plugin});
        connector = broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();

        connection = new ActiveMQConnectionFactory(connector.getConnectUri()).createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @After
    public void tearDown() throws Exception {
        if (session != null) session.close();
        if (connection != null) connection.close();
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    public void testPublishSpanCreatedOnSend() throws Exception {
        MessageProducer producer = session.createProducer(session.createQueue(queueName));
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        TextMessage message = session.createTextMessage("test");
        producer.send(message);
        producer.close();

        // Allow async broker processing to complete
        Thread.sleep(500);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertTrue("Expected at least one span", spans.size() >= 1);

        SpanData publishSpan = findSpan(spans, queueName + " publish");
        assertNotNull("Publish span should exist", publishSpan);
        assertEquals(SpanKind.PRODUCER, publishSpan.getKind());
        assertEquals("activemq", publishSpan.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system")));
        assertEquals(queueName, publishSpan.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.destination.name")));
        assertEquals("publish", publishSpan.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.operation")));
    }

    @Test
    public void testTraceContextPropagatedInMessageProperties() throws Exception {
        MessageProducer producer = session.createProducer(session.createQueue(queueName));
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        TextMessage message = session.createTextMessage("propagation test");
        producer.send(message);
        producer.close();

        MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
        jakarta.jms.Message received = consumer.receive(5000);
        assertNotNull("Should receive message", received);

        // The message should have traceparent property injected by the plugin
        String traceparent = received.getStringProperty("traceparent");
        assertNotNull("traceparent property should be set", traceparent);
        assertTrue("traceparent should follow W3C format", traceparent.startsWith("00-"));

        consumer.close();
    }

    @Test
    public void testDeliverSpanCreatedOnDispatch() throws Exception {
        MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));

        MessageProducer producer = session.createProducer(session.createQueue(queueName));
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(session.createTextMessage("deliver test"));
        producer.close();

        jakarta.jms.Message received = consumer.receive(5000);
        assertNotNull("Should receive message", received);
        consumer.close();

        // Allow time for postProcessDispatch
        Thread.sleep(500);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        SpanData deliverSpan = findSpan(spans, queueName + " deliver");
        assertNotNull("Deliver span should exist", deliverSpan);
        assertEquals(SpanKind.CONSUMER, deliverSpan.getKind());
        assertEquals("activemq", deliverSpan.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system")));
        assertEquals("deliver", deliverSpan.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.operation")));
    }

    @Test
    public void testAckSpanCreatedOnAcknowledge() throws Exception {
        MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));

        MessageProducer producer = session.createProducer(session.createQueue(queueName));
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(session.createTextMessage("ack test"));
        producer.close();

        jakarta.jms.Message received = consumer.receive(5000);
        assertNotNull("Should receive message", received);
        consumer.close();

        // Allow time for ack processing
        Thread.sleep(500);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        SpanData ackSpan = findSpan(spans, queueName + " ack");
        assertNotNull("Ack span should exist", ackSpan);
        assertEquals(SpanKind.INTERNAL, ackSpan.getKind());
        assertEquals("activemq", ackSpan.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system")));
        assertEquals("ack", ackSpan.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.operation")));
    }

    @Test
    public void testDisabledProducerTracing() throws Exception {
        // Stop and reconfigure with tracing disabled
        connection.close();
        broker.stop();
        broker.waitUntilStopped();
        spanExporter.reset();

        OpenTelemetryBrokerPlugin plugin = new OpenTelemetryBrokerPlugin();
        plugin.setTraceProducer(false);
        plugin.setTraceConsumer(false);
        plugin.setTraceAcknowledge(false);

        broker = new BrokerService();
        broker.setBrokerName("otelTestBroker2");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setPlugins(new BrokerPlugin[]{plugin});
        connector = broker.addConnector("tcp://localhost:0");
        broker.start();
        broker.waitUntilStarted();

        connection = new ActiveMQConnectionFactory(connector.getConnectUri()).createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(session.createQueue(queueName));
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        producer.send(session.createTextMessage("no trace"));
        producer.close();

        MessageConsumer consumer = session.createConsumer(session.createQueue(queueName));
        consumer.receive(5000);
        consumer.close();

        Thread.sleep(500);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertFalse("Should have no publish spans when tracing disabled",
                spans.stream().anyMatch(s -> s.getName().contains("publish")));
        assertFalse("Should have no deliver spans when tracing disabled",
                spans.stream().anyMatch(s -> s.getName().contains("deliver")));
        assertFalse("Should have no ack spans when tracing disabled",
                spans.stream().anyMatch(s -> s.getName().contains("ack")));
    }

    private SpanData findSpan(List<SpanData> spans, String name) {
        return spans.stream()
                .filter(s -> s.getName().equals(name))
                .findFirst()
                .orElse(null);
    }
}
