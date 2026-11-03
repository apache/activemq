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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A broker plugin that provides OpenTelemetry distributed tracing support.
 * Traces message send, dispatch, and acknowledge operations with proper
 * context propagation via W3C TraceContext.
 *
 * <p>For topic destinations, each subscriber dispatch creates its own span,
 * so a single published message may produce multiple deliver spans.
 *
 * <p>The deliver span is opened in {@code preProcessDispatch} and ended in
 * {@code postProcessDispatch}, so it measures the broker-side dispatch of the
 * message to the consumer's transport, not the consumer's own processing time.
 * It is a child of the publish span. The acknowledge span is an independent
 * span: an acknowledgement carries no message trace context, so it cannot be
 * parented to the publish/deliver spans.
 *
 * <p>The OpenTelemetry instance is resolved lazily on first use (from
 * {@link GlobalOpenTelemetry} unless one is set explicitly via
 * {@link #setOpenTelemetry(OpenTelemetry)}), so the SDK may be registered
 * after the plugin is instantiated but must be registered before the first
 * traced message.
 *
 * @org.apache.xbean.XBean element="openTelemetryPlugin"
 */
public class OpenTelemetryBrokerPlugin extends BrokerPluginSupport {

    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryBrokerPlugin.class);
    private static final String INSTRUMENTATION_NAME = "org.apache.activemq";

    // Semantic convention attribute keys (https://opentelemetry.io/docs/specs/semconv/messaging/)
    static final AttributeKey<String> ATTR_MESSAGING_SYSTEM = AttributeKey.stringKey("messaging.system");
    static final AttributeKey<String> ATTR_DESTINATION_NAME = AttributeKey.stringKey("messaging.destination.name");
    static final AttributeKey<String> ATTR_OPERATION_TYPE = AttributeKey.stringKey("messaging.operation.type");
    static final AttributeKey<String> ATTR_MESSAGE_ID = AttributeKey.stringKey("messaging.message.id");
    static final AttributeKey<String> ATTR_CLIENT_ID = AttributeKey.stringKey("messaging.client.id");

    // Semantic convention values
    static final String DEFAULT_MESSAGING_SYSTEM = "activemq";
    static final String OPERATION_PUBLISH = "publish";
    static final String OPERATION_DELIVER = "deliver";
    static final String OPERATION_SETTLE = "settle";

    // Span name suffixes
    static final String SPAN_SUFFIX_PUBLISH = " publish";
    static final String SPAN_SUFFIX_DELIVER = " deliver";
    static final String SPAN_SUFFIX_ACK = " ack";

    private static final String UNKNOWN_DESTINATION = "unknown";

    // Cap in-flight dispatch spans to avoid unbounded growth when postProcessDispatch is missed.
    // The eldest entry is ended and evicted if this limit is exceeded, so under more than this
    // many concurrent in-flight dispatches the oldest deliver spans end early (truncated).
    private static final int MAX_DISPATCH_SPANS = 1000;

    private boolean traceProducer = true;
    private boolean traceConsumer = true;
    private boolean traceAcknowledge = true;

    private String messagingSystem = DEFAULT_MESSAGING_SYSTEM;

    private volatile OpenTelemetry openTelemetry;
    private volatile TextMapPropagator propagator;
    private volatile Tracer tracer;

    private final Map<MessageDispatch, Span> dispatchSpans = Collections.synchronizedMap(
            new LinkedHashMap<MessageDispatch, Span>(256, 0.75f, false) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<MessageDispatch, Span> eldest) {
                    if (size() > MAX_DISPATCH_SPANS) {
                        LOG.warn("dispatchSpans exceeded max size; ending oldest span to prevent leak");
                        eldest.getValue().end();
                        return true;
                    }
                    return false;
                }
            });

    public OpenTelemetryBrokerPlugin() {
    }

    /**
     * Use the given OpenTelemetry instance instead of {@link GlobalOpenTelemetry}.
     */
    public void setOpenTelemetry(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
        this.tracer = null;
        this.propagator = null;
    }

    public String getMessagingSystem() {
        return messagingSystem;
    }

    /**
     * Value of the {@code messaging.system} attribute set on every span (default: {@code activemq}).
     */
    public void setMessagingSystem(String messagingSystem) {
        this.messagingSystem = messagingSystem;
    }

    private Tracer tracer() {
        Tracer t = tracer;
        if (t == null) {
            OpenTelemetry otel = openTelemetry;
            t = otel != null ? otel.getTracer(INSTRUMENTATION_NAME) : GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
            tracer = t;
        }
        return t;
    }

    private TextMapPropagator propagator() {
        TextMapPropagator p = propagator;
        if (p == null) {
            OpenTelemetry otel = openTelemetry;
            p = (otel != null ? otel.getPropagators() : GlobalOpenTelemetry.getPropagators()).getTextMapPropagator();
            propagator = p;
        }
        return p;
    }

    private SpanBuilder newSpan(String name, SpanKind kind, String destinationName, String operationType) {
        return tracer().spanBuilder(name)
                .setSpanKind(kind)
                .setAttribute(ATTR_MESSAGING_SYSTEM, messagingSystem)
                .setAttribute(ATTR_DESTINATION_NAME, destinationName)
                .setAttribute(ATTR_OPERATION_TYPE, operationType);
    }

    public boolean isTraceProducer() {
        return traceProducer;
    }

    public void setTraceProducer(boolean traceProducer) {
        this.traceProducer = traceProducer;
    }

    public boolean isTraceConsumer() {
        return traceConsumer;
    }

    public void setTraceConsumer(boolean traceConsumer) {
        this.traceConsumer = traceConsumer;
    }

    public boolean isTraceAcknowledge() {
        return traceAcknowledge;
    }

    public void setTraceAcknowledge(boolean traceAcknowledge) {
        this.traceAcknowledge = traceAcknowledge;
    }

    @Override
    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        if (!traceProducer) {
            super.send(producerExchange, messageSend);
            return;
        }

        Context parentContext = propagator().extract(Context.current(), messageSend, ActiveMQMessageTextMapGetter.INSTANCE);

        String destinationName = messageSend.getDestination().getPhysicalName();
        SpanBuilder spanBuilder = newSpan(destinationName + SPAN_SUFFIX_PUBLISH, SpanKind.PRODUCER,
                destinationName, OPERATION_PUBLISH)
                .setParent(parentContext);

        if (messageSend.getMessageId() != null) {
            spanBuilder.setAttribute(ATTR_MESSAGE_ID, messageSend.getMessageId().toString());
        }
        if (producerExchange.getConnectionContext() != null
                && producerExchange.getConnectionContext().getClientId() != null) {
            spanBuilder.setAttribute(ATTR_CLIENT_ID, producerExchange.getConnectionContext().getClientId());
        }

        Span span = spanBuilder.startSpan();
        try {
            Context contextWithSpan = parentContext.with(span);
            propagator().inject(contextWithSpan, messageSend, ActiveMQMessageTextMapSetter.INSTANCE);

            super.send(producerExchange, messageSend);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, String.valueOf(e.getMessage()));
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public void preProcessDispatch(MessageDispatch messageDispatch) {
        if (traceConsumer && messageDispatch != null && messageDispatch.getMessage() != null) {
            try {
                Message message = messageDispatch.getMessage();
                Context extractedContext = propagator().extract(Context.current(), message, ActiveMQMessageTextMapGetter.INSTANCE);

                String destinationName = message.getDestination().getPhysicalName();
                SpanBuilder spanBuilder = newSpan(destinationName + SPAN_SUFFIX_DELIVER, SpanKind.CONSUMER,
                        destinationName, OPERATION_DELIVER)
                        .setParent(extractedContext);

                if (message.getMessageId() != null) {
                    spanBuilder.setAttribute(ATTR_MESSAGE_ID, message.getMessageId().toString());
                }

                Span span = spanBuilder.startSpan();
                dispatchSpans.put(messageDispatch, span);
            } catch (Exception e) {
                LOG.warn("Failed to create deliver span", e);
            }
        }
        super.preProcessDispatch(messageDispatch);
    }

    @Override
    public void postProcessDispatch(MessageDispatch messageDispatch) {
        if (messageDispatch != null) {
            Span span = dispatchSpans.remove(messageDispatch);
            if (span != null) {
                span.end();
            }
        }
        super.postProcessDispatch(messageDispatch);
    }

    @Override
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        if (!traceAcknowledge) {
            super.acknowledge(consumerExchange, ack);
            return;
        }

        String destinationName = ack.getDestination() != null ? ack.getDestination().getPhysicalName() : UNKNOWN_DESTINATION;
        SpanBuilder spanBuilder = newSpan(destinationName + SPAN_SUFFIX_ACK, SpanKind.INTERNAL,
                destinationName, OPERATION_SETTLE);

        if (ack.getLastMessageId() != null) {
            spanBuilder.setAttribute(ATTR_MESSAGE_ID, ack.getLastMessageId().toString());
        }
        if (consumerExchange.getConnectionContext() != null
                && consumerExchange.getConnectionContext().getClientId() != null) {
            spanBuilder.setAttribute(ATTR_CLIENT_ID, consumerExchange.getConnectionContext().getClientId());
        }

        Span span = spanBuilder.startSpan();
        try {
            super.acknowledge(consumerExchange, ack);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, String.valueOf(e.getMessage()));
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
