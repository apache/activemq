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
 * @org.apache.xbean.XBean element="openTelemetryPlugin"
 */
public class OpenTelemetryBrokerPlugin extends BrokerPluginSupport {

    private static final Logger LOG = LoggerFactory.getLogger(OpenTelemetryBrokerPlugin.class);
    private static final String INSTRUMENTATION_NAME = "org.apache.activemq";

    // Cap in-flight dispatch spans to avoid unbounded growth when postProcessDispatch is missed.
    // The eldest entry is ended and evicted if this limit is exceeded.
    private static final int MAX_DISPATCH_SPANS = 1000;

    private boolean traceProducer = true;
    private boolean traceConsumer = true;
    private boolean traceAcknowledge = true;

    private final TextMapPropagator propagator;
    private final Tracer tracer;

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
        this.propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
        this.tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
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

        Context parentContext = propagator.extract(Context.current(), messageSend, ActiveMQMessageTextMapGetter.INSTANCE);

        String destinationName = messageSend.getDestination().getPhysicalName();
        SpanBuilder spanBuilder = tracer.spanBuilder(destinationName + " publish")
                .setSpanKind(SpanKind.PRODUCER)
                .setParent(parentContext)
                .setAttribute("messaging.system.name", "activemq")
                .setAttribute("messaging.destination.name", destinationName)
                .setAttribute("messaging.operation.type", "publish");

        if (messageSend.getMessageId() != null) {
            spanBuilder.setAttribute("messaging.message.id", messageSend.getMessageId().toString());
        }
        if (producerExchange.getConnectionContext() != null
                && producerExchange.getConnectionContext().getClientId() != null) {
            spanBuilder.setAttribute("messaging.client.id", producerExchange.getConnectionContext().getClientId());
        }

        Span span = spanBuilder.startSpan();
        try {
            Context contextWithSpan = parentContext.with(span);
            propagator.inject(contextWithSpan, messageSend, ActiveMQMessageTextMapSetter.INSTANCE);

            super.send(producerExchange, messageSend);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
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
                Context extractedContext = propagator.extract(Context.current(), message, ActiveMQMessageTextMapGetter.INSTANCE);

                String destinationName = message.getDestination().getPhysicalName();
                SpanBuilder spanBuilder = tracer.spanBuilder(destinationName + " deliver")
                        .setSpanKind(SpanKind.CONSUMER)
                        .setParent(extractedContext)
                        .setAttribute("messaging.system.name", "activemq")
                        .setAttribute("messaging.destination.name", destinationName)
                        .setAttribute("messaging.operation.type", "deliver");

                if (message.getMessageId() != null) {
                    spanBuilder.setAttribute("messaging.message.id", message.getMessageId().toString());
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

        String destinationName = ack.getDestination() != null ? ack.getDestination().getPhysicalName() : "unknown";
        SpanBuilder spanBuilder = tracer.spanBuilder(destinationName + " ack")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("messaging.system.name", "activemq")
                .setAttribute("messaging.destination.name", destinationName)
                .setAttribute("messaging.operation.type", "ack");

        if (ack.getLastMessageId() != null) {
            spanBuilder.setAttribute("messaging.message.id", ack.getLastMessageId().toString());
        }
        if (consumerExchange.getConnectionContext() != null
                && consumerExchange.getConnectionContext().getClientId() != null) {
            spanBuilder.setAttribute("messaging.client.id", consumerExchange.getConnectionContext().getClientId());
        }

        Span span = spanBuilder.startSpan();
        try {
            super.acknowledge(consumerExchange, ack);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
