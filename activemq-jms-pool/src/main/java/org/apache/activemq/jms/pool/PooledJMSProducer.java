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
package org.apache.activemq.jms.pool;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import jakarta.jms.CompletionListener;
import jakarta.jms.DeliveryMode;
import jakarta.jms.Destination;
import jakarta.jms.IllegalStateRuntimeException;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageFormatRuntimeException;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;

/**
 * JMS 2.0 {@link JMSProducer} backed by a pooled {@link Session} and its
 * anonymous {@link MessageProducer}. QoS overrides and message properties
 * are accumulated locally and applied at send time.
 */
public class PooledJMSProducer implements JMSProducer {

    private final Session session;
    private final MessageProducer producer;

    private int deliveryMode = DeliveryMode.PERSISTENT;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    private long deliveryDelay = 0;
    private boolean disableMessageID = false;
    private boolean disableMessageTimestamp = false;
    private CompletionListener completionListener;

    private String jmsCorrelationID;
    private byte[] jmsCorrelationIDBytes;
    private String jmsType;
    private Destination jmsReplyTo;

    private Map<String, Object> properties;

    PooledJMSProducer(Session session, MessageProducer producer) throws JMSException {
        this.session = session;
        this.producer = producer;
        this.deliveryMode = producer.getDeliveryMode();
        this.priority = producer.getPriority();
        this.timeToLive = producer.getTimeToLive();
    }

    @Override
    public JMSProducer send(Destination destination, Message message) {
        checkSessionOpen();
        if (message == null) {
            throw new MessageFormatRuntimeException("Message must not be null");
        }
        try {
            applyHeaders(message);
            applyProperties(message);
            if (completionListener != null) {
                // Pass the listener through; the underlying provider either sends
                // asynchronously or rejects the call if it lacks support.
                producer.send(destination, message, deliveryMode, priority, timeToLive, completionListener);
            } else {
                producer.send(destination, message, deliveryMode, priority, timeToLive);
            }
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, String body) {
        checkSessionOpen();
        try {
            var message = session.createTextMessage(body);
            send(destination, message);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, Map<String, Object> body) {
        checkSessionOpen();
        try {
            var message = session.createMapMessage();
            if (body != null) {
                for (var entry : body.entrySet()) {
                    message.setObject(entry.getKey(), entry.getValue());
                }
            }
            send(destination, message);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, byte[] body) {
        checkSessionOpen();
        try {
            var message = session.createBytesMessage();
            if (body != null) {
                message.writeBytes(body);
            }
            send(destination, message);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, Serializable body) {
        checkSessionOpen();
        try {
            var message = session.createObjectMessage(body);
            send(destination, message);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer setDeliveryMode(int deliveryMode) {
        if (deliveryMode != DeliveryMode.PERSISTENT && deliveryMode != DeliveryMode.NON_PERSISTENT) {
            throw new JMSRuntimeException("Unknown delivery mode: " + deliveryMode);
        }
        this.deliveryMode = deliveryMode;
        return this;
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public JMSProducer setPriority(int priority) {
        if (priority < 0 || priority > 9) {
            throw new JMSRuntimeException("Priority must be between 0 and 9");
        }
        this.priority = priority;
        return this;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public JMSProducer setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    @Override
    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public JMSProducer setDeliveryDelay(long deliveryDelay) {
        this.deliveryDelay = deliveryDelay;
        return this;
    }

    @Override
    public long getDeliveryDelay() {
        return deliveryDelay;
    }

    @Override
    public JMSProducer setDisableMessageID(boolean value) {
        this.disableMessageID = value;
        return this;
    }

    @Override
    public boolean getDisableMessageID() {
        return disableMessageID;
    }

    @Override
    public JMSProducer setDisableMessageTimestamp(boolean value) {
        this.disableMessageTimestamp = value;
        return this;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        return disableMessageTimestamp;
    }

    @Override
    public JMSProducer setAsync(CompletionListener completionListener) {
        this.completionListener = completionListener;
        return this;
    }

    @Override
    public CompletionListener getAsync() {
        return completionListener;
    }

    @Override
    public JMSProducer setJMSCorrelationID(String correlationID) {
        this.jmsCorrelationID = correlationID;
        return this;
    }

    @Override
    public String getJMSCorrelationID() {
        return jmsCorrelationID;
    }

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
        this.jmsCorrelationIDBytes = correlationID != null ? correlationID.clone() : null;
        return this;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        return jmsCorrelationIDBytes;
    }

    @Override
    public JMSProducer setJMSType(String type) {
        this.jmsType = type;
        return this;
    }

    @Override
    public String getJMSType() {
        return jmsType;
    }

    @Override
    public JMSProducer setJMSReplyTo(Destination replyTo) {
        this.jmsReplyTo = replyTo;
        return this;
    }

    @Override
    public Destination getJMSReplyTo() {
        return jmsReplyTo;
    }

    @Override
    public JMSProducer setProperty(String name, boolean value) {
        checkPropertyName(name);
        getOrCreateProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, byte value) {
        checkPropertyName(name);
        getOrCreateProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, short value) {
        checkPropertyName(name);
        getOrCreateProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, int value) {
        checkPropertyName(name);
        getOrCreateProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, long value) {
        checkPropertyName(name);
        getOrCreateProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, float value) {
        checkPropertyName(name);
        getOrCreateProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, double value) {
        checkPropertyName(name);
        getOrCreateProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, String value) {
        checkPropertyName(name);
        getOrCreateProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, Object value) {
        checkPropertyName(name);
        getOrCreateProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer clearProperties() {
        if (properties != null) {
            properties.clear();
        }
        return this;
    }

    @Override
    public boolean propertyExists(String name) {
        return properties != null && properties.containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) {
        var value = getProperty(name);
        if (value == null || value instanceof String) {
            return Boolean.valueOf((String) value);
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        throw invalidConversion(name, value, "boolean");
    }

    @Override
    public byte getByteProperty(String name) {
        var value = getProperty(name);
        if (value == null || value instanceof String) {
            return Byte.valueOf((String) value);
        }
        if (value instanceof Byte) {
            return (Byte) value;
        }
        throw invalidConversion(name, value, "byte");
    }

    @Override
    public short getShortProperty(String name) {
        var value = getProperty(name);
        if (value == null || value instanceof String) {
            return Short.valueOf((String) value);
        }
        if (value instanceof Byte || value instanceof Short) {
            return ((Number) value).shortValue();
        }
        throw invalidConversion(name, value, "short");
    }

    @Override
    public int getIntProperty(String name) {
        var value = getProperty(name);
        if (value == null || value instanceof String) {
            return Integer.valueOf((String) value);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
            return ((Number) value).intValue();
        }
        throw invalidConversion(name, value, "int");
    }

    @Override
    public long getLongProperty(String name) {
        var value = getProperty(name);
        if (value == null || value instanceof String) {
            return Long.valueOf((String) value);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return ((Number) value).longValue();
        }
        throw invalidConversion(name, value, "long");
    }

    @Override
    public float getFloatProperty(String name) {
        var value = getProperty(name);
        if (value == null || value instanceof String) {
            return Float.valueOf((String) value);
        }
        if (value instanceof Float) {
            return (Float) value;
        }
        throw invalidConversion(name, value, "float");
    }

    @Override
    public double getDoubleProperty(String name) {
        var value = getProperty(name);
        if (value == null || value instanceof String) {
            return Double.valueOf((String) value);
        }
        if (value instanceof Float || value instanceof Double) {
            return ((Number) value).doubleValue();
        }
        throw invalidConversion(name, value, "double");
    }

    @Override
    public String getStringProperty(String name) {
        var value = getProperty(name);
        return value != null ? value.toString() : null;
    }

    @Override
    public Object getObjectProperty(String name) {
        return getProperty(name);
    }

    @Override
    public Set<String> getPropertyNames() {
        // The spec requires an unmodifiable set that is a live view backed by
        // this producer, so materialize the property map even when empty.
        return Collections.unmodifiableSet(getOrCreateProperties().keySet());
    }

    /**
     * Rejects sends once the backing pooled session has been returned to the
     * pool (context closed, or an XA transaction completed and renewed the
     * context's session). Without this check send with a pre-built message
     * would bypass the session and silently use the pooled anonymous producer,
     * which may already be loaned to another borrower.
     */
    private void checkSessionOpen() {
        if (session instanceof PooledSession && ((PooledSession) session).isClosed()) {
            throw new IllegalStateRuntimeException("The producer's pooled session is closed");
        }
    }

    private void applyHeaders(Message message) throws JMSException {
        if (jmsCorrelationID != null) {
            message.setJMSCorrelationID(jmsCorrelationID);
        }
        if (jmsCorrelationIDBytes != null) {
            message.setJMSCorrelationIDAsBytes(jmsCorrelationIDBytes);
        }
        if (jmsType != null) {
            message.setJMSType(jmsType);
        }
        if (jmsReplyTo != null) {
            message.setJMSReplyTo(jmsReplyTo);
        }
    }

    private void applyProperties(Message message) throws JMSException {
        if (properties != null) {
            for (var entry : properties.entrySet()) {
                message.setObjectProperty(entry.getKey(), entry.getValue());
            }
        }
    }

    private Map<String, Object> getOrCreateProperties() {
        if (properties == null) {
            properties = new LinkedHashMap<>();
        }
        return properties;
    }

    private Object getProperty(String name) {
        return properties != null ? properties.get(name) : null;
    }

    private static void checkPropertyName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Property name must not be null or empty");
        }
    }

    private MessageFormatRuntimeException invalidConversion(String name, Object value, String type) {
        return new MessageFormatRuntimeException(
            "Property " + name + " was " + value.getClass().getName() + " and cannot be read as " + type);
    }
}
