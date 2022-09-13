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
package org.apache.activemq;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.TypeConversionSupport;

public class ActiveMQProducer implements JMSProducer {

    private final ActiveMQContext activemqContext;
    private final ActiveMQMessageProducer activemqMessageProducer;

    // QoS override of defaults on a per-JMSProducer instance basis
    private String correlationId = null;
    private byte[] correlationIdBytes = null;
    private Integer deliveryMode = null;
    private Boolean disableMessageID = false;
    private Boolean disableMessageTimestamp = false;
    private Integer priority = null;
    private Destination replyTo = null;
    private Long timeToLive = null;
    private String type = null;

    // Properties applied to all messages on a per-JMS producer instance basis
    private Map<String, Object> messageProperties = null;

    ActiveMQProducer(ActiveMQContext activemqContext, ActiveMQMessageProducer activemqMessageProducer) {
        this.activemqContext = activemqContext;
        this.activemqMessageProducer = activemqMessageProducer;
    }

    @Override
    public JMSProducer send(Destination destination, Message message) {
        try {
            if(this.correlationId != null) {
                message.setJMSCorrelationID(this.correlationId);
            }

            if(this.correlationIdBytes != null) {
                message.setJMSCorrelationIDAsBytes(this.correlationIdBytes);
            }

            if(this.replyTo != null) {
                message.setJMSReplyTo(this.replyTo);
            }

            if(this.type != null) {
                message.setJMSType(this.type);
            }

            if(messageProperties != null && !messageProperties.isEmpty()) {
                for(Map.Entry<String, Object> propertyEntry : messageProperties.entrySet()) {
                    message.setObjectProperty(propertyEntry.getKey(), propertyEntry.getValue());
                }
            }

            activemqMessageProducer.send(destination, message, getDeliveryMode(), getPriority(), getTimeToLive(), getDisableMessageID(), getDisableMessageTimestamp(), null);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, String body) {
        TextMessage textMessage = activemqContext.createTextMessage(body);
        send(destination, textMessage);
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, Map<String, Object> body) {
        MapMessage mapMessage = activemqContext.createMapMessage();

        if (body != null) {
            try {
               for (Map.Entry<String, Object> mapEntry : body.entrySet()) {
                  final String key = mapEntry.getKey();
                  final Object value = mapEntry.getValue();
                  final Class<?> valueObject = value.getClass();
                  if (String.class.isAssignableFrom(valueObject)) {
                      mapMessage.setString(key, String.class.cast(value));
                  } else if (Integer.class.isAssignableFrom(valueObject)) {
                      mapMessage.setInt(key, Integer.class.cast(value));
                  } else if (Long.class.isAssignableFrom(valueObject)) {
                      mapMessage.setLong(key, Long.class.cast(value));
                  } else if (Double.class.isAssignableFrom(valueObject)) {
                      mapMessage.setDouble(key, Double.class.cast(value));
                  } else if (Boolean.class.isAssignableFrom(valueObject)) {
                      mapMessage.setBoolean(key, Boolean.class.cast(value));
                  } else if (Character.class.isAssignableFrom(valueObject)) {
                      mapMessage.setChar(key, Character.class.cast(value));
                  } else if (Short.class.isAssignableFrom(valueObject)) {
                      mapMessage.setShort(key, Short.class.cast(value));
                  } else if (Float.class.isAssignableFrom(valueObject)) {
                      mapMessage.setFloat(key, Float.class.cast(value));
                  } else if (Byte.class.isAssignableFrom(valueObject)) {
                      mapMessage.setByte(key, Byte.class.cast(value));
                  } else if (byte[].class.isAssignableFrom(valueObject)) {
                      byte[] array = byte[].class.cast(value);
                      mapMessage.setBytes(key, array, 0, array.length);
                  } else {
                      mapMessage.setObject(key, value);
                  }
               }
            } catch (JMSException e) {
               throw new MessageFormatRuntimeException(e.getMessage());
            }
         }
         send(destination, mapMessage);
         return this;
    }

    @Override
    public JMSProducer send(Destination destination, byte[] body) {
        BytesMessage bytesMessage = activemqContext.createBytesMessage();

        try {
            if(body != null) {
                bytesMessage.writeBytes(body);
            }
            send(destination, bytesMessage);
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, Serializable body) {
        ObjectMessage objectMessage = activemqContext.createObjectMessage(body);
        send(destination, objectMessage);
        return this;
    }

    @Override
    public JMSProducer setDisableMessageID(boolean disableMessageID) {
        this.disableMessageID = disableMessageID;
        return this;
    }

    @Override
    public boolean getDisableMessageID() {
        if(this.disableMessageID != null) {
            return this.disableMessageID;
        }

        try {
            return this.activemqMessageProducer.getDisableMessageID();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSProducer setDisableMessageTimestamp(boolean disableMessageTimestamp) {
        this.disableMessageTimestamp = disableMessageTimestamp;
        return this;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        if(this.disableMessageTimestamp != null) {
            return this.disableMessageTimestamp;
        }

        try {
            return this.activemqMessageProducer.getDisableMessageTimestamp();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSProducer setDeliveryMode(int deliveryMode) {
        if (deliveryMode != DeliveryMode.PERSISTENT && deliveryMode != DeliveryMode.NON_PERSISTENT) {
            throw new IllegalStateRuntimeException("unknown delivery mode: " + deliveryMode);
        }
        this.deliveryMode = deliveryMode;
        return this;
    }

    @Override
    public int getDeliveryMode() {
        if(deliveryMode != null) {
            return deliveryMode;
        }

        try {
            return this.activemqMessageProducer.getDeliveryMode();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSProducer setPriority(int priority) {
        if (priority < 0 || priority > 9) {
            throw new IllegalStateRuntimeException("default priority must be a value between 0 and 9");
        }
        this.priority = priority;
        return this;
    }

    @Override
    public int getPriority() {
        if(priority != null) {
            return priority;
        }

        try {
            return this.activemqMessageProducer.getPriority();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSProducer setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    @Override
    public long getTimeToLive() {
        if(timeToLive != null) {
            return timeToLive;
        }

        try {
            return this.activemqMessageProducer.getTimeToLive();
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSProducer setDeliveryDelay(long deliveryDelay) {
        throw new UnsupportedOperationException("setDeliveryDelay(long) is not supported");
    }

    @Override
    public long getDeliveryDelay() {
        throw new UnsupportedOperationException("getDeliveryDelay() is not supported");
    }

    @Override
    public JMSProducer setAsync(CompletionListener completionListener) {
        throw new UnsupportedOperationException("setAsync(CompletionListener) is not supported");
    }

    @Override
    public CompletionListener getAsync() {
        throw new UnsupportedOperationException("getAsync() is not supported");
    }

    @Override
    public JMSProducer setProperty(String name, boolean value) {
        checkProperty(name, value);
        getCreatedMessageProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, byte value) {
        checkProperty(name, value);
        getCreatedMessageProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, short value) {
        checkProperty(name, value);
        getCreatedMessageProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, int value) {
        checkProperty(name, value);
        getCreatedMessageProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, long value) {
        checkProperty(name, value);
        getCreatedMessageProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, float value) {
        checkProperty(name, value);
        getCreatedMessageProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, double value) {
        checkProperty(name, value);
        getCreatedMessageProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, String value) {
        checkProperty(name, value);
        getCreatedMessageProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, Object value) {
        checkProperty(name, value);
        getCreatedMessageProperties().put(name, value);
        return this;
    }

    @Override
    public JMSProducer clearProperties() {
        getCreatedMessageProperties().clear();
        return this;
    }

    @Override
    public boolean propertyExists(String name) {
        if(name == null || name.isEmpty()) {
            return false;
        }
        return getCreatedMessageProperties().containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) {
        Object value = getCreatedMessageProperties().get(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Boolean rc = (Boolean)TypeConversionSupport.convert(value, Boolean.class);
        if (rc == null) {
            throw new MessageFormatRuntimeException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a boolean");
        }
        return rc.booleanValue();
    }

    @Override
    public byte getByteProperty(String name) {
        Object value = getCreatedMessageProperties().get(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Byte rc = (Byte)TypeConversionSupport.convert(value, Byte.class);
        if (rc == null) {
            throw new MessageFormatRuntimeException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a byte");
        }
        return rc.byteValue();
    }

    @Override
    public short getShortProperty(String name) {
        Object value = getCreatedMessageProperties().get(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Short rc = (Short)TypeConversionSupport.convert(value, Short.class);
        if (rc == null) {
            throw new MessageFormatRuntimeException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a short");
        }
        return rc.shortValue();
    }

    @Override
    public int getIntProperty(String name) {
        Object value = getCreatedMessageProperties().get(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Integer rc = (Integer)TypeConversionSupport.convert(value, Integer.class);
        if (rc == null) {
            throw new MessageFormatRuntimeException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as an integer");
        }
        return rc.intValue();
    }

    @Override
    public long getLongProperty(String name) {
        Object value = getCreatedMessageProperties().get(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Long rc = (Long)TypeConversionSupport.convert(value, Long.class);
        if (rc == null) {
            throw new MessageFormatRuntimeException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a long");
        }
        return rc.longValue();
    }

    @Override
    public float getFloatProperty(String name) {
        Object value = getCreatedMessageProperties().get(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Float rc = (Float)TypeConversionSupport.convert(value, Float.class);
        if (rc == null) {
            throw new MessageFormatRuntimeException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a float");
        }
        return rc.floatValue();
    }

    @Override
    public double getDoubleProperty(String name) {
        Object value = getCreatedMessageProperties().get(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Double rc = (Double)TypeConversionSupport.convert(value, Double.class);
        if (rc == null) {
            throw new MessageFormatRuntimeException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a double");
        }
        return rc.doubleValue();
    }

    @Override
    public String getStringProperty(String name) {
        Object value = getCreatedMessageProperties().get(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        String rc = (String)TypeConversionSupport.convert(value, String.class);
        if (rc == null) {
            throw new MessageFormatRuntimeException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a string");
        }
        return rc;
    }

    @Override
    public Object getObjectProperty(String name) {
        if (name == null) {
            throw new NullPointerException("Property name cannot be null");
        }

        // TODO: Update PropertyExpression to handle converting message headers to properties for JMSProducer.
        //PropertyExpression expression = new PropertyExpression(name);
        //return expression.evaluate(this);
        return getCreatedMessageProperties().get(name);
    }

    @Override
    public Set<String> getPropertyNames() {
        return getCreatedMessageProperties().keySet();
    }

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
        if(correlationID != null) {
            this.correlationIdBytes = Arrays.copyOf(correlationID, correlationID.length);
        }
        return this;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        return this.correlationIdBytes;
    }

    @Override
    public JMSProducer setJMSCorrelationID(String correlationID) {
        this.correlationId = correlationID;
        return this;
    }

    @Override
    public String getJMSCorrelationID() {
        return this.correlationId;
    }

    @Override
    public JMSProducer setJMSType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getJMSType() {
        return this.type;
    }

    @Override
    public JMSProducer setJMSReplyTo(Destination replyTo) {
        this.replyTo = replyTo;
        return this;
    }

    @Override
    public Destination getJMSReplyTo() {
        return replyTo;
    }

    private void checkProperty(String name, Object value) {
        ActiveMQMessageProducerSupport.validateValidPropertyName(name);
        ActiveMQMessageProducerSupport.validateValidPropertyValue(name, value);
    }

    private Map<String, Object> getCreatedMessageProperties() {
        if(messageProperties == null) {
            messageProperties = new HashMap<>();
        }
        return messageProperties;
    }
}
