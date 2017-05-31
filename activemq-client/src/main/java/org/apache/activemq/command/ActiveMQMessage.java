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
package org.apache.activemq.command;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.scheduler.CronParser;
import org.apache.activemq.filter.PropertyExpression;
import org.apache.activemq.state.CommandVisitor;
import org.apache.activemq.util.Callback;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.TypeConversionSupport;
import org.fusesource.hawtbuf.UTF8Buffer;

/**
 *
 * @openwire:marshaller code="23"
 */
public class ActiveMQMessage extends Message implements org.apache.activemq.Message, ScheduledMessage {
    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.ACTIVEMQ_MESSAGE;
    public static final String DLQ_DELIVERY_FAILURE_CAUSE_PROPERTY = "dlqDeliveryFailureCause";
    public static final String BROKER_PATH_PROPERTY = "JMSActiveMQBrokerPath";

    private static final Map<String, PropertySetter> JMS_PROPERTY_SETERS = new HashMap<String, PropertySetter>();

    protected transient Callback acknowledgeCallback;

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    @Override
    public Message copy() {
        ActiveMQMessage copy = new ActiveMQMessage();
        copy(copy);
        return copy;
    }

    protected void copy(ActiveMQMessage copy) {
        super.copy(copy);
        copy.acknowledgeCallback = acknowledgeCallback;
    }

    @Override
    public int hashCode() {
        MessageId id = getMessageId();
        if (id != null) {
            return id.hashCode();
        } else {
            return super.hashCode();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != getClass()) {
            return false;
        }

        ActiveMQMessage msg = (ActiveMQMessage) o;
        MessageId oMsg = msg.getMessageId();
        MessageId thisMsg = this.getMessageId();
        return thisMsg != null && oMsg != null && oMsg.equals(thisMsg);
    }

    @Override
    public void acknowledge() throws JMSException {
        if (acknowledgeCallback != null) {
            try {
                acknowledgeCallback.execute();
            } catch (JMSException e) {
                throw e;
            } catch (Throwable e) {
                throw JMSExceptionSupport.create(e);
            }
        }
    }

    @Override
    public void clearBody() throws JMSException {
        setContent(null);
        readOnlyBody = false;
    }

    @Override
    public String getJMSMessageID() {
        MessageId messageId = this.getMessageId();
        if (messageId == null) {
            return null;
        }
        return messageId.toString();
    }

    /**
     * Seems to be invalid because the parameter doesn't initialize MessageId
     * instance variables ProducerId and ProducerSequenceId
     *
     * @param value
     * @throws JMSException
     */
    @Override
    public void setJMSMessageID(String value) throws JMSException {
        if (value != null) {
            try {
                MessageId id = new MessageId(value);
                this.setMessageId(id);
            } catch (NumberFormatException e) {
                // we must be some foreign JMS provider or strange user-supplied
                // String
                // so lets set the IDs to be 1
                MessageId id = new MessageId();
                id.setTextView(value);
                this.setMessageId(id);
            }
        } else {
            this.setMessageId(null);
        }
    }

    /**
     * This will create an object of MessageId. For it to be valid, the instance
     * variable ProducerId and producerSequenceId must be initialized.
     *
     * @param producerId
     * @param producerSequenceId
     * @throws JMSException
     */
    public void setJMSMessageID(ProducerId producerId, long producerSequenceId) throws JMSException {
        MessageId id = null;
        try {
            id = new MessageId(producerId, producerSequenceId);
            this.setMessageId(id);
        } catch (Throwable e) {
            throw JMSExceptionSupport.create("Invalid message id '" + id + "', reason: " + e.getMessage(), e);
        }
    }

    @Override
    public long getJMSTimestamp() {
        return this.getTimestamp();
    }

    @Override
    public void setJMSTimestamp(long timestamp) {
        this.setTimestamp(timestamp);
    }

    @Override
    public String getJMSCorrelationID() {
        return this.getCorrelationId();
    }

    @Override
    public void setJMSCorrelationID(String correlationId) {
        this.setCorrelationId(correlationId);
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return encodeString(this.getCorrelationId());
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationId) throws JMSException {
        this.setCorrelationId(decodeString(correlationId));
    }

    @Override
    public String getJMSXMimeType() {
        return "jms/message";
    }

    protected static String decodeString(byte[] data) throws JMSException {
        try {
            if (data == null) {
                return null;
            }
            return new String(data, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JMSException("Invalid UTF-8 encoding: " + e.getMessage());
        }
    }

    protected static byte[] encodeString(String data) throws JMSException {
        try {
            if (data == null) {
                return null;
            }
            return data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JMSException("Invalid UTF-8 encoding: " + e.getMessage());
        }
    }

    @Override
    public Destination getJMSReplyTo() {
        return this.getReplyTo();
    }

    @Override
    public void setJMSReplyTo(Destination destination) throws JMSException {
        this.setReplyTo(ActiveMQDestination.transform(destination));
    }

    @Override
    public Destination getJMSDestination() {
        return this.getDestination();
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        this.setDestination(ActiveMQDestination.transform(destination));
    }

    @Override
    public int getJMSDeliveryMode() {
        return this.isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
    }

    @Override
    public void setJMSDeliveryMode(int mode) {
        this.setPersistent(mode == DeliveryMode.PERSISTENT);
    }

    @Override
    public boolean getJMSRedelivered() {
        return this.isRedelivered();
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) {
        this.setRedelivered(redelivered);
    }

    @Override
    public String getJMSType() {
        return this.getType();
    }

    @Override
    public void setJMSType(String type) {
        this.setType(type);
    }

    @Override
    public long getJMSExpiration() {
        return this.getExpiration();
    }

    @Override
    public void setJMSExpiration(long expiration) {
        this.setExpiration(expiration);
    }

    @Override
    public int getJMSPriority() {
        return this.getPriority();
    }

    @Override
    public void setJMSPriority(int priority) {
        this.setPriority((byte) priority);
    }

    @Override
    public void clearProperties() {
        super.clearProperties();
        readOnlyProperties = false;
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        try {
            return (this.getProperties().containsKey(name) || getObjectProperty(name)!= null);
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Enumeration getPropertyNames() throws JMSException {
        try {
            Vector<String> result = new Vector<String>(this.getProperties().keySet());
            if( getRedeliveryCounter()!=0 ) {
                result.add("JMSXDeliveryCount");
            }
            if( getGroupID()!=null ) {
                result.add("JMSXGroupID");
            }
            if( getGroupID()!=null ) {
                result.add("JMSXGroupSeq");
            }
            if( getUserID()!=null ) {
                result.add("JMSXUserID");
            }
            return result.elements();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    /**
     * return all property names, including standard JMS properties and JMSX properties
     * @return  Enumeration of all property names on this message
     * @throws JMSException
     */
    @SuppressWarnings("rawtypes")
    public Enumeration getAllPropertyNames() throws JMSException {
        try {
            Vector<String> result = new Vector<String>(this.getProperties().keySet());
            result.addAll(JMS_PROPERTY_SETERS.keySet());
            return result.elements();
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    interface PropertySetter {
        void set(Message message, Object value) throws MessageFormatException;
    }

    static {
        JMS_PROPERTY_SETERS.put("JMSXDeliveryCount", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                Integer rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSXDeliveryCount cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setRedeliveryCounter(rc.intValue() - 1);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSXGroupID", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                String rc = (String) TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSXGroupID cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setGroupID(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSXGroupSeq", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                Integer rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSXGroupSeq cannot be set from a " + value.getClass().getName() + ".");
                }
                message.setGroupSequence(rc.intValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSCorrelationID", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                String rc = (String) TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSCorrelationID cannot be set from a " + value.getClass().getName() + ".");
                }
                ((ActiveMQMessage) message).setJMSCorrelationID(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSDeliveryMode", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                Integer rc = null;
                try {
                    rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
                } catch (NumberFormatException nfe) {
                    if (value instanceof String) {
                        if (((String) value).equalsIgnoreCase("PERSISTENT")) {
                            rc = DeliveryMode.PERSISTENT;
                        } else if (((String) value).equalsIgnoreCase("NON_PERSISTENT")) {
                            rc = DeliveryMode.NON_PERSISTENT;
                        } else {
                            throw nfe;
                        }
                    }
                }
                if (rc == null) {
                    Boolean bool = (Boolean) TypeConversionSupport.convert(value, Boolean.class);
                    if (bool == null) {
                        throw new MessageFormatException("Property JMSDeliveryMode cannot be set from a " + value.getClass().getName() + ".");
                    } else {
                        rc = bool.booleanValue() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT;
                    }
                }
                ((ActiveMQMessage) message).setJMSDeliveryMode(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSExpiration", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                Long rc = (Long) TypeConversionSupport.convert(value, Long.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSExpiration cannot be set from a " + value.getClass().getName() + ".");
                }
                ((ActiveMQMessage) message).setJMSExpiration(rc.longValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSPriority", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                Integer rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSPriority cannot be set from a " + value.getClass().getName() + ".");
                }
                ((ActiveMQMessage) message).setJMSPriority(rc.intValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSRedelivered", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                Boolean rc = (Boolean) TypeConversionSupport.convert(value, Boolean.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSRedelivered cannot be set from a " + value.getClass().getName() + ".");
                }
                ((ActiveMQMessage) message).setJMSRedelivered(rc.booleanValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSReplyTo", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                ActiveMQDestination rc = (ActiveMQDestination) TypeConversionSupport.convert(value, ActiveMQDestination.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSReplyTo cannot be set from a " + value.getClass().getName() + ".");
                }
                ((ActiveMQMessage) message).setReplyTo(rc);
            }
        });
        JMS_PROPERTY_SETERS.put("JMSTimestamp", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                Long rc = (Long) TypeConversionSupport.convert(value, Long.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSTimestamp cannot be set from a " + value.getClass().getName() + ".");
                }
                ((ActiveMQMessage) message).setJMSTimestamp(rc.longValue());
            }
        });
        JMS_PROPERTY_SETERS.put("JMSType", new PropertySetter() {
            @Override
            public void set(Message message, Object value) throws MessageFormatException {
                String rc = (String) TypeConversionSupport.convert(value, String.class);
                if (rc == null) {
                    throw new MessageFormatException("Property JMSType cannot be set from a " + value.getClass().getName() + ".");
                }
                ((ActiveMQMessage) message).setJMSType(rc);
            }
        });
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        setObjectProperty(name, value, true);
    }

    public void setObjectProperty(String name, Object value, boolean checkReadOnly) throws JMSException {

        if (checkReadOnly) {
            checkReadOnlyProperties();
        }
        if (name == null || name.equals("")) {
            throw new IllegalArgumentException("Property name cannot be empty or null");
        }

        if (value instanceof UTF8Buffer) {
            value = value.toString();
        }

        checkValidObject(value);
        value = convertScheduled(name, value);
        PropertySetter setter = JMS_PROPERTY_SETERS.get(name);

        if (setter != null && value != null) {
            setter.set(this, value);
        } else {
            try {
                this.setProperty(name, value);
            } catch (IOException e) {
                throw JMSExceptionSupport.create(e);
            }
        }
    }

    public void setProperties(Map<String, ?> properties) throws JMSException {
        for (Map.Entry<String, ?> entry : properties.entrySet()) {
            // Lets use the object property method as we may contain standard
            // extension headers like JMSXGroupID
            setObjectProperty(entry.getKey(), entry.getValue());
        }
    }

    protected void checkValidObject(Object value) throws MessageFormatException {

        boolean valid = value instanceof Boolean || value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long;
        valid = valid || value instanceof Float || value instanceof Double || value instanceof Character || value instanceof String || value == null;

        if (!valid) {

            ActiveMQConnection conn = getConnection();
            // conn is null if we are in the broker rather than a JMS client
            if (conn == null || conn.isNestedMapAndListEnabled()) {
                if (!(value instanceof Map || value instanceof List)) {
                    throw new MessageFormatException("Only objectified primitive objects, String, Map and List types are allowed but was: " + value + " type: " + value.getClass());
                }
            } else {
                throw new MessageFormatException("Only objectified primitive objects and String types are allowed but was: " + value + " type: " + value.getClass());
            }
        }
    }

    protected void checkValidScheduled(String name, Object value) throws MessageFormatException {
        if (AMQ_SCHEDULED_DELAY.equals(name) || AMQ_SCHEDULED_PERIOD.equals(name) || AMQ_SCHEDULED_REPEAT.equals(name)) {
            if (value instanceof Long == false && value instanceof Integer == false) {
                throw new MessageFormatException(name + " should be long or int value");
            }
        }
        if (AMQ_SCHEDULED_CRON.equals(name)) {
            CronParser.validate(value.toString());
        }
    }

    protected Object convertScheduled(String name, Object value) throws MessageFormatException {
        Object result = value;
        if (AMQ_SCHEDULED_DELAY.equals(name)){
            result = TypeConversionSupport.convert(value, Long.class);
        }
        else if (AMQ_SCHEDULED_PERIOD.equals(name)){
            result = TypeConversionSupport.convert(value, Long.class);
        }
        else if (AMQ_SCHEDULED_REPEAT.equals(name)){
            result = TypeConversionSupport.convert(value, Integer.class);
        }
        return result;
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        if (name == null) {
            throw new NullPointerException("Property name cannot be null");
        }

        // PropertyExpression handles converting message headers to properties.
        PropertyExpression expression = new PropertyExpression(name);
        return expression.evaluate(this);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            return false;
        }
        Boolean rc = (Boolean) TypeConversionSupport.convert(value, Boolean.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a boolean");
        }
        return rc.booleanValue();
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Byte rc = (Byte) TypeConversionSupport.convert(value, Byte.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a byte");
        }
        return rc.byteValue();
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Short rc = (Short) TypeConversionSupport.convert(value, Short.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a short");
        }
        return rc.shortValue();
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Integer rc = (Integer) TypeConversionSupport.convert(value, Integer.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as an integer");
        }
        return rc.intValue();
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NumberFormatException("property " + name + " was null");
        }
        Long rc = (Long) TypeConversionSupport.convert(value, Long.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a long");
        }
        return rc.longValue();
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Float rc = (Float) TypeConversionSupport.convert(value, Float.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a float");
        }
        return rc.floatValue();
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        Object value = getObjectProperty(name);
        if (value == null) {
            throw new NullPointerException("property " + name + " was null");
        }
        Double rc = (Double) TypeConversionSupport.convert(value, Double.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a double");
        }
        return rc.doubleValue();
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        Object value = null;
        if (name.equals("JMSXUserID")) {
            value = getUserID();
            if (value == null) {
                value = getObjectProperty(name);
            }
        } else {
            value = getObjectProperty(name);
        }
        if (value == null) {
            return null;
        }
        String rc = (String) TypeConversionSupport.convert(value, String.class);
        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a String");
        }
        return rc;
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        setBooleanProperty(name, value, true);
    }

    public void setBooleanProperty(String name, boolean value, boolean checkReadOnly) throws JMSException {
        setObjectProperty(name, Boolean.valueOf(value), checkReadOnly);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        setObjectProperty(name, Byte.valueOf(value));
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        setObjectProperty(name, Short.valueOf(value));
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        setObjectProperty(name, Integer.valueOf(value));
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        setObjectProperty(name, Long.valueOf(value));
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        setObjectProperty(name, new Float(value));
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        setObjectProperty(name, new Double(value));
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        setObjectProperty(name, value);
    }

    private void checkReadOnlyProperties() throws MessageNotWriteableException {
        if (readOnlyProperties) {
            throw new MessageNotWriteableException("Message properties are read-only");
        }
    }

    protected void checkReadOnlyBody() throws MessageNotWriteableException {
        if (readOnlyBody) {
            throw new MessageNotWriteableException("Message body is read-only");
        }
    }

    public Callback getAcknowledgeCallback() {
        return acknowledgeCallback;
    }

    public void setAcknowledgeCallback(Callback acknowledgeCallback) {
        this.acknowledgeCallback = acknowledgeCallback;
    }

    /**
     * Send operation event listener. Used to get the message ready to be sent.
     */
    public void onSend() throws JMSException {
        setReadOnlyBody(true);
        setReadOnlyProperties(true);
    }

    @Override
    public Response visit(CommandVisitor visitor) throws Exception {
        return visitor.processMessage(this);
    }

    @Override
    public void storeContent() {
    }

    @Override
    public void storeContentAndClear() {
        storeContent();
    }
}
