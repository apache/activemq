/*
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
package org.apache.activemq.transport.amqp.message;

import java.io.DataInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.zip.InflaterInputStream;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.transport.amqp.AmqpProtocolException;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.qpid.proton.amqp.Binary;

public class ActiveMQJMSVendor {

    final public static ActiveMQJMSVendor INSTANCE = new ActiveMQJMSVendor();

    private ActiveMQJMSVendor() {
    }

    /**
     * @return a new vendor specific Message instance.
     */
    public Message createMessage() {
        return new ActiveMQMessage();
    }

    /**
     * @return a new vendor specific BytesMessage instance.
     */
    public BytesMessage createBytesMessage() {
        return new ActiveMQBytesMessage();
    }

    /**
     * @return a new vendor specific BytesMessage instance with the given payload.
     */
    public BytesMessage createBytesMessage(byte[] content, int offset, int length) {
        ActiveMQBytesMessage message = new ActiveMQBytesMessage();
        message.setContent(new ByteSequence(content, offset, length));
        return message;
    }

    /**
     * @return a new vendor specific StreamMessage instance.
     */
    public StreamMessage createStreamMessage() {
        return new ActiveMQStreamMessage();
    }

    /**
     * @return a new vendor specific TextMessage instance.
     */
    public TextMessage createTextMessage() {
        return new ActiveMQTextMessage();
    }

    /**
     * @return a new vendor specific TextMessage instance with the given string in the body.
     */
    public TextMessage createTextMessage(String text) {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        try {
            message.setText(text);
        } catch (MessageNotWriteableException ex) {}

        return message;
    }

    /**
     * @return a new vendor specific ObjectMessage instance.
     */
    public ObjectMessage createObjectMessage() {
        return new ActiveMQObjectMessage();
    }

    /**
     * @return a new vendor specific ObjectMessage instance with the serialized form given.
     */
    public ObjectMessage createObjectMessage(byte[] content, int offset, int length) {
        ActiveMQObjectMessage message = new ActiveMQObjectMessage();
        message.setContent(new ByteSequence(content, offset, length));
        return message;
    }

    /**
     * @return a new vendor specific MapMessage instance.
     */
    public MapMessage createMapMessage() {
        return new ActiveMQMapMessage();
    }

    /**
     * @return a new vendor specific MapMessage instance with the given map as its content.
     */
    public MapMessage createMapMessage(Map<String, Object> content) throws JMSException {
        ActiveMQMapMessage message = new ActiveMQMapMessage();
        final Set<Map.Entry<String, Object>> set = content.entrySet();
        for (Map.Entry<String, Object> entry : set) {
            message.setObject(entry.getKey(), entry.getValue());
        }
        return message;
    }

    /**
     * Creates a new JMS Destination instance from the given name.
     *
     * @param name
     *      the name to use to construct the new Destination
     *
     * @return a new JMS Destination object derived from the given name.
     */
    public Destination createDestination(String name) {
        return ActiveMQDestination.createDestination(name, ActiveMQDestination.QUEUE_TYPE);
    }

    /**
     * Set the given value as the JMSXUserID on the message instance.
     *
     * @param message
     *      the message to be updated.
     * @param value
     *      the value to apply to the message.
     */
    public void setJMSXUserID(Message msg, String value) {
        ((ActiveMQMessage) msg).setUserID(value);
    }

    /**
     * Set the given value as the JMSXGroupID on the message instance.
     *
     * @param message
     *      the message to be updated.
     * @param value
     *      the value to apply to the message.
     */
    public void setJMSXGroupID(Message msg, String value) {
        ((ActiveMQMessage) msg).setGroupID(value);
    }

    /**
     * Set the given value as the JMSXGroupSequence on the message instance.
     *
     * @param message
     *      the message to be updated.
     * @param value
     *      the value to apply to the message.
     */
    public void setJMSXGroupSequence(Message msg, int value) {
        ((ActiveMQMessage) msg).setGroupSequence(value);
    }

    /**
     * Set the given value as the JMSXDeliveryCount on the message instance.
     *
     * @param message
     *      the message to be updated.
     * @param value
     *      the value to apply to the message.
     */
    public void setJMSXDeliveryCount(Message msg, long value) {
        ((ActiveMQMessage) msg).setRedeliveryCounter((int) value);
    }

    /**
     * Convert the given JMS Destination into the appropriate AMQP address string
     * for assignment to the 'to' or 'replyTo' field of an AMQP message.
     *
     * @param destination
     *      the JMS Destination instance to be converted.
     *
     * @return the converted string address to assign to the message.
     */
    public String toAddress(Destination dest) {
        return ((ActiveMQDestination) dest).getQualifiedName();
    }

    /**
     * Given an Message instance return the original Message ID that was assigned the
     * Message when it was first processed by the broker.  For an AMQP message this
     * should be the original value of the message's MessageId field with the correct
     * type preserved.
     *
     * @param message
     *      the message which is being accessed.
     *
     * @return the original MessageId assigned to this Message instance.
     */
    public Object getOriginalMessageId(Message message) {
        Object result;
        MessageId msgId = ((ActiveMQMessage)message).getMessageId();
        if (msgId.getTextView() != null) {
            try {
                result = AMQPMessageIdHelper.INSTANCE.toIdObject(msgId.getTextView());
            } catch (AmqpProtocolException e) {
                result = msgId.getTextView().toString();
            }
        } else {
            result = msgId.toString();
        }

        return result;
    }

    /**
     * Return the encoded form of the BytesMessage as an AMQP Binary instance.
     *
     * @param message
     *      the Message whose binary encoded body is needed.
     *
     * @return a Binary instance containing the encoded message body.
     *
     * @throws JMSException if an error occurs while fetching the binary payload.
     */
    public Binary getBinaryFromMessageBody(BytesMessage message) throws JMSException {
        ActiveMQBytesMessage bytesMessage = (ActiveMQBytesMessage) message;
        Binary result = null;

        if (bytesMessage.getContent() != null) {
            ByteSequence contents = bytesMessage.getContent();

            if (bytesMessage.isCompressed()) {
                int length = (int) bytesMessage.getBodyLength();
                byte[] uncompressed = new byte[length];
                bytesMessage.readBytes(uncompressed);

                result = new Binary(uncompressed);
            } else {
                return new Binary(contents.getData(), contents.getOffset(), contents.getLength());
            }
        }

        return result;
    }

    /**
     * Return the encoded form of the BytesMessage as an AMQP Binary instance.
     *
     * @param message
     *      the Message whose binary encoded body is needed.
     *
     * @return a Binary instance containing the encoded message body.
     *
     * @throws JMSException if an error occurs while fetching the binary payload.
     */
    public Binary getBinaryFromMessageBody(ObjectMessage message) throws JMSException {
        ActiveMQObjectMessage objectMessage = (ActiveMQObjectMessage) message;
        Binary result = null;

        if (objectMessage.getContent() != null) {
            ByteSequence contents = objectMessage.getContent();

            if (objectMessage.isCompressed()) {
                try (ByteArrayOutputStream os = new ByteArrayOutputStream();
                     ByteArrayInputStream is = new ByteArrayInputStream(contents);
                     InflaterInputStream iis = new InflaterInputStream(is);) {

                    byte value;
                    while ((value = (byte) iis.read()) != -1) {
                        os.write(value);
                    }

                    ByteSequence expanded = os.toByteSequence();
                    result = new Binary(expanded.getData(), expanded.getOffset(), expanded.getLength());
                } catch (Exception cause) {
                   throw JMSExceptionSupport.create(cause);
               }
            } else {
                return new Binary(contents.getData(), contents.getOffset(), contents.getLength());
            }
        }

        return result;
    }

    /**
     * Return the encoded form of the Message as an AMQP Binary instance.
     *
     * @param message
     *      the Message whose binary encoded body is needed.
     *
     * @return a Binary instance containing the encoded message body.
     *
     * @throws JMSException if an error occurs while fetching the binary payload.
     */
    public Binary getBinaryFromMessageBody(TextMessage message) throws JMSException {
        ActiveMQTextMessage textMessage = (ActiveMQTextMessage) message;
        Binary result = null;

        if (textMessage.getContent() != null) {
            ByteSequence contents = textMessage.getContent();

            if (textMessage.isCompressed()) {
                try (ByteArrayInputStream is = new ByteArrayInputStream(contents);
                     InflaterInputStream iis = new InflaterInputStream(is);
                     DataInputStream dis = new DataInputStream(iis);) {

                    int size = dis.readInt();
                    byte[] uncompressed = new byte[size];
                    dis.readFully(uncompressed);

                    result = new Binary(uncompressed);
                } catch (Exception cause) {
                    throw JMSExceptionSupport.create(cause);
                }
            } else {
                // Message includes a size prefix of four bytes for the OpenWire marshaler
                result = new Binary(contents.getData(), contents.getOffset() + 4, contents.getLength() - 4);
            }
        } else if (textMessage.getText() != null) {
            result = new Binary(textMessage.getText().getBytes(StandardCharsets.UTF_8));
        }

        return result;
    }

    /**
     * Return the underlying Map from the JMS MapMessage instance.
     *
     * @param message
     *      the MapMessage whose underlying Map is requested.
     *
     * @return the underlying Map used to store the value in the given MapMessage.
     *
     * @throws JMSException if an error occurs in constructing or fetching the Map.
     */
    public Map<String, Object> getMapFromMessageBody(MapMessage message) throws JMSException {
        final HashMap<String, Object> map = new HashMap<String, Object>();
        final ActiveMQMapMessage mapMessage = (ActiveMQMapMessage) message;

        final Map<String, Object> contentMap = mapMessage.getContentMap();
        if (contentMap != null) {
            map.putAll(contentMap);
        }

        return contentMap;
    }

    /**
     * Sets the given Message Property on the given message overriding any read-only
     * state on the Message long enough for the property to be added.
     *
     * @param message
     *      the message to set the property on.
     * @param key
     *      the String key for the new Message property
     * @param value
     *      the Object to assign to the new Message property.
     *
     * @throws JMSException if an error occurs while setting the property.
     */
    public void setMessageProperty(Message message, String key, Object value) throws JMSException {
        final ActiveMQMessage amqMessage = (ActiveMQMessage) message;

        boolean oldValue = amqMessage.isReadOnlyProperties();

        amqMessage.setReadOnlyProperties(false);
        amqMessage.setObjectProperty(key, value);
        amqMessage.setReadOnlyProperties(oldValue);
    }
}
