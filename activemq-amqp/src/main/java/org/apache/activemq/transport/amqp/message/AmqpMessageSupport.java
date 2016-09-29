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
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.InflaterInputStream;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

/**
 * Support class containing constant values and static methods that are
 * used to map to / from AMQP Message types being sent or received.
 */
public final class AmqpMessageSupport {

    // Message Properties used to map AMQP to JMS and back

    public static final String JMS_AMQP_PREFIX = "JMS_AMQP_";
    public static final int JMS_AMQP_PREFIX_LENGTH = JMS_AMQP_PREFIX.length();

    public static final String MESSAGE_FORMAT = "MESSAGE_FORMAT";
    public static final String ORIGINAL_ENCODING = "ORIGINAL_ENCODING";
    public static final String NATIVE = "NATIVE";
    public static final String HEADER = "HEADER";
    public static final String PROPERTIES = "PROPERTIES";

    public static final String FIRST_ACQUIRER = "FirstAcquirer";
    public static final String CONTENT_TYPE = "ContentType";
    public static final String CONTENT_ENCODING = "ContentEncoding";
    public static final String REPLYTO_GROUP_ID = "ReplyToGroupID";

    public static final String DELIVERY_ANNOTATION_PREFIX = "DA_";
    public static final String MESSAGE_ANNOTATION_PREFIX = "MA_";
    public static final String FOOTER_PREFIX = "FT_";

    public static final String JMS_AMQP_HEADER = JMS_AMQP_PREFIX + HEADER;
    public static final String JMS_AMQP_PROPERTIES = JMS_AMQP_PREFIX + PROPERTIES;
    public static final String JMS_AMQP_ORIGINAL_ENCODING = JMS_AMQP_PREFIX + ORIGINAL_ENCODING;
    public static final String JMS_AMQP_MESSAGE_FORMAT = JMS_AMQP_PREFIX + MESSAGE_FORMAT;
    public static final String JMS_AMQP_NATIVE = JMS_AMQP_PREFIX + NATIVE;
    public static final String JMS_AMQP_FIRST_ACQUIRER = JMS_AMQP_PREFIX + FIRST_ACQUIRER;
    public static final String JMS_AMQP_CONTENT_TYPE = JMS_AMQP_PREFIX + CONTENT_TYPE;
    public static final String JMS_AMQP_CONTENT_ENCODING = JMS_AMQP_PREFIX + CONTENT_ENCODING;
    public static final String JMS_AMQP_REPLYTO_GROUP_ID = JMS_AMQP_PREFIX + REPLYTO_GROUP_ID;
    public static final String JMS_AMQP_DELIVERY_ANNOTATION_PREFIX = JMS_AMQP_PREFIX + DELIVERY_ANNOTATION_PREFIX;
    public static final String JMS_AMQP_MESSAGE_ANNOTATION_PREFIX = JMS_AMQP_PREFIX + MESSAGE_ANNOTATION_PREFIX;
    public static final String JMS_AMQP_FOOTER_PREFIX = JMS_AMQP_PREFIX + FOOTER_PREFIX;

    // Message body type definitions
    public static final Binary EMPTY_BINARY = new Binary(new byte[0]);
    public static final Data EMPTY_BODY = new Data(EMPTY_BINARY);
    public static final Data NULL_OBJECT_BODY;

    public static final short AMQP_UNKNOWN = 0;
    public static final short AMQP_NULL = 1;
    public static final short AMQP_DATA = 2;
    public static final short AMQP_SEQUENCE = 3;
    public static final short AMQP_VALUE_NULL = 4;
    public static final short AMQP_VALUE_STRING = 5;
    public static final short AMQP_VALUE_BINARY = 6;
    public static final short AMQP_VALUE_MAP = 7;
    public static final short AMQP_VALUE_LIST = 8;

    static {
        byte[] bytes;
        try {
            bytes = getSerializedBytes(null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialise null object body", e);
        }

        NULL_OBJECT_BODY = new Data(new Binary(bytes));
    }

    /**
     * Content type used to mark Data sections as containing a serialized java object.
     */
    public static final String SERIALIZED_JAVA_OBJECT_CONTENT_TYPE = "application/x-java-serialized-object";

    /**
     * Content type used to mark Data sections as containing arbitrary bytes.
     */
    public static final String OCTET_STREAM_CONTENT_TYPE = "application/octet-stream";

    /**
     * Lookup and return the correct Proton Symbol instance based on the given key.
     *
     * @param key
     *        the String value name of the Symbol to locate.
     *
     * @return the Symbol value that matches the given key.
     */
    public static Symbol getSymbol(String key) {
        return Symbol.valueOf(key);
    }

    /**
     * Safe way to access message annotations which will check internal structure and
     * either return the annotation if it exists or null if the annotation or any annotations
     * are present.
     *
     * @param key
     *        the String key to use to lookup an annotation.
     * @param message
     *        the AMQP message object that is being examined.
     *
     * @return the given annotation value or null if not present in the message.
     */
    public static Object getMessageAnnotation(String key, Message message) {
        if (message != null && message.getMessageAnnotations() != null) {
            Map<Symbol, Object> annotations = message.getMessageAnnotations().getValue();
            return annotations.get(AmqpMessageSupport.getSymbol(key));
        }

        return null;
    }

    /**
     * Check whether the content-type field of the properties section (if present) in
     * the given message matches the provided string (where null matches if there is
     * no content type present.
     *
     * @param contentType
     *        content type string to compare against, or null if none
     * @param message
     *        the AMQP message object that is being examined.
     *
     * @return true if content type matches
     */
    public static boolean isContentType(String contentType, Message message) {
        if (contentType == null) {
            return message.getContentType() == null;
        } else {
            return contentType.equals(message.getContentType());
        }
    }

    /**
     * @param contentType the contentType of the received message
     * @return the character set to use, or null if not to treat the message as text
     */
    public static Charset getCharsetForTextualContent(String contentType) {
        try {
            return AmqpContentTypeSupport.parseContentTypeForTextualCharset(contentType);
        } catch (InvalidContentTypeException e) {
            return null;
        }
    }

    private static byte[] getSerializedBytes(Serializable value) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            oos.writeObject(value);
            oos.flush();
            oos.close();

            return baos.toByteArray();
        }
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
    public static Binary getBinaryFromMessageBody(ActiveMQBytesMessage message) throws JMSException {
        Binary result = null;

        if (message.getContent() != null) {
            ByteSequence contents = message.getContent();

            if (message.isCompressed()) {
                int length = (int) message.getBodyLength();
                byte[] uncompressed = new byte[length];
                message.readBytes(uncompressed);

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
    public static Binary getBinaryFromMessageBody(ActiveMQObjectMessage message) throws JMSException {
        Binary result = null;

        if (message.getContent() != null) {
            ByteSequence contents = message.getContent();

            if (message.isCompressed()) {
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
    public static Binary getBinaryFromMessageBody(ActiveMQTextMessage message) throws JMSException {
        Binary result = null;

        if (message.getContent() != null) {
            ByteSequence contents = message.getContent();

            if (message.isCompressed()) {
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
        } else if (message.getText() != null) {
            result = new Binary(message.getText().getBytes(StandardCharsets.UTF_8));
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
    public static Map<String, Object> getMapFromMessageBody(ActiveMQMapMessage message) throws JMSException {
        final HashMap<String, Object> map = new LinkedHashMap<String, Object>();

        final Map<String, Object> contentMap = message.getContentMap();
        if (contentMap != null) {
            for (Entry<String, Object> entry : contentMap.entrySet()) {
                Object value = entry.getValue();
                if (value instanceof byte[]) {
                    value = new Binary((byte[]) value);
                }
                map.put(entry.getKey(), value);
            }
        }

        return map;
    }
}
