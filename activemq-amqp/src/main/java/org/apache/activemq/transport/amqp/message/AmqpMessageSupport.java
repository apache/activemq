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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Map;

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;

/**
 * Support class containing constant values and static methods that are
 * used to map to / from AMQP Message types being sent or received.
 */
public final class AmqpMessageSupport {

    public static final Binary EMPTY_BINARY = new Binary(new byte[0]);
    public static final Data EMPTY_BODY = new Data(EMPTY_BINARY);
    public static final Data NULL_OBJECT_BODY;

    public static final String AMQP_ORIGINAL_ENCODING_KEY = "JMS_AMQP_ORIGINAL_ENCODING";

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
}
