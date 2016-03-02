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
package org.apache.activemq.transport.mqtt;

import java.io.UnsupportedEncodingException;

import org.fusesource.mqtt.codec.CONNECT;
import org.fusesource.mqtt.codec.DISCONNECT;
import org.fusesource.mqtt.codec.PINGREQ;
import org.fusesource.mqtt.codec.PUBACK;
import org.fusesource.mqtt.codec.PUBCOMP;
import org.fusesource.mqtt.codec.PUBLISH;
import org.fusesource.mqtt.codec.PUBREC;
import org.fusesource.mqtt.codec.PUBREL;
import org.fusesource.mqtt.codec.SUBSCRIBE;
import org.fusesource.mqtt.codec.UNSUBSCRIBE;

/**
 * A set of static methods useful for handling MQTT based client connections.
 */
public class MQTTProtocolSupport {

    private static final int TOPIC_NAME_MIN_LENGTH = 1;
    private static final int TOPIC_NAME_MAX_LENGTH = 65535;

    private static final String MULTI_LEVEL_WILDCARD = "#";
    private static final String SINGLE_LEVEL_WILDCARD = "+";

    private static final char MULTI_LEVEL_WILDCARD_CHAR = '#';
    private static final char SINGLE_LEVEL_WILDCARD_CHAR = '+';
    private static final char TOPIC_LEVEL_SEPERATOR_CHAR = '/';

    /**
     * Converts an MQTT formatted Topic name into a suitable ActiveMQ Destination
     * name string.
     *
     * @param name
     *        the MQTT formatted topic name.
     *
     * @return an destination name that fits the ActiveMQ conventions.
     */
    public static String convertMQTTToActiveMQ(String name) {
        char[] chars = name.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            switch(chars[i]) {
                case '#':
                    chars[i] = '>';
                    break;
                case '>':
                    chars[i] = '#';
                    break;
                case '+':
                    chars[i] = '*';
                    break;
                case '*':
                    chars[i] = '+';
                    break;
                case '/':
                    chars[i] = '.';
                    break;
                case '.':
                    chars[i] = '/';
                    break;
            }
        }
        String rc = new String(chars);
        return rc;
    }

    /**
     * Converts an ActiveMQ destination name into a correctly formatted
     * MQTT destination name.
     *
     * @param destinationName
     *        the ActiveMQ destination name to process.
     *
     * @return a destination name formatted for MQTT.
     */
    public static String convertActiveMQToMQTT(String destinationName) {
        char[] chars = destinationName.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            switch(chars[i]) {
                case '>':
                    chars[i] = '#';
                    break;
                case '#':
                    chars[i] = '>';
                    break;
                case '*':
                    chars[i] = '+';
                    break;
                case '+':
                    chars[i] = '*';
                    break;
                case '.':
                    chars[i] = '/';
                    break;
                case '/':
                    chars[i] = '.';
                    break;
            }
        }
        String rc = new String(chars);
        return rc;
    }

    /**
     * Given an MQTT header byte, determine the command type that the header
     * represents.
     *
     * @param header
     *        the byte value for the MQTT frame header.
     *
     * @return a string value for the given command type.
     */
    public static String commandType(byte header) {
        byte messageType = (byte) ((header & 0xF0) >>> 4);
        switch (messageType) {
            case PINGREQ.TYPE:
                return "PINGREQ";
            case CONNECT.TYPE:
                return "CONNECT";
            case DISCONNECT.TYPE:
                return "DISCONNECT";
            case SUBSCRIBE.TYPE:
                return "SUBSCRIBE";
            case UNSUBSCRIBE.TYPE:
                return "UNSUBSCRIBE";
            case PUBLISH.TYPE:
                return "PUBLISH";
            case PUBACK.TYPE:
                return "PUBACK";
            case PUBREC.TYPE:
                return "PUBREC";
            case PUBREL.TYPE:
                return "PUBREL";
            case PUBCOMP.TYPE:
                return "PUBCOMP";
            default:
                return "UNKNOWN";
        }
    }

    /**
     * Validate that the Topic names given by client commands are valid
     * based on the MQTT protocol specification.
     *
     * @param topicName
     *      the given Topic name provided by the client.
     *
     * @throws MQTTProtocolException if the value given is invalid.
     */
    public static void validate(String topicName) throws MQTTProtocolException {
        int topicLen = 0;
        try {
            topicLen = topicName.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            throw new MQTTProtocolException("Topic name contained invalid UTF-8 encoding.");
        }

        // Spec: Unless stated otherwise all UTF-8 encoded strings can have any length in
        //       the range 0 to 65535 bytes.
        if (topicLen < TOPIC_NAME_MIN_LENGTH || topicLen > TOPIC_NAME_MAX_LENGTH) {
            throw new MQTTProtocolException("Topic name given had invliad length.");
        }

        // 4.7.1.2 and 4.7.1.3 these can stand alone
        if (MULTI_LEVEL_WILDCARD.equals(topicName) || SINGLE_LEVEL_WILDCARD.equals(topicName)) {
            return;
        }

        // Spec: 4.7.1.2
        //  The multi-level wildcard character MUST be specified either on its own or following a
        //  topic level separator. In either case it MUST be the last character specified in the
        //  Topic Filter [MQTT-4.7.1-2].
        int numWildCards = 0;
        for (int i = 0; i < topicName.length(); ++i) {
            if (topicName.charAt(i) == MULTI_LEVEL_WILDCARD_CHAR) {
                numWildCards++;

                // If prev exists it must be a separator
                if (i > 0 && topicName.charAt(i - 1) != TOPIC_LEVEL_SEPERATOR_CHAR) {
                    throw new MQTTProtocolException("The multi level wildcard must stand alone: " + topicName);
                }
            }

            if (numWildCards > 1) {
                throw new MQTTProtocolException("Topic Filter can only have one multi-level filter: " + topicName);
            }
        }

        if (topicName.contains(MULTI_LEVEL_WILDCARD) && !topicName.endsWith(MULTI_LEVEL_WILDCARD)) {
            throw new MQTTProtocolException("The multi-level filter must be at the end of the Topic name: " + topicName);
        }

        // Spec: 4.7.1.3
        // The single-level wildcard can be used at any level in the Topic Filter, including
        // first and last levels. Where it is used it MUST occupy an entire level of the filter
        //
        // [MQTT-4.7.1-3]. It can be used at more than one level in the Topic Filter and can be
        // used in conjunction with the multilevel wildcard.
        for (int i = 0; i < topicName.length(); ++i) {
            if (topicName.charAt(i) != SINGLE_LEVEL_WILDCARD_CHAR) {
                continue;
            }

            // If prev exists it must be a separator
            if (i > 0 && topicName.charAt(i - 1) != TOPIC_LEVEL_SEPERATOR_CHAR) {
                throw new MQTTProtocolException("The single level wildcard must stand alone: " + topicName);
            }

            // If next exists it must be a separator
            if (i < topicName.length() - 1 && topicName.charAt(i + 1) != TOPIC_LEVEL_SEPERATOR_CHAR) {
                throw new MQTTProtocolException("The single level wildcard must stand alone: " + topicName);
            }
        }
    }
}
