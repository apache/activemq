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
}
