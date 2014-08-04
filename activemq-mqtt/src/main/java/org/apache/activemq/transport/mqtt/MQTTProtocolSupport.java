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
        return destinationName.replace('.', '/');
    }
}
