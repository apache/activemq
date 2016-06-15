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
package org.apache.activemq.transport.auto;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.util.IntrospectionSupport;

/**
 *
 *
 */
public class AutoTransportUtils {

    //wireformats
    public static String ALL = "all";
    public static String OPENWIRE = "default";
    public static String STOMP = "stomp";
    public static String AMQP = "amqp";
    public static String MQTT = "mqtt";

    //transports
    public static String AUTO = "auto";

    public static Map<String, Map<String, Object>> extractWireFormatOptions(Map<String, String> options ) {
        Map<String, Map<String, Object>> wireFormatOptions = new HashMap<>();
        if (options != null) {
            wireFormatOptions.put(OPENWIRE, IntrospectionSupport.extractProperties(options, "wireFormat.default."));
            wireFormatOptions.put(STOMP, IntrospectionSupport.extractProperties(options, "wireFormat.stomp."));
            wireFormatOptions.put(AMQP, IntrospectionSupport.extractProperties(options, "wireFormat.amqp."));
            wireFormatOptions.put(MQTT, IntrospectionSupport.extractProperties(options, "wireFormat.mqtt."));
            wireFormatOptions.put(ALL, IntrospectionSupport.extractProperties(options, "wireFormat."));
        }
        return wireFormatOptions;
    }

    public static Set<String> parseProtocols(String protocolString) {
        Set<String> protocolSet = new HashSet<>();;
        if (protocolString != null && !protocolString.isEmpty()) {
            protocolSet.addAll(Arrays.asList(protocolString.split(",")));
        }
        return protocolSet;
    }
}
