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
package org.apache.activemq.broker.util.opentelemetry;

import java.io.IOException;
import java.util.Collections;

import io.opentelemetry.context.propagation.TextMapGetter;
import org.apache.activemq.command.Message;

public class ActiveMQMessageTextMapGetter implements TextMapGetter<Message> {

    public static final ActiveMQMessageTextMapGetter INSTANCE = new ActiveMQMessageTextMapGetter();

    @Override
    public Iterable<String> keys(Message message) {
        try {
            if (message.getProperties() != null) {
                return message.getProperties().keySet();
            }
        } catch (IOException e) {
            // ignore
        }
        return Collections.emptyList();
    }

    @Override
    public String get(Message message, String key) {
        try {
            Object value = message.getProperty(key);
            if (value instanceof String) {
                return (String) value;
            }
        } catch (IOException e) {
            // ignore
        }
        return null;
    }
}
