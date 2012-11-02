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
package org.apache.activemq.jndi;

import java.util.Hashtable;

import javax.naming.Context;

public class ActiveMQWASInitialContextFactoryTest extends JNDITestSupport {

    @SuppressWarnings("unchecked")
    public void testTransformEnvironment() {
        Hashtable<Object, Object> originalEnvironment = new Hashtable<Object, Object>();
        originalEnvironment.put("java.naming.connectionFactoryNames", "ConnectionFactory");
        originalEnvironment.put("java.naming.topic.jms.systemMessageTopic", "jms/systemMessageTopic");
        originalEnvironment.put(Context.PROVIDER_URL, "tcp://localhost:61616;tcp://localhost:61617");
        originalEnvironment.put("non-string", Integer.valueOf(43));
        originalEnvironment.put("java.naming.queue", "jms/systemMessageQueue");

        Hashtable<Object, Object> transformedEnvironment = new ActiveMQWASInitialContextFactory().transformEnvironment(originalEnvironment);
        assertEquals("ConnectionFactory", "ConnectionFactory", transformedEnvironment.get("connectionFactoryNames"));
        assertEquals("topic.jm", "jms/systemMessageTopic", transformedEnvironment.get("topic.jms/systemMessageTopic"));
        assertEquals("java.naming.provider.url", "tcp://localhost:61616,tcp://localhost:61617", transformedEnvironment.get("java.naming.provider.url"));
        assertNull("non-string", transformedEnvironment.get("non-string"));

        assertEquals("queue", "jms/systemMessageQueue", transformedEnvironment.get("java.naming.queue"));
    }

}
