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
package org.apache.activemq.transport.amqp.joram;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.objectweb.jtests.jms.conform.connection.ConnectionTest;
import org.objectweb.jtests.jms.conform.connection.TopicConnectionTest;
import org.objectweb.jtests.jms.conform.message.MessageBodyTest;
import org.objectweb.jtests.jms.conform.message.MessageDefaultTest;
import org.objectweb.jtests.jms.conform.message.MessageTypeTest;
import org.objectweb.jtests.jms.conform.message.headers.MessageHeaderTest;
import org.objectweb.jtests.jms.conform.message.properties.JMSXPropertyTest;
import org.objectweb.jtests.jms.conform.message.properties.MessagePropertyConversionTest;
import org.objectweb.jtests.jms.conform.message.properties.MessagePropertyTest;
import org.objectweb.jtests.jms.conform.queue.QueueBrowserTest;
import org.objectweb.jtests.jms.conform.queue.TemporaryQueueTest;
import org.objectweb.jtests.jms.conform.selector.SelectorSyntaxTest;
import org.objectweb.jtests.jms.conform.selector.SelectorTest;
import org.objectweb.jtests.jms.conform.session.QueueSessionTest;
import org.objectweb.jtests.jms.conform.session.SessionTest;
import org.objectweb.jtests.jms.conform.session.TopicSessionTest;
import org.objectweb.jtests.jms.conform.topic.TemporaryTopicTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    TopicSessionTest.class,
    MessageHeaderTest.class,
    QueueBrowserTest.class,
    MessageTypeTest.class,
    TemporaryTopicTest.class,
    TopicConnectionTest.class,
    SelectorSyntaxTest.class,
    QueueSessionTest.class,
    SelectorTest.class,
    TemporaryQueueTest.class,
    ConnectionTest.class,
    SessionTest.class,
    JMSXPropertyTest.class,
    MessageBodyTest.class,
    MessageDefaultTest.class,
    MessagePropertyConversionTest.class,
    MessagePropertyTest.class
})

public class JoramJmsTest {

    @Rule
    public Timeout timeout = new Timeout(10 * 1000);

    @BeforeClass
    public static void beforeClass() throws Exception {
        System.setProperty("joram.jms.test.file", getJmsTestFileName());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        System.clearProperty("joram.jms.test.file");
    }

    public static String getJmsTestFileName() {
        return "provider.properties";
    }
}
