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
package org.apache.activemq.joramtests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

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
import org.objectweb.jtests.jms.conform.session.UnifiedSessionTest;
import org.objectweb.jtests.jms.conform.topic.TemporaryTopicTest;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class JoramJmsTest extends TestCase {

    static {
        System.setProperty("org.apache.activemq.SERIALIZABLE_PACKAGES", "java.util,java.lang");
    }

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(SelectorTest.class);
        suite.addTestSuite(ConnectionTest.class);
        suite.addTestSuite(TopicConnectionTest.class);
        suite.addTestSuite(MessageHeaderTest.class);
        suite.addTestSuite(MessageBodyTest.class);
        suite.addTestSuite(MessageDefaultTest.class);
        suite.addTestSuite(MessageTypeTest.class);
        suite.addTestSuite(JMSXPropertyTest.class);
        suite.addTestSuite(MessagePropertyConversionTest.class);
        suite.addTestSuite(TemporaryQueueTest.class);
        suite.addTestSuite(SelectorSyntaxTest.class);
        suite.addTestSuite(QueueSessionTest.class);
        suite.addTestSuite(SessionTest.class);
        suite.addTestSuite(TopicSessionTest.class);
        suite.addTestSuite(TemporaryTopicTest.class);
        suite.addTestSuite(UnifiedSessionTest.class);
        suite.addTestSuite(QueueBrowserTest.class);
        suite.addTestSuite(MessagePropertyTest.class);
        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
