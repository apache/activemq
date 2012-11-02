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

    public static Test suite() {
        TestSuite suite = new TestSuite();

        // Passing tests
        suite.addTestSuite(SelectorSyntaxTest.class);
        suite.addTestSuite(QueueSessionTest.class);
        suite.addTestSuite(SelectorTest.class);
        suite.addTestSuite(TemporaryQueueTest.class);
        suite.addTestSuite(ConnectionTest.class);
        suite.addTestSuite(SessionTest.class);
        suite.addTestSuite(JMSXPropertyTest.class);
        suite.addTestSuite(MessageBodyTest.class);
        suite.addTestSuite(MessageDefaultTest.class);
        suite.addTestSuite(MessagePropertyConversionTest.class);
        suite.addTestSuite(MessagePropertyTest.class);

        if (false ) {

// TODO: Fails due to https://issues.apache.org/jira/browse/PROTON-110 and DestinationImpl vs QueueImpl mapping issues
        suite.addTestSuite(MessageHeaderTest.class);
// TODO: Fails due to JMS client setup browser before getEnumeration() gets called.
        suite.addTestSuite(QueueBrowserTest.class);
// TODO: Should work with qpid 0.19-SNAPSHOT when patch for https://issues.apache.org/jira/browse/QPID-4409
        suite.addTestSuite(UnifiedSessionTest.class);
// TODO: Fails due to inconsistent ObjectMessage mapping in the JMS client.
        suite.addTestSuite(MessageTypeTest.class);
//TODO: Should work with qpid 0.19-SNAPSHOT
        suite.addTestSuite(TemporaryTopicTest.class);
// TODO: Should work with qpid 0.19-SNAPSHOT when patch for https://issues.apache.org/jira/browse/QPID-4408 is applied
        suite.addTestSuite(TopicConnectionTest.class);
        suite.addTestSuite(TopicSessionTest.class);


        }
        return suite;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
