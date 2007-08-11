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
package org.apache.activemq;

import java.io.File;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import junit.framework.TestCase;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Useful base class for unit test cases
 * 
 * @version $Revision: 1.5 $
 */
public class TestSupport extends TestCase {

    protected ActiveMQConnectionFactory connectionFactory;
    protected boolean topic = true;

    public TestSupport() {
        super();
    }

    public TestSupport(String name) {
        super(name);
    }

    protected ActiveMQMessage createMessage() {
        return new ActiveMQMessage();
    }

    protected Destination createDestination(String subject) {
        if (topic) {
            return new ActiveMQTopic(subject);
        } else {
            return new ActiveMQQueue(subject);
        }
    }

    protected Destination createDestination() {
        return createDestination(getDestinationString());
    }

    /**
     * Returns the name of the destination used in this test case
     */
    protected String getDestinationString() {
        return getClass().getName() + "." + getName();
    }

    /**
     * @param messsage
     * @param firstSet
     * @param secondSet
     */
    protected void assertTextMessagesEqual(String messsage, Message[] firstSet, Message[] secondSet)
        throws JMSException {
        assertEquals("Message count does not match: " + messsage, firstSet.length, secondSet.length);
        for (int i = 0; i < secondSet.length; i++) {
            TextMessage m1 = (TextMessage)firstSet[i];
            TextMessage m2 = (TextMessage)secondSet[i];
            assertFalse("Message " + (i + 1) + " did not match : " + messsage + ": expected {" + m1
                        + "}, but was {" + m2 + "}", m1 == null ^ m2 == null);
            assertEquals("Message " + (i + 1) + " did not match: " + messsage + ": expected {" + m1
                         + "}, but was {" + m2 + "}", m1.getText(), m2.getText());
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
    }

    /**
     * Factory method to create a new connection
     */
    protected Connection createConnection() throws Exception {
        return getConnectionFactory().createConnection();
    }

    public ActiveMQConnectionFactory getConnectionFactory() throws Exception {
        if (connectionFactory == null) {
            connectionFactory = createConnectionFactory();
            assertTrue("Should have created a connection factory!", connectionFactory != null);
        }
        return connectionFactory;
    }

    protected String getConsumerSubject() {
        return getSubject();
    }

    protected String getProducerSubject() {
        return getSubject();
    }

    protected String getSubject() {
        return getName();
    }

    public static void recursiveDelete(File f) {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        f.delete();
    }

    public static void removeMessageStore() {
        if (System.getProperty("activemq.store.dir") != null) {
            recursiveDelete(new File(System.getProperty("activemq.store.dir")));
        }
        if (System.getProperty("derby.system.home") != null) {
            recursiveDelete(new File(System.getProperty("derby.system.home")));
        }
    }
}
