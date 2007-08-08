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
package org.apache.activemq.usecases;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import junit.framework.TestCase;

/**
 * @author Oliver Belikan
 * @version $Revision: 1.1.1.1 $
 */
public class ExceptionListenerTest extends TestCase implements ExceptionListener {
    boolean isException;

    public ExceptionListenerTest(String arg) {
        super(arg);
    }

    public void testOnException() throws Exception {
        /*
         * TODO not sure yet if this is a valid test
         * System.setProperty("activemq.persistenceAdapter",
         * "org.apache.activemq.store.vm.VMPersistenceAdapter"); //
         * configuration of container and all protocolls BrokerContainerImpl
         * container = new BrokerContainerImpl("DefaultBroker");
         * BrokerConnectorImpl connector = new BrokerConnectorImpl(container,
         * "vm://localhost", new DefaultWireFormat()); container.start();
         * ActiveMQConnectionFactory factory = new
         * ActiveMQConnectionFactory("vm://localhost"); factory.start();
         * Connection connection = factory.createConnection();
         * connection.setExceptionListener(this); connection.start(); Session
         * session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         * Destination destination = session.createTopic(getClass().getName());
         * MessageProducer producer = session.createProducer(destination); try {
         * Thread.currentThread().sleep(1000); } catch (Exception e) { }
         * container.stop(); // now lets try send try {
         * producer.send(session.createTextMessage("This will never get
         * anywhere")); } catch (JMSException e) { log.info("Caught: " + e); }
         * try { Thread.currentThread().sleep(1000); } catch (Exception e) { }
         * assertTrue("Should have received an exception", isException);
         */
    }

    public void onException(JMSException e) {
        isException = true;
    }
}
