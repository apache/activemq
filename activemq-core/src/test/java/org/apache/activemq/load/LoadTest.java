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
package org.apache.activemq.load;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.3 $
 */
public class LoadTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(LoadTest.class);
    
    protected BrokerService broker;
    protected String bindAddress="tcp://localhost:61616";
    
    protected LoadController controller;
    protected LoadClient[] clients;
    protected ConnectionFactory factory;
    protected Destination destination;
    protected int numberOfClients = 10;
    protected int deliveryMode = DeliveryMode.PERSISTENT;
    protected int batchSize = 1000;
    protected int numberOfBatches = 4;
    protected int timeout = Integer.MAX_VALUE;
    protected boolean connectionPerMessage = true;
    protected Connection managementConnection;
    protected Session managementSession;

    /**
     * Sets up a test where the producer and consumer have their own connection.
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker(bindAddress);
        }
        factory = createConnectionFactory(bindAddress);
        managementConnection = factory.createConnection();
        managementSession = managementConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        Destination startDestination = createDestination(managementSession, getClass()+".start");
        Destination endDestination = createDestination(managementSession, getClass()+".end");
        LOG.info("Running with " + numberOfClients + " clients");
        controller = new LoadController(factory);
        controller.setBatchSize(batchSize);
        controller.setNumberOfBatches(numberOfBatches);
        controller.setDeliveryMode(deliveryMode);
        controller.setConnectionPerMessage(connectionPerMessage);
        controller.setStartDestination(startDestination);
        controller.setControlDestination(endDestination);
        controller.setTimeout(timeout);
        clients = new LoadClient[numberOfClients];
        for (int i = 0; i < numberOfClients; i++) {
            Destination inDestination = null;
            if (i==0) {
                inDestination = startDestination;
            }else {
                inDestination = createDestination(managementSession, getClass() + ".client."+(i));
            }
            Destination outDestination = null;
            if (i==(numberOfClients-1)) {
                outDestination = endDestination;
            }else {
                outDestination = createDestination(managementSession, getClass() + ".client."+(i+1));
            }
            LoadClient client = new LoadClient("client("+i+")",factory);
            client.setTimeout(timeout);
            client.setDeliveryMode(deliveryMode);
            client.setConnectionPerMessage(connectionPerMessage);
            client.setStartDestination(inDestination);
            client.setNextDestination(outDestination);
            clients[i] = client;
        }
        
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        managementConnection.close();
        for (int i = 0; i < numberOfClients; i++) {
            clients[i].stop();
        }
        controller.stop();
        if (broker != null) {
            broker.stop();
            broker = null;
        }
    }

    protected Destination createDestination(Session s, String destinationName) throws JMSException {
        return s.createQueue(destinationName);
    }

    /**
     * Factory method to create a new broker
     * 
     * @throws Exception
     */
    protected BrokerService createBroker(String uri) throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer,uri);
        answer.start();
        return answer;
    }

    
    
    protected void configureBroker(BrokerService answer,String uri) throws Exception {
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector(uri);
        answer.setUseShutdownHook(false);
    }

    protected ActiveMQConnectionFactory createConnectionFactory(String uri) throws Exception {
        return new ActiveMQConnectionFactory(uri);
    }

    public void testLoad() throws JMSException, InterruptedException {
        for (int i = 0; i < numberOfClients; i++) {
            clients[i].start();
        }
        controller.start();
        controller.stop();
        
    }

}
