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

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class ExpiredMessagesWithNoConsumerTest extends CombinationTestSupport {

    private static final Log LOG = LogFactory.getLog(ExpiredMessagesWithNoConsumerTest.class);

    private static final int expiryPeriod = 1000;
    
	BrokerService broker;
	Connection connection;
	Session session;
	MessageProducer producer;
	public ActiveMQDestination destination = new ActiveMQQueue("test");
	
    public static Test suite() {
        return suite(ExpiredMessagesWithNoConsumerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
	
	protected void setUp() throws Exception {
		broker = new BrokerService();
		broker.setBrokerName("localhost");
		broker.setDataDirectory("data/");
		broker.setUseJmx(true);
		broker.setDeleteAllMessagesOnStartup(true);
		broker.addConnector("tcp://localhost:61616");
			
		PolicyMap policyMap = new PolicyMap();
		PolicyEntry defaultEntry = new PolicyEntry();
		defaultEntry.setExpireMessagesPeriod(expiryPeriod);
		defaultEntry.setMaxExpirePageSize(200);
		// so memory is not consumed by DLQ turn if off
		defaultEntry.setDeadLetterStrategy(null);
		defaultEntry.setMemoryLimit(200*1000);
		policyMap.setDefaultEntry(defaultEntry);
        broker.setDestinationPolicy(policyMap);
		
		broker.start();
		
		broker.waitUntilStarted();
	}
		
	public void testExpiredMessages() throws Exception {
		
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		connection = factory.createConnection();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		producer = session.createProducer(destination);
		producer.setTimeToLive(100);
		connection.start();
		final long sendCount = 2000;		
		
		Thread producingThread = new Thread("Producing Thread") {
            public void run() {
                try {
                	int i = 0;
                	long tStamp = System.currentTimeMillis();
                	while (i++ < sendCount) {
                		producer.send(session.createTextMessage("test"));
                		if (i%100 == 0) {
                		    LOG.info("sent: " + i + " @ " + ((System.currentTimeMillis() - tStamp) / 100)  + "m/ms");
                		    tStamp = System.currentTimeMillis() ;
                		}
                	}
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
		};
		
		producingThread.start();
		
		final long expiry = System.currentTimeMillis() + 20*1000;
		while (producingThread.isAlive() && expiry > System.currentTimeMillis()) {
		    producingThread.join(1000);
		}
        
		assertTrue("producer completed within time ", !producingThread.isAlive());
		
		Thread.sleep(2*expiryPeriod);
        DestinationViewMBean view = createView(destination);
        assertEquals("All sent have expired ", sendCount, view.getExpiredCount());
	}
	
	protected DestinationViewMBean createView(ActiveMQDestination destination) throws Exception {
		 MBeanServer mbeanServer = broker.getManagementContext().getMBeanServer();
		 String domain = "org.apache.activemq";
		 ObjectName name;
		if (destination.isQueue()) {
			name = new ObjectName(domain + ":BrokerName=localhost,Type=Queue,Destination=test");
		} else {
			name = new ObjectName(domain + ":BrokerName=localhost,Type=Topic,Destination=test");
		}
		return (DestinationViewMBean)MBeanServerInvocationHandler.newProxyInstance(mbeanServer, name, DestinationViewMBean.class, true);
	}

	protected void tearDown() throws Exception {
		connection.stop();
		broker.stop();
		broker.waitUntilStopped();
	}

	

	
}
