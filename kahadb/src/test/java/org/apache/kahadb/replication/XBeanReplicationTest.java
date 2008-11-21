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
package org.apache.kahadb.replication;

import java.io.File;
import java.io.IOException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ServerStats;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.apache.zookeeper.server.persistence.FileTxnLog;

public class XBeanReplicationTest extends TestCase {

	
	private static final String BROKER1_URI = "tcp://localhost:61616";
	private static final String BROKER2_URI = "tcp://localhost:61617";

	private Destination destination = new ActiveMQQueue("TEST_QUEUE");
	
    private static final int PORT = 2181;
    private Factory serverFactory;

	public void testReplication() throws Exception {
		
        startZooKeeper();

		BrokerService broker1 = BrokerFactory.createBroker("xbean:broker1/ha.xml");
        broker1.start();
        
        // Wait for the broker to get setup..
        Thread.sleep(7000);
        
        sendMesagesTo(BROKER1_URI, 100, "Pass 1: ");

        // Create broker 2 which will join in and sync up with the existing master.
        BrokerService broker2 = BrokerFactory.createBroker("xbean:broker2/ha.xml");
        broker2.start();
		
        // Give it some time to sync up..
        Thread.sleep(1000);
        
        // Stopping broker1 should make broker2 the master.
        broker1.stop();

        Thread.sleep(1000);
        
        // Did all the messages get synced up?
		assertReceived(100, BROKER2_URI);
		// Send some more message... 
        sendMesagesTo(BROKER2_URI, 50, "Pass 2: ");
		
		// Start broker1 up again.. it should re-sync with master 2 
		broker1.start();
        // Give it some time to sync up..
        Thread.sleep(1000);
		
		// stopping the master.. 
		broker2.stop();
		
		// Did the new state get synced right?
        assertReceived(50, BROKER1_URI);
		
        broker1.stop();
        
        stopZooKeeper();

	}

    private void stopZooKeeper() {
        serverFactory.shutdown();
        ServerStats.unregister();
    }

    private void startZooKeeper() throws IOException, InterruptedException {
        ServerStats.registerAsConcrete();
        File zooKeeperData = new File("target/test-data/zookeeper-"+System.currentTimeMillis());
        zooKeeperData.mkdirs();

        // Reduces startup time..
        System.setProperty("zookeeper.preAllocSize", "100");
        FileTxnLog.setPreallocSize(100);
        ZooKeeperServer zs = new ZooKeeperServer(zooKeeperData, zooKeeperData, 3000);
        
        serverFactory = new NIOServerCnxn.Factory(PORT);
        serverFactory.startup(zs);
    }

	private void assertReceived(int count, String brokerUri) throws JMSException {
		ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerUri);
		Connection con = cf.createConnection();
		con.start();
		try {
			Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageConsumer consumer = session.createConsumer(destination);
			for (int i = 0; i < count; i++) {
				TextMessage m = (TextMessage) consumer.receive(1000);
				if( m==null ) {
					fail("Failed to receive message: "+i);
				}
				System.out.println("Got: "+m.getText());
			}
		} finally {
			try { con.close(); } catch (Throwable e) {}
		}
	}

	private void sendMesagesTo(String brokerUri, int count, String msg) throws JMSException {
		ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerUri);
		Connection con = cf.createConnection();
		try {
			Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = session.createProducer(destination);
			for (int i = 0; i < count; i++) {
				producer.send(session.createTextMessage(msg+i));
			}
		} finally {
			try { con.close(); } catch (Throwable e) {}
		}
	}
	
}
