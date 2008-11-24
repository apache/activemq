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
import java.util.Arrays;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

public class ReplicationTest extends TestCase {

	
	private static final String BROKER1_URI = "tcp://localhost:61001";
	private static final String BROKER2_URI = "tcp://localhost:61002";

	private static final String BROKER1_REPLICATION_ID = "kdbr://localhost:60001";
	private static final String BROKER2_REPLICATION_ID = "kdbr://localhost:60002";
	
	private Destination destination = new ActiveMQQueue("TEST_QUEUE");

	public void testReplication() throws Exception {
		
		// This cluster object will control who becomes the master.
		StaticClusterStateManager cluster = new StaticClusterStateManager();
		
		ReplicationService rs1 = new ReplicationService();
		rs1.setMinimumReplicas(0);
		rs1.setUri(BROKER1_REPLICATION_ID);
		rs1.setCluster(cluster);
		rs1.setDirectory(new File("target/replication-test/broker1"));
		rs1.setBrokerURI("broker://("+BROKER1_URI+")/broker1");
		rs1.start();

        ReplicationService rs2 = new ReplicationService();
        rs2.setMinimumReplicas(0);
        rs2.setUri(BROKER2_REPLICATION_ID);
        rs2.setCluster(cluster);
        rs2.setDirectory(new File("target/replication-test/broker2"));
        rs2.setBrokerURI("broker://(" + BROKER2_URI + ")/broker2");
        rs2.start();
		
//		// None of the brokers should be accepting connections since they are not masters.
//		try {
//			sendMesagesTo(1, BROKER1_URI);
//			fail("Connection failure expected.");
//		} catch( JMSException e ) {
//		}
		
		// Make b1 the master.
		ClusterState clusterState = new ClusterState();
		clusterState.setMaster(BROKER1_REPLICATION_ID);
		cluster.setClusterState(clusterState);
		
		try {
			sendMesagesTo(BROKER1_URI, 100, "Pass 1: ");
		} catch( JMSException e ) {
			fail("b1 did not become a master.");
		}
		
		// Make broker 2 a salve.
		clusterState = new ClusterState();
		clusterState.setMaster(BROKER1_REPLICATION_ID);
		String[] slaves = {BROKER2_REPLICATION_ID};
		clusterState.setSlaves(Arrays.asList(slaves));
		cluster.setClusterState(clusterState);
		
		Thread.sleep(1000);
		
		
		try {
			sendMesagesTo(BROKER1_URI, 100, "Pass 2: ");
		} catch( JMSException e ) {
			fail("Failed to send more messages...");
		}
		
		Thread.sleep(2000);
		
		// Make broker 2 the master.
		clusterState = new ClusterState();
		clusterState.setMaster(BROKER2_REPLICATION_ID);
		cluster.setClusterState(clusterState);

		Thread.sleep(1000);
		
		assertReceived(200, BROKER2_URI);
		
		rs2.stop();		
		rs1.stop();
		
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
