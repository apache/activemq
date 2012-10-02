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

package org.apache.activemq.bugs;

import java.io.IOException;

import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class VMTransportClosureTest extends EmbeddedBrokerTestSupport {
	private static final Log LOG = LogFactory
			.getLog(VMTransportClosureTest.class);
	private static final long MAX_TEST_TIME_MILLIS = 300000; // 5min
	private static final int NUM_ATTEMPTS = 100000;

	public void setUp() throws Exception {
		setAutoFail(true);
		setMaxTestTime(MAX_TEST_TIME_MILLIS);
		super.setUp();
	}

	/**
	 * EmbeddedBrokerTestSupport.createBroker() binds the broker to a VM
	 * transport address, which results in a call to
	 * VMTransportFactory.doBind(location):
	 * <p>
	 * <code>
	 *     public TransportServer doBind(URI location) throws IOException {
	 *        return bind(location, false);
	 *}
	 *</code>
	 * </p>
	 * As a result, VMTransportServer.disposeOnDisconnect is <code>false</code>.
	 * To expose the bug, we need to have VMTransportServer.disposeOnDisconnect
	 * <code>true</code>, which is the case when the VMTransportServer is not
	 * already bound when the first connection is made.
	 */
	@Override
	protected BrokerService createBroker() throws Exception {
		BrokerService answer = new BrokerService();
		answer.setPersistent(isPersistent());
		// answer.addConnector(bindAddress);
		return answer;
	}

	/**
	 * This test demonstrates how the "disposeOnDisonnect" feature of
	 * VMTransportServer can incorrectly close all VM connections to the local
	 * broker.
	 */
	public void testPrematureClosure() throws Exception {

		// Open a persistent connection to the local broker. The persistent
		// connection is maintained through the test and should prevent the
		// VMTransportServer from stopping itself when the local transport is
		// closed.
		ActiveMQConnection persistentConn = (ActiveMQConnection) createConnection();
		persistentConn.start();
		Session session = persistentConn.createSession(true,
				Session.SESSION_TRANSACTED);
		MessageProducer producer = session.createProducer(destination);

		for (int i = 0; i < NUM_ATTEMPTS; i++) {
			LOG.info("Attempt: " + i);

			// Open and close a local transport connection. As is done by by
			// most users of the transport, ensure that the transport is stopped
			// when closed by the peer (via ShutdownInfo). Closing the local
			// transport should not affect the persistent connection.
			final Transport localTransport = TransportFactory.connect(broker
					.getVmConnectorURI());
			localTransport.setTransportListener(new TransportListener() {
				public void onCommand(Object command) {
					if (command instanceof ShutdownInfo) {
						try {
							localTransport.stop();
						} catch (Exception ex) {
							throw new RuntimeException(ex);
						}
					}
				}

				public void onException(IOException error) {
					// ignore
				}

				public void transportInterupted() {
					// ignore
				}

				public void transportResumed() {
					// ignore
				}
			});

			localTransport.start();
			localTransport.stop();

			// Ensure that the persistent connection is still usable.
			producer.send(session.createMessage());
			session.rollback();
		}

		persistentConn.close();
	}
}
