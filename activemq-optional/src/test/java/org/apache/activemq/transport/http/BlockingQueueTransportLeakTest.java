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
package org.apache.activemq.transport.http;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.transport.discovery.simple.SimpleDiscoveryAgent;
import org.apache.activemq.transport.http.BlockingQueueTransport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.logging.Logger;

/**
 * This test demonstrates that HttpTunnelServlet leaks BlockingQueueTransport
 * objects whenever a network bridge gets created and closed over HTTP.
 * <p>
 * <b>NOTE:</b> This test requires a modified version of
 * BlockingQueueTransport; the modification is for the purpose of detecting when
 * the object is removed from memory.
 */
public class BlockingQueueTransportLeakTest {
	private static final long INACTIVITY_TIMEOUT = 5000;
	private static final Logger LOG = Logger
			.getLogger(BlockingQueueTransportLeakTest.class.getName());

	// Change this URL to be an unused port. The inactivity timeout is required
	// per AMQ-3016.
	private static final String REMOTE_BROKER_HTTP_URL = "http://localhost:50000?transport.useInactivityMonitor=true&transport.initialDelayTime=0&transport.readCheckTime="
			+ INACTIVITY_TIMEOUT;
	private BrokerService localBroker = new BrokerService();
	private BrokerService remoteBroker = new BrokerService();

	@Before
	public void init() throws Exception {
		localBroker.setBrokerName("localBroker");
		localBroker.setPersistent(false);
		localBroker.setUseJmx(false);
		localBroker.setSchedulerSupport(false);

		remoteBroker.setBrokerName("remoteBroker");
		remoteBroker.setPersistent(false);
		remoteBroker.setUseJmx(false);
		remoteBroker.addConnector(REMOTE_BROKER_HTTP_URL);
		remoteBroker.setSchedulerSupport(false);
	}

	@After
	public void cleanup() throws Exception {
		try {
			localBroker.stop();
		} finally {
			remoteBroker.stop();
		}
	}

	/**
	 * This test involves a local broker which establishes a network bridge to a
	 * remote broker using the HTTP protocol. The local broker stops and the
	 * remote broker cleans up the bridge connection.
	 * <p>
	 * This test demonstrates how the BlockingQueueTransport, which is created
	 * by HttpTunnelServlet for each bridge, is held in memory indefinitely.
	 */
	@Test
	public void httpTest() throws Exception {
		final long BRIDGE_TIMEOUT = 10000;
		final long GC_TIMEOUT = 30000;

		// Add a network connector to the local broker that will create a bridge
		// to the remote broker.
		DiscoveryNetworkConnector dnc = new DiscoveryNetworkConnector();
		SimpleDiscoveryAgent da = new SimpleDiscoveryAgent();
		da.setServices(REMOTE_BROKER_HTTP_URL);
		dnc.setDiscoveryAgent(da);
		localBroker.addNetworkConnector(dnc);

		// Add an interceptor to the remote broker that signals when the bridge
		// connection has been added and removed.
		BrokerPlugin[] plugins = new BrokerPlugin[1];
		plugins[0] = new BrokerPlugin() {
			public Broker installPlugin(Broker broker) throws Exception {
				return new BrokerFilter(broker) {
					@Override
					public void addConnection(ConnectionContext context,
							ConnectionInfo info) throws Exception {
						super.addConnection(context, info);
						synchronized (remoteBroker) {
							remoteBroker.notifyAll();
						}
					}

					@Override
					public void removeConnection(ConnectionContext context,
							ConnectionInfo info, Throwable error)
							throws Exception {
						super.removeConnection(context, info, error);
						synchronized (remoteBroker) {
							remoteBroker.notifyAll();
						}
					}
				};
			}
		};
		remoteBroker.setPlugins(plugins);

		// Start the remote broker so that it available for the local broker to
		// connect to.
		remoteBroker.start();

		// Start and stop the local broker. Synchronization is used to ensure
		// that the bridge is created before the local broker stops,
		// and that the test waits for the remote broker to remove the bridge.
		synchronized (remoteBroker) {
			localBroker.start();
			remoteBroker.wait(BRIDGE_TIMEOUT);

			// Verify that the remote bridge connection has been created by the
			// remote broker.
			Assert.assertEquals(1,
					remoteBroker.getRegionBroker().getClients().length);

			localBroker.stop();
			remoteBroker.wait(BRIDGE_TIMEOUT);

			// Verify that the remote bridge connection has been closed by the
			// remote broker.
			Assert.assertEquals(0,
					remoteBroker.getRegionBroker().getClients().length);
		}

		// Initialize the countdown latch with the expected number of remote
		// bridge connections that should be garbage collected.
		BlockingQueueTransport.finalizeLatch = new CountDownLatch(1);

		// Run the GC and verify that the remote bridge connections are no
		// longer in memory. Some GC's are slow to respond, so give a second
		// prod if necessary.
		// This assertion fails with finalizeLatch.getCount() returning 1.
		LOG.info("Triggering first GC...");
		System.gc();
		BlockingQueueTransport.finalizeLatch.await(GC_TIMEOUT,
				TimeUnit.MILLISECONDS);

		if (BlockingQueueTransport.finalizeLatch.getCount() != 0) {
			LOG.info("Triggering second GC...");
			System.gc();
			BlockingQueueTransport.finalizeLatch.await(GC_TIMEOUT,
					TimeUnit.MILLISECONDS);
		}
		Assert.assertEquals(0, BlockingQueueTransport.finalizeLatch.getCount());
	}
}