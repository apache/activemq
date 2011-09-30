package org.apache.activemq.bugs;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.network.DiscoveryNetworkConnector;
import org.apache.activemq.thread.Task;
import org.apache.activemq.thread.TaskRunner;
import org.apache.activemq.thread.TaskRunnerFactory;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.discovery.simple.SimpleDiscoveryAgent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This test involves the creation of a local and remote broker, both of which
 * communicate over VM and TCP. The local broker establishes a bridge to the
 * remote broker for the purposes of verifying that broker info is only
 * transfered once the local broker's ID is known to the bridge support.
 */
public class AMQ3014Test {
	// Change this URL to be an unused port.
	private static final String REMOTE_BROKER_URL = "tcp://localhost:50000";

	private List<BrokerInfo> remoteBrokerInfos = Collections
			.synchronizedList(new ArrayList<BrokerInfo>());

	private BrokerService localBroker = new BrokerService();

	// Override the "remote" broker so that it records all (remote) BrokerInfos
	// that it receives.
	private BrokerService remoteBroker = new BrokerService() {
		@Override
		protected TransportConnector createTransportConnector(URI brokerURI)
				throws Exception {
			TransportServer transport = TransportFactory.bind(this, brokerURI);
			return new TransportConnector(transport) {
				@Override
				protected Connection createConnection(Transport transport)
						throws IOException {
					Connection connection = super.createConnection(transport);
					final TransportListener proxiedListener = transport
							.getTransportListener();
					transport.setTransportListener(new TransportListener() {

						@Override
						public void onCommand(Object command) {
							if (command instanceof BrokerInfo) {
								remoteBrokerInfos.add((BrokerInfo) command);
							}
							proxiedListener.onCommand(command);
						}

						@Override
						public void onException(IOException error) {
							proxiedListener.onException(error);
						}

						@Override
						public void transportInterupted() {
							proxiedListener.transportInterupted();
						}

						@Override
						public void transportResumed() {
							proxiedListener.transportResumed();
						}
					});
					return connection;
				}

			};
		}
	};

	@Before
	public void init() throws Exception {
		localBroker.setBrokerName("localBroker");
		localBroker.setPersistent(false);
		localBroker.setUseJmx(false);
		localBroker.setSchedulerSupport(false);

		remoteBroker.setBrokerName("remoteBroker");
		remoteBroker.setPersistent(false);
		remoteBroker.setUseJmx(false);
		remoteBroker.addConnector(REMOTE_BROKER_URL);
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
	 * This test verifies that the local broker's ID is typically known by the
	 * bridge support before the local broker's BrokerInfo is sent to the remote
	 * broker.
	 */
	@Test
	public void NormalCaseTest() throws Exception {
		runTest(0, 3000);
	}

	/**
	 * This test verifies that timing can arise under which the local broker's
	 * ID is not known by the bridge support before the local broker's
	 * BrokerInfo is sent to the remote broker.
	 */
	@Test
	public void DelayedCaseTest() throws Exception {
		runTest(500, 3000);
	}

	private void runTest(final long taskRunnerDelay, long timeout)
			throws Exception {
		// Add a network connector to the local broker that will create a bridge
		// to the remote broker.
		DiscoveryNetworkConnector dnc = new DiscoveryNetworkConnector();
		SimpleDiscoveryAgent da = new SimpleDiscoveryAgent();
		da.setServices(REMOTE_BROKER_URL);
		dnc.setDiscoveryAgent(da);
		localBroker.addNetworkConnector(dnc);

		// Before starting the local broker, intercept the task runner factory
		// so that the
		// local VMTransport dispatcher is artificially delayed.
		final TaskRunnerFactory realTaskRunnerFactory = localBroker
				.getTaskRunnerFactory();
		localBroker.setTaskRunnerFactory(new TaskRunnerFactory() {
			public TaskRunner createTaskRunner(Task task, String name) {
				final TaskRunner realTaskRunner = realTaskRunnerFactory
						.createTaskRunner(task, name);
				if (name.startsWith("ActiveMQ Connection Dispatcher: ")) {
					return new TaskRunner() {
						@Override
						public void shutdown() throws InterruptedException {
							realTaskRunner.shutdown();
						}

						@Override
						public void shutdown(long timeout)
								throws InterruptedException {
							realTaskRunner.shutdown(timeout);
						}

						@Override
						public void wakeup() throws InterruptedException {
							Thread.sleep(taskRunnerDelay);
							realTaskRunner.wakeup();
						}
					};
				} else {
					return realTaskRunnerFactory.createTaskRunner(task, name);
				}
			}
		});

		// Start the brokers and wait for the bridge to be created; the remote
		// broker is started first to ensure it is available for the local
		// broker to connect to.
		remoteBroker.start();
		localBroker.start();

		// Wait for the remote broker to receive the local broker's BrokerInfo
		// and then verify the local broker's ID is known.
		long startTimeMillis = System.currentTimeMillis();
		while (remoteBrokerInfos.isEmpty()
				&& (System.currentTimeMillis() - startTimeMillis) < timeout) {
			Thread.sleep(100);
		}

		Assert.assertFalse("Timed out waiting for bridge to form.",
				remoteBrokerInfos.isEmpty());
		;
		Assert.assertNotNull("Local broker ID is null.", remoteBrokerInfos.get(
				0).getBrokerId());
	}
}
