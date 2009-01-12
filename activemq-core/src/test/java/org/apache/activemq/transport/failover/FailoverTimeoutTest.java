package org.apache.activemq.transport.failover;

import java.net.URI;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;

public class FailoverTimeoutTest extends TestCase {
	
	private static final String QUEUE_NAME = "test.failovertimeout";

	public void testTimeout() throws Exception {
		
		long timeout = 1000;
		URI tcpUri = new URI("tcp://localhost:61616");
		BrokerService bs = new BrokerService();
		bs.setUseJmx(false);
		bs.addConnector(tcpUri);
		bs.start();
		
		ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("failover:(" + tcpUri + ")?timeout=" + timeout);
		Connection connection = cf.createConnection();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageProducer producer = session.createProducer(session
				.createQueue(QUEUE_NAME));
		TextMessage message = session.createTextMessage("Test message");
		producer.send(message);
		
		bs.stop();
		
		try {
			producer.send(message);
		} catch (JMSException jmse) {
			jmse.printStackTrace();
			assertEquals("Failover timeout of " + timeout + " ms reached.", jmse.getMessage());
		}
		
		bs = new BrokerService();
		
		bs.setUseJmx(false);
		bs.addConnector(tcpUri);
		bs.start();
		
		producer.send(message);
		
		bs.stop();
	}
	
}
