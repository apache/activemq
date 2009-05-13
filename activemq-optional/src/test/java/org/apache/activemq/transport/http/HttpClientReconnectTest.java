package org.apache.activemq.transport.http;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;

public class HttpClientReconnectTest extends TestCase {
	
	BrokerService broker;
	ActiveMQConnectionFactory factory;

	protected void setUp() throws Exception {
		broker = new BrokerService();
		broker.addConnector("http://localhost:61666?trace=true");
		broker.setPersistent(false);
		broker.setUseJmx(false);
		broker.deleteAllMessages();
		broker.start();
		factory = new ActiveMQConnectionFactory("http://localhost:61666?trace=true");
	}

	protected void tearDown() throws Exception {
		broker.stop();
	}
	
	public void testReconnectClient() throws Exception {
		for (int i = 0; i < 100; i++) {
			sendAndReceiveMessage(i);
		}
	}
	
	private void sendAndReceiveMessage(int i) throws Exception {
		Connection conn = factory.createConnection();
		Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		conn.start();
		Destination dest = new ActiveMQQueue("test");
		MessageProducer producer = sess.createProducer(dest);
		MessageConsumer consumer = sess.createConsumer(dest);
		String messageText = "test " + i;
		try {
			producer.send(sess.createTextMessage(messageText));
			TextMessage msg = (TextMessage)consumer.receive(1000);
			assertEquals(messageText, msg.getText());
		} finally {
			producer.close();
			consumer.close();
			conn.close();
			sess.close();
		}
	}
	
	

}
