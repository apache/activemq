package org.apache.activemq.conversions;

import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.apache.qpid.amqp_1_0.jms.impl.TopicImpl;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;

import javax.jms.*;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 */
public class AmqpAndMqttTest extends CombinationTestSupport {

    protected BrokerService broker;
    private TransportConnector amqpConnector;
    private TransportConnector mqttConnector;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    @Override
    protected void tearDown() throws Exception {
        if( broker!=null ) {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;
        }
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        amqpConnector = broker.addConnector("amqp://0.0.0.0:0");
        mqttConnector = broker.addConnector("mqtt://0.0.0.0:0");
        return broker;
    }


    public void testFromMqttToAmqp() throws Exception {
        Connection amqp = createAmqpConnection();
        Session session = amqp.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(session.createTopic("topic://FOO"));

        final BlockingConnection mqtt = createMQTTConnection().blockingConnection();
        mqtt.connect();
        byte[] payload = bytes("Hello World");
        mqtt.publish("FOO", payload, QoS.AT_LEAST_ONCE, false);
        mqtt.disconnect();

        Message msg = consumer.receive(1000 * 5);
        assertNotNull(msg);
        assertTrue(msg instanceof BytesMessage);

        BytesMessage bmsg = (BytesMessage) msg;
        byte[] actual = new byte[(int) bmsg.getBodyLength()];
        bmsg.readBytes(actual);
        assertTrue(Arrays.equals(actual, payload));
        amqp.close();
    }

    private byte[] bytes(String value) {
        try {
            return value.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }


    protected MQTT createMQTTConnection() throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setConnectAttemptsMax(1);
        mqtt.setReconnectAttemptsMax(0);
        mqtt.setHost("localhost", mqttConnector.getConnectUri().getPort());
        return mqtt;
    }

    public Connection createAmqpConnection() throws Exception {
        final ConnectionFactoryImpl factory = new ConnectionFactoryImpl("localhost", amqpConnector.getConnectUri().getPort(), "admin", "password");
        final Connection connection = factory.createConnection();
        connection.setExceptionListener(new ExceptionListener() {
            @Override
            public void onException(JMSException exception) {
                exception.printStackTrace();
            }
        });
        connection.start();
        return connection;
    }

}
