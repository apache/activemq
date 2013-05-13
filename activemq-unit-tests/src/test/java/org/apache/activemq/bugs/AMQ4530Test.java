package org.apache.activemq.bugs;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularDataSupport;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.CompositeDataConstants;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ4530Test {

    private static BrokerService brokerService;
    private static String TEST_QUEUE = "testQueue";
    private static ActiveMQQueue queue = new ActiveMQQueue(TEST_QUEUE);
    private static String BROKER_ADDRESS = "tcp://localhost:0";
    private static String KEY = "testproperty";
    private static String VALUE = "propvalue";

    private ActiveMQConnectionFactory connectionFactory;
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(true);
        connectionUri = brokerService.addConnector(BROKER_ADDRESS).getPublishableConnectString();
        brokerService.start();
        brokerService.waitUntilStarted();

        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
        sendMessage();
    }

    public void sendMessage() throws Exception {
        final Connection conn = connectionFactory.createConnection();
        try {
            conn.start();
            final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination queue = session.createQueue(TEST_QUEUE);
            final Message toSend = session.createMessage();
            toSend.setStringProperty(KEY, VALUE);
            final MessageProducer producer = session.createProducer(queue);
            producer.send(queue, toSend);
        } finally {
            conn.close();
        }
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testStringPropertiesFromCompositeData() throws Exception {
        final QueueViewMBean queueView = getProxyToQueueViewMBean();
        final CompositeData message = queueView.browse()[0];
        assertNotNull(message);
        TabularDataSupport stringProperties = (TabularDataSupport) message.get(CompositeDataConstants.STRING_PROPERTIES);
        assertNotNull(stringProperties);
        assertThat(stringProperties.size(), is(greaterThan(0)));
        Map.Entry<Object, Object> compositeDataEntry = (Map.Entry<Object, Object>) stringProperties.entrySet().toArray()[0];
        CompositeData stringEntry = (CompositeData) compositeDataEntry.getValue();
        assertThat(String.valueOf(stringEntry.get("key")), equalTo(KEY));
        assertThat(String.valueOf(stringEntry.get("value")), equalTo(VALUE));
    }

    private QueueViewMBean getProxyToQueueViewMBean() throws MalformedObjectNameException, NullPointerException,
            JMSException {
        final ObjectName queueViewMBeanName = new ObjectName("org.apache.activemq:type=Broker,brokerName=localhost,destinationType=Queue,destinationName=" + queue.getQueueName());
        final QueueViewMBean proxy = (QueueViewMBean) brokerService.getManagementContext().newProxyInstance(
                queueViewMBeanName, QueueViewMBean.class, true);
        return proxy;
    }

}
