package org.apache.activemq.broker;

import junit.framework.Test;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements the test case attached to:
 * https://issues.apache.org/jira/browse/AMQ-4351
 *
 * This version avoids the spring deps.
 */
public class AMQ4351Test extends BrokerTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ4351Test.class);

    public static Test suite() {
        return suite(AMQ4351Test.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();

        // Lets clean up often.
        broker.setOfflineDurableSubscriberTaskSchedule(500);
        broker.setOfflineDurableSubscriberTimeout(2000); // lets delete durable subs much faster.

        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        jdbc.setDataSource(dataSource);

        jdbc.deleteAllMessages();
        broker.setPersistenceAdapter(jdbc);
        return broker;
    }

    ActiveMQConnectionFactory connectionFactory;
    ActiveMQTopic destination = new ActiveMQTopic("TEST");

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        connectionFactory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
    }

    class ProducingClient implements Runnable {
        final AtomicLong size = new AtomicLong();
        final AtomicBoolean done = new AtomicBoolean();
        CountDownLatch doneLatch = new CountDownLatch(1);

        Connection connection;
        Session session;
        MessageProducer producer;

        ProducingClient() throws JMSException {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            producer = session.createProducer(destination);
        }

        private void sendMessage() {
            try {
                producer.send(session.createTextMessage("Test"));
                long i = size.incrementAndGet();
                if( (i % 1000) == 0 ) {
                    LOG.info("produced " + i + ".");
                }
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        public void start() {
            new Thread(this, "ProducingClient").start();
        }

        public void stop() throws InterruptedException {
            done.set(true);
            if( !doneLatch.await(20, TimeUnit.MILLISECONDS) ) {
                try {
                    connection.close();
                    doneLatch.await();
                } catch (JMSException e) {
                }
            }
        }

        @Override
        public void run() {
            try {
                try {
                    while (!done.get()) {
                        sendMessage();
                        Thread.sleep(10);
                    }
                } finally {
                    connection.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                done.set(true);
            } finally {
                doneLatch.countDown();
            }
        }
    }

    class ConsumingClient implements Runnable {
        final String name;
        final AtomicLong size = new AtomicLong();
        final AtomicBoolean done = new AtomicBoolean();
        CountDownLatch doneLatch = new CountDownLatch(1);

        public ConsumingClient(String name) {
            this.name = name;
        }

        public void start() {
            LOG.info("Starting JMS listener " + name);
            new Thread(this, "ConsumingClient: "+name).start();
        }

        public void stopAsync() {
            done.set(true);
        }

        public void stop() throws InterruptedException {
            stopAsync();
            doneLatch.await();
        }

        @Override
        public void run() {
            try {
                Connection connection = connectionFactory.createConnection();
                connection.setClientID(name);
                connection.start();
                try {
                    Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
                    MessageConsumer consumer = session.createDurableSubscriber(destination, name, null, false);
                    while( !done.get() ) {
                        Message msg = consumer.receive(100);
                        if(msg!=null ) {
                            size.incrementAndGet();
                            session.commit();
                        }
                    }
                } finally {
                    connection.close();
                    LOG.info("Stopped JMS listener " + name);
                }
            } catch (Exception e) {
                e.printStackTrace();
                done.set(true);
            } finally {
                doneLatch.countDown();
            }
        }

    }

    public void testAMQ4351() throws InterruptedException, JMSException {
        LOG.info("Start test.");

        ProducingClient producer = new ProducingClient();
        ConsumingClient listener1 = new ConsumingClient("subscriber-1");
        ConsumingClient listener2 = new ConsumingClient("subscriber-2");
        ConsumingClient listener3 = new ConsumingClient("subscriber-3");
        try {

            listener1.start();
            listener2.start();
            listener3.start();
            int subs = 100;

            List<ConsumingClient> subscribers = new ArrayList<ConsumingClient>(subs);
            for (int i = 4; i < subs; i++) {
                ConsumingClient client = new ConsumingClient("subscriber-" + i);
                subscribers.add(client);
                client.start();
            }

            LOG.info("All subscribers started.");
            producer.sendMessage();

            LOG.info("Stopping 97 subscribers....");
            for (ConsumingClient client : subscribers) {
                client.stopAsync();
            }

            // Start producing messages for 10 minutes, at high rate
            LOG.info("Starting mass message producer...");
            producer.start();


            long lastSize = listener1.size.get();
            for( int i=0 ; i < 10; i++ ) {
                Thread.sleep(1000);
                long size = listener1.size.get();
                LOG.info("Listener 1: consumed: "+(size - lastSize));
                assertTrue( size > lastSize );
                lastSize = size;
            }
        } finally {
            LOG.info("Stopping clients");
            listener1.stop();
            listener2.stop();
            listener3.stop();
            producer.stop();
        }
    }

}
