package org.apache.activemq.store.jdbc;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.derby.jdbc.EmbeddedDataSource;


public class JDBCTestMemory extends TestCase {

    ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
    Connection conn;
    Session sess;
    Destination dest;
    
    BrokerService broker;
    
    protected void setUp() throws Exception {
        broker = createBroker();
        broker.start();
        broker.waitUntilStarted();
    }

    protected void tearDown() throws Exception {
        broker.stop();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(true);
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        jdbc.setDataSource(dataSource);
        
        jdbc.deleteAllMessages();
        broker.setPersistenceAdapter(jdbc);
        broker.addConnector("tcp://0.0.0.0:61616");
        return broker;
    }
    
    protected BrokerService createRestartedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        broker.setUseJmx(true);
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        jdbc.setDataSource(dataSource);
        broker.setPersistenceAdapter(jdbc);
        broker.addConnector("tcp://0.0.0.0:61616");
        return broker;
    }
    
    public void init() throws Exception {
        conn = factory.createConnection();
        conn.start();
        sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        dest = sess.createQueue("test");        
    }
    
    public void testRecovery() throws Exception {
        init();
        MessageProducer producer = sess.createProducer(dest);
        for (int i = 0; i < 1000; i++) {
            producer.send(sess.createTextMessage("test"));
        }
        producer.close();
        sess.close();
        conn.close();
        
        broker.stop();
        broker.waitUntilStopped();
        broker = createRestartedBroker();
        broker.start();
        broker.waitUntilStarted();
        
        init();
        
        for (int i = 0; i < 10; i++) {
            new Thread("Producer " + i) {

                public void run() {
                    try {
                        MessageProducer producer = sess.createProducer(dest);
                        for (int i = 0; i < 15000; i++) {
                            producer.send(sess.createTextMessage("test"));
                            if (i % 100 == 0) {
                                System.out.println(getName() + " sent message " + i);
                            }
                        }
                        producer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                
            }.start();
            
            new Thread("Consumer " + i) {

                public void run() {
                    try {
                        MessageConsumer consumer = sess.createConsumer(dest);
                        for (int i = 0; i < 15000; i++) {
                            consumer.receive(2000);
                            if (i % 100 == 0) {
                                System.out.println(getName() + " received message " + i);
                            }
                        }
                        consumer.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                
            }.start();
        }
        
        // Check out JConsole
        System.in.read();
        sess.close();
        conn.close();
    }
    
}
