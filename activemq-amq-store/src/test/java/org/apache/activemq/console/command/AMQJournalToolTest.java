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
package org.apache.activemq.console.command;

import static org.junit.Assert.assertNull;

import java.io.File;
import java.net.URI;
import java.util.HashMap;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.lang.reflect.Field;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.console.command.store.amq.AMQJournalTool;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.amq.AMQPersistenceAdapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * Testcase for AMQ-3665. Tests the AMQJournalTool, which uses Velocity to 
 * render the output.
 * 
 * This test checks that auditing a journal file with 2000 msgs stored
 * does not increase the introspectionCache of the VelocityContext beyond 20
 * entries. This is the case when template caching is turned on in Velocity. 
 * If Velocity template caching is not turned on in @see CustomResourceLoader.java, 
 * then auditing a large journal fills up the Velocity's context internal 
 * introspectionCache HashMap, until the JVM eventually runs out of memory. 
 * See <a href="https://issues.apache.org/jira/browse/AMQ-3665">AMQ-3665</a> 
 * for more details.
 * 
 * This test does the following
 * 1) start embedded broker using AMQ store
 * 2) start producer to send 2000 msgs 
 * 3) start consumer to receive all 2000 msgs
 * 4) run journal-audit on the store
 * 5) verify the VelocityContext introspectionCache size has not grown > 20 
 *    entries, as only 3 template instances need to be cached.
 * 
 * @author tmielke
 */
public class AMQJournalToolTest {
    private static final Log LOG = LogFactory.getLog(AMQJournalToolTest.class);
    
    private static BrokerService broker = null; 
    
    protected static final int NUM_MSGS = 2000;
    protected static final String DATA_DIR = "target/activemq-data/";
    public static final String DESTNAME = "AMQ-3665";
    
    // allow other threads to mark this test as failed. 
    protected static String failMsg = null;
   
    
   @Test
   public void testAMQJournalTool() throws Exception {

	   // 1. start broker
	   this.startBroker();
    	    	
	   // 2. send a few persistent msgs to broker 
	   // create producer and consumer threads, pass in a context object
	   Consumer consumerWorker = new Consumer();
	   Thread consumer = new Thread(consumerWorker);
	   consumer.start();
		
	   // Wait on worker to establish subscriptions so counts aren't off
	   synchronized(consumerWorker.init) {
		   consumerWorker.init.wait();
       }
        
	   LOG.info("Starting producer");
	   Thread producer = new Thread(new Producer());
	   producer.start();
       	
       //wait for threads to finish
	   LOG.info("Waiting for producer and consumer to join.");
	   producer.join();
	   consumer.join(); 
	   LOG.info("Producer and Consumer finished.");
	   
	   // check no other thread indicated a problem
	   assertNull(failMsg);
	   broker.stop();
	   broker.waitUntilStopped();
	   
	   // 3. start AMQJournalReaderTool and read the journal
	   LOG.info("Starting journal audit.");
	   AMQJournalTool consumerTool = new AMQJournalTool();
	   consumerTool.getDirs().add(new File(DATA_DIR + "Broker1/journal/"));
	   consumerTool.execute();	   
	   LOG.info("journal audit finished.");
       
	   // 4. verify cacheSize of VelocityContext internal introspectionCache	   
	   int cacheSize = getVelocityIntrospectionCacheSize(consumerTool);
	   LOG.info("VelocityContext introspectionCacheSize is of size " + cacheSize);
	   
	   Assert.assertTrue("VelocityContext introspectionCache too high: " +
			   cacheSize + 
			   "It may not have template caching turned on in Velocity (AMQ-3665).", 
			   cacheSize < 20);
       consumerTool = null;
   }
   
   
    /** 
     * Creates a broker instance but does not start it.
     * 
     * @param brokerUri - transport uri of broker
     * @param brokerName - name for the broker
     * @return a BrokerService instance with transport uri and broker name set
     * @throws Exception
     */
    protected BrokerService createBroker(URI brokerUri, String brokerName) throws Exception {
        BrokerService broker = BrokerFactory.createBroker(brokerUri);
        broker.setBrokerName(brokerName);
        broker.setDataDirectory(DATA_DIR);
        
        PersistenceAdapter store = new AMQPersistenceAdapter();
        broker.setPersistenceAdapter(store);
        return broker;
    }

    /**
     * Starts all broker instances.
     *     
     * @throws Exception
     */
    protected void startBroker() throws Exception {
    	
    	if (broker != null) {
    		broker.start();
    		broker.waitUntilStarted();
    	}
    	broker.deleteAllMessages();
    	
        LOG.info("Broker started.");
        sleep(1000);
    }
    

    @Before
    public void setUp() throws Exception {
       
        final String options = "?persistent=true&useJmx=false&deleteAllMessagesOnStartup=true";
        broker = createBroker(new URI("broker:(tcp://localhost:61616)" + options), "Broker1");
    }
    
    
    @After
    public void tearDown() throws Exception {
        
        if (broker != null && broker.isStarted()) {
    		broker.stop();
    		LOG.info("Broker stopped.");
        } else 
        	LOG.info("Broker already stopped.");
    }
    

    private void sleep(int milliSecondTime) {
        try {
            Thread.sleep(milliSecondTime);
        } catch (InterruptedException ignored) {
        	LOG.warn(ignored.getMessage());
        }    
    }

    
    /**
     * Returns the size of the VelocityContext internal introspectionCache
     * (HashMap) that is used by the AMQ journal reader. 
     * As the VelocityContext as well as its internal introspectionCache
     * has only private access, we use reflection APIs to get the cache Size.
     * 
     * @param reader - the journal reader instance that was used to read the 
     * 	journal
     * @return - size of the introspectionCache
     */
    public int getVelocityIntrospectionCacheSize(AMQJournalTool reader) {
    	
    	// First get the VelocityContext from the journal reader
    	Object context = null;
    	Field fields[] = reader.getClass().getDeclaredFields();
        for (int i = 0; i < fields.length; ++i) {
          if (LOG.isDebugEnabled())
          	  LOG.debug("Checking field " + fields[i].getName());
            
          if ("context".equals(fields[i].getName())) {
            try {
              fields[i].setAccessible(true);
              context = fields[i].get(reader); 
              break;
            } 
            catch (IllegalAccessException ex) {
              Assert.fail ("IllegalAccessException accessing 'context'");
            }
          }
        }
        
        // Next get the introspectionCache member of VelocityContext
        Object cache = null;
        Class parent = context.getClass().getSuperclass().getSuperclass();
        LOG.debug(parent.toString());
        
        if (!parent.toString().endsWith("org.apache.velocity.context.InternalContextBase")) {
        	Assert.fail("Unable to retrieve introspectionCache via reflection APIs");
        }

        Field fields2[]  = parent.getDeclaredFields();
        for (int i = 0; i < fields2.length; ++i) {
          if (LOG.isDebugEnabled())
        	  LOG.debug("Checking field " + fields2[i].getName());
          if ("introspectionCache".equals(fields2[i].getName())) {
            try {
              fields2[i].setAccessible(true);
              cache = fields2[i].get(context);
              break;
            } 
            catch (IllegalAccessException ex) {
              Assert.fail ("IllegalAccessException accessing 'introspectionCache'");
            }
          }
        }
        return ((HashMap)cache).size();
    }


/**
 * Message producer running as a separate thread.
 *
 */
class Producer implements Runnable{
	
	private Log log = LogFactory.getLog(Producer.class);
	
    public Producer() {
       
    }	
	
    /**
     * Connect to broker and send messages.
     */
    public void run() {
		
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
       
        try {
            ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory("failover:(tcp://localhost:61616)");
            connection = amq.createConnection();
			
            connection.setExceptionListener(new javax.jms.ExceptionListener() { 
                    public void onException(javax.jms.JMSException e) {
                        e.printStackTrace();
                    } 
	        });

            connection.start();
	        
            // Create a Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(AMQJournalToolTest.DESTNAME);
            
            // Create a MessageProducer from the Session to the Topic or Queue
            producer = session.createProducer(destination);
	
            long counter = 0;
	        
            // Create message 
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("Hello world! From: ");
            stringBuilder.append(Thread.currentThread().getName());
            stringBuilder.append(" : ");
            stringBuilder.append(this.hashCode());
            stringBuilder.append(":");
            stringBuilder.append(counter);
            
            String text = stringBuilder.toString();
            TextMessage message = session.createTextMessage(text);
            
            // send messages
            log.info("Sending " + AMQJournalToolTest.NUM_MSGS + " messages.");
            for (int i = 0; i < AMQJournalToolTest.NUM_MSGS; i++) {
                
                log.debug("Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
                producer.send(message);
                counter ++;
                Thread.sleep(10);

                if ((counter % 1000) == 0)
                    log.info("sent " + counter + " messages");
            } 
        } catch (Exception ex) {
            log.error(ex);
            AMQJournalToolTest.failMsg = ex.getMessage();
            return;
        }
        finally {
            try {
                if (producer != null) 
                    producer.close();
                if (session != null)
                    session.close();
                if (connection != null)
                    connection.close();
            } catch (Exception e) {
                log.error("Problem closing down JMS objects: " + e);
                AMQJournalToolTest.failMsg = e.getMessage();
            }
        }
    }
}
 
/**   
 *
 * Message consumer running as a separate thread.
 * 
 *
 */
class Consumer implements Runnable{

    public Object init = new Object();

    private Log log = LogFactory.getLog(Consumer.class);
	
    public Consumer() {
       
    }
		
    /**
     * Connect to broker and receive messages.
     */
    public void run() {
		
        Connection connection = null;
        Session session = null;
        MessageConsumer consumer = null;
        // ?randomize=false
		String url = "failover:(tcp://localhost:61616)";
        try {
            ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(url);
            connection = amq.createConnection();
			
            connection.setExceptionListener(new javax.jms.ExceptionListener() { 
                    public void onException(javax.jms.JMSException e) {
                        e.printStackTrace();
                    } 
	        });
            connection.start();

            // Create a Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	
            // Create the destination (Topic or Queue)
            Destination  destination = session.createQueue(AMQJournalToolTest.DESTNAME);
            
            //Create a MessageConsumer from the Session to the Topic or Queue
            consumer = session.createConsumer(destination);
            
            long counter = 0;
            log.info("Consumer connected to " + url);

            synchronized(init) {
                init.notifyAll();
            }

            // Wait for a message
            for (int i = 0; i < AMQJournalToolTest.NUM_MSGS; i++) {
                Message message2 = consumer.receive();
	    
                if (message2 instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message2;
                    String text = textMessage.getText();
                    log.debug("Received: " + text);
                } else {
                    log.warn("Received message of unsupported type. Expecting TextMessage. " + message2); 	     
                }
                counter ++;
                if ((counter % 1000) == 0)
                    log.info("received " + counter + " messages");
            } 
            log.info("Received all " + AMQJournalToolTest.NUM_MSGS + " messages.");
        } catch(Exception e) {
            log.error("Error in Consumer: " + e);
            AMQJournalToolTest.failMsg = e.getMessage();
            return;
        }
        finally {
            try {
                if (consumer != null)
                    consumer.close();
                if (session != null)
                    session.close();
                if (connection != null)
                    connection.close();
            } catch (Exception ex) {
                log.error("Error closing down JMS objects: " + ex);
                AMQJournalToolTest.failMsg = ex.getMessage();
            }
        }
    }
  }
}
