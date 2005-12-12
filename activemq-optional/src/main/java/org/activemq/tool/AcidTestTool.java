/** 
 * 
 * Copyright 2004 Protique Ltd
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/

package org.activemq.tool;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;

import org.activemq.ActiveMQConnectionFactory;
import org.activemq.command.ActiveMQQueue;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

/**
 * @version $Revision$
 */
public class AcidTestTool extends TestCase {

    private Random random = new Random();
    private byte data[];
    private int workerCount = 10;
    private PrintWriter statWriter;

    // Worker configuration.
    protected int recordSize = 1024;
    protected int batchSize = 5;
    protected int workerThinkTime = 500;
    AtomicBoolean ignoreJMSErrors = new AtomicBoolean(false);

    protected Destination target;
    private ActiveMQConnectionFactory factory;
    private Connection connection;
    
    AtomicInteger publishedBatches = new AtomicInteger(0);
    AtomicInteger consumedBatches = new AtomicInteger(0);
    
    List errors = Collections.synchronizedList(new ArrayList());

    private interface Worker extends Runnable {
        public boolean waitForExit(long i) throws InterruptedException;
    }
    
    private final class ProducerWorker implements Worker {

        Session session;
        private MessageProducer producer;
        private BytesMessage message;
        CountDownLatch doneLatch = new CountDownLatch(1);
        private final String workerId;

        ProducerWorker(Session session, String workerId) throws JMSException {
            this.session = session;
            this.workerId = workerId;
            producer = session.createProducer(target);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            message = session.createBytesMessage();
            message.setStringProperty("workerId", workerId);
            message.writeBytes(data);
        }

        public void run() {
            try {
                for( int batchId=0; true; batchId++ ) {
//				    System.out.println("Sending batch: "+workerId+" "+batchId);
                    for( int msgId=0; msgId < batchSize; msgId++ ) {
    	                // Sleep some random amount of time less than workerThinkTime
    	                try {
    	                    Thread.sleep(random.nextInt(workerThinkTime));
    	                } catch (InterruptedException e1) {
    	                    return;
    	                }			
    	                
					    message.setIntProperty("batch-id",batchId);
					    message.setIntProperty("msg-id",msgId);
	
					    
					    producer.send(message);    		                
                    } 
                    session.commit();
                    publishedBatches.incrementAndGet();	                    
//				    System.out.println("Commited send batch: "+workerId+" "+batchId);
                }
			} catch (JMSException e) {
			    if( !ignoreJMSErrors.get() ) {
				    e.printStackTrace();
				    errors.add(e);
			    }
				return;
			} catch (Throwable e) {
			    e.printStackTrace();
			    errors.add(e);
				return;
            } finally {
                System.out.println("Producer exiting.");
                doneLatch.countDown();
            }
        }

        public boolean waitForExit(long i) throws InterruptedException {
            return doneLatch.await(i, TimeUnit.MILLISECONDS);
        }
    }
    
    private final class ConsumerWorker implements Worker {

        Session session;
        private MessageConsumer consumer;
        private final long timeout;
        CountDownLatch doneLatch = new CountDownLatch(1);
        private final String workerId;
        
        ConsumerWorker(Session session, String workerId, long timeout) throws JMSException {
            this.session = session;
            this.workerId = workerId;
            this.timeout = timeout;
            consumer = session.createConsumer(target,"workerId='"+workerId+"'");
        }

        public void run() {
            
            try {
                int batchId=0;
                while( true ) {
                    for( int msgId=0; msgId < batchSize; msgId++ ) {

                        // Sleep some random amount of time less than workerThinkTime
    	                try {
    	                    Thread.sleep(random.nextInt(workerThinkTime));
    	                } catch (InterruptedException e1) {
    	                    return;
    	                }			
                        
	                    Message message = consumer.receive(timeout);	                    
	                    if( msgId > 0 ) {
	                        assertNotNull(message);	                        
	                        assertEquals(message.getIntProperty("batch-id"), batchId);
	                        assertEquals(message.getIntProperty("msg-id"), msgId);
	                    } else {
	                        if( message==null ) {
	                            System.out.println("At end of batch an don't have a next batch to process.  done.");
		                        return;
	                        }
	                        assertEquals(msgId, message.getIntProperty("msg-id") );
	                        batchId = message.getIntProperty("batch-id");
//	    				    System.out.println("Receiving batch: "+workerId+" "+batchId);
	                    }	     
	                    
                    } 
                    session.commit();
                    consumedBatches.incrementAndGet();
//				    System.out.println("Commited receive batch: "+workerId+" "+batchId);
                }
			} catch (JMSException e) {
			    if( !ignoreJMSErrors.get() ) {
				    e.printStackTrace();
				    errors.add(e);
			    }
				return;
			} catch (Throwable e) {
			    e.printStackTrace();
			    errors.add(e);
				return;
            } finally {
                System.out.println("Consumer exiting.");
                doneLatch.countDown();
            }
        }

        public boolean waitForExit(long i) throws InterruptedException {
            return doneLatch.await(i, TimeUnit.MILLISECONDS);
        }
    }
    
    /**
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        this.target = new ActiveMQQueue(getClass().getName());
    }

    protected void tearDown() throws Exception {
        if( connection!=null ) {
            try { connection.close(); } catch (Throwable ignore) {}
            connection = null;
        }
    }
    
    /**
     * @throws InterruptedException
     * @throws JMSException
     * @throws JMSException
     * 
     */
    private void reconnect() throws InterruptedException, JMSException {
        if( connection!=null ) {
            try { connection.close(); } catch (Throwable ignore) {}
            connection = null;
        }
        
        long reconnectDelay=1000;
        JMSException lastError=null;
        
        while( connection == null) {
            if( reconnectDelay > 1000*10 ) {
                reconnectDelay = 1000*10;
            }
	        try {
	            connection = factory.createConnection();
	            connection.start();
	        } catch (JMSException e) {
                lastError = e;
	            Thread.sleep(reconnectDelay);
	            reconnectDelay*=2;
	        }
        }
    }

    /**
     * @throws Throwable
     * @throws IOException
     * 
     */
    public void testAcidTransactions() throws Throwable {

        System.out.println("Client threads write records using: Record Size: " + recordSize + ", Batch Size: "
                + batchSize + ", Worker Think Time: " + workerThinkTime);

        // Create the record and fill it with some values.
        data = new byte[recordSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        System.out.println("==============================================");
        System.out.println("===> Start the server now.");
        System.out.println("==============================================");
        reconnect();
        
        System.out.println("Starting " + workerCount + " Workers...");
        ArrayList workers = new ArrayList();
        for( int i=0; i< workerCount; i++ ){        
            String workerId = "worker-"+i;
            
            Worker w = new ConsumerWorker(connection.createSession(true,Session.SESSION_TRANSACTED), workerId, 1000*5);
            workers.add(w);
            new Thread(w,"Consumer:"+workerId).start();

            w = new ProducerWorker(connection.createSession(true,Session.SESSION_TRANSACTED), workerId);
            workers.add(w);
            new Thread(w,"Producer:"+workerId).start();
        }        

        System.out.println("Waiting for "+(workerCount*10)+" batches to be delivered.");

        //
        // Wait for about 5 batches of messages per worker to be consumed before restart. 
        // 
        while( publishedBatches.get() <  workerCount*5) {
            System.out.println("Stats: Produced Batches: "+this.publishedBatches.get()+", Consumed Batches: "+this.consumedBatches.get());
            Thread.sleep(1000);
        }
        
        System.out.println("==============================================");
        System.out.println("===> Server is under load now.  Kill it!");
        System.out.println("==============================================");
        ignoreJMSErrors.set(true);

        // Wait for all the workers to finish.
        System.out.println("Waiting for all workers to exit due to server shutdown.");
        for (Iterator iter = workers.iterator(); iter.hasNext();) {
            Worker worker = (Worker) iter.next();
            while( !worker.waitForExit(1000) ) {
                System.out.println("==============================================");
                System.out.println("===> Server is under load now.  Kill it!");
                System.out.println("==============================================");                
                System.out.println("Stats: Produced Batches: "+this.publishedBatches.get()+", Consumed Batches: "+this.consumedBatches.get());            
            }
        }
        workers.clear();
        
        // No errors should have occured so far.
        if( errors.size()>0 )
            throw (Throwable) errors.get(0);
        
        System.out.println("==============================================");
        System.out.println("===> Start the server now.");
        System.out.println("==============================================");
        reconnect();

        System.out.println("Restarted.");
        
        // Validate the all transactions were commited as a uow.  Looking for partial commits.
        for( int i=0; i< workerCount; i++ ){
            String workerId = "worker-"+i;
            Worker w = new ConsumerWorker(connection.createSession(true,Session.SESSION_TRANSACTED), workerId, 5*1000);
            workers.add(w);
            new Thread(w, "Consumer:"+workerId).start();
        }

        System.out.println("Waiting for restarted consumers to finish consuming all messages..");
        for (Iterator iter = workers.iterator(); iter.hasNext();) {
            Worker worker = (Worker) iter.next();
            while( !worker.waitForExit(1000*5) ) {
                System.out.println("Waiting for restarted consumers to finish consuming all messages..");
                System.out.println("Stats: Produced Batches: "+this.publishedBatches.get()+", Consumed Batches: "+this.consumedBatches.get());            
            }
        }
        workers.clear();

        System.out.println("Workers finished..");
        System.out.println("Stats: Produced Batches: "+this.publishedBatches.get()+", Consumed Batches: "+this.consumedBatches.get());                    
        
        if( errors.size()>0 )
            throw (Throwable) errors.get(0);
        
    }
    
    public static void main(String[] args) {
        try {
            AcidTestTool tool = new AcidTestTool();
            tool.setUp();
            tool.testAcidTransactions();
            tool.tearDown();
        } catch (Throwable e) {
            System.out.println("Test Failed: "+e.getMessage());
            e.printStackTrace();
        }
    }
}