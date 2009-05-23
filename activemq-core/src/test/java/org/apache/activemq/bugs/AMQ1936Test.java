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
package org.apache.activemq.bugs;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.log4j.Logger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.TextMessage;
import javax.naming.NamingException;
import junit.framework.TestCase;
/**
 * A AMQ1936Test
 *
 */
public class AMQ1936Test extends TestCase{
    private final static Logger logger = Logger.getLogger( AMQ1936Test.class );
    private final static String TEST_QUEUE_NAME     = "dynamicQueues/duplicate.message.test.queue";
    ////--
    //
    private final static long TEST_MESSAGE_COUNT    = 60000;    // The number of test messages to use
    //
    ////--
    private final static int CONSUMER_COUNT         = 2;        // The number of message receiver instances
    private final static boolean TRANSACTED_RECEIVE = true; // Flag used by receiver which indicates messages should be processed within a JMS transaction

    private ThreadPoolExecutor threadPool           = new ThreadPoolExecutor( CONSUMER_COUNT,CONSUMER_COUNT, Long.MAX_VALUE,TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>() );
    private ThreadedMessageReceiver[] receivers     = new ThreadedMessageReceiver[ CONSUMER_COUNT ];
    private BrokerService broker                    = null;
    static QueueConnectionFactory connectionFactory = null;
   
   

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        broker = new BrokerService();
        broker.getSystemUsage().getMemoryUsage().setLimit(5*1024*1024);
        broker.setBrokerName("test");
        broker.setDeleteAllMessagesOnStartup(true);
        broker.start();
        connectionFactory        = new ActiveMQConnectionFactory("vm://test");;
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        
        if( threadPool!=null ) {
            // signal receivers to stop
            for( ThreadedMessageReceiver receiver: receivers) {
                receiver.setShouldStop( true );
            }
            
            logger.info("Waiting for receivers to shutdown..");
            if( ! threadPool.awaitTermination( 10, TimeUnit.SECONDS ) ) {
                logger.warn("Not all receivers completed shutdown.");
            } else {
                logger.info("All receivers shutdown successfully..");
            }
        }
        
        logger.debug("Stoping the broker.");
        
        if( broker!=null ) {
            broker.stop();
        }
    }
    
    private void sendTextMessage( String queueName, String msg ) throws JMSException, NamingException {
        QueueConnectionFactory connectionFactory        = new ActiveMQConnectionFactory("vm://test");
        QueueConnection queueConnection                 = null;
        QueueSession session                            = null;
        QueueSender sender                              = null;
        Queue queue                                     = null;
        TextMessage message                                 = null;
        
        try {
            
            // Create the queue connection
            queueConnection = connectionFactory.createQueueConnection();
    
            session = queueConnection.createQueueSession( false, QueueSession.AUTO_ACKNOWLEDGE );
            queue = session.createQueue(TEST_QUEUE_NAME);
            sender = session.createSender( queue );
            sender.setDeliveryMode( DeliveryMode.PERSISTENT );

            message = session.createTextMessage( msg );
            
            // send the message
            sender.send( message );
    
            if( session.getTransacted()) {
                session.commit();
            }

            logger.info( "Message successfully sent to : " + queue.getQueueName( ) + " messageid: " + message.getJMSMessageID( )
                        + " content:" + message.getText());
        } finally {
            if( sender!=null ) {
                sender.close();
            }
            if( session!=null ) {
                session.close();
            }
            if( queueConnection!=null ) {
                queueConnection.close();
            }
        }
    }
     
    
    public void testForDuplicateMessages( ) throws Exception {
        final ConcurrentHashMap<String,String> messages = new ConcurrentHashMap<String, String>( );
        final Object lock                               = new Object( );
        final CountDownLatch duplicateSignal            = new CountDownLatch( 1 );
        final AtomicInteger messageCount                = new AtomicInteger( 0 );
        
        // add 1/2 the number of our total messages
        for( int i = 0; i < TEST_MESSAGE_COUNT/2; i++ ) {
            if( duplicateSignal.getCount()==0 ) {
                fail( "Duplicate message id detected" );
            }
            sendTextMessage( TEST_QUEUE_NAME, String.valueOf(i) );
        }
        
        // create a number of consumers to read of the messages and start them with a handler which simply stores the message ids
        // in a Map and checks for a duplicate
        for( int i = 0; i < CONSUMER_COUNT; i++ ) {
            receivers[i] = new ThreadedMessageReceiver(TEST_QUEUE_NAME, new IMessageHandler( ) {
            
                public void onMessage( Message message ) throws Exception {
                    synchronized( lock ) {
                        logger.info( "Received message:" + message.getJMSMessageID() +  " with content: " + ((TextMessage)message).getText() );

                        messageCount.incrementAndGet();
                        
                        if(  messages.containsKey( message.getJMSMessageID()) ) {
                            duplicateSignal.countDown( );
                            logger.fatal( "duplicate message id detected:" + message.getJMSMessageID() );
                            fail( "Duplicate message id detected:" + message.getJMSMessageID() );
                        } else {
                            messages.put( message.getJMSMessageID(), message.getJMSMessageID() );
                        }
                    }
                }
            });
            threadPool.submit( receivers[i]);
        }
        
        // starting adding the remaining messages
        for(int i = 0; i < TEST_MESSAGE_COUNT/2; i++ ) {
            if( duplicateSignal.getCount()==0) {
                fail( "Duplicate message id detected" );
            }
            sendTextMessage( TEST_QUEUE_NAME, String.valueOf( i ) );
        }

        // allow some time for messages to be delivered to receivers.
        Thread.sleep( 5000 );
        
        assertEquals( "Number of messages received does not match the number sent", TEST_MESSAGE_COUNT, messages.size( ) );
        assertEquals( TEST_MESSAGE_COUNT,  messageCount.get() );
    }
    
    private final static class ThreadedMessageReceiver implements Runnable {
       
        private String queueName            = null;
        private IMessageHandler handler     = null;
        private AtomicBoolean shouldStop    = new AtomicBoolean( false );
        
        public ThreadedMessageReceiver(String queueName, IMessageHandler handler ) {
         
            this.queueName      = queueName;
            this.handler        = handler;
        }

        public void run( ) {
           
            QueueConnection queueConnection                 = null;
            QueueSession session                            = null;
            QueueReceiver receiver                          = null;
            Queue queue                                     = null;
            Message message                                 = null;
            try {
                try {
                 
                    queueConnection = connectionFactory.createQueueConnection( );
                    // create a transacted session
                    session = queueConnection.createQueueSession( TRANSACTED_RECEIVE, QueueSession.AUTO_ACKNOWLEDGE );
                    queue = session.createQueue(TEST_QUEUE_NAME);
                    receiver = session.createReceiver( queue );

                    // start the connection
                    queueConnection.start( );
                    
                    logger.info( "Receiver " + Thread.currentThread().getName() + " connected." );
                    
                    // start receive loop
                    while( ! ( shouldStop.get() || Thread.currentThread().isInterrupted()) ) {
                        try {
                            message = receiver.receive( 200 );
                        } catch( Exception e) {
                            //
                            // ignore interrupted exceptions
                            //
                            if( e instanceof InterruptedException || e.getCause() instanceof InterruptedException ) {
                                /* ignore */
                            } else {
                                throw e;
                            }
                        }
                        
                        if( message!=null && this.handler!=null ) {
                            this.handler.onMessage(message);
                        }
                        
                        // commit session on successful handling of message
                        if( session.getTransacted()) {
                            session.commit();
                        }
                    }
                    
                    logger.info( "Receiver " + Thread.currentThread().getName() + " shutting down." );
                    
                } finally {
                    if( receiver!=null ) {
                        try {
                            receiver.close();
                        } catch (JMSException e)  { 
                            logger.warn(e); 
                        }
                    }
                    if( session!=null ) {
                        try {
                            session.close();
                        } catch (JMSException e)  { 
                            logger.warn(e); 
                        }
                    }
                    if( queueConnection!=null ) {
                        queueConnection.close();
                    }
                }
            } catch ( JMSException e ) {
                logger.error(e);
                e.printStackTrace();
            } catch (NamingException e) {
                logger.error(e);
            } catch (Exception e) {
                logger.error(e);
                e.printStackTrace();
            }
        }

        public Boolean getShouldStop() {
            return shouldStop.get();
        }

        public void setShouldStop(Boolean shouldStop) {
            this.shouldStop.set(shouldStop);
        }
    }
    
    public interface IMessageHandler {
        void onMessage( Message message ) throws Exception;
    }
    

}
