/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.ra;

import java.lang.reflect.Method;

import javax.jms.Connection;
import javax.jms.ConnectionConsumer;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;
import javax.resource.ResourceException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.Work;
import javax.resource.spi.work.WorkException;
import javax.resource.spi.work.WorkManager;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$ $Date$
 */
public class ActiveMQEndpointWorker {

    private static final Log log = LogFactory.getLog(ActiveMQEndpointWorker.class);

    /**
     * 
     */
    public static final Method ON_MESSAGE_METHOD;

    private static final long INITIAL_RECONNECT_DELAY = 1000; // 1 second.
    private static final long MAX_RECONNECT_DELAY = 1000*30; // 30 seconds.
    private static final ThreadLocal<Session> threadLocal = new ThreadLocal<Session>();
    
    static {
        try {
            ON_MESSAGE_METHOD = MessageListener.class.getMethod("onMessage", new Class[]{Message.class});
        }
        catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected MessageResourceAdapter adapter;
    protected ActiveMQEndpointActivationKey endpointActivationKey;
    protected MessageEndpointFactory endpointFactory;
    protected WorkManager workManager;
    protected boolean transacted;
    
    
    private ConnectionConsumer consumer;
    private ServerSessionPoolImpl serverSessionPool;
    private ActiveMQDestination dest;
    private boolean running;
    private Work connectWork;
    protected ActiveMQConnection connection;
    
    private long reconnectDelay=INITIAL_RECONNECT_DELAY;


    /**
     * @param s
     */
    public static void safeClose(Session s) {
        try {
            if (s != null) {
                s.close();
            }
        }
        catch (JMSException e) {
        	//
        }
    }

    /**
     * @param c
     */
    public static void safeClose(Connection c) {
        try {
            if (c != null) {
                c.close();
            }
        }
        catch (JMSException e) {
        	//
        }
    }

    /**
     * @param cc
     */
    public static void safeClose(ConnectionConsumer cc) {
        try {
            if (cc != null) {
                cc.close();
            }
        }
        catch (JMSException e) {
        	//
        }
    }

    /**
     * 
     */
    public ActiveMQEndpointWorker(final MessageResourceAdapter adapter, ActiveMQEndpointActivationKey key) throws ResourceException {
        this.endpointActivationKey = key;
        this.adapter = adapter;
        this.endpointFactory = endpointActivationKey.getMessageEndpointFactory();
        this.workManager = adapter.getBootstrapContext().getWorkManager();
        try {
            this.transacted = endpointFactory.isDeliveryTransacted(ON_MESSAGE_METHOD);
        }
        catch (NoSuchMethodException e) {
            throw new ResourceException("Endpoint does not implement the onMessage method.");
        }
        
        connectWork = new Work() {

            public void release() {
            	//
            }

            synchronized public void run() {
                if( !isRunning() )
                    return;
                if( connection!=null )
                    return;
                
                MessageActivationSpec activationSpec = endpointActivationKey.getActivationSpec();
                try {
                    connection = adapter.makeConnection(activationSpec);
                    connection.start();
                    connection.setExceptionListener(new ExceptionListener() {
                        public void onException(JMSException error) {
                            if (!serverSessionPool.isClosing()) {
                                reconnect(error);
                            }
                        }
                    });

                    if (activationSpec.isDurableSubscription()) {
                        consumer = connection.createDurableConnectionConsumer(
                                (Topic) dest,
                                activationSpec.getSubscriptionName(), 
                                emptyToNull(activationSpec.getMessageSelector()),
                                serverSessionPool, 
                                activationSpec.getMaxMessagesPerSessionsIntValue(),
                                activationSpec.getNoLocalBooleanValue());
                    } else {
                        consumer = connection.createConnectionConsumer(
                                dest, 
                                emptyToNull(activationSpec.getMessageSelector()), 
                                serverSessionPool, 
                                activationSpec.getMaxMessagesPerSessionsIntValue(),
                                activationSpec.getNoLocalBooleanValue());
                    }

                } catch (JMSException error) {
                    log.debug("Fail to to connect: "+error, error);
                    reconnect(error);
                }
            }
        };

        MessageActivationSpec activationSpec = endpointActivationKey.getActivationSpec();
        if ("javax.jms.Queue".equals(activationSpec.getDestinationType())) {
            dest = new ActiveMQQueue(activationSpec.getDestination());
        } else if ("javax.jms.Topic".equals(activationSpec.getDestinationType())) {
            dest = new ActiveMQTopic(activationSpec.getDestination());
        } else {
            throw new ResourceException("Unknown destination type: " + activationSpec.getDestinationType());
        }

    }

    /**
     * 
     */
    synchronized public void start() throws WorkException, ResourceException {
        if (running)
            return;
        running = true;

        log.debug("Starting");
        serverSessionPool = new ServerSessionPoolImpl(this, endpointActivationKey.getActivationSpec().getMaxSessionsIntValue());
        connect();
        log.debug("Started");
    }

    /**
     * 
     */
    synchronized public void stop() throws InterruptedException {
        if (!running)
            return;
        running = false;
        serverSessionPool.close();
        disconnect();        
    }

    private boolean isRunning() {
        return running;
    }    

    synchronized private void connect() {
        if (!running)
            return;

        try {
            workManager.scheduleWork(connectWork, WorkManager.INDEFINITE, null, null);
        } catch (WorkException e) {
            running = false;
            log.error("Work Manager did not accept work: ",e);
        }
    }

    /**
     * 
     */
    synchronized private void disconnect() {
        safeClose(consumer);
        consumer=null;
        safeClose(connection);
        connection=null;
    }

    private void reconnect(JMSException error){
        log.debug("Reconnect cause: ",error);
        long reconnectDelay;
        synchronized(this) {
            reconnectDelay = this.reconnectDelay;
            // Only log errors if the server is really down.. And not a temp failure.
            if (reconnectDelay == MAX_RECONNECT_DELAY) {
                log.error("Endpoint connection to JMS broker failed: " + error.getMessage());
                log.error("Endpoint will try to reconnect to the JMS broker in "+(MAX_RECONNECT_DELAY/1000)+" seconds");
            }
        }
        try {
            disconnect();
            Thread.sleep(reconnectDelay);
            
            synchronized(this) {
                // Use exponential rollback.
                this.reconnectDelay*=2;
                if (this.reconnectDelay > MAX_RECONNECT_DELAY)
                    this.reconnectDelay=MAX_RECONNECT_DELAY;
            }
            connect();
        } catch(InterruptedException e) {
        	//
        }
    }

    protected void registerThreadSession(Session session) {
        threadLocal.set(session);
    }

    protected void unregisterThreadSession(Session session) {
        threadLocal.set(null);
    }

    private String emptyToNull(String value) {
        if (value == null || value.length() == 0) {
            return null;
        }
        return value;
    }

}
