/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activecluster.impl;

import java.io.Serializable;
import java.util.Map;
import java.util.Timer;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.activecluster.Cluster;
import org.apache.activecluster.ClusterListener;
import org.apache.activecluster.DestinationMarshaller;
import org.apache.activecluster.LocalNode;
import org.apache.activecluster.Service;
import org.apache.activecluster.election.ElectionStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.*;
/**
 * A default implementation of ActiveCluster which uses standard JMS operations
 *
 * @version $Revision: 1.6 $
 */
public class DefaultCluster implements Cluster {

    private final static Log log = LogFactory.getLog(DefaultCluster.class);

    private StateServiceImpl stateService;
    private LocalNode localNode;
    private Destination destination;
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private MessageConsumer consumer;
    private Timer timer;
    private DestinationMarshaller marshaller;
    private AtomicBoolean started = new AtomicBoolean(false);
    private Object clusterLock = new Object();

    /**
     * Construct this beast
     * @param localNode
     * @param dataDestination
     * @param destination
     * @param marshaller
     * @param connection
     * @param session
     * @param producer
     * @param timer
     * @param inactiveTime
     * @throws JMSException
     */
    public DefaultCluster(final LocalNode localNode,Destination dataDestination,Destination destination,
                    DestinationMarshaller marshaller,Connection connection,Session session,MessageProducer producer,
                    Timer timer,long inactiveTime) throws JMSException{
        this.localNode=localNode;
        this.destination=destination;
        this.marshaller=marshaller;
        this.connection=connection;
        this.session=session;
        this.producer=producer;
        this.timer=timer;
        if(producer==null){
            throw new IllegalArgumentException("No producer specified!");
        }
        // now lets subscribe the service to the updates from the data topic
        consumer=session.createConsumer(dataDestination,null,true);
        log.info("Creating data consumer on topic: "+dataDestination);
        this.stateService=new StateServiceImpl(this,clusterLock,new Runnable(){
            public void run(){
                if(localNode instanceof ReplicatedLocalNode){
                    ((ReplicatedLocalNode) localNode).pingRemoteNodes();
                }
            }
        },timer,inactiveTime);
        consumer.setMessageListener(new StateConsumer(stateService,marshaller));
    }

    public void addClusterListener(ClusterListener listener) {
        stateService.addClusterListener(listener);
    }

    public void removeClusterListener(ClusterListener listener) {
        stateService.removeClusterListener(listener);
    }

    public Destination getDestination() {
        return destination;
    }

    public LocalNode getLocalNode() {
        return localNode;
    }

    public Map getNodes() {
        return stateService.getNodes();
    }

    public void setElectionStrategy(ElectionStrategy strategy) {
        stateService.setElectionStrategy(strategy);
    }

        
   public void send(Destination replyTo, Message message) throws JMSException{
       producer.send(replyTo,message);
   }

    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return getSession().createConsumer(destination);
    }

    public MessageConsumer createConsumer(Destination destination, String selector) throws JMSException {
        return getSession().createConsumer(destination, selector);
    }

    public MessageConsumer createConsumer(Destination destination, String selector, boolean noLocal) throws JMSException {
        return getSession().createConsumer(destination, selector, noLocal);
    }

    public Message createMessage() throws JMSException {
        return getSession().createMessage();
    }

    public BytesMessage createBytesMessage() throws JMSException {
        return getSession().createBytesMessage();
    }

    public MapMessage createMapMessage() throws JMSException {
        return getSession().createMapMessage();
    }

    public ObjectMessage createObjectMessage() throws JMSException {
        return getSession().createObjectMessage();
    }

    public ObjectMessage createObjectMessage(Serializable object) throws JMSException {
        return getSession().createObjectMessage(object);
    }

    public StreamMessage createStreamMessage() throws JMSException {
        return getSession().createStreamMessage();
    }

    public TextMessage createTextMessage() throws JMSException {
        return getSession().createTextMessage();
    }

    public TextMessage createTextMessage(String text) throws JMSException {
        return getSession().createTextMessage(text);
    }

    public void start() throws JMSException {
        if (started.compareAndSet(false, true)) {
            connection.start();
        }
    }

    public void stop() throws JMSException {
        try {
            if (localNode instanceof Service) {
                ((Service) localNode).stop();
            }
            timer.cancel();
            session.close();
            connection.stop();
            connection.close();
        }
        finally {
            connection = null;
            session = null;
        }
    }

    public boolean waitForClusterToComplete(int expectedCount, long timeout) throws InterruptedException {
        timeout = timeout > 0 ? timeout : Long.MAX_VALUE;
        long increment = 500;
        increment = increment < timeout ? increment : timeout;
        long waitTime = timeout;
        long start = System.currentTimeMillis();
        synchronized (clusterLock) {
            while (stateService.getNodes().size() < expectedCount && started.get() && waitTime > 0) {
                clusterLock.wait(increment);
                waitTime = timeout - (System.currentTimeMillis() - start);
            }
        }
        return stateService.getNodes().size() >= expectedCount;
    }

    protected Session getSession() throws JMSException {
        if (session == null) {
            throw new JMSException("Cannot perform operation, this cluster connection is now closed");
        }
        return session;
    }
    
    /**
     * Create a named Destination
     * @param name
     * @return the Destinatiion 
     * @throws JMSException
     */
    public Destination createDestination(String name) throws JMSException{
        Destination result = getSession().createTopic(name);
        return result;
    }
}
