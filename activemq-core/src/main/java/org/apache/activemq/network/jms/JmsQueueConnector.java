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
 */
package org.apache.activemq.network.jms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.naming.NamingException;
/**
 * A Bridge to other JMS Queue providers
 * 
 * @org.xbean.XBean
 *
 * @version $Revision: 1.1.1.1 $
 */
public class JmsQueueConnector extends JmsConnector{
    private static final Log log=LogFactory.getLog(JmsQueueConnector.class);
    private String outboundQueueConnectionFactoryName;
    private String localConnectionFactoryName;
    private QueueConnectionFactory outboundQueueConnectionFactory;
    private QueueConnectionFactory localQueueConnectionFactory;
    private QueueConnection outboundQueueConnection;
    private QueueConnection localQueueConnection;
    private InboundQueueBridge[] inboundQueueBridges;
    private OutboundQueueBridge[] outboundQueueBridges;
    
   
   
   

    public boolean init(){
        boolean result=super.init();
        if(result){
            try{
                initializeForeignQueueConnection();
                initializeLocalQueueConnection();
                initializeInboundQueueBridges();
                initializeOutboundQueueBridges();
            }catch(Exception e){
                log.error("Failed to initialize the JMSConnector",e);
            }
        }
        return result;
    }   
    

    
    /**
     * @return Returns the inboundQueueBridges.
     */
    public InboundQueueBridge[] getInboundQueueBridges(){
        return inboundQueueBridges;
    }

    /**
     * @param inboundQueueBridges
     *            The inboundQueueBridges to set.
     */
    public void setInboundQueueBridges(InboundQueueBridge[] inboundQueueBridges){
        this.inboundQueueBridges=inboundQueueBridges;
    }

    /**
     * @return Returns the outboundQueueBridges.
     */
    public OutboundQueueBridge[] getOutboundQueueBridges(){
        return outboundQueueBridges;
    }

    /**
     * @param outboundQueueBridges
     *            The outboundQueueBridges to set.
     */
    public void setOutboundQueueBridges(OutboundQueueBridge[] outboundQueueBridges){
        this.outboundQueueBridges=outboundQueueBridges;
    }

    /**
     * @return Returns the localQueueConnectionFactory.
     */
    public QueueConnectionFactory getLocalQueueConnectionFactory(){
        return localQueueConnectionFactory;
    }

    /**
     * @param localQueueConnectionFactory
     *            The localQueueConnectionFactory to set.
     */
    public void setLocalQueueConnectionFactory(QueueConnectionFactory localConnectionFactory){
        this.localQueueConnectionFactory=localConnectionFactory;
    }

    /**
     * @return Returns the outboundQueueConnectionFactory.
     */
    public QueueConnectionFactory getOutboundQueueConnectionFactory(){
        return outboundQueueConnectionFactory;
    }

    /**
     * @return Returns the outboundQueueConnectionFactoryName.
     */
    public String getOutboundQueueConnectionFactoryName(){
        return outboundQueueConnectionFactoryName;
    }

    /**
     * @param outboundQueueConnectionFactoryName
     *            The outboundQueueConnectionFactoryName to set.
     */
    public void setOutboundQueueConnectionFactoryName(String foreignQueueConnectionFactoryName){
        this.outboundQueueConnectionFactoryName=foreignQueueConnectionFactoryName;
    }

    /**
     * @return Returns the localConnectionFactoryName.
     */
    public String getLocalConnectionFactoryName(){
        return localConnectionFactoryName;
    }

    /**
     * @param localConnectionFactoryName
     *            The localConnectionFactoryName to set.
     */
    public void setLocalConnectionFactoryName(String localConnectionFactoryName){
        this.localConnectionFactoryName=localConnectionFactoryName;
    }

    /**
     * @return Returns the localQueueConnection.
     */
    public QueueConnection getLocalQueueConnection(){
        return localQueueConnection;
    }

    /**
     * @param localQueueConnection
     *            The localQueueConnection to set.
     */
    public void setLocalQueueConnection(QueueConnection localQueueConnection){
        this.localQueueConnection=localQueueConnection;
    }

    /**
     * @return Returns the outboundQueueConnection.
     */
    public QueueConnection getOutboundQueueConnection(){
        return outboundQueueConnection;
    }

    /**
     * @param outboundQueueConnection
     *            The outboundQueueConnection to set.
     */
    public void setOutboundQueueConnection(QueueConnection foreignQueueConnection){
        this.outboundQueueConnection=foreignQueueConnection;
    }

    /**
     * @param outboundQueueConnectionFactory
     *            The outboundQueueConnectionFactory to set.
     */
    public void setOutboundQueueConnectionFactory(QueueConnectionFactory foreignQueueConnectionFactory){
        this.outboundQueueConnectionFactory=foreignQueueConnectionFactory;
    }

    
    protected void initializeForeignQueueConnection() throws NamingException,JMSException{
        if(outboundQueueConnection==null){
            // get the connection factories
            if(outboundQueueConnectionFactory==null){
                // look it up from JNDI
                if(outboundQueueConnectionFactoryName!=null){
                    outboundQueueConnectionFactory=(QueueConnectionFactory) jndiOutboundTemplate.lookup(
                                    outboundQueueConnectionFactoryName,QueueConnectionFactory.class);
                    if(outboundUsername!=null){
                        outboundQueueConnection=outboundQueueConnectionFactory.createQueueConnection(outboundUsername,
                                        outboundPassword);
                    }else{
                        outboundQueueConnection=outboundQueueConnectionFactory.createQueueConnection();
                    }
                }else {
                    throw new JMSException("Cannot create localConnection - no information");
                }
            }else {
                if(outboundUsername!=null){
                    outboundQueueConnection=outboundQueueConnectionFactory.createQueueConnection(outboundUsername,
                                    outboundPassword);
                }else{
                    outboundQueueConnection=outboundQueueConnectionFactory.createQueueConnection();
                }
            }
        }
        outboundQueueConnection.start();
    }

    protected void initializeLocalQueueConnection() throws NamingException,JMSException{
        if(localQueueConnection==null){
            // get the connection factories
            if(localQueueConnectionFactory==null){
                if(embeddedConnectionFactory==null){
                    // look it up from JNDI
                    if(localConnectionFactoryName!=null){
                        localQueueConnectionFactory=(QueueConnectionFactory) jndiLocalTemplate.lookup(
                                        localConnectionFactoryName,QueueConnectionFactory.class);
                        if(localUsername!=null){
                            localQueueConnection=localQueueConnectionFactory.createQueueConnection(localUsername,
                                            localPassword);
                        }else{
                            localQueueConnection=localQueueConnectionFactory.createQueueConnection();
                        }
                    }else {
                        throw new JMSException("Cannot create localConnection - no information");
                    }
                }else{
                    localQueueConnection = embeddedConnectionFactory.createQueueConnection();
                }
            }else {
                if(localUsername!=null){
                    localQueueConnection=localQueueConnectionFactory.createQueueConnection(localUsername,
                                    localPassword);
                }else{
                    localQueueConnection=localQueueConnectionFactory.createQueueConnection();
                }
            }
        }
        localQueueConnection.start();
    }

    protected void initializeInboundQueueBridges() throws JMSException{
        if(inboundQueueBridges!=null){
            QueueSession outboundSession = outboundQueueConnection.createQueueSession(false,Session.AUTO_ACKNOWLEDGE);
            QueueSession localSession = localQueueConnection.createQueueSession(false,Session.AUTO_ACKNOWLEDGE);
            for(int i=0;i<inboundQueueBridges.length;i++){
                InboundQueueBridge bridge=inboundQueueBridges[i];
                String queueName=bridge.getInboundQueueName();
                Queue activemqQueue=createActiveMQQueue(localSession,queueName);
                Queue foreignQueue=createForeignQueue(outboundSession,queueName);
                bridge.setConsumerQueue(foreignQueue);
                bridge.setProducerQueue(activemqQueue);
                bridge.setProducerConnection(localQueueConnection);
                bridge.setConsumerConnection(outboundQueueConnection);
                if(bridge.getJmsMessageConvertor()==null){
                    bridge.setJmsMessageConvertor(getInboundMessageConvertor());
                }
                bridge.setJmsConnector(this);
                addInboundBridge(bridge);
            }
            outboundSession.close();
            localSession.close();
        }
    }

    protected void initializeOutboundQueueBridges() throws JMSException{
        if(outboundQueueBridges!=null){
            QueueSession outboundSession = outboundQueueConnection.createQueueSession(false,Session.AUTO_ACKNOWLEDGE);
            QueueSession localSession = localQueueConnection.createQueueSession(false,Session.AUTO_ACKNOWLEDGE);
            for(int i=0;i<outboundQueueBridges.length;i++){
                OutboundQueueBridge bridge=outboundQueueBridges[i];
                String queueName=bridge.getOutboundQueueName();
                Queue activemqQueue=createActiveMQQueue(localSession,queueName);
                Queue foreignQueue=createForeignQueue(outboundSession,queueName);
                bridge.setConsumerQueue(activemqQueue);
                bridge.setProducerQueue(foreignQueue);
                bridge.setProducerConnection(outboundQueueConnection);
                bridge.setConsumerConnection(localQueueConnection);
                bridge.setDoHandleReplyTo(false);
                if(bridge.getJmsMessageConvertor()==null){
                    bridge.setJmsMessageConvertor(getOutboundMessageConvertor());
                }
                bridge.setJmsConnector(this);
                addOutboundBridge(bridge);
            }
            outboundSession.close();
            localSession.close();
        }
    }
    
    protected Destination createReplyToBridge(Destination destination, Connection consumerConnection, Connection producerConnection){
        Queue queue = (Queue)destination;
        OutboundQueueBridge bridge = (OutboundQueueBridge) replyToBridges.get(queue);
        if (bridge == null){
            bridge = new OutboundQueueBridge(){
                //we only handle replyTo destinations - inbound
                protected Destination processReplyToDestination (Destination destination){
                    return null;
                }
            };
            try{
                QueueSession localSession = localQueueConnection.createQueueSession(false,Session.AUTO_ACKNOWLEDGE);
                Queue localQueue = localSession.createTemporaryQueue();
                localSession.close();
                bridge.setConsumerQueue(localQueue);
                bridge.setProducerQueue(queue);
                bridge.setProducerConnection(outboundQueueConnection);
                bridge.setConsumerConnection(localQueueConnection);
                bridge.setDoHandleReplyTo(false);
                if(bridge.getJmsMessageConvertor()==null){
                    bridge.setJmsMessageConvertor(getOutboundMessageConvertor());
                }
                bridge.setJmsConnector(this);
                bridge.start();
                log.info("Created replyTo bridge for " + queue);
            }catch(Exception e){
               log.error("Failed to create replyTo bridge for queue: " + queue,e);
               return null;
            }
            replyToBridges.put(queue, bridge);
        }
        return bridge.getConsumerQueue();
    }
    
    protected Queue createActiveMQQueue(QueueSession session,String queueName) throws JMSException{
        return session.createQueue(queueName);
    }
    
    protected Queue createForeignQueue(QueueSession session,String queueName) throws JMSException{
        Queue result = null;
        try{
            result = session.createQueue(queueName);
        }catch(JMSException e){
            //look-up the Queue
            try{
                result = (Queue) jndiOutboundTemplate.lookup(queueName, Queue.class);
            }catch(NamingException e1){
                String errStr = "Failed to look-up Queue for name: " + queueName;
                log.error(errStr,e);
                JMSException jmsEx =  new JMSException(errStr);
                jmsEx.setLinkedException(e1);
                throw jmsEx;
            }
        }
        return result;
    }

    
}