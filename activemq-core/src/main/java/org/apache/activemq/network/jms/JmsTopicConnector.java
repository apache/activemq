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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.naming.NamingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Bridge to other JMS Topic providers
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class JmsTopicConnector extends JmsConnector{
    private static final Log log=LogFactory.getLog(JmsTopicConnector.class);
    private String outboundTopicConnectionFactoryName;
    private String localConnectionFactoryName;
    private TopicConnectionFactory outboundTopicConnectionFactory;
    private TopicConnectionFactory localTopicConnectionFactory;
    private TopicConnection outboundTopicConnection;
    private TopicConnection localTopicConnection;
    private InboundTopicBridge[] inboundTopicBridges;
    private OutboundTopicBridge[] outboundTopicBridges;
    private String outboundUsername;
    private String outboundPassword;
    private String localUsername;
    private String localPassword;
   
   
   

    public boolean init(){
        boolean result=super.init();
        if(result){
            try{
                initializeForeignTopicConnection();
                initializeLocalTopicConnection();
                initializeInboundTopicBridges();
                initializeOutboundTopicBridges();
            }catch(Exception e){
                log.error("Failed to initialize the JMSConnector",e);
            }
        }
        return result;
    }   
    

    
    /**
     * @return Returns the inboundTopicBridges.
     */
    public InboundTopicBridge[] getInboundTopicBridges(){
        return inboundTopicBridges;
    }

    /**
     * @param inboundTopicBridges
     *            The inboundTopicBridges to set.
     */
    public void setInboundTopicBridges(InboundTopicBridge[] inboundTopicBridges){
        this.inboundTopicBridges=inboundTopicBridges;
    }

    /**
     * @return Returns the outboundTopicBridges.
     */
    public OutboundTopicBridge[] getOutboundTopicBridges(){
        return outboundTopicBridges;
    }

    /**
     * @param outboundTopicBridges
     *            The outboundTopicBridges to set.
     */
    public void setOutboundTopicBridges(OutboundTopicBridge[] outboundTopicBridges){
        this.outboundTopicBridges=outboundTopicBridges;
    }

    /**
     * @return Returns the localTopicConnectionFactory.
     */
    public TopicConnectionFactory getLocalTopicConnectionFactory(){
        return localTopicConnectionFactory;
    }

    /**
     * @param localTopicConnectionFactory
     *            The localTopicConnectionFactory to set.
     */
    public void setLocalTopicConnectionFactory(TopicConnectionFactory localConnectionFactory){
        this.localTopicConnectionFactory=localConnectionFactory;
    }

    /**
     * @return Returns the outboundTopicConnectionFactory.
     */
    public TopicConnectionFactory getOutboundTopicConnectionFactory(){
        return outboundTopicConnectionFactory;
    }

    /**
     * @return Returns the outboundTopicConnectionFactoryName.
     */
    public String getOutboundTopicConnectionFactoryName(){
        return outboundTopicConnectionFactoryName;
    }

    /**
     * @param outboundTopicConnectionFactoryName
     *            The outboundTopicConnectionFactoryName to set.
     */
    public void setOutboundTopicConnectionFactoryName(String foreignTopicConnectionFactoryName){
        this.outboundTopicConnectionFactoryName=foreignTopicConnectionFactoryName;
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
     * @return Returns the localTopicConnection.
     */
    public TopicConnection getLocalTopicConnection(){
        return localTopicConnection;
    }

    /**
     * @param localTopicConnection
     *            The localTopicConnection to set.
     */
    public void setLocalTopicConnection(TopicConnection localTopicConnection){
        this.localTopicConnection=localTopicConnection;
    }

    /**
     * @return Returns the outboundTopicConnection.
     */
    public TopicConnection getOutboundTopicConnection(){
        return outboundTopicConnection;
    }

    /**
     * @param outboundTopicConnection
     *            The outboundTopicConnection to set.
     */
    public void setOutboundTopicConnection(TopicConnection foreignTopicConnection){
        this.outboundTopicConnection=foreignTopicConnection;
    }

    /**
     * @param outboundTopicConnectionFactory
     *            The outboundTopicConnectionFactory to set.
     */
    public void setOutboundTopicConnectionFactory(TopicConnectionFactory foreignTopicConnectionFactory){
        this.outboundTopicConnectionFactory=foreignTopicConnectionFactory;
    }

    /**
     * @return Returns the outboundPassword.
     */
    public String getOutboundPassword(){
        return outboundPassword;
    }

    /**
     * @param outboundPassword
     *            The outboundPassword to set.
     */
    public void setOutboundPassword(String foreignPassword){
        this.outboundPassword=foreignPassword;
    }

    /**
     * @return Returns the outboundUsername.
     */
    public String getOutboundUsername(){
        return outboundUsername;
    }

    /**
     * @param outboundUsername
     *            The outboundUsername to set.
     */
    public void setOutboundUsername(String foreignUsername){
        this.outboundUsername=foreignUsername;
    }

    /**
     * @return Returns the localPassword.
     */
    public String getLocalPassword(){
        return localPassword;
    }

    /**
     * @param localPassword
     *            The localPassword to set.
     */
    public void setLocalPassword(String localPassword){
        this.localPassword=localPassword;
    }

    /**
     * @return Returns the localUsername.
     */
    public String getLocalUsername(){
        return localUsername;
    }

    /**
     * @param localUsername
     *            The localUsername to set.
     */
    public void setLocalUsername(String localUsername){
        this.localUsername=localUsername;
    }
    
    /**
     * @return Returns the replyToDestinationCacheSize.
     */
    public int getReplyToDestinationCacheSize(){
        return replyToDestinationCacheSize;
    }

    /**
     * @param replyToDestinationCacheSize The replyToDestinationCacheSize to set.
     */
    public void setReplyToDestinationCacheSize(int temporaryTopicCacheSize){
        this.replyToDestinationCacheSize=temporaryTopicCacheSize;
    }

    protected void initializeForeignTopicConnection() throws NamingException,JMSException{
        if(outboundTopicConnection==null){
            // get the connection factories
            if(outboundTopicConnectionFactory==null){
                // look it up from JNDI
                if(outboundTopicConnectionFactoryName!=null){
                    outboundTopicConnectionFactory=(TopicConnectionFactory) jndiTemplate.lookup(
                                    outboundTopicConnectionFactoryName,TopicConnectionFactory.class);
                    if(outboundUsername!=null){
                        outboundTopicConnection=outboundTopicConnectionFactory.createTopicConnection(outboundUsername,
                                        outboundPassword);
                    }else{
                        outboundTopicConnection=outboundTopicConnectionFactory.createTopicConnection();
                    }
                }else {
                    throw new JMSException("Cannot create localConnection - no information");
                }
            }else {
                if(outboundUsername!=null){
                    outboundTopicConnection=outboundTopicConnectionFactory.createTopicConnection(outboundUsername,
                                    outboundPassword);
                }else{
                    outboundTopicConnection=outboundTopicConnectionFactory.createTopicConnection();
                }
            }
        }
        outboundTopicConnection.start();
    }

    protected void initializeLocalTopicConnection() throws NamingException,JMSException{
        if(localTopicConnection==null){
            // get the connection factories
            if(localTopicConnectionFactory==null){
                if(embeddedConnectionFactory==null){
                    // look it up from JNDI
                    if(localConnectionFactoryName!=null){
                        localTopicConnectionFactory=(TopicConnectionFactory) jndiTemplate.lookup(
                                        localConnectionFactoryName,TopicConnectionFactory.class);
                        if(localUsername!=null){
                            localTopicConnection=localTopicConnectionFactory.createTopicConnection(localUsername,
                                            localPassword);
                        }else{
                            localTopicConnection=localTopicConnectionFactory.createTopicConnection();
                        }
                    }else {
                        throw new JMSException("Cannot create localConnection - no information");
                    }
                }else{
                    localTopicConnection = embeddedConnectionFactory.createTopicConnection();
                }
            }else {
                if(localUsername!=null){
                    localTopicConnection=localTopicConnectionFactory.createTopicConnection(localUsername,
                                    localPassword);
                }else{
                    localTopicConnection=localTopicConnectionFactory.createTopicConnection();
                }
            }
        }
        localTopicConnection.start();
    }

    protected void initializeInboundTopicBridges() throws JMSException{
        if(inboundTopicBridges!=null){
            TopicSession outboundSession = outboundTopicConnection.createTopicSession(false,Session.AUTO_ACKNOWLEDGE);
            TopicSession localSession = localTopicConnection.createTopicSession(false,Session.AUTO_ACKNOWLEDGE);
            for(int i=0;i<inboundTopicBridges.length;i++){
                InboundTopicBridge bridge=inboundTopicBridges[i];
                String topicName=bridge.getInboundTopicName();
                Topic activemqTopic=createActiveMQTopic(localSession,topicName);
                Topic foreignTopic=createForeignTopic(outboundSession,topicName);
                bridge.setConsumerTopic(foreignTopic);
                bridge.setProducerTopic(activemqTopic);
                bridge.setProducerConnection(localTopicConnection);
                bridge.setConsumerConnection(outboundTopicConnection);
                if(bridge.getJmsMessageConvertor()==null){
                    bridge.setJmsMessageConvertor(getJmsMessageConvertor());
                }
                bridge.setJmsTopicConnector(this);
                addInboundBridge(bridge);
            }
            outboundSession.close();
            localSession.close();
        }
    }

    protected void initializeOutboundTopicBridges() throws JMSException{
        if(outboundTopicBridges!=null){
            TopicSession outboundSession = outboundTopicConnection.createTopicSession(false,Session.AUTO_ACKNOWLEDGE);
            TopicSession localSession = localTopicConnection.createTopicSession(false,Session.AUTO_ACKNOWLEDGE);
            for(int i=0;i<outboundTopicBridges.length;i++){
                OutboundTopicBridge bridge=outboundTopicBridges[i];
                String topicName=bridge.getOutboundTopicName();
                Topic activemqTopic=createActiveMQTopic(localSession,topicName);
                Topic foreignTopic=createForeignTopic(outboundSession,topicName);
                bridge.setConsumerTopic(activemqTopic);
                bridge.setProducerTopic(foreignTopic);
                bridge.setProducerConnection(outboundTopicConnection);
                bridge.setConsumerConnection(localTopicConnection);
                bridge.setDoHandleReplyTo(false);
                if(bridge.getJmsMessageConvertor()==null){
                    bridge.setJmsMessageConvertor(getJmsMessageConvertor());
                }
                bridge.setJmsTopicConnector(this);
                addOutboundBridge(bridge);
            }
            outboundSession.close();
            localSession.close();
        }
    }
    
    protected Destination createReplyToTopicBridge(Topic topic, TopicConnection consumerConnection, TopicConnection producerConnection){
        OutboundTopicBridge bridge = (OutboundTopicBridge) replyToBridges.get(topic);
        if (bridge == null){
            bridge = new OutboundTopicBridge(){
                //we only handle replyTo destinations - inbound
                protected Destination processReplyToDestination (Destination destination){
                    return null;
                }
            };
            try{
                TopicSession localSession = localTopicConnection.createTopicSession(false,Session.AUTO_ACKNOWLEDGE);
                Topic localTopic = localSession.createTemporaryTopic();
                localSession.close();
                bridge.setConsumerTopic(localTopic);
                bridge.setProducerTopic(topic);
                bridge.setProducerConnection(outboundTopicConnection);
                bridge.setConsumerConnection(localTopicConnection);
                bridge.setDoHandleReplyTo(false);
                if(bridge.getJmsMessageConvertor()==null){
                    bridge.setJmsMessageConvertor(getJmsMessageConvertor());
                }
                bridge.setJmsTopicConnector(this);
                bridge.start();
                log.info("Created replyTo bridge for " + topic);
            }catch(Exception e){
               log.error("Failed to create replyTo bridge for topic: " + topic,e);
               return null;
            }
            replyToBridges.put(topic, bridge);
        }
        return bridge.getConsumerTopic();
    }
    
    protected Topic createActiveMQTopic(TopicSession session,String topicName) throws JMSException{
        return session.createTopic(topicName);
    }
    
    protected Topic createForeignTopic(TopicSession session,String topicName) throws JMSException{
        Topic result = null;
        try{
            result = session.createTopic(topicName);
        }catch(JMSException e){
            //look-up the Topic
            try{
                result = (Topic) jndiTemplate.lookup(topicName, Topic.class);
            }catch(NamingException e1){
                String errStr = "Failed to look-up Topic for name: " + topicName;
                log.error(errStr,e);
                JMSException jmsEx =  new JMSException(errStr);
                jmsEx.setLinkedException(e1);
                throw jmsEx;
            }
        }
        return result;
    }

    
}