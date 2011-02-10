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
package org.apache.activemq.network.jms;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Bridge to other JMS Topic providers
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class JmsTopicConnector extends JmsConnector {
    private static final Logger LOG = LoggerFactory.getLogger(JmsTopicConnector.class);
    private String outboundTopicConnectionFactoryName;
    private String localConnectionFactoryName;
    private TopicConnectionFactory outboundTopicConnectionFactory;
    private TopicConnectionFactory localTopicConnectionFactory;
    private TopicConnection outboundTopicConnection;
    private TopicConnection localTopicConnection;
    private InboundTopicBridge[] inboundTopicBridges;
    private OutboundTopicBridge[] outboundTopicBridges;

    public boolean init() {
        boolean result = super.init();
        if (result) {
            try {
                initializeForeignTopicConnection();
                initializeLocalTopicConnection();
                initializeInboundJmsMessageConvertor();
                initializeOutboundJmsMessageConvertor();
                initializeInboundTopicBridges();
                initializeOutboundTopicBridges();
            } catch (Exception e) {
                LOG.error("Failed to initialize the JMSConnector", e);
            }
        }
        return result;
    }

    /**
     * @return Returns the inboundTopicBridges.
     */
    public InboundTopicBridge[] getInboundTopicBridges() {
        return inboundTopicBridges;
    }

    /**
     * @param inboundTopicBridges The inboundTopicBridges to set.
     */
    public void setInboundTopicBridges(InboundTopicBridge[] inboundTopicBridges) {
        this.inboundTopicBridges = inboundTopicBridges;
    }

    /**
     * @return Returns the outboundTopicBridges.
     */
    public OutboundTopicBridge[] getOutboundTopicBridges() {
        return outboundTopicBridges;
    }

    /**
     * @param outboundTopicBridges The outboundTopicBridges to set.
     */
    public void setOutboundTopicBridges(OutboundTopicBridge[] outboundTopicBridges) {
        this.outboundTopicBridges = outboundTopicBridges;
    }

    /**
     * @return Returns the localTopicConnectionFactory.
     */
    public TopicConnectionFactory getLocalTopicConnectionFactory() {
        return localTopicConnectionFactory;
    }

    /**
     * @param localTopicConnectionFactory The localTopicConnectionFactory to
     *                set.
     */
    public void setLocalTopicConnectionFactory(TopicConnectionFactory localConnectionFactory) {
        this.localTopicConnectionFactory = localConnectionFactory;
    }

    /**
     * @return Returns the outboundTopicConnectionFactory.
     */
    public TopicConnectionFactory getOutboundTopicConnectionFactory() {
        return outboundTopicConnectionFactory;
    }

    /**
     * @return Returns the outboundTopicConnectionFactoryName.
     */
    public String getOutboundTopicConnectionFactoryName() {
        return outboundTopicConnectionFactoryName;
    }

    /**
     * @param outboundTopicConnectionFactoryName The
     *                outboundTopicConnectionFactoryName to set.
     */
    public void setOutboundTopicConnectionFactoryName(String foreignTopicConnectionFactoryName) {
        this.outboundTopicConnectionFactoryName = foreignTopicConnectionFactoryName;
    }

    /**
     * @return Returns the localConnectionFactoryName.
     */
    public String getLocalConnectionFactoryName() {
        return localConnectionFactoryName;
    }

    /**
     * @param localConnectionFactoryName The localConnectionFactoryName to set.
     */
    public void setLocalConnectionFactoryName(String localConnectionFactoryName) {
        this.localConnectionFactoryName = localConnectionFactoryName;
    }

    /**
     * @return Returns the localTopicConnection.
     */
    public TopicConnection getLocalTopicConnection() {
        return localTopicConnection;
    }

    /**
     * @param localTopicConnection The localTopicConnection to set.
     */
    public void setLocalTopicConnection(TopicConnection localTopicConnection) {
        this.localTopicConnection = localTopicConnection;
    }

    /**
     * @return Returns the outboundTopicConnection.
     */
    public TopicConnection getOutboundTopicConnection() {
        return outboundTopicConnection;
    }

    /**
     * @param outboundTopicConnection The outboundTopicConnection to set.
     */
    public void setOutboundTopicConnection(TopicConnection foreignTopicConnection) {
        this.outboundTopicConnection = foreignTopicConnection;
    }

    /**
     * @param outboundTopicConnectionFactory The outboundTopicConnectionFactory
     *                to set.
     */
    public void setOutboundTopicConnectionFactory(TopicConnectionFactory foreignTopicConnectionFactory) {
        this.outboundTopicConnectionFactory = foreignTopicConnectionFactory;
    }

    public void restartProducerConnection() throws NamingException, JMSException {
        outboundTopicConnection = null;
        initializeForeignTopicConnection();
    }

    protected void initializeForeignTopicConnection() throws NamingException, JMSException {
        if (outboundTopicConnection == null) {
            // get the connection factories
            if (outboundTopicConnectionFactory == null) {
                // look it up from JNDI
                if (outboundTopicConnectionFactoryName != null) {
                    outboundTopicConnectionFactory = (TopicConnectionFactory)jndiOutboundTemplate
                        .lookup(outboundTopicConnectionFactoryName, TopicConnectionFactory.class);
                    if (outboundUsername != null) {
                        outboundTopicConnection = outboundTopicConnectionFactory
                            .createTopicConnection(outboundUsername, outboundPassword);
                    } else {
                        outboundTopicConnection = outboundTopicConnectionFactory.createTopicConnection();
                    }
                } else {
                    throw new JMSException("Cannot create localConnection - no information");
                }
            } else {
                if (outboundUsername != null) {
                    outboundTopicConnection = outboundTopicConnectionFactory
                        .createTopicConnection(outboundUsername, outboundPassword);
                } else {
                    outboundTopicConnection = outboundTopicConnectionFactory.createTopicConnection();
                }
            }
        }
        if (localClientId != null && localClientId.length() > 0) {
            outboundTopicConnection.setClientID(getOutboundClientId());
        }
        outboundTopicConnection.start();
    }

    protected void initializeLocalTopicConnection() throws NamingException, JMSException {
        if (localTopicConnection == null) {
            // get the connection factories
            if (localTopicConnectionFactory == null) {
                if (embeddedConnectionFactory == null) {
                    // look it up from JNDI
                    if (localConnectionFactoryName != null) {
                        localTopicConnectionFactory = (TopicConnectionFactory)jndiLocalTemplate
                            .lookup(localConnectionFactoryName, TopicConnectionFactory.class);
                        if (localUsername != null) {
                            localTopicConnection = localTopicConnectionFactory
                                .createTopicConnection(localUsername, localPassword);
                        } else {
                            localTopicConnection = localTopicConnectionFactory.createTopicConnection();
                        }
                    } else {
                        throw new JMSException("Cannot create localConnection - no information");
                    }
                } else {
                    localTopicConnection = embeddedConnectionFactory.createTopicConnection();
                }
            } else {
                if (localUsername != null) {
                    localTopicConnection = localTopicConnectionFactory.createTopicConnection(localUsername,
                                                                                             localPassword);
                } else {
                    localTopicConnection = localTopicConnectionFactory.createTopicConnection();
                }
            }
        }
        if (localClientId != null && localClientId.length() > 0) {
            localTopicConnection.setClientID(getLocalClientId());
        }
        localTopicConnection.start();
    }

    protected void initializeInboundJmsMessageConvertor() {
        inboundMessageConvertor.setConnection(localTopicConnection);
    }

    protected void initializeOutboundJmsMessageConvertor() {
        outboundMessageConvertor.setConnection(outboundTopicConnection);
    }

    protected void initializeInboundTopicBridges() throws JMSException {
        if (inboundTopicBridges != null) {
            TopicSession outboundSession = outboundTopicConnection
                .createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSession localSession = localTopicConnection.createTopicSession(false,
                                                                                Session.AUTO_ACKNOWLEDGE);
            for (int i = 0; i < inboundTopicBridges.length; i++) {
                InboundTopicBridge bridge = inboundTopicBridges[i];
                String localTopicName = bridge.getLocalTopicName();
                Topic activemqTopic = createActiveMQTopic(localSession, localTopicName);
                String topicName = bridge.getInboundTopicName();
                Topic foreignTopic = createForeignTopic(outboundSession, topicName);
                bridge.setConsumerTopic(foreignTopic);
                bridge.setProducerTopic(activemqTopic);
                bridge.setProducerConnection(localTopicConnection);
                bridge.setConsumerConnection(outboundTopicConnection);
                if (bridge.getJmsMessageConvertor() == null) {
                    bridge.setJmsMessageConvertor(getInboundMessageConvertor());
                }
                bridge.setJmsConnector(this);
                addInboundBridge(bridge);
            }
            outboundSession.close();
            localSession.close();
        }
    }

    protected void initializeOutboundTopicBridges() throws JMSException {
        if (outboundTopicBridges != null) {
            TopicSession outboundSession = outboundTopicConnection
                .createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSession localSession = localTopicConnection.createTopicSession(false,
                                                                                Session.AUTO_ACKNOWLEDGE);
            for (int i = 0; i < outboundTopicBridges.length; i++) {
                OutboundTopicBridge bridge = outboundTopicBridges[i];
                String localTopicName = bridge.getLocalTopicName();
                Topic activemqTopic = createActiveMQTopic(localSession, localTopicName);
                String topicName = bridge.getOutboundTopicName();
                Topic foreignTopic = createForeignTopic(outboundSession, topicName);
                bridge.setConsumerTopic(activemqTopic);
                bridge.setProducerTopic(foreignTopic);
                bridge.setProducerConnection(outboundTopicConnection);
                bridge.setConsumerConnection(localTopicConnection);
                if (bridge.getJmsMessageConvertor() == null) {
                    bridge.setJmsMessageConvertor(getOutboundMessageConvertor());
                }
                bridge.setJmsConnector(this);
                addOutboundBridge(bridge);
            }
            outboundSession.close();
            localSession.close();
        }
    }

    protected Destination createReplyToBridge(Destination destination, Connection replyToProducerConnection,
                                              Connection replyToConsumerConnection) {
        Topic replyToProducerTopic = (Topic)destination;
        boolean isInbound = replyToProducerConnection.equals(localTopicConnection);

        if (isInbound) {
            InboundTopicBridge bridge = (InboundTopicBridge)replyToBridges.get(replyToProducerTopic);
            if (bridge == null) {
                bridge = new InboundTopicBridge() {
                    protected Destination processReplyToDestination(Destination destination) {
                        return null;
                    }
                };
                try {
                    TopicSession replyToConsumerSession = ((TopicConnection)replyToConsumerConnection)
                        .createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
                    Topic replyToConsumerTopic = replyToConsumerSession.createTemporaryTopic();
                    replyToConsumerSession.close();
                    bridge.setConsumerTopic(replyToConsumerTopic);
                    bridge.setProducerTopic(replyToProducerTopic);
                    bridge.setProducerConnection((TopicConnection)replyToProducerConnection);
                    bridge.setConsumerConnection((TopicConnection)replyToConsumerConnection);
                    bridge.setDoHandleReplyTo(false);
                    if (bridge.getJmsMessageConvertor() == null) {
                        bridge.setJmsMessageConvertor(getInboundMessageConvertor());
                    }
                    bridge.setJmsConnector(this);
                    bridge.start();
                    LOG.info("Created replyTo bridge for " + replyToProducerTopic);
                } catch (Exception e) {
                    LOG.error("Failed to create replyTo bridge for topic: " + replyToProducerTopic, e);
                    return null;
                }
                replyToBridges.put(replyToProducerTopic, bridge);
            }
            return bridge.getConsumerTopic();
        } else {
            OutboundTopicBridge bridge = (OutboundTopicBridge)replyToBridges.get(replyToProducerTopic);
            if (bridge == null) {
                bridge = new OutboundTopicBridge() {
                    protected Destination processReplyToDestination(Destination destination) {
                        return null;
                    }
                };
                try {
                    TopicSession replyToConsumerSession = ((TopicConnection)replyToConsumerConnection)
                        .createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
                    Topic replyToConsumerTopic = replyToConsumerSession.createTemporaryTopic();
                    replyToConsumerSession.close();
                    bridge.setConsumerTopic(replyToConsumerTopic);
                    bridge.setProducerTopic(replyToProducerTopic);
                    bridge.setProducerConnection((TopicConnection)replyToProducerConnection);
                    bridge.setConsumerConnection((TopicConnection)replyToConsumerConnection);
                    bridge.setDoHandleReplyTo(false);
                    if (bridge.getJmsMessageConvertor() == null) {
                        bridge.setJmsMessageConvertor(getOutboundMessageConvertor());
                    }
                    bridge.setJmsConnector(this);
                    bridge.start();
                    LOG.info("Created replyTo bridge for " + replyToProducerTopic);
                } catch (Exception e) {
                    LOG.error("Failed to create replyTo bridge for topic: " + replyToProducerTopic, e);
                    return null;
                }
                replyToBridges.put(replyToProducerTopic, bridge);
            }
            return bridge.getConsumerTopic();
        }
    }

    protected Topic createActiveMQTopic(TopicSession session, String topicName) throws JMSException {
        return session.createTopic(topicName);
    }

    protected Topic createForeignTopic(TopicSession session, String topicName) throws JMSException {
        Topic result = null;
        try {
            result = session.createTopic(topicName);
        } catch (JMSException e) {
            // look-up the Topic
            try {
                result = (Topic)jndiOutboundTemplate.lookup(topicName, Topic.class);
            } catch (NamingException e1) {
                String errStr = "Failed to look-up Topic for name: " + topicName;
                LOG.error(errStr, e);
                JMSException jmsEx = new JMSException(errStr);
                jmsEx.setLinkedException(e1);
                throw jmsEx;
            }
        }
        return result;
    }

}
