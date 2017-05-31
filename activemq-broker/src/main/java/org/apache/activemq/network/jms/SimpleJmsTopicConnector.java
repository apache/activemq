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
import javax.jms.ExceptionListener;
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
 */
public class SimpleJmsTopicConnector extends JmsConnector {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleJmsTopicConnector.class);
    private String outboundTopicConnectionFactoryName;
    private String localConnectionFactoryName;
    private TopicConnectionFactory outboundTopicConnectionFactory;
    private TopicConnectionFactory localTopicConnectionFactory;
    private InboundTopicBridge[] inboundTopicBridges;
    private OutboundTopicBridge[] outboundTopicBridges;

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
     * @param localTopicConnectionFactory The localTopicConnectionFactory to set.
     */
    public void setLocalTopicConnectionFactory(TopicConnectionFactory localTopicConnectionFactory) {
        this.localTopicConnectionFactory = localTopicConnectionFactory;
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
     * @param foreignTopicConnectionFactoryName The foreignTopicConnectionFactoryName to set.
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
        return (TopicConnection) localConnection.get();
    }

    /**
     * @param localTopicConnection The localTopicConnection to set.
     */
    public void setLocalTopicConnection(TopicConnection localTopicConnection) {
        this.localConnection.set(localTopicConnection);
    }

    /**
     * @return Returns the outboundTopicConnection.
     */
    public TopicConnection getOutboundTopicConnection() {
        return (TopicConnection) foreignConnection.get();
    }

    /**
     * @param foreignTopicConnection The foreignTopicConnection to set.
     */
    public void setOutboundTopicConnection(TopicConnection foreignTopicConnection) {
        this.foreignConnection.set(foreignTopicConnection);
    }

    /**
     * @param foreignTopicConnectionFactory The foreignTopicConnectionFactory to set.
     */
    public void setOutboundTopicConnectionFactory(TopicConnectionFactory foreignTopicConnectionFactory) {
        this.outboundTopicConnectionFactory = foreignTopicConnectionFactory;
    }

    @Override
    protected void initializeForeignConnection() throws NamingException, JMSException {

        TopicConnection newConnection = null;

        try {
            if (foreignConnection.get() == null) {
                // get the connection factories
                if (outboundTopicConnectionFactory == null) {
                    // look it up from JNDI
                    if (outboundTopicConnectionFactoryName != null) {
                        outboundTopicConnectionFactory = jndiOutboundTemplate
                            .lookup(outboundTopicConnectionFactoryName, TopicConnectionFactory.class);
                        if (outboundUsername != null) {
                            newConnection = outboundTopicConnectionFactory
                                .createTopicConnection(outboundUsername, outboundPassword);
                        } else {
                            newConnection = outboundTopicConnectionFactory.createTopicConnection();
                        }
                    } else {
                        throw new JMSException("Cannot create foreignConnection - no information");
                    }
                } else {
                    if (outboundUsername != null) {
                        newConnection = outboundTopicConnectionFactory
                            .createTopicConnection(outboundUsername, outboundPassword);
                    } else {
                        newConnection = outboundTopicConnectionFactory.createTopicConnection();
                    }
                }
            } else {
                // Clear if for now in case something goes wrong during the init.
                newConnection = (TopicConnection) foreignConnection.getAndSet(null);
            }

            // Register for any async error notifications now so we can reset in the
            // case where there's not a lot of activity and a connection drops.
            newConnection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    handleConnectionFailure(foreignConnection.get());
                }
            });

            if (outboundClientId != null && outboundClientId.length() > 0) {
                newConnection.setClientID(getOutboundClientId());
            }
            newConnection.start();

            outboundMessageConvertor.setConnection(newConnection);

            // Configure the bridges with the new Outbound connection.
            initializeInboundDestinationBridgesOutboundSide(newConnection);
            initializeOutboundDestinationBridgesOutboundSide(newConnection);

            // At this point all looks good, so this our current connection now.
            foreignConnection.set(newConnection);
        } catch (Exception ex) {
            if (newConnection != null) {
                try {
                    newConnection.close();
                } catch (Exception ignore) {}
            }

            throw ex;
        }
    }

    @Override
    protected void initializeLocalConnection() throws NamingException, JMSException {

        TopicConnection newConnection = null;

        try {
            if (localConnection.get() == null) {
                // get the connection factories
                if (localTopicConnectionFactory == null) {
                    if (embeddedConnectionFactory == null) {
                        // look it up from JNDI
                        if (localConnectionFactoryName != null) {
                            localTopicConnectionFactory = jndiLocalTemplate
                                .lookup(localConnectionFactoryName, TopicConnectionFactory.class);
                            if (localUsername != null) {
                                newConnection = localTopicConnectionFactory
                                    .createTopicConnection(localUsername, localPassword);
                            } else {
                                newConnection = localTopicConnectionFactory.createTopicConnection();
                            }
                        } else {
                            throw new JMSException("Cannot create localConnection - no information");
                        }
                    } else {
                        newConnection = embeddedConnectionFactory.createTopicConnection();
                    }
                } else {
                    if (localUsername != null) {
                        newConnection = localTopicConnectionFactory.
                                createTopicConnection(localUsername, localPassword);
                    } else {
                        newConnection = localTopicConnectionFactory.createTopicConnection();
                    }
                }

            } else {
                // Clear if for now in case something goes wrong during the init.
                newConnection = (TopicConnection) localConnection.getAndSet(null);
            }

            // Register for any async error notifications now so we can reset in the
            // case where there's not a lot of activity and a connection drops.
            newConnection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException exception) {
                    handleConnectionFailure(localConnection.get());
                }
            });

            if (localClientId != null && localClientId.length() > 0) {
                newConnection.setClientID(getLocalClientId());
            }
            newConnection.start();

            inboundMessageConvertor.setConnection(newConnection);

            // Configure the bridges with the new Local connection.
            initializeInboundDestinationBridgesLocalSide(newConnection);
            initializeOutboundDestinationBridgesLocalSide(newConnection);

            // At this point all looks good, so this our current connection now.
            localConnection.set(newConnection);
        } catch (Exception ex) {
            if (newConnection != null) {
                try {
                    newConnection.close();
                } catch (Exception ignore) {}
            }

            throw ex;
        }
    }

    protected void initializeInboundDestinationBridgesOutboundSide(TopicConnection connection) throws JMSException {
        if (inboundTopicBridges != null) {
            TopicSession outboundSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

            for (InboundTopicBridge bridge : inboundTopicBridges) {
                String TopicName = bridge.getInboundTopicName();
                Topic foreignTopic = createForeignTopic(outboundSession, TopicName);
                bridge.setConsumer(null);
                bridge.setConsumerTopic(foreignTopic);
                bridge.setConsumerConnection(connection);
                bridge.setJmsConnector(this);
                addInboundBridge(bridge);
            }
            outboundSession.close();
        }
    }

    protected void initializeInboundDestinationBridgesLocalSide(TopicConnection connection) throws JMSException {
        if (inboundTopicBridges != null) {
            TopicSession localSession = connection.createTopicSession(false,Session.AUTO_ACKNOWLEDGE);

            for (InboundTopicBridge bridge : inboundTopicBridges) {
                String localTopicName = bridge.getLocalTopicName();
                Topic activemqTopic = createActiveMQTopic(localSession, localTopicName);
                bridge.setProducerTopic(activemqTopic);
                bridge.setProducerConnection(connection);
                if (bridge.getJmsMessageConvertor() == null) {
                    bridge.setJmsMessageConvertor(getInboundMessageConvertor());
                }
                bridge.setJmsConnector(this);
                addInboundBridge(bridge);
            }
            localSession.close();
        }
    }

    protected void initializeOutboundDestinationBridgesOutboundSide(TopicConnection connection) throws JMSException {
        if (outboundTopicBridges != null) {
            TopicSession outboundSession = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

            for (OutboundTopicBridge bridge : outboundTopicBridges) {
                String topicName = bridge.getOutboundTopicName();
                Topic foreignTopic = createForeignTopic(outboundSession, topicName);
                bridge.setProducerTopic(foreignTopic);
                bridge.setProducerConnection(connection);
                if (bridge.getJmsMessageConvertor() == null) {
                    bridge.setJmsMessageConvertor(getOutboundMessageConvertor());
                }
                bridge.setJmsConnector(this);
                addOutboundBridge(bridge);
            }
            outboundSession.close();
        }
    }

    protected void initializeOutboundDestinationBridgesLocalSide(TopicConnection connection) throws JMSException {
        if (outboundTopicBridges != null) {
            TopicSession localSession =
                    connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

            for (OutboundTopicBridge bridge : outboundTopicBridges) {
                String localTopicName = bridge.getLocalTopicName();
                Topic activemqTopic = createActiveMQTopic(localSession, localTopicName);
                bridge.setConsumer(null);
                bridge.setConsumerTopic(activemqTopic);
                bridge.setConsumerConnection(connection);
                bridge.setJmsConnector(this);
                addOutboundBridge(bridge);
            }
            localSession.close();
        }
    }

    @Override
    protected Destination createReplyToBridge(Destination destination, Connection replyToProducerConnection,
                                              Connection replyToConsumerConnection) {
        Topic replyToProducerTopic = (Topic)destination;
        boolean isInbound = replyToProducerConnection.equals(localConnection.get());

        if (isInbound) {
            InboundTopicBridge bridge = (InboundTopicBridge)replyToBridges.get(replyToProducerTopic);
            if (bridge == null) {
                bridge = new InboundTopicBridge() {
                    @Override
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
                    LOG.info("Created replyTo bridge for {}", replyToProducerTopic);
                } catch (Exception e) {
                    LOG.error("Failed to create replyTo bridge for topic: {}", replyToProducerTopic, e);
                    return null;
                }
                replyToBridges.put(replyToProducerTopic, bridge);
            }
            return bridge.getConsumerTopic();
        } else {
            OutboundTopicBridge bridge = (OutboundTopicBridge)replyToBridges.get(replyToProducerTopic);
            if (bridge == null) {
                bridge = new OutboundTopicBridge() {
                    @Override
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
                    LOG.info("Created replyTo bridge for {}", replyToProducerTopic);
                } catch (Exception e) {
                    LOG.error("Failed to create replyTo bridge for topic: {}", replyToProducerTopic, e);
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

        if (preferJndiDestinationLookup) {
            try {
                // look-up the Queue
                result = jndiOutboundTemplate.lookup(topicName, Topic.class);
            } catch (NamingException e) {
                try {
                    result = session.createTopic(topicName);
                } catch (JMSException e1) {
                    String errStr = "Failed to look-up or create Topic for name: " + topicName;
                    LOG.error(errStr, e);
                    JMSException jmsEx = new JMSException(errStr);
                    jmsEx.setLinkedException(e1);
                    throw jmsEx;
                }
            }
        } else {
            try {
                result = session.createTopic(topicName);
            } catch (JMSException e) {
                // look-up the Topic
                try {
                    result = jndiOutboundTemplate.lookup(topicName, Topic.class);
                } catch (NamingException e1) {
                    String errStr = "Failed to look-up Topic for name: " + topicName;
                    LOG.error(errStr, e);
                    JMSException jmsEx = new JMSException(errStr);
                    jmsEx.setLinkedException(e1);
                    throw jmsEx;
                }
            }
        }

        return result;
    }
}
