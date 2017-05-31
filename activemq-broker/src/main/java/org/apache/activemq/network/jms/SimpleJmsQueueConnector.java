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
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class SimpleJmsQueueConnector extends JmsConnector {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleJmsQueueConnector.class);
    private String outboundQueueConnectionFactoryName;
    private String localConnectionFactoryName;
    private QueueConnectionFactory outboundQueueConnectionFactory;
    private QueueConnectionFactory localQueueConnectionFactory;
    private InboundQueueBridge[] inboundQueueBridges;
    private OutboundQueueBridge[] outboundQueueBridges;

    /**
     * @return Returns the inboundQueueBridges.
     */
    public InboundQueueBridge[] getInboundQueueBridges() {
        return inboundQueueBridges;
    }

    /**
     * @param inboundQueueBridges The inboundQueueBridges to set.
     */
    public void setInboundQueueBridges(InboundQueueBridge[] inboundQueueBridges) {
        this.inboundQueueBridges = inboundQueueBridges;
    }

    /**
     * @return Returns the outboundQueueBridges.
     */
    public OutboundQueueBridge[] getOutboundQueueBridges() {
        return outboundQueueBridges;
    }

    /**
     * @param outboundQueueBridges The outboundQueueBridges to set.
     */
    public void setOutboundQueueBridges(OutboundQueueBridge[] outboundQueueBridges) {
        this.outboundQueueBridges = outboundQueueBridges;
    }

    /**
     * @return Returns the localQueueConnectionFactory.
     */
    public QueueConnectionFactory getLocalQueueConnectionFactory() {
        return localQueueConnectionFactory;
    }

    /**
     * @param localConnectionFactory The localQueueConnectionFactory to
     *                set.
     */
    public void setLocalQueueConnectionFactory(QueueConnectionFactory localConnectionFactory) {
        this.localQueueConnectionFactory = localConnectionFactory;
    }

    /**
     * @return Returns the outboundQueueConnectionFactory.
     */
    public QueueConnectionFactory getOutboundQueueConnectionFactory() {
        return outboundQueueConnectionFactory;
    }

    /**
     * @return Returns the outboundQueueConnectionFactoryName.
     */
    public String getOutboundQueueConnectionFactoryName() {
        return outboundQueueConnectionFactoryName;
    }

    /**
     * @param foreignQueueConnectionFactoryName The
     *                foreignQueueConnectionFactoryName to set.
     */
    public void setOutboundQueueConnectionFactoryName(String foreignQueueConnectionFactoryName) {
        this.outboundQueueConnectionFactoryName = foreignQueueConnectionFactoryName;
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
     * @return Returns the localQueueConnection.
     */
    public QueueConnection getLocalQueueConnection() {
        return (QueueConnection) localConnection.get();
    }

    /**
     * @param localQueueConnection The localQueueConnection to set.
     */
    public void setLocalQueueConnection(QueueConnection localQueueConnection) {
        this.localConnection.set(localQueueConnection);
    }

    /**
     * @return Returns the outboundQueueConnection.
     */
    public QueueConnection getOutboundQueueConnection() {
        return (QueueConnection) foreignConnection.get();
    }

    /**
     * @param foreignQueueConnection The foreignQueueConnection to set.
     */
    public void setOutboundQueueConnection(QueueConnection foreignQueueConnection) {
        this.foreignConnection.set(foreignQueueConnection);
    }

    /**
     * @param foreignQueueConnectionFactory The foreignQueueConnectionFactory to set.
     */
    public void setOutboundQueueConnectionFactory(QueueConnectionFactory foreignQueueConnectionFactory) {
        this.outboundQueueConnectionFactory = foreignQueueConnectionFactory;
    }

    @Override
    protected void initializeForeignConnection() throws NamingException, JMSException {

        QueueConnection newConnection = null;

        try {
            if (foreignConnection.get() == null) {
                // get the connection factories
                if (outboundQueueConnectionFactory == null) {
                    // look it up from JNDI
                    if (outboundQueueConnectionFactoryName != null) {
                        outboundQueueConnectionFactory = jndiOutboundTemplate
                            .lookup(outboundQueueConnectionFactoryName, QueueConnectionFactory.class);
                        if (outboundUsername != null) {
                            newConnection = outboundQueueConnectionFactory
                                .createQueueConnection(outboundUsername, outboundPassword);
                        } else {
                            newConnection = outboundQueueConnectionFactory.createQueueConnection();
                        }
                    } else {
                        throw new JMSException("Cannot create foreignConnection - no information");
                    }
                } else {
                    if (outboundUsername != null) {
                        newConnection = outboundQueueConnectionFactory
                            .createQueueConnection(outboundUsername, outboundPassword);
                    } else {
                        newConnection = outboundQueueConnectionFactory.createQueueConnection();
                    }
                }
            } else {
                // Clear if for now in case something goes wrong during the init.
                newConnection = (QueueConnection) foreignConnection.getAndSet(null);
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

        QueueConnection newConnection = null;

        try {
            if (localConnection.get() == null) {
                // get the connection factories
                if (localQueueConnectionFactory == null) {
                    if (embeddedConnectionFactory == null) {
                        // look it up from JNDI
                        if (localConnectionFactoryName != null) {
                            localQueueConnectionFactory = jndiLocalTemplate
                                .lookup(localConnectionFactoryName, QueueConnectionFactory.class);
                            if (localUsername != null) {
                                newConnection = localQueueConnectionFactory
                                    .createQueueConnection(localUsername, localPassword);
                            } else {
                                newConnection = localQueueConnectionFactory.createQueueConnection();
                            }
                        } else {
                            throw new JMSException("Cannot create localConnection - no information");
                        }
                    } else {
                        newConnection = embeddedConnectionFactory.createQueueConnection();
                    }
                } else {
                    if (localUsername != null) {
                        newConnection = localQueueConnectionFactory.
                                createQueueConnection(localUsername, localPassword);
                    } else {
                        newConnection = localQueueConnectionFactory.createQueueConnection();
                    }
                }

            } else {
                // Clear if for now in case something goes wrong during the init.
                newConnection = (QueueConnection) localConnection.getAndSet(null);
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

    protected void initializeInboundDestinationBridgesOutboundSide(QueueConnection connection) throws JMSException {
        if (inboundQueueBridges != null) {
            QueueSession outboundSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

            for (InboundQueueBridge bridge : inboundQueueBridges) {
                String queueName = bridge.getInboundQueueName();
                Queue foreignQueue = createForeignQueue(outboundSession, queueName);
                bridge.setConsumer(null);
                bridge.setConsumerQueue(foreignQueue);
                bridge.setConsumerConnection(connection);
                bridge.setJmsConnector(this);
                addInboundBridge(bridge);
            }
            outboundSession.close();
        }
    }

    protected void initializeInboundDestinationBridgesLocalSide(QueueConnection connection) throws JMSException {
        if (inboundQueueBridges != null) {
            QueueSession localSession = connection.createQueueSession(false,Session.AUTO_ACKNOWLEDGE);

            for (InboundQueueBridge bridge : inboundQueueBridges) {
                String localQueueName = bridge.getLocalQueueName();
                Queue activemqQueue = createActiveMQQueue(localSession, localQueueName);
                bridge.setProducerQueue(activemqQueue);
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

    protected void initializeOutboundDestinationBridgesOutboundSide(QueueConnection connection) throws JMSException {
        if (outboundQueueBridges != null) {
            QueueSession outboundSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

            for (OutboundQueueBridge bridge : outboundQueueBridges) {
                String queueName = bridge.getOutboundQueueName();
                Queue foreignQueue = createForeignQueue(outboundSession, queueName);
                bridge.setProducerQueue(foreignQueue);
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

    protected void initializeOutboundDestinationBridgesLocalSide(QueueConnection connection) throws JMSException {
        if (outboundQueueBridges != null) {
            QueueSession localSession =
                    connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

            for (OutboundQueueBridge bridge : outboundQueueBridges) {
                String localQueueName = bridge.getLocalQueueName();
                Queue activemqQueue = createActiveMQQueue(localSession, localQueueName);
                bridge.setConsumer(null);
                bridge.setConsumerQueue(activemqQueue);
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
        Queue replyToProducerQueue = (Queue)destination;
        boolean isInbound = replyToProducerConnection.equals(localConnection.get());

        if (isInbound) {
            InboundQueueBridge bridge = (InboundQueueBridge)replyToBridges.get(replyToProducerQueue);
            if (bridge == null) {
                bridge = new InboundQueueBridge() {
                    @Override
                    protected Destination processReplyToDestination(Destination destination) {
                        return null;
                    }
                };
                try {
                    QueueSession replyToConsumerSession = ((QueueConnection)replyToConsumerConnection)
                        .createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                    Queue replyToConsumerQueue = replyToConsumerSession.createTemporaryQueue();
                    replyToConsumerSession.close();
                    bridge.setConsumerQueue(replyToConsumerQueue);
                    bridge.setProducerQueue(replyToProducerQueue);
                    bridge.setProducerConnection((QueueConnection)replyToProducerConnection);
                    bridge.setConsumerConnection((QueueConnection)replyToConsumerConnection);
                    bridge.setDoHandleReplyTo(false);
                    if (bridge.getJmsMessageConvertor() == null) {
                        bridge.setJmsMessageConvertor(getInboundMessageConvertor());
                    }
                    bridge.setJmsConnector(this);
                    bridge.start();
                    LOG.info("Created replyTo bridge for {}", replyToProducerQueue);
                } catch (Exception e) {
                    LOG.error("Failed to create replyTo bridge for queue: {}", replyToProducerQueue, e);
                    return null;
                }
                replyToBridges.put(replyToProducerQueue, bridge);
            }
            return bridge.getConsumerQueue();
        } else {
            OutboundQueueBridge bridge = (OutboundQueueBridge)replyToBridges.get(replyToProducerQueue);
            if (bridge == null) {
                bridge = new OutboundQueueBridge() {
                    @Override
                    protected Destination processReplyToDestination(Destination destination) {
                        return null;
                    }
                };
                try {
                    QueueSession replyToConsumerSession = ((QueueConnection)replyToConsumerConnection)
                        .createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
                    Queue replyToConsumerQueue = replyToConsumerSession.createTemporaryQueue();
                    replyToConsumerSession.close();
                    bridge.setConsumerQueue(replyToConsumerQueue);
                    bridge.setProducerQueue(replyToProducerQueue);
                    bridge.setProducerConnection((QueueConnection)replyToProducerConnection);
                    bridge.setConsumerConnection((QueueConnection)replyToConsumerConnection);
                    bridge.setDoHandleReplyTo(false);
                    if (bridge.getJmsMessageConvertor() == null) {
                        bridge.setJmsMessageConvertor(getOutboundMessageConvertor());
                    }
                    bridge.setJmsConnector(this);
                    bridge.start();
                    LOG.info("Created replyTo bridge for {}", replyToProducerQueue);
                } catch (Exception e) {
                    LOG.error("Failed to create replyTo bridge for queue: {}", replyToProducerQueue, e);
                    return null;
                }
                replyToBridges.put(replyToProducerQueue, bridge);
            }
            return bridge.getConsumerQueue();
        }
    }

    protected Queue createActiveMQQueue(QueueSession session, String queueName) throws JMSException {
        return session.createQueue(queueName);
    }

    protected Queue createForeignQueue(QueueSession session, String queueName) throws JMSException {
        Queue result = null;

        if (preferJndiDestinationLookup) {
            try {
                // look-up the Queue
                result = jndiOutboundTemplate.lookup(queueName, Queue.class);
            } catch (NamingException e) {
                try {
                    result = session.createQueue(queueName);
                } catch (JMSException e1) {
                    String errStr = "Failed to look-up or create Queue for name: " + queueName;
                    LOG.error(errStr, e);
                    JMSException jmsEx = new JMSException(errStr);
                    jmsEx.setLinkedException(e1);
                    throw jmsEx;
                }
            }
        } else {
            try {
                result = session.createQueue(queueName);
            } catch (JMSException e) {
                // look-up the Queue
                try {
                    result = jndiOutboundTemplate.lookup(queueName, Queue.class);
                } catch (NamingException e1) {
                    String errStr = "Failed to look-up Queue for name: " + queueName;
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
