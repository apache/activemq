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
package org.apache.activemq.tool;

import java.util.ArrayList;
import java.util.List;

import javax.jms.*;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.tool.properties.JmsClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJmsClient {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJmsClient.class);

    private static final String QUEUE_SCHEME = "queue://";
    private static final String TOPIC_SCHEME = "topic://";
    private static final String TEMP_QUEUE_SCHEME = "temp-queue://";
    private static final String TEMP_TOPIC_SCHEME = "temp-topic://";
    public static final String DESTINATION_SEPARATOR = ",";

    protected ConnectionFactory factory;
    protected Connection jmsConnection;
    protected Session jmsSession;

    protected int destCount = 1;
    protected int destIndex;
    protected String clientName = "";

    private int internalTxCounter = 0;

    public AbstractJmsClient(ConnectionFactory factory) {
        this.factory = factory;
    }

    public abstract JmsClientProperties getClient();

    public abstract void setClient(JmsClientProperties client);

    public ConnectionFactory getFactory() {
        return factory;
    }

    public void setFactory(ConnectionFactory factory) {
        this.factory = factory;
    }

    public int getDestCount() {
        return destCount;
    }

    public void setDestCount(int destCount) {
        this.destCount = destCount;
    }

    public int getDestIndex() {
        return destIndex;
    }

    public void setDestIndex(int destIndex) {
        this.destIndex = destIndex;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public Connection getConnection() throws JMSException {
        if (jmsConnection == null) {
            jmsConnection = factory.createConnection();
            jmsConnection.setClientID(getClientName());
            LOG.info("Creating JMS Connection: Provider=" + getClient().getJmsProvider() + ", JMS Spec=" + getClient().getJmsVersion());
        }
        return jmsConnection;
    }

    public Session getSession() throws JMSException {
        if (jmsSession == null) {
            int ackMode;
            if (getClient().getSessAckMode().equalsIgnoreCase(JmsClientProperties.SESSION_AUTO_ACKNOWLEDGE)) {
                ackMode = Session.AUTO_ACKNOWLEDGE;
            } else if (getClient().getSessAckMode().equalsIgnoreCase(JmsClientProperties.SESSION_CLIENT_ACKNOWLEDGE)) {
                ackMode = Session.CLIENT_ACKNOWLEDGE;
            } else if (getClient().getSessAckMode().equalsIgnoreCase(JmsClientProperties.SESSION_DUPS_OK_ACKNOWLEDGE)) {
                ackMode = Session.DUPS_OK_ACKNOWLEDGE;
            } else if (getClient().getSessAckMode().equalsIgnoreCase(JmsClientProperties.SESSION_TRANSACTED)) {
                ackMode = Session.SESSION_TRANSACTED;
            } else {
                ackMode = Session.AUTO_ACKNOWLEDGE;
            }
            jmsSession = getConnection().createSession(getClient().isSessTransacted(), ackMode);
        }
        return jmsSession;
    }

    public Destination[] createDestinations(int destCount) throws JMSException {
        final String destName = getClient().getDestName();
        ArrayList<Destination> destinations = new ArrayList<>();
        if (destName.contains(DESTINATION_SEPARATOR)) {
            if (getClient().isDestComposite() && (destCount == 1)) {
                // user was explicit about which destinations to make composite
                String[] simpleNames = mapToSimpleNames(destName.split(DESTINATION_SEPARATOR));
                String joinedSimpleNames = join(simpleNames, DESTINATION_SEPARATOR);

                // use the type of the 1st destination for the Destination instance
                byte destinationType = getDestinationType(destName);
                destinations.add(createCompositeDestination(destinationType, joinedSimpleNames, 1));
            } else {
                LOG.info("User requested multiple destinations, splitting: {}", destName);
                // either composite with multiple destinations to be suffixed
                // or multiple non-composite destinations
                String[] destinationNames = destName.split(DESTINATION_SEPARATOR);
                for (String splitDestName : destinationNames) {
                    addDestinations(destinations, splitDestName, destCount);
                }
            }
        } else {
            addDestinations(destinations, destName, destCount);
        }
        return destinations.toArray(new Destination[] {});
    }

    private String join(String[] stings, String separator) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < stings.length; i++) {
            if (i > 0) {
                sb.append(separator);
            }
            sb.append(stings[i]);
        }
        return sb.toString();
    }

    private void addDestinations(List<Destination> destinations, String destName, int destCount) throws JMSException {
        boolean destComposite = getClient().isDestComposite();
        if ((destComposite) && (destCount > 1)) {
            destinations.add(createCompositeDestination(destName, destCount));
        } else {
            for (int i = 0; i < destCount; i++) {
                destinations.add(createDestination(withDestinationSuffix(destName, i, destCount)));
            }
        }
    }

    private String withDestinationSuffix(String name, int destIndex, int destCount) {
        return (destCount == 1) ? name : name + "." + destIndex;
    }

    protected Destination createCompositeDestination(String destName, int destCount) throws JMSException {
        return createCompositeDestination(getDestinationType(destName), destName, destCount);
    }

    protected Destination createCompositeDestination(byte destinationType, String destName, int destCount) throws JMSException {
        String simpleName = getSimpleName(destName);

        String compDestName = "";
        for (int i = 0; i < destCount; i++) {
            if (i > 0) {
                compDestName += ",";
            }
            compDestName += withDestinationSuffix(simpleName, i, destCount);
        }

        LOG.info("Creating composite destination: {}", compDestName);
        Destination destination;
        Session session = getSession();
        if (destinationType == ActiveMQDestination.TOPIC_TYPE) {
            destination = session.createTopic(compDestName);
        } else if (destinationType == ActiveMQDestination.QUEUE_TYPE) {
            destination = session.createQueue(compDestName);
        } else {
            throw new UnsupportedOperationException(
                    "Cannot create composite destinations using temporary queues or topics.");
        }
        assert (destination != null);
        return destination;
    }

    private String[] mapToSimpleNames(String[] destNames) {
        assert (destNames != null);
        String[] simpleNames = new String[destNames.length];
        for (int i = 0; i < destNames.length; i++) {
            simpleNames[i] = getSimpleName(destNames[i]);
        }
        return simpleNames;
    }

    protected String getSimpleName(String destName) {
        String simpleName;
        if (destName.startsWith(QUEUE_SCHEME)) {
            simpleName = destName.substring(QUEUE_SCHEME.length());
        } else if (destName.startsWith(TOPIC_SCHEME)) {
            simpleName = destName.substring(TOPIC_SCHEME.length());
        } else if (destName.startsWith(TEMP_QUEUE_SCHEME)) {
            simpleName = destName.substring(TEMP_QUEUE_SCHEME.length());
        } else if (destName.startsWith(TEMP_TOPIC_SCHEME)) {
            simpleName = destName.substring(TEMP_TOPIC_SCHEME.length());
        } else {
            simpleName = destName;
        }
        return simpleName;
    }

    protected byte getDestinationType(String destName) {
        assert (destName != null);
        if (destName.startsWith(QUEUE_SCHEME)) {
            return ActiveMQDestination.QUEUE_TYPE;
        } else if (destName.startsWith(TEMP_QUEUE_SCHEME)) {
            return ActiveMQDestination.TEMP_QUEUE_TYPE;
        } else if (destName.startsWith(TEMP_TOPIC_SCHEME)) {
            return ActiveMQDestination.TEMP_TOPIC_TYPE;
        } else {
            return ActiveMQDestination.TOPIC_TYPE;
        }
    }

    protected Destination createDestination(String destName) throws JMSException {
        String simpleName = getSimpleName(destName);
        byte destinationType = getDestinationType(destName);

        if (destinationType == ActiveMQDestination.QUEUE_TYPE) {
            LOG.info("Creating queue: {}", destName);
            return getSession().createQueue(simpleName);
        } else if (destinationType == ActiveMQDestination.TOPIC_TYPE) {
            LOG.info("Creating topic: {}", destName);
            return getSession().createTopic(simpleName);
        } else {
            return createTemporaryDestination(destName);
        }
    }

    protected Destination createTemporaryDestination(String destName) throws JMSException {
        byte destinationType = getDestinationType(destName);

        if (destinationType == ActiveMQDestination.TEMP_QUEUE_TYPE) {
            LOG.warn("Creating temporary queue. Requested name ({}) ignored.", destName);
            TemporaryQueue temporaryQueue = getSession().createTemporaryQueue();
            LOG.info("Temporary queue created: {}", temporaryQueue.getQueueName());
            return temporaryQueue;
        } else if (destinationType == ActiveMQDestination.TEMP_TOPIC_TYPE) {
            LOG.warn("Creating temporary topic. Requested name ({}) ignored.", destName);
            TemporaryTopic temporaryTopic = getSession().createTemporaryTopic();
            LOG.info("Temporary topic created: {}", temporaryTopic.getTopicName());
            return temporaryTopic;
        } else {
            throw new IllegalArgumentException("Unrecognized destination type: " + destinationType);
        }
    }

    /**
     * Helper method that checks if session is
     * transacted and whether to commit the tx based on commitAfterXMsgs
     * property.
     *
     * @return true if transaction was committed.
     * @throws JMSException in case the call to JMS Session.commit() fails.
     */
    public boolean commitTxIfNecessary() throws JMSException {
        internalTxCounter++;
        if (getClient().isSessTransacted()) {
            if ((internalTxCounter % getClient().getCommitAfterXMsgs()) == 0) {
                LOG.debug("Committing transaction.");
                internalTxCounter = 0;
                getSession().commit();
                return true;
            }
        }
        return false;
    }
}
