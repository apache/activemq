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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.tool.properties.JmsClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractJmsClient {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJmsClient.class);

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

    public Destination[] createDestination(int destIndex, int destCount) throws JMSException {

        if (getClient().isDestComposite()) {
            return new Destination[] {
                createCompositeDestination(getClient().getDestName(), destIndex, destCount)
            };
        } else {
            Destination[] dest = new Destination[destCount];
            for (int i = 0; i < destCount; i++) {
                dest[i] = createDestination(getClient().getDestName() + "." + (destIndex + i));
            }

            return dest;
        }
    }

    public Destination createCompositeDestination(int destIndex, int destCount) throws JMSException {
        return createCompositeDestination(getClient().getDestName(), destIndex, destCount);
    }

    protected Destination createCompositeDestination(String name, int destIndex, int destCount) throws JMSException {
        String compDestName;
        String simpleName;

        if (name.startsWith("queue://")) {
            simpleName = name.substring("queue://".length());
        } else if (name.startsWith("topic://")) {
            simpleName = name.substring("topic://".length());
        } else {
            simpleName = name;
        }

        int i;
        compDestName = name + "." + destIndex + ","; // First destination
        for (i = 1; i < destCount - 1; i++) {
            compDestName += simpleName + "." + (destIndex + i) + ",";
        }
        // Last destination (minus the comma)
        compDestName += simpleName + "." + (destIndex + i);

        return createDestination(compDestName);
    }

    protected Destination createDestination(String name) throws JMSException {
        if (name.startsWith("queue://")) {
            return getSession().createQueue(name.substring("queue://".length()));
        } else if (name.startsWith("topic://")) {
            return getSession().createTopic(name.substring("topic://".length()));
        } else {
            return getSession().createTopic(name);
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
