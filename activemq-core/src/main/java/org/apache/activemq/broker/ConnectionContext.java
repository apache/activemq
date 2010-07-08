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
package org.apache.activemq.broker;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.security.MessageAuthorizationPolicy;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.transaction.Transaction;

/**
 * Used to hold context information needed to process requests sent to a broker.
 * 
 * @version $Revision: 1.5 $
 */
public class ConnectionContext {

    private Connection connection;
    private Connector connector;
    private Broker broker;
    private boolean inRecoveryMode;
    private Transaction transaction;
    private ConcurrentHashMap<TransactionId, Transaction> transactions;
    private SecurityContext securityContext;
    private ConnectionId connectionId;
    private String clientId;
    private String userName;
    private boolean reconnect;
    private WireFormatInfo wireFormatInfo;
    private Object longTermStoreContext;
    private boolean producerFlowControl = true;
    private MessageAuthorizationPolicy messageAuthorizationPolicy;
    private boolean networkConnection;
    private boolean faultTolerant;
    private final AtomicBoolean stopping = new AtomicBoolean();
    private final MessageEvaluationContext messageEvaluationContext;
    private boolean dontSendReponse;
    private boolean clientMaster = true;

    public ConnectionContext() {
    	this.messageEvaluationContext = new MessageEvaluationContext();
    }
    
    public ConnectionContext(MessageEvaluationContext messageEvaluationContext) {
    	this.messageEvaluationContext=messageEvaluationContext;
    }
    
    public ConnectionContext(ConnectionInfo info) {
    	this();
        setClientId(info.getClientId());
        setUserName(info.getUserName());
        setConnectionId(info.getConnectionId());
    }
    
    public ConnectionContext copy() {
        ConnectionContext rc = new ConnectionContext(this.messageEvaluationContext);
        rc.connection = this.connection;
        rc.connector = this.connector;
        rc.broker = this.broker;
        rc.inRecoveryMode = this.inRecoveryMode;
        rc.transaction = this.transaction;
        rc.transactions = this.transactions;
        rc.securityContext = this.securityContext;
        rc.connectionId = this.connectionId;
        rc.clientId = this.clientId;
        rc.userName = this.userName;
        rc.reconnect = this.reconnect;
        rc.wireFormatInfo = this.wireFormatInfo;
        rc.longTermStoreContext = this.longTermStoreContext;
        rc.producerFlowControl = this.producerFlowControl;
        rc.messageAuthorizationPolicy = this.messageAuthorizationPolicy;
        rc.networkConnection = this.networkConnection;
        rc.faultTolerant = this.faultTolerant;
        rc.stopping.set(this.stopping.get());
        rc.dontSendReponse = this.dontSendReponse;
        rc.clientMaster = this.clientMaster;
        return rc;
    }


    public SecurityContext getSecurityContext() {
        return securityContext;
    }

    public void setSecurityContext(SecurityContext subject) {
        this.securityContext = subject;
        if (subject != null) {
            setUserName(subject.getUserName());
        } else {
            setUserName(null);
        }
    }

    /**
     * @return the broker being used.
     */
    public Broker getBroker() {
        return broker;
    }

    /**
     * @param broker being used
     */
    public void setBroker(Broker broker) {
        this.broker = broker;
    }

    /**
     * @return the connection being used
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * @param connection being used
     */
    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    /**
     * @return the transaction being used.
     */
    public Transaction getTransaction() {
        return transaction;
    }

    /**
     * @param transaction being used.
     */
    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    /**
     * @return the connector being used.
     */
    public Connector getConnector() {
        return connector;
    }

    /**
     * @param connector being used.
     */
    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public MessageAuthorizationPolicy getMessageAuthorizationPolicy() {
        return messageAuthorizationPolicy;
    }

    /**
     * Sets the policy used to decide if the current connection is authorized to
     * consume a given message
     */
    public void setMessageAuthorizationPolicy(MessageAuthorizationPolicy messageAuthorizationPolicy) {
        this.messageAuthorizationPolicy = messageAuthorizationPolicy;
    }

    /**
     * @return
     */
    public boolean isInRecoveryMode() {
        return inRecoveryMode;
    }

    public void setInRecoveryMode(boolean inRecoveryMode) {
        this.inRecoveryMode = inRecoveryMode;
    }

    public ConcurrentHashMap<TransactionId, Transaction> getTransactions() {
        return transactions;
    }

    public void setTransactions(ConcurrentHashMap<TransactionId, Transaction> transactions) {
        this.transactions = transactions;
    }

    public boolean isInTransaction() {
        return transaction != null;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public boolean isReconnect() {
        return reconnect;
    }

    public void setReconnect(boolean reconnect) {
        this.reconnect = reconnect;
    }

    public WireFormatInfo getWireFormatInfo() {
        return wireFormatInfo;
    }

    public void setWireFormatInfo(WireFormatInfo wireFormatInfo) {
        this.wireFormatInfo = wireFormatInfo;
    }

    public ConnectionId getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

    public String getUserName() {
        return userName;
    }

    protected void setUserName(String userName) {
        this.userName = userName;
    }

    public MessageEvaluationContext getMessageEvaluationContext() {
        return messageEvaluationContext;
    }

    public Object getLongTermStoreContext() {
        return longTermStoreContext;
    }

    public void setLongTermStoreContext(Object longTermStoreContext) {
        this.longTermStoreContext = longTermStoreContext;
    }

    public boolean isProducerFlowControl() {
        return producerFlowControl;
    }

    public void setProducerFlowControl(boolean disableProducerFlowControl) {
        this.producerFlowControl = disableProducerFlowControl;
    }

    public boolean isAllowedToConsume(MessageReference n) throws IOException {
        if (messageAuthorizationPolicy != null) {
            return messageAuthorizationPolicy.isAllowedToConsume(this, n.getMessage());
        }
        return true;
    }

    public synchronized boolean isNetworkConnection() {
        return networkConnection;
    }

    public synchronized void setNetworkConnection(boolean networkConnection) {
        this.networkConnection = networkConnection;
    }

    public AtomicBoolean getStopping() {
        return stopping;
    }

    public void setDontSendReponse(boolean b) {
        this.dontSendReponse = b;
    }

    public boolean isDontSendReponse() {
        return dontSendReponse;
    }

    /**
     * @return the slave
     */
    public boolean isSlave() {
        return (this.broker != null && this.broker.getBrokerService().isSlave()) || !this.clientMaster;
    }

    /**
     * @return the clientMaster
     */
    public boolean isClientMaster() {
        return this.clientMaster;
    }

    /**
     * @param clientMaster the clientMaster to set
     */
    public void setClientMaster(boolean clientMaster) {
        this.clientMaster = clientMaster;
    }

    public boolean isFaultTolerant() {
        return faultTolerant;
    }

    public void setFaultTolerant(boolean faultTolerant) {
        this.faultTolerant = faultTolerant;
    }

}
