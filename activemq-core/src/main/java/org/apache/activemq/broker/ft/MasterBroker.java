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
package org.apache.activemq.broker.ft;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ConsumerBrokerExchange;
import org.apache.activemq.broker.InsertableMutableBrokerFilter;
import org.apache.activemq.broker.MutableBrokerFilter;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionControl;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.transport.MutexTransport;
import org.apache.activemq.transport.ResponseCorrelator;
import org.apache.activemq.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Message Broker which passes messages to a slave
 * 
 * 
 */
public class MasterBroker extends InsertableMutableBrokerFilter {

    private static final Logger LOG = LoggerFactory.getLogger(MasterBroker.class);
    private Transport slave;
    private AtomicBoolean started = new AtomicBoolean(false);

    private Map<ConsumerId, ConsumerId> consumers = new ConcurrentHashMap<ConsumerId, ConsumerId>();
    
    /**
     * Constructor
     * 
     * @param parent
     * @param transport
     */
    public MasterBroker(MutableBrokerFilter parent, Transport transport) {
        super(parent);
        this.slave = transport;
        this.slave = new MutexTransport(slave);
        this.slave = new ResponseCorrelator(slave);
        this.slave.setTransportListener(transport.getTransportListener());
    }

    /**
     * start processing this broker
     */
    public void startProcessing() {
        started.set(true);
        try {
            Connection[] connections = getClients();
            ConnectionControl command = new ConnectionControl();
            command.setFaultTolerant(true);
            if (connections != null) {
                for (int i = 0; i < connections.length; i++) {
                    if (connections[i].isActive() && connections[i].isManageable()) {
                        connections[i].dispatchAsync(command);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to get Connections", e);
        }
    }

    /**
     * stop the broker
     * 
     * @throws Exception
     */
    public void stop() throws Exception {
        stopProcessing();
    }

    /**
     * stop processing this broker
     */
    public void stopProcessing() {
        if (started.compareAndSet(true, false)) {
            remove();
        }
    }

    /**
     * A client is establishing a connection with the broker.
     * 
     * @param context
     * @param info
     * @throws Exception
     */
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        super.addConnection(context, info);
        sendAsyncToSlave(info);
    }

    /**
     * A client is disconnecting from the broker.
     * 
     * @param context the environment the operation is being executed under.
     * @param info
     * @param error null if the client requested the disconnect or the error
     *                that caused the client to disconnect.
     * @throws Exception
     */
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        super.removeConnection(context, info, error);
        sendAsyncToSlave(new RemoveInfo(info.getConnectionId()));
    }

    /**
     * Adds a session.
     * 
     * @param context
     * @param info
     * @throws Exception
     */
    public void addSession(ConnectionContext context, SessionInfo info) throws Exception {
        super.addSession(context, info);
        sendAsyncToSlave(info);
    }

    /**
     * Removes a session.
     * 
     * @param context
     * @param info
     * @throws Exception
     */
    public void removeSession(ConnectionContext context, SessionInfo info) throws Exception {
        super.removeSession(context, info);
        sendAsyncToSlave(new RemoveInfo(info.getSessionId()));
    }

    /**
     * Adds a producer.
     * 
     * @param context the enviorment the operation is being executed under.
     * @param info
     * @throws Exception
     */
    public void addProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        super.addProducer(context, info);
        sendAsyncToSlave(info);
    }

    /**
     * Removes a producer.
     * 
     * @param context the environment the operation is being executed under.
     * @param info
     * @throws Exception
     */
    public void removeProducer(ConnectionContext context, ProducerInfo info) throws Exception {
        super.removeProducer(context, info);
        sendAsyncToSlave(new RemoveInfo(info.getProducerId()));
    }

    /**
     * add a consumer
     * 
     * @param context
     * @param info
     * @return the associated subscription
     * @throws Exception
     */
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        sendSyncToSlave(info);
        consumers.put(info.getConsumerId(), info.getConsumerId());
        return super.addConsumer(context, info);
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info)
            throws Exception {
        super.removeConsumer(context, info);
        consumers.remove(info.getConsumerId());
        sendSyncToSlave(new RemoveInfo(info.getConsumerId()));
   }

    /**
     * remove a subscription
     * 
     * @param context
     * @param info
     * @throws Exception
     */
    public void removeSubscription(ConnectionContext context, RemoveSubscriptionInfo info) throws Exception {
        super.removeSubscription(context, info);
        sendAsyncToSlave(info);
    }
    
    @Override
    public void addDestinationInfo(ConnectionContext context,
            DestinationInfo info) throws Exception {
        super.addDestinationInfo(context, info);
        if (info.getDestination().isTemporary()) {
            sendAsyncToSlave(info);
        }
    }

    @Override
    public void removeDestinationInfo(ConnectionContext context, DestinationInfo info) throws Exception {
        super.removeDestinationInfo(context, info);
        if (info.getDestination().isTemporary()) {
            sendAsyncToSlave(info);
        }
    }
    
    /**
     * begin a transaction
     * 
     * @param context
     * @param xid
     * @throws Exception
     */
    public void beginTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        TransactionInfo info = new TransactionInfo(context.getConnectionId(), xid, TransactionInfo.BEGIN);
        sendAsyncToSlave(info);
        super.beginTransaction(context, xid);
    }

    /**
     * Prepares a transaction. Only valid for xa transactions.
     * 
     * @param context
     * @param xid
     * @return the state
     * @throws Exception
     */
    public int prepareTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        TransactionInfo info = new TransactionInfo(context.getConnectionId(), xid, TransactionInfo.PREPARE);
        sendSyncToSlave(info);
        int result = super.prepareTransaction(context, xid);
        return result;
    }

    /**
     * Rollsback a transaction.
     * 
     * @param context
     * @param xid
     * @throws Exception
     */
    public void rollbackTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        TransactionInfo info = new TransactionInfo(context.getConnectionId(), xid, TransactionInfo.ROLLBACK);
        sendAsyncToSlave(info);
        super.rollbackTransaction(context, xid);
    }

    /**
     * Commits a transaction.
     * 
     * @param context
     * @param xid
     * @param onePhase
     * @throws Exception
     */
    public void commitTransaction(ConnectionContext context, TransactionId xid, boolean onePhase) throws Exception {
        TransactionInfo info = new TransactionInfo(context.getConnectionId(), xid, TransactionInfo.COMMIT_ONE_PHASE);
        sendSyncToSlave(info);
        super.commitTransaction(context, xid, onePhase);
    }

    /**
     * Forgets a transaction.
     * 
     * @param context
     * @param xid
     * @throws Exception
     */
    public void forgetTransaction(ConnectionContext context, TransactionId xid) throws Exception {
        TransactionInfo info = new TransactionInfo(context.getConnectionId(), xid, TransactionInfo.FORGET);
        sendAsyncToSlave(info);
        super.forgetTransaction(context, xid);
    }

    /**
     * Notifiy the Broker that a dispatch will happen
     * Do in 'pre' so that slave will avoid getting ack before dispatch
     * similar logic to send() below.
     * @param messageDispatch
     */
    public void preProcessDispatch(MessageDispatch messageDispatch) {
        super.preProcessDispatch(messageDispatch);
        MessageDispatchNotification mdn = new MessageDispatchNotification();
        mdn.setConsumerId(messageDispatch.getConsumerId());
        mdn.setDeliverySequenceId(messageDispatch.getDeliverySequenceId());
        mdn.setDestination(messageDispatch.getDestination());
        if (messageDispatch.getMessage() != null) {
            Message msg = messageDispatch.getMessage();
            mdn.setMessageId(msg.getMessageId());
            if (consumers.containsKey(messageDispatch.getConsumerId())) {
                sendSyncToSlave(mdn);
            }
        }
    }

    /**
     * @param context
     * @param message
     * @throws Exception
     */
    public void send(ProducerBrokerExchange producerExchange, Message message) throws Exception {
        /**
         * A message can be dispatched before the super.send() method returns so -
         * here the order is switched to avoid problems on the slave with
         * receiving acks for messages not received yet
         * copy ensures we don't mess with the correlator and command ids
         */
        sendSyncToSlave(message.copy());
        super.send(producerExchange, message);
    }

    /**
     * @param context
     * @param ack
     * @throws Exception
     */
    public void acknowledge(ConsumerBrokerExchange consumerExchange, MessageAck ack) throws Exception {
        sendToSlave(ack);
        super.acknowledge(consumerExchange, ack);
    }

    public boolean isFaultTolerantConfiguration() {
        return true;
    }

    protected void sendToSlave(Message message) {
        if (message.isResponseRequired()) {
            sendSyncToSlave(message);
        } else {
            sendAsyncToSlave(message);
        }
    }

    protected void sendToSlave(MessageAck ack) {
        if (ack.isResponseRequired()) {
            sendAsyncToSlave(ack);
        } else {
            sendSyncToSlave(ack);
        }
    }

    protected void sendAsyncToSlave(Command command) {
        try {
            slave.oneway(command);
        } catch (Throwable e) {
            LOG.error("Slave Failed", e);
            stopProcessing();
        }
    }

    protected void sendSyncToSlave(Command command) {
        try {
            Response response = (Response)slave.request(command);
            if (response.isException()) {
                ExceptionResponse er = (ExceptionResponse)response;
                LOG.error("Slave Failed", er.getException());
            }
        } catch (Throwable e) {
            LOG.error("Slave Failed", e);
        }
    }
}
