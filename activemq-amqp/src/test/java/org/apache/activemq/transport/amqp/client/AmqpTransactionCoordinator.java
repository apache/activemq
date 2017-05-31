/*
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
package org.apache.activemq.transport.amqp.client;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.TransactionRolledBackException;

import org.apache.activemq.transport.amqp.client.util.AsyncResult;
import org.apache.activemq.transport.amqp.client.util.IOExceptionSupport;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transaction.TxnCapability;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the AMQP Transaction coordinator link used by the transaction context
 * of a session to control the lifetime of a given transaction.
 */
public class AmqpTransactionCoordinator extends AmqpAbstractResource<Sender> {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionCoordinator.class);

    private final byte[] OUTBOUND_BUFFER = new byte[64];

    private final AmqpSession session;
    private final AmqpTransferTagGenerator tagGenerator = new AmqpTransferTagGenerator();

    private List<Delivery> pendingDeliveries = new LinkedList<Delivery>();
    private Map<AmqpTransactionId, AsyncResult> pendingRequests = new HashMap<AmqpTransactionId, AsyncResult>();

    public AmqpTransactionCoordinator(AmqpSession session) {
        this.session = session;
    }

    @Override
    public void processDeliveryUpdates(AmqpConnection connection) throws IOException {
        try {
            Iterator<Delivery> deliveries = pendingDeliveries.iterator();
            while (deliveries.hasNext()) {
                Delivery pendingDelivery = deliveries.next();
                if (!pendingDelivery.remotelySettled()) {
                    continue;
                }

                DeliveryState state = pendingDelivery.getRemoteState();
                AmqpTransactionId txId = (AmqpTransactionId) pendingDelivery.getContext();
                AsyncResult pendingRequest = pendingRequests.get(txId);

                if (pendingRequest == null) {
                    throw new IllegalStateException("Pending tx operation with no pending request");
                }

                if (state instanceof Declared) {
                    LOG.debug("New TX started: {}", txId.getTxId());
                    Declared declared = (Declared) state;
                    txId.setRemoteTxId(declared.getTxnId());
                    pendingRequest.onSuccess();
                } else if (state instanceof Rejected) {
                    LOG.debug("Last TX request failed: {}", txId.getTxId());
                    Rejected rejected = (Rejected) state;
                    Exception cause = AmqpSupport.convertToException(rejected.getError());
                    JMSException failureCause = null;
                    if (txId.isCommit()) {
                        failureCause = new TransactionRolledBackException(cause.getMessage());
                    } else {
                        failureCause = new JMSException(cause.getMessage());
                    }

                    pendingRequest.onFailure(failureCause);
                } else {
                    LOG.debug("Last TX request succeeded: {}", txId.getTxId());
                    pendingRequest.onSuccess();
                }

                // Clear state data
                pendingDelivery.settle();
                pendingRequests.remove(txId);
                deliveries.remove();
            }

            super.processDeliveryUpdates(connection);
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
        }
    }

    public void declare(AmqpTransactionId txId, AsyncResult request) throws Exception {
        if (txId.getRemoteTxId() != null) {
            throw new IllegalStateException("Declar called while a TX is still Active.");
        }

        if (isClosed()) {
            request.onFailure(new JMSException("Cannot start new transaction: Coordinator remotely closed"));
            return;
        }

        Message message = Message.Factory.create();
        Declare declare = new Declare();
        message.setBody(new AmqpValue(declare));

        Delivery pendingDelivery = getEndpoint().delivery(tagGenerator.getNextTag());
        pendingDelivery.setContext(txId);

        // Store away for completion
        pendingDeliveries.add(pendingDelivery);
        pendingRequests.put(txId, request);

        sendTxCommand(message);
    }

    public void discharge(AmqpTransactionId txId, AsyncResult request, boolean commit) throws Exception {

        if (isClosed()) {
            Exception failureCause = null;

            if (commit) {
                failureCause = new TransactionRolledBackException("Transaction inbout: Coordinator remotely closed");
            } else {
                failureCause = new JMSException("Rollback cannot complete: Coordinator remotely closed");
            }

            request.onFailure(failureCause);
            return;
        }

        // Store the context of this action in the transaction ID for later completion.
        txId.setState(commit ? AmqpTransactionId.COMMIT_MARKER : AmqpTransactionId.ROLLBACK_MARKER);

        Message message = Message.Factory.create();
        Discharge discharge = new Discharge();
        discharge.setFail(!commit);
        discharge.setTxnId(txId.getRemoteTxId());
        message.setBody(new AmqpValue(discharge));

        Delivery pendingDelivery = getEndpoint().delivery(tagGenerator.getNextTag());
        pendingDelivery.setContext(txId);

        // Store away for completion
        pendingDeliveries.add(pendingDelivery);
        pendingRequests.put(txId, request);

        sendTxCommand(message);
    }

    //----- Base class overrides ---------------------------------------------//

    @Override
    public void remotelyClosed(AmqpConnection connection) {

        Exception txnError = AmqpSupport.convertToException(getEndpoint().getRemoteCondition());

        // Alert any pending operation that the link failed to complete the pending
        // begin / commit / rollback operation.
        for (AsyncResult pendingRequest : pendingRequests.values()) {
            pendingRequest.onFailure(txnError);
        }

        // Purge linkages to pending operations.
        pendingDeliveries.clear();
        pendingRequests.clear();

        // Override the base class version because we do not want to propagate
        // an error up to the client if remote close happens as that is an
        // acceptable way for the remote to indicate the discharge could not
        // be applied.

        if (getEndpoint() != null) {
            getEndpoint().close();
            getEndpoint().free();
        }

        LOG.debug("Transaction Coordinator link {} was remotely closed", getEndpoint());
    }

    //----- Internal implementation ------------------------------------------//

    private void sendTxCommand(Message message) throws IOException {
        int encodedSize = 0;
        byte[] buffer = OUTBOUND_BUFFER;
        while (true) {
            try {
                encodedSize = message.encode(buffer, 0, buffer.length);
                break;
            } catch (BufferOverflowException e) {
                buffer = new byte[buffer.length * 2];
            }
        }

        Sender sender = getEndpoint();
        sender.send(buffer, 0, encodedSize);
        sender.advance();
    }


    @Override
    protected void doOpen() {
        Coordinator coordinator = new Coordinator();
        coordinator.setCapabilities(TxnCapability.LOCAL_TXN);
        Source source = new Source();

        String coordinatorName = "qpid-jms:coordinator:" + session.getConnection().getConnectionId();

        Sender sender = session.getEndpoint().sender(coordinatorName);
        sender.setSource(source);
        sender.setTarget(coordinator);
        sender.setSenderSettleMode(SenderSettleMode.UNSETTLED);
        sender.setReceiverSettleMode(ReceiverSettleMode.FIRST);

        setEndpoint(sender);

        super.doOpen();
    }

    @Override
    protected void doOpenInspection() {
        // TODO
    }

    @Override
    protected void doClosedInspection() {
        // TODO
    }
}
