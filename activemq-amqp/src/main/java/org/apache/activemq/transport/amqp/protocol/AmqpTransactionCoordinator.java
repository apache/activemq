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
package org.apache.activemq.transport.amqp.protocol;

import static org.apache.activemq.transport.amqp.AmqpSupport.toBytes;
import static org.apache.activemq.transport.amqp.AmqpSupport.toLong;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.transport.amqp.AmqpProtocolConverter;
import org.apache.activemq.transport.amqp.ResponseHandler;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.Declare;
import org.apache.qpid.proton.amqp.transaction.Declared;
import org.apache.qpid.proton.amqp.transaction.Discharge;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.message.Message;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the AMQP Transaction Coordinator support to manage local
 * transactions between an AMQP client and the broker.
 */
public class AmqpTransactionCoordinator extends AmqpAbstractReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpTransactionCoordinator.class);

    private final Set<AmqpSession> txSessions = new HashSet<AmqpSession>();

    /**
     * Creates a new Transaction coordinator used to manage AMQP transactions.
     *
     * @param session
     *        the AmqpSession under which the coordinator was created.
     * @param endpoint
     *        the AMQP receiver link endpoint for this coordinator.
     */
    public AmqpTransactionCoordinator(AmqpSession session, Receiver endpoint) {
        super(session, endpoint);
    }

    @Override
    protected void processDelivery(final Delivery delivery, Buffer deliveryBytes) throws Exception {
        Message message = Proton.message();
        int offset = deliveryBytes.offset;
        int len = deliveryBytes.length;

        while (len > 0) {
            final int decoded = message.decode(deliveryBytes.data, offset, len);
            assert decoded > 0 : "Make progress decoding the message";
            offset += decoded;
            len -= decoded;
        }

        final AmqpSession session = (AmqpSession) getEndpoint().getSession().getContext();
        final ConnectionId connectionId = session.getConnection().getConnectionId();
        final Object action = ((AmqpValue) message.getBody()).getValue();

        LOG.debug("COORDINATOR received: {}, [{}]", action, deliveryBytes);
        if (action instanceof Declare) {
            Declare declare = (Declare) action;
            if (declare.getGlobalId() != null) {
                throw new Exception("don't know how to handle a declare /w a set GlobalId");
            }

            LocalTransactionId txId = session.getConnection().getNextTransactionId();
            TransactionInfo txInfo = new TransactionInfo(connectionId, txId, TransactionInfo.BEGIN);
            session.getConnection().registerTransaction(txId, this);
            sendToActiveMQ(txInfo, null);
            LOG.trace("started transaction {}", txId);

            Declared declared = new Declared();
            declared.setTxnId(new Binary(toBytes(txId.getValue())));
            delivery.disposition(declared);
            delivery.settle();
        } else if (action instanceof Discharge) {
            final Discharge discharge = (Discharge) action;
            final LocalTransactionId txId = new LocalTransactionId(connectionId, toLong(discharge.getTxnId()));
            final byte operation;

            if (discharge.getFail()) {
                LOG.trace("rollback transaction {}", txId);
                operation = TransactionInfo.ROLLBACK;
            } else {
                LOG.trace("commit transaction {}", txId);
                operation = TransactionInfo.COMMIT_ONE_PHASE;
            }

            for (AmqpSession txSession : txSessions) {
                if (operation == TransactionInfo.ROLLBACK) {
                    txSession.rollback(txId);
                } else {
                    txSession.commit(txId);
                }
            }

            txSessions.clear();
            session.getConnection().unregisterTransaction(txId);

            TransactionInfo txinfo = new TransactionInfo(connectionId, txId, operation);
            sendToActiveMQ(txinfo, new ResponseHandler() {
                @Override
                public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                    if (response.isException()) {
                        ExceptionResponse er = (ExceptionResponse) response;
                        Rejected rejected = new Rejected();
                        rejected.setError(new ErrorCondition(Symbol.valueOf("failed"), er.getException().getMessage()));
                        delivery.disposition(rejected);
                    } else {
                        delivery.disposition(Accepted.getInstance());
                    }

                    LOG.debug("TX: {} settling {}", operation, action);
                    delivery.settle();
                    session.pumpProtonToSocket();
                }
            });

            if (operation == TransactionInfo.ROLLBACK) {
                session.flushPendingMessages();
            }

        } else {
            throw new Exception("Expected coordinator message type: " + action.getClass());
        }

        replenishCredit();
    }

    private void replenishCredit() {
        if (getEndpoint().getCredit() <= (getConfiguredReceiverCredit() * .2)) {
            LOG.debug("Sending more credit ({}) to transaction coordinator on session {}", getConfiguredReceiverCredit() - getEndpoint().getCredit(), session.getSessionId());
            getEndpoint().flow(getConfiguredReceiverCredit() - getEndpoint().getCredit());
            session.pumpProtonToSocket();
        }
    }

    @Override
    public ActiveMQDestination getDestination() {
        return null;
    }

    @Override
    public void setDestination(ActiveMQDestination destination) {
    }

    public void enlist(AmqpSession session) {
        txSessions.add(session);
    }
}
