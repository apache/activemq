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
package org.apache.activemq.transport.amqp.protocol;

import static org.apache.activemq.transport.amqp.AmqpSupport.toLong;

import java.io.IOException;

import javax.jms.Destination;
import javax.jms.ResourceAllocationException;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.transport.amqp.AmqpProtocolConverter;
import org.apache.activemq.transport.amqp.ResponseHandler;
import org.apache.activemq.transport.amqp.message.AMQPNativeInboundTransformer;
import org.apache.activemq.transport.amqp.message.AMQPRawInboundTransformer;
import org.apache.activemq.transport.amqp.message.EncodedMessage;
import org.apache.activemq.transport.amqp.message.InboundTransformer;
import org.apache.activemq.transport.amqp.message.JMSMappingInboundTransformer;
import org.apache.activemq.util.LongSequenceGenerator;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.TransactionalState;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.fusesource.hawtbuf.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An AmqpReceiver wraps the AMQP Receiver end of a link from the remote peer
 * which holds the corresponding Sender which transfers message accross the
 * link.  The AmqpReceiver handles all incoming deliveries by converting them
 * or wrapping them into an ActiveMQ message object and forwarding that message
 * on to the appropriate ActiveMQ Destination.
 */
public class AmqpReceiver extends AmqpAbstractReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpReceiver.class);

    private final ProducerInfo producerInfo;
    private final LongSequenceGenerator messageIdGenerator = new LongSequenceGenerator();

    private InboundTransformer inboundTransformer;

    private int sendsInFlight;

    /**
     * Create a new instance of an AmqpReceiver
     *
     * @param session
     *        the Session that is the parent of this AmqpReceiver instance.
     * @param endpoint
     *        the AMQP receiver endpoint that the class manages.
     * @param producerInfo
     *        the ProducerInfo instance that contains this sender's configuration.
     */
    public AmqpReceiver(AmqpSession session, Receiver endpoint, ProducerInfo producerInfo) {
        super(session, endpoint);

        this.producerInfo = producerInfo;
    }

    @Override
    public void close() {
        if (!isClosed() && isOpened()) {
            sendToActiveMQ(new RemoveInfo(getProducerId()), new ResponseHandler() {

                @Override
                public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                    AmqpReceiver.super.close();
                }
            });
        } else {
            super.close();
        }
    }

    //----- Configuration accessors ------------------------------------------//

    /**
     * @return the ActiveMQ ProducerId used to register this Receiver on the Broker.
     */
    public ProducerId getProducerId() {
        return producerInfo.getProducerId();
    }

    @Override
    public ActiveMQDestination getDestination() {
        return producerInfo.getDestination();
    }

    @Override
    public void setDestination(ActiveMQDestination destination) {
        producerInfo.setDestination(destination);
    }

    /**
     * If the Sender that initiated this Receiver endpoint did not define an address
     * then it is using anonymous mode and message are to be routed to the address
     * that is defined in the AMQP message 'To' field.
     *
     * @return true if this Receiver should operate in anonymous mode.
     */
    public boolean isAnonymous() {
        return producerInfo.getDestination() == null;
    }

    //----- Internal Implementation ------------------------------------------//

    protected InboundTransformer getTransformer() {
        if (inboundTransformer == null) {
            String transformer = session.getConnection().getConfiguredTransformer();
            if (transformer.equalsIgnoreCase(InboundTransformer.TRANSFORMER_JMS)) {
                inboundTransformer = new JMSMappingInboundTransformer();
            } else if (transformer.equalsIgnoreCase(InboundTransformer.TRANSFORMER_NATIVE)) {
                inboundTransformer = new AMQPNativeInboundTransformer();
            } else if (transformer.equalsIgnoreCase(InboundTransformer.TRANSFORMER_RAW)) {
                inboundTransformer = new AMQPRawInboundTransformer();
            } else {
                LOG.warn("Unknown transformer type {} using native one instead", transformer);
                inboundTransformer = new AMQPNativeInboundTransformer();
            }
        }
        return inboundTransformer;
    }

    @Override
    protected void processDelivery(final Delivery delivery, Buffer deliveryBytes) throws Exception {
        if (!isClosed()) {
            EncodedMessage em = new EncodedMessage(delivery.getMessageFormat(), deliveryBytes.data, deliveryBytes.offset, deliveryBytes.length);

            InboundTransformer transformer = getTransformer();
            ActiveMQMessage message = transformer.transform(em);

            current = null;

            if (isAnonymous()) {
                Destination toDestination = message.getJMSDestination();
                if (toDestination == null || !(toDestination instanceof ActiveMQDestination)) {
                    Rejected rejected = new Rejected();
                    ErrorCondition condition = new ErrorCondition();
                    condition.setCondition(Symbol.valueOf("failed"));
                    condition.setDescription("Missing to field for message sent to an anonymous producer");
                    rejected.setError(condition);
                    delivery.disposition(rejected);
                    return;
                }
            } else {
                message.setJMSDestination(getDestination());
            }

            message.setProducerId(getProducerId());

            // Always override the AMQP client's MessageId with our own.  Preserve
            // the original in the TextView property for later Ack.
            MessageId messageId = new MessageId(getProducerId(), messageIdGenerator.getNextSequenceId());

            MessageId amqpMessageId = message.getMessageId();
            if (amqpMessageId != null) {
                if (amqpMessageId.getTextView() != null) {
                    messageId.setTextView(amqpMessageId.getTextView());
                } else {
                    messageId.setTextView(amqpMessageId.toString());
                }
            }

            message.setMessageId(messageId);

            LOG.trace("Inbound Message:{} from Producer:{}",
                      message.getMessageId(), getProducerId() + ":" + messageId.getProducerSequenceId());

            final DeliveryState remoteState = delivery.getRemoteState();
            if (remoteState != null && remoteState instanceof TransactionalState) {
                TransactionalState txState = (TransactionalState) remoteState;
                TransactionId txId = new LocalTransactionId(session.getConnection().getConnectionId(), toLong(txState.getTxnId()));
                session.enlist(txId);
                message.setTransactionId(txId);
            }

            message.onSend();

            sendsInFlight++;

            sendToActiveMQ(message, createResponseHandler(delivery));
        }
    }

    private ResponseHandler createResponseHandler(final Delivery delivery) {
        return new ResponseHandler() {

            @Override
            public void onResponse(AmqpProtocolConverter converter, Response response) throws IOException {
                if (!delivery.remotelySettled()) {
                    if (response.isException()) {
                        ExceptionResponse error = (ExceptionResponse) response;
                        Rejected rejected = new Rejected();
                        ErrorCondition condition = new ErrorCondition();

                        if (error.getException() instanceof SecurityException) {
                            condition.setCondition(AmqpError.UNAUTHORIZED_ACCESS);
                        } else if (error.getException() instanceof ResourceAllocationException) {
                            condition.setCondition(AmqpError.RESOURCE_LIMIT_EXCEEDED);
                        } else {
                            condition.setCondition(Symbol.valueOf("failed"));
                        }

                        condition.setDescription(error.getException().getMessage());
                        rejected.setError(condition);
                        delivery.disposition(rejected);
                    } else {
                        final DeliveryState remoteState = delivery.getRemoteState();
                        if (remoteState != null && remoteState instanceof TransactionalState) {
                            TransactionalState txAccepted = new TransactionalState();
                            txAccepted.setOutcome(Accepted.getInstance());
                            txAccepted.setTxnId(((TransactionalState) remoteState).getTxnId());

                            delivery.disposition(txAccepted);
                        } else {
                            delivery.disposition(Accepted.getInstance());
                        }
                    }
                }

                if (getEndpoint().getCredit() + --sendsInFlight <= (getConfiguredReceiverCredit() * .3)) {
                    LOG.trace("Sending more credit ({}) to producer: {}", getConfiguredReceiverCredit() * .7, getProducerId());
                    getEndpoint().flow((int) (getConfiguredReceiverCredit() * .7));
                }

                delivery.settle();
                session.pumpProtonToSocket();
            }
        };
    }
}
