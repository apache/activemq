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
package org.apache.activemq.transport.stomp;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.TransactionId;

/**
 * Keeps track of the STOMP subscription so that acking is correctly done.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
public class StompSubscription {

    public static final String AUTO_ACK = Stomp.Headers.Subscribe.AckModeValues.AUTO;
    public static final String CLIENT_ACK = Stomp.Headers.Subscribe.AckModeValues.CLIENT;
    public static final String INDIVIDUAL_ACK = Stomp.Headers.Subscribe.AckModeValues.INDIVIDUAL;

    private final ProtocolConverter protocolConverter;
    private final String subscriptionId;
    private final ConsumerInfo consumerInfo;

    private final LinkedHashMap<MessageId, MessageDispatch> dispatchedMessage = new LinkedHashMap<MessageId, MessageDispatch>();
    private final LinkedList<MessageDispatch> unconsumedMessage = new LinkedList<MessageDispatch>();

    private String ackMode = AUTO_ACK;
    private ActiveMQDestination destination;
    private String transformation;


    public StompSubscription(ProtocolConverter stompTransport, String subscriptionId, ConsumerInfo consumerInfo, String transformation) {
        this.protocolConverter = stompTransport;
        this.subscriptionId = subscriptionId;
        this.consumerInfo = consumerInfo;
        this.transformation = transformation;
    }

    void onMessageDispatch(MessageDispatch md) throws IOException, JMSException {
        ActiveMQMessage message = (ActiveMQMessage)md.getMessage();
        if (ackMode == CLIENT_ACK) {
            synchronized (this) {
                dispatchedMessage.put(message.getMessageId(), md);
            }
        } else if (ackMode == INDIVIDUAL_ACK) {
            synchronized (this) {
                dispatchedMessage.put(message.getMessageId(), md);
            }
        } else if (ackMode == AUTO_ACK) {
            MessageAck ack = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, 1);
            protocolConverter.getStompTransport().sendToActiveMQ(ack);
        }

        boolean ignoreTransformation = false;

        if (transformation != null && !( message instanceof ActiveMQBytesMessage ) ) {
       		message.setReadOnlyProperties(false);
        	message.setStringProperty(Stomp.Headers.TRANSFORMATION, transformation);
        } else {
        	if (message.getStringProperty(Stomp.Headers.TRANSFORMATION) != null) {
        		ignoreTransformation = true;
        	}
        }

        StompFrame command = protocolConverter.convertMessage(message, ignoreTransformation);

        command.setAction(Stomp.Responses.MESSAGE);
        if (subscriptionId != null) {
            command.getHeaders().put(Stomp.Headers.Message.SUBSCRIPTION, subscriptionId);
        }

        protocolConverter.getStompTransport().sendToStomp(command);
    }

    synchronized void onStompAbort(TransactionId transactionId) {
    	unconsumedMessage.clear();
    }

    synchronized void onStompCommit(TransactionId transactionId) {
    	for (Iterator iter = dispatchedMessage.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = (Entry)iter.next();
            MessageId id = (MessageId)entry.getKey();
            MessageDispatch msg = (MessageDispatch)entry.getValue();
            if (unconsumedMessage.contains(msg)) {
            	iter.remove();
            }
    	}
    	unconsumedMessage.clear();
    }

    synchronized MessageAck onStompMessageAck(String messageId, TransactionId transactionId) {

    	MessageId msgId = new MessageId(messageId);

        if (!dispatchedMessage.containsKey(msgId)) {
            return null;
        }

        MessageAck ack = new MessageAck();
        ack.setDestination(consumerInfo.getDestination());
        ack.setConsumerId(consumerInfo.getConsumerId());

        if (ackMode == CLIENT_ACK) {
        	ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
            int count = 0;
            for (Iterator iter = dispatchedMessage.entrySet().iterator(); iter.hasNext();) {

                Map.Entry entry = (Entry)iter.next();
                MessageId id = (MessageId)entry.getKey();
                MessageDispatch msg = (MessageDispatch)entry.getValue();

                if (ack.getFirstMessageId() == null) {
                    ack.setFirstMessageId(id);
                }

                if (transactionId != null) {
                	if (!unconsumedMessage.contains(msg)) {
                		unconsumedMessage.add(msg);
                	}
                } else {
                	iter.remove();
                }


                count++;

                if (id.equals(msgId)) {
                    ack.setLastMessageId(id);
                    break;
                }

            }
            ack.setMessageCount(count);
            if (transactionId != null) {
            	ack.setTransactionId(transactionId);
            }
        }
        else if (ackMode == INDIVIDUAL_ACK) {
            ack.setAckType(MessageAck.INDIVIDUAL_ACK_TYPE);
            ack.setMessageID(msgId);
            if (transactionId != null) {
            	unconsumedMessage.add(dispatchedMessage.get(msgId));
            	ack.setTransactionId(transactionId);
            }
            dispatchedMessage.remove(msgId);
        }
        return ack;
    }

    public String getAckMode() {
        return ackMode;
    }

    public void setAckMode(String ackMode) {
        this.ackMode = ackMode;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    public ConsumerInfo getConsumerInfo() {
        return consumerInfo;
    }

}
