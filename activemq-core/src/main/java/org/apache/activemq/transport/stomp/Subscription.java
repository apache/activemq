/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.stomp;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.RemoveInfo;

public class Subscription {
    
    public static final int AUTO_ACK = 1;
    public static final int CLIENT_ACK = 2;

    public static final String NO_ID = "~~ NO SUCH THING ~~%%@#!Q";

    private ActiveMQDestination destination;
    private int ackMode = AUTO_ACK;
    private StompWireFormat format;

    private final String subscriptionId;
    private final ConsumerInfo consumerInfo;
    private final LinkedList dispatchedMessages = new LinkedList();
    
    public Subscription(StompWireFormat format, String subscriptionId, ConsumerInfo consumerInfo) {
        this.format = format;
        this.subscriptionId = subscriptionId;
        this.consumerInfo = consumerInfo;
    }

    void setDestination(ActiveMQDestination actual_dest) {
        this.destination = actual_dest;
    }

    void receive(MessageDispatch md, DataOutput out) throws IOException, JMSException {

        ActiveMQMessage m = (ActiveMQMessage) md.getMessage();

        if (ackMode == CLIENT_ACK) {
            Subscription sub = format.getSubcription(md.getConsumerId());
            sub.addMessageDispatch(md);
            format.getDispachedMap().put(m.getJMSMessageID(), sub);
        }
        else if (ackMode == AUTO_ACK) {
            MessageAck ack = new MessageAck(md, MessageAck.STANDARD_ACK_TYPE, 1);
            format.enqueueCommand(ack);
        }
        
        
        FrameBuilder builder = new FrameBuilder(Stomp.Responses.MESSAGE);
        builder.addHeaders(m);
        
        if( m.getDataStructureType() == ActiveMQTextMessage.DATA_STRUCTURE_TYPE ) {
            builder.setBody(((ActiveMQTextMessage)m).getText().getBytes("UTF-8"));
        } else if( m.getDataStructureType() == ActiveMQBytesMessage.DATA_STRUCTURE_TYPE ) {
            ActiveMQBytesMessage msg = (ActiveMQBytesMessage)m;
            byte data[] = new byte[(int) msg.getBodyLength()];
            msg.readBytes(data);
            builder.setBody(data);
        }
        
        if (subscriptionId!=null) {
            builder.addHeader(Stomp.Headers.Message.SUBSCRIPTION, subscriptionId);
        }
        
        out.write(builder.toFrame());
    }

    private void addMessageDispatch(MessageDispatch md) {
        dispatchedMessages.addLast(md);
    }

    ActiveMQDestination getDestination() {
        return destination;
    }

    public void setAckMode(int clientAck) {
        this.ackMode = clientAck;
    }

    public RemoveInfo close() {
        return new RemoveInfo(consumerInfo.getConsumerId());
    }

    public ConsumerInfo getConsumerInfo() {
        return consumerInfo;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public MessageAck createMessageAck(String message_id) {
        MessageAck ack = new MessageAck();
        ack.setDestination(consumerInfo.getDestination());
        ack.setAckType(MessageAck.STANDARD_ACK_TYPE);
        ack.setConsumerId(consumerInfo.getConsumerId());
        
        int count=0;
        for (Iterator iter = dispatchedMessages.iterator(); iter.hasNext();) {
            
            MessageDispatch md = (MessageDispatch) iter.next();
            String id = ((ActiveMQMessage)md.getMessage()).getJMSMessageID();
            if( ack.getFirstMessageId()==null )
                ack.setFirstMessageId(md.getMessage().getMessageId());

            format.getDispachedMap().remove(id);
            iter.remove();
            count++;
            if( id.equals(message_id)  ) {
                ack.setLastMessageId(md.getMessage().getMessageId());
            }
        }
        ack.setMessageCount(count);
        return ack;
    }
}
