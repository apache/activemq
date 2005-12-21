/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
*
**/
package org.activemq.pool;

import org.activemq.ActiveMQMessageProducer;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

/**
 * A pooled {@link MessageProducer}
 *
 * @version $Revision: 1.1 $
 */
public class PooledProducer implements MessageProducer {
    private ActiveMQMessageProducer messageProducer;
    private Destination destination;
    private int deliveryMode;
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int priority;
    private long timeToLive;

    public PooledProducer(ActiveMQMessageProducer messageProducer, Destination destination) throws JMSException {
        this.messageProducer = messageProducer;
        this.destination = destination;

        this.deliveryMode = messageProducer.getDeliveryMode();
        this.disableMessageID = messageProducer.getDisableMessageID();
        this.disableMessageTimestamp = messageProducer.getDisableMessageTimestamp();
        this.priority = messageProducer.getPriority();
        this.timeToLive = messageProducer.getTimeToLive();
    }

    public void close() throws JMSException {
    }

    public void send(Destination destination, Message message) throws JMSException {
        send(destination, message, getDeliveryMode(), getPriority(), getTimeToLive());
    }

    public void send(Message message) throws JMSException {
        send(destination, message, getDeliveryMode(), getPriority(), getTimeToLive());
    }

    public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    public void send(Destination destination, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        if (destination == null) {
            destination = this.destination;
        }
        ActiveMQMessageProducer messageProducer = getMessageProducer();

        // just in case let only one thread send at once
        synchronized (messageProducer) {
            messageProducer.send(destination, message, deliveryMode, priority, timeToLive);
        }
    }

    public Destination getDestination() {
        return destination;
    }

    public int getDeliveryMode() {
        return deliveryMode;
    }

    public void setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    public boolean getDisableMessageID() {
        return disableMessageID;
    }

    public void setDisableMessageID(boolean disableMessageID) {
        this.disableMessageID = disableMessageID;
    }

    public boolean getDisableMessageTimestamp() {
        return disableMessageTimestamp;
    }

    public void setDisableMessageTimestamp(boolean disableMessageTimestamp) {
        this.disableMessageTimestamp = disableMessageTimestamp;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public long getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
    }

    // Implementation methods
    //-------------------------------------------------------------------------
    protected ActiveMQMessageProducer getMessageProducer() {
        return messageProducer;
    }
}
