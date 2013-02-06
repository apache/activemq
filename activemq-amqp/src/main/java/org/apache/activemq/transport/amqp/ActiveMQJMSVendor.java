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
package org.apache.activemq.transport.amqp;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.StreamMessage;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMapMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQObjectMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQStreamMessage;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.qpid.proton.jms.JMSVendor;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ActiveMQJMSVendor extends JMSVendor {

    final public static ActiveMQJMSVendor INSTANCE = new ActiveMQJMSVendor();

    private ActiveMQJMSVendor() {}

    @Override
    public BytesMessage createBytesMessage() {
        return new ActiveMQBytesMessage();
    }

    @Override
    public StreamMessage createStreamMessage() {
        return new ActiveMQStreamMessage();
    }

    @Override
    public Message createMessage() {
        return new ActiveMQMessage();
    }

    @Override
    public TextMessage createTextMessage() {
        return new ActiveMQTextMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() {
        return new ActiveMQObjectMessage();
    }

    @Override
    public MapMessage createMapMessage() {
        return new ActiveMQMapMessage();
    }

    @Override
    @SuppressWarnings("deprecation")
    public Destination createDestination(String name) {
        return super.createDestination(name, Destination.class);
    }

    public <T extends Destination> T createDestination(String name, Class<T> kind) {
        if( kind == Queue.class ) {
            return kind.cast(new ActiveMQQueue(name));
        }
        if( kind == Topic.class ) {
            return kind.cast(new ActiveMQTopic(name));
        }
        if( kind == TemporaryQueue.class ) {
            return kind.cast(new ActiveMQTempQueue(name));
        }
        if( kind == TemporaryTopic.class ) {
            return kind.cast(new ActiveMQTempTopic(name));
        }
        return kind.cast(ActiveMQDestination.createDestination(name, ActiveMQDestination.QUEUE_TYPE));
    }

    @Override
    public void setJMSXUserID(Message msg, String value) {
        ((ActiveMQMessage)msg).setUserID(value);
    }

    @Override
    public void setJMSXGroupID(Message msg, String value) {
        ((ActiveMQMessage)msg).setGroupID(value);
    }

    @Override
    public void setJMSXGroupSequence(Message msg, int value) {
        ((ActiveMQMessage)msg).setGroupSequence(value);
    }

    @Override
    public void setJMSXDeliveryCount(Message msg, long value) {
        ((ActiveMQMessage)msg).setRedeliveryCounter((int) value);
    }

    @Override
    public String toAddress(Destination dest) {
        return ((ActiveMQDestination)dest).getQualifiedName();
    }
}
