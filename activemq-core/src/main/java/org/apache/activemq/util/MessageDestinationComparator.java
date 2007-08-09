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
package org.apache.activemq.util;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.activemq.command.ActiveMQMessage;

/**
 * A comparator which works on SendCommand objects to compare the destinations
 * 
 * @version $Revision$
 */
public class MessageDestinationComparator extends MessageComparatorSupport {

    protected int compareMessages(Message message1, Message message2) {
        return compareComparators(getComparable(getDestination(message1)), getComparable(getDestination(message2)));
    }

    protected Destination getDestination(Message message) {
        if (message instanceof ActiveMQMessage) {
            ActiveMQMessage amqMessage = (ActiveMQMessage)message;
            return amqMessage.getDestination();
        }
        try {
            return message.getJMSDestination();
        } catch (JMSException e) {
            return null;
        }
    }

    protected Comparable getComparable(Destination destination) {
        if (destination != null) {
            return destination.toString();
        }
        return null;
    }

}
