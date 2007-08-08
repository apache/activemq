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
package org.apache.activemq;

import org.apache.activemq.command.ActiveMQMessage;

import javax.jms.Message;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;

/**
 * A plugin strategy for transforming a message before it is sent by the JMS client or before it is
 * dispatched to the JMS consumer
 *
 * @version $Revision$
 */
public interface MessageTransformer {

    /**
     * Transforms the given message inside the producer before it is sent to the JMS bus.
     */
    public Message producerTransform(Session session, MessageProducer producer, Message message) throws JMSException;

    /**
     * Transforms the given message inside the consumer before being dispatched to the client code
     */
    public Message consumerTransform(Session session, MessageConsumer consumer, Message message)throws JMSException;
}
