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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;

/**
 * Represents a hook to allow the support of custom destinations
 * such as to support <a href="http://activemq.apache.org/camel/">Apache Camel</a>
 * to create and manage endpoints
 *
 * 
 */
public interface CustomDestination extends Destination {

    // Consumers
    //-----------------------------------------------------------------------
    MessageConsumer createConsumer(ActiveMQSession session, String messageSelector);
    MessageConsumer createConsumer(ActiveMQSession session, String messageSelector, boolean noLocal);

    TopicSubscriber createSubscriber(ActiveMQSession session, String messageSelector, boolean noLocal);
    TopicSubscriber createDurableSubscriber(ActiveMQSession session, String name, String messageSelector, boolean noLocal);

    QueueReceiver createReceiver(ActiveMQSession session, String messageSelector);

    // Producers
    //-----------------------------------------------------------------------
    MessageProducer createProducer(ActiveMQSession session) throws JMSException;

    TopicPublisher createPublisher(ActiveMQSession session) throws JMSException;

    QueueSender createSender(ActiveMQSession session) throws JMSException;
}
