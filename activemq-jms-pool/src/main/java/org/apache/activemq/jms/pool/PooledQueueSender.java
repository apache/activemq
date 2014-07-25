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
package org.apache.activemq.jms.pool;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;

/**
 * {@link QueueSender} instance that is created and managed by the PooledConnection.
 */
public class PooledQueueSender extends PooledProducer implements QueueSender {

    public PooledQueueSender(QueueSender messageProducer, Destination destination) throws JMSException {
        super(messageProducer, destination);
    }

    @Override
    public void send(Queue queue, Message message, int i, int i1, long l) throws JMSException {
        getQueueSender().send(queue, message, i, i1, l);
    }

    @Override
    public void send(Queue queue, Message message) throws JMSException {
        getQueueSender().send(queue, message);
    }

    @Override
    public Queue getQueue() throws JMSException {
        return (Queue) getDestination();
    }

    protected QueueSender getQueueSender() {
        return (QueueSender) getMessageProducer();
    }
}
