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

import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;

/**
 * JMS 2.0 {@link JMSConsumer} backed by a pooled {@link MessageConsumer}.
 */
public class PooledJMSConsumer implements JMSConsumer {

    private final PooledJMSContext context;
    private final MessageConsumer consumer;

    PooledJMSConsumer(PooledJMSContext context, MessageConsumer consumer) {
        this.context = context;
        this.consumer = consumer;
    }

    @Override
    public String getMessageSelector() {
        try {
            return consumer.getMessageSelector();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
        try {
            return consumer.getMessageListener();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSRuntimeException {
        try {
            consumer.setMessageListener(listener);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public Message receive() {
        try {
            return trackReceived(consumer.receive());
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public Message receive(long timeout) {
        try {
            return trackReceived(consumer.receive(timeout));
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public Message receiveNoWait() {
        try {
            return trackReceived(consumer.receiveNoWait());
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    private Message trackReceived(Message message) {
        if (message != null && context != null) {
            context.onMessageReceived(message);
        }
        return message;
    }

    @Override
    public <T> T receiveBody(Class<T> c) {
        var message = receive();
        if (message == null) {
            return null;
        }
        try {
            return message.getBody(c);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
        var message = receive(timeout);
        if (message == null) {
            return null;
        }
        try {
            return message.getBody(c);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        var message = receiveNoWait();
        if (message == null) {
            return null;
        }
        try {
            return message.getBody(c);
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (JMSException e) {
            throw JmsPoolExceptionSupport.toRuntimeException(e);
        }
    }

}
