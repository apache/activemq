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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

/**
 * A {@link MessageConsumer} which was created by {@link PooledSession}.
 */
public class PooledMessageConsumer implements MessageConsumer {

    private final PooledSession session;
    private final MessageConsumer delegate;

    /**
     * Wraps the message consumer.
     *
     * @param session  the pooled session
     * @param delegate the created consumer to wrap
     */
    public PooledMessageConsumer(PooledSession session, MessageConsumer delegate) {
        this.session = session;
        this.delegate = delegate;
    }

    @Override
    public void close() throws JMSException {
        // ensure session removes consumer as its closed now
        session.onConsumerClose(delegate);
        delegate.close();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return delegate.getMessageListener();
    }

    @Override
    public String getMessageSelector() throws JMSException {
        return delegate.getMessageSelector();
    }

    @Override
    public Message receive() throws JMSException {
        return delegate.receive();
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        return delegate.receive(timeout);
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        return delegate.receiveNoWait();
    }

    @Override
    public void setMessageListener(MessageListener listener) throws JMSException {
        delegate.setMessageListener(listener);
    }

    @Override
    public String toString() {
        return "PooledMessageConsumer { " + delegate + " }";
    }
}
