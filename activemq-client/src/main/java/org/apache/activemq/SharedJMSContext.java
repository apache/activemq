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

import java.util.concurrent.atomic.AtomicLong;

import jakarta.jms.InvalidDestinationRuntimeException;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Topic;

import org.apache.activemq.annotation.Experimental;
import org.apache.activemq.util.JMSExceptionSupport;

/**
 * A {@link JMSContext} that supports JMS 3.1 shared topic subscriptions.
 *
 * <p>The base {@link ActiveMQContext} leaves the four
 * {@code createSharedConsumer} / {@code createSharedDurableConsumer} methods as
 * unimplemented stubs. This subclass wires them to the underlying session,
 * which — because the context is created over a {@link SharedTopicConnection} —
 * is a {@link SharedTopicSession} that carries the shared subscription logic.
 *
 * <p>Instances are produced by {@link SharedTopicConnectionFactory}'s
 * {@code createContext} methods; there is no need to construct one directly.
 */
@Experimental("Tech Preview for JMS 3.1 shared topic subscriptions")
public class SharedJMSContext extends ActiveMQContext {

    SharedJMSContext(final ActiveMQConnection activemqConnection) {
        super(activemqConnection);
    }

    SharedJMSContext(final ActiveMQConnection activemqConnection, final int sessionMode) {
        super(activemqConnection, sessionMode);
    }

    private SharedJMSContext(final ActiveMQConnection activemqConnection, final int sessionMode,
            final AtomicLong connectionCounter) {
        super(activemqConnection, sessionMode, connectionCounter);
    }

    /**
     * Child contexts created over the same connection keep shared subscription
     * support by staying a {@code SharedJMSContext}.
     */
    @Override
    protected JMSContext newChildContext(int sessionMode) {
        return new SharedJMSContext(activemqConnection, sessionMode, connectionCounter);
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName) {
        checkContextState();
        if (topic == null) {
            throw new InvalidDestinationRuntimeException("Topic cannot be null");
        }
        try {
            if (getAutoStart()) {
                start();
            }
            return new ActiveMQConsumer(this,
                    activemqSession.createSharedConsumer(topic, sharedSubscriptionName));
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String sharedSubscriptionName,
            String messageSelector) {
        checkContextState();
        if (topic == null) {
            throw new InvalidDestinationRuntimeException("Topic cannot be null");
        }
        try {
            if (getAutoStart()) {
                start();
            }
            return new ActiveMQConsumer(this,
                    activemqSession.createSharedConsumer(topic, sharedSubscriptionName, messageSelector));
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name) {
        checkContextState();
        if (topic == null) {
            throw new InvalidDestinationRuntimeException("Topic cannot be null");
        }
        try {
            if (getAutoStart()) {
                start();
            }
            return new ActiveMQConsumer(this,
                    activemqSession.createSharedDurableConsumer(topic, name));
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String name,
            String messageSelector) {
        checkContextState();
        if (topic == null) {
            throw new InvalidDestinationRuntimeException("Topic cannot be null");
        }
        try {
            if (getAutoStart()) {
                start();
            }
            return new ActiveMQConsumer(this,
                    activemqSession.createSharedDurableConsumer(topic, name, messageSelector));
        } catch (JMSException e) {
            throw JMSExceptionSupport.convertToJMSRuntimeException(e);
        }
    }
}
