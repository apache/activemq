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
package org.apache.activemq.test.rollback;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;


public class DelegatingTransactionalMessageListener implements MessageListener {
    private static final transient Log log = LogFactory.getLog(DelegatingTransactionalMessageListener.class);

    private final MessageListener underlyingListener;
    private boolean transacted = true;
    private int ackMode = Session.AUTO_ACKNOWLEDGE;
    private Session session;

    public DelegatingTransactionalMessageListener(MessageListener underlyingListener, Connection connection, Destination destination) {
        this.underlyingListener = underlyingListener;

        try {
            session = connection.createSession(transacted, ackMode);
            MessageConsumer consumer = session.createConsumer(destination);
            consumer.setMessageListener(this);
        }
        catch (JMSException e) {
            throw new IllegalStateException("Could not listen to " + destination, e);
        }
    }

    public void onMessage(Message message) {
        try {
            underlyingListener.onMessage(message);
            session.commit();
        }
        catch (Throwable e) {
            rollback();
        }
    }

    private void rollback() {
        try {
            session.rollback();
        }
        catch (JMSException e) {
            log.error("Failed to rollback: " + e, e);
        }
    }

    public Session getSession() {
        return session;
    }
}
