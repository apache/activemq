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
import javax.jms.MessageProducer;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;

/**
 * Used to store a pooled session instance and any resources that can
 * be left open and carried along with the pooled instance such as the
 * anonymous producer used for all MessageProducer instances created
 * from this pooled session when enabled.
 */
public class SessionHolder {

    private final Session session;
    private volatile MessageProducer producer;
    private volatile TopicPublisher publisher;
    private volatile QueueSender sender;

    public SessionHolder(Session session) {
        this.session = session;
    }

    public void close() throws JMSException {
        try {
            session.close();
        } finally {
            producer = null;
            publisher = null;
            sender = null;
        }
    }

    public Session getSession() {
        return session;
    }

    public MessageProducer getOrCreateProducer() throws JMSException {
        if (producer == null) {
            synchronized (this) {
                if (producer == null) {
                    producer = session.createProducer(null);
                }
            }
        }

        return producer;
    }

    public TopicPublisher getOrCreatePublisher() throws JMSException {
        if (publisher == null) {
            synchronized (this) {
                if (publisher == null) {
                    publisher = ((TopicSession) session).createPublisher(null);
                }
            }
        }

        return publisher;
    }

    public QueueSender getOrCreateSender() throws JMSException {
        if (sender == null) {
            synchronized (this) {
                if (sender == null) {
                    sender = ((QueueSession) session).createSender(null);
                }
            }
        }

        return sender;
    }

    @Override
    public String toString() {
        return session.toString();
    }
}
