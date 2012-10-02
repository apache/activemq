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
package org.apache.activemq.tool;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;

/**
 * 
 */
public class MemConsumer extends MemMessageIdList implements MessageListener {

    static long ctr;

    protected Connection connection;
    protected MessageConsumer consumer;
    protected long counter;
    protected boolean isParent;
    protected boolean inOrder = true;


    public MemConsumer() {
        super();
    }

    public MemConsumer(ConnectionFactory fac, Destination dest, String consumerName) throws JMSException {
        connection = fac.createConnection();
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        if (dest instanceof Topic && consumerName != null && consumerName.length() > 0) {
            consumer = s.createDurableSubscriber((Topic) dest, consumerName);
        } else {
            consumer = s.createConsumer(dest);
        }
        consumer.setMessageListener(this);
    }

    public MemConsumer(ConnectionFactory fac, Destination dest) throws JMSException {
        this(fac, dest, null);
    }

    public void start() throws JMSException {
        connection.start();
    }

    public void stop() throws JMSException {
        connection.stop();
    }

    public void shutDown() throws JMSException {
        connection.close();
    }


    public Message receive() throws JMSException {
        return consumer.receive();
    }

    public Message receive(long wait) throws JMSException {
        return consumer.receive(wait);
    }

    public void onMessage(Message msg) {
        super.onMessage(msg);

        if (isParent) {
            try {
                long ctr = msg.getLongProperty("counter");
                if (counter != ctr) {
                    inOrder = false;
                }
                counter++;

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public boolean isInOrder() {
        return inOrder;
    }


    public void setAsParent(boolean isParent) {
        this.isParent = isParent;
    }

    public boolean isParent() {
        return this.isParent;
    }


}
