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
package org.apache.activemq.perf;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageListener;
import jakarta.jms.Session;
import jakarta.jms.Topic;
import org.apache.activemq.ActiveMQMessageAudit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PerfConsumer implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(PerfConsumer.class);
    protected Connection connection;
    protected MessageConsumer consumer;
    protected long sleepDuration;
    protected long initialDelay;
    protected boolean enableAudit = false;
    protected ActiveMQMessageAudit audit = new ActiveMQMessageAudit(16 * 1024,20);
    protected boolean firstMessage =true;
    protected String lastMsgId;

    protected PerfRate rate = new PerfRate();

    public PerfConsumer(ConnectionFactory fac, Destination dest, String consumerName) throws JMSException {
        connection = fac.createConnection();
        connection.setClientID(consumerName);
        Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        if (dest instanceof Topic && consumerName != null && consumerName.length() > 0) {
            consumer = s.createDurableSubscriber((Topic)dest, consumerName);
        } else {
            consumer = s.createConsumer(dest);
        }
        consumer.setMessageListener(this);
    }

    public PerfConsumer(ConnectionFactory fac, Destination dest) throws JMSException {
        this(fac, dest, null);
    }

    public void start() throws JMSException {
        connection.start();
        rate.reset();
    }

    public void stop() throws JMSException {
        connection.stop();
    }

    public void shutDown() throws JMSException {
        connection.close();
    }

    public PerfRate getRate() {
        return rate;
    }

    public void onMessage(Message msg) {
        if (firstMessage) {
            firstMessage=false;
            if (getInitialDelay() > 0) {
                try {
                    Thread.sleep(getInitialDelay());
                } catch (InterruptedException e) {
                }
            }
        }
        rate.increment();
        try {
            if (enableAudit && !this.audit.isInOrder(msg.getJMSMessageID())) {
                LOG.error("Message out of order!!" + msg.getJMSMessageID() + " LAST = " + lastMsgId);
            }
            if (enableAudit && this.audit.isDuplicate(msg)){
                LOG.error("Duplicate Message!" + msg);
            }
            lastMsgId=msg.getJMSMessageID();
        } catch (JMSException e1) {
            e1.printStackTrace();
        }
        try {
            if (sleepDuration != 0) {
                Thread.sleep(sleepDuration);
            }
        } catch (InterruptedException e) {
        }
    }

    public synchronized long getSleepDuration() {
        return sleepDuration;
    }

    public synchronized void setSleepDuration(long sleepDuration) {
        this.sleepDuration = sleepDuration;
    }

    public boolean isEnableAudit() {
        return enableAudit;
    }

    public void setEnableAudit(boolean doAudit) {
        this.enableAudit = doAudit;
    }

    /**
     * @return the initialDelay
     */
    public long getInitialDelay() {
        return initialDelay;
    }

    /**
     * @param initialDelay the initialDelay to set
     */
    public void setInitialDelay(long initialDelay) {
        this.initialDelay = initialDelay;
    }
}
