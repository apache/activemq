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
 * @version $Revision: 1.3 $
 */
public class PerfConsumer implements MessageListener {
    protected Connection connection;
    protected MessageConsumer consumer;
    protected long sleepDuration;

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
        rate.increment();
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
}
